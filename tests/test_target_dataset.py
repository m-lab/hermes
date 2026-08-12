"""--target staging must be airtight: no query and no table may name production.

The pipeline writes with ``INSERT INTO`` and ``DELETE``, so a single surviving
``hermes_union`` reference in a staging run does not fail loudly -- it silently
mutates production. These tests are the guard on that.
"""

from __future__ import annotations

import pytest

from hermes.pipeline import init_staging, union
from hermes.sql import loader

STAGING = union.STAGING_DATASET
PROD = union.DEFAULT_DATASET

#: Every query file the pipeline renders, plus the DDL bootstrap files.
ALL_QUERY_FILES = sorted(p.name for p in (loader.paths.sql_root() / "queries").glob("*.sql"))


def _staging_params(**extra) -> dict[str, object]:
    params = {
        "DAY": "2026-08-08",
        "ONE_WEEK_EARLIER": "2026-08-01",
        "DETECTION_GRANULARITY": "metro",
        "DS": STAGING,
        "METRO_POLYGONS": union.metro_polygons_for(STAGING),
    }
    params.update(extra)
    return params


def test_targets_map_to_distinct_datasets():
    assert union.dataset_for_target("prod") == PROD
    assert union.dataset_for_target("staging") == STAGING
    assert PROD != STAGING


def test_unknown_target_is_rejected():
    with pytest.raises(ValueError, match="Unsupported target"):
        union.dataset_for_target("production")


def test_metro_polygons_follows_the_target():
    """A staging run must not read production geometry."""
    assert union.metro_polygons_for(PROD).endswith(".hermes.metro_polygons_v2")
    assert union.metro_polygons_for(STAGING).endswith(".hermes_staging.metro_polygons_v2")


def test_default_params_point_at_production():
    """An un-parameterised caller must get production, never staging."""
    assert loader.DEFAULT_PARAMS["DS"] == PROD
    assert loader.DEFAULT_PARAMS["METRO_POLYGONS"] == union.metro_polygons_for(PROD)


@pytest.mark.parametrize("sql_file", ALL_QUERY_FILES)
def test_every_query_renders_without_a_production_reference(sql_file):
    """The regression this prevents: a hard-coded table escaping ${DS}."""
    rendered = loader.load_query(sql_file, _staging_params())
    union._assert_dataset_isolated(rendered, sql_file, STAGING)
    assert f"{union.PROJECT}.{PROD}." not in rendered


@pytest.mark.parametrize("sql_file", ALL_QUERY_FILES)
def test_no_query_leaves_ds_unsubstituted(sql_file):
    """safe_substitute would happily ship a literal ${DS} to BigQuery."""
    rendered = loader.load_query(sql_file, _staging_params())
    assert "${DS}" not in rendered
    assert "${METRO_POLYGONS}" not in rendered


def test_guard_fires_on_a_leaked_production_reference():
    leaked = f"SELECT 1 FROM `{union.PROJECT}.{PROD}.events_with_as_and_geoloc`"
    with pytest.raises(ValueError, match="production table reference survived"):
        union._assert_dataset_isolated(leaked, "synthetic.sql", STAGING)


def test_guard_is_inert_for_production_runs():
    """Production naturally contains production references; the guard must not fire."""
    leaked = f"SELECT 1 FROM `{union.PROJECT}.{PROD}.events_with_as_and_geoloc`"
    union._assert_dataset_isolated(leaked, "synthetic.sql", PROD)


def test_every_output_and_delete_table_moves_with_the_dataset():
    for table in union.delete_tables(STAGING, include_giga=True):
        assert f".{STAGING}." in table, table
        assert f".{PROD}." not in table, table
    assert union.final_output_table(STAGING).endswith(f"{STAGING}.events_explained_daily")
    assert union.giga_output_table(STAGING).endswith(f"{STAGING}.giga_meter_measurements")


def test_delete_set_is_the_same_shape_in_both_datasets():
    """Staging must clear exactly what production clears -- no more, no less."""
    prod = {t.rsplit(".", 1)[1] for t in union.delete_tables(PROD, include_giga=True)}
    stg = {t.rsplit(".", 1)[1] for t in union.delete_tables(STAGING, include_giga=True)}
    assert prod == stg


def test_giga_is_only_deleted_when_asked():
    assert union.giga_output_table() not in union.delete_tables()
    assert union.giga_output_table() in union.delete_tables(include_giga=True)


def test_staging_bootstrap_covers_every_table_a_run_touches():
    targets = set(init_staging.staging_targets())
    assert targets == set(union.delete_tables(STAGING, include_giga=True))
    assert all(f".{STAGING}." in t for t in targets)


def test_worker_forwards_the_dataset_it_unpacks(monkeypatch):
    """The regression: the worker unpacked `dataset` and then dropped it.

    _run_sql_steps' `dataset` default is production, so a dropped argument does
    not fail -- the staging worker silently resolves production tables. It only
    surfaced because production happens to lack a column staging has.
    """
    seen: dict[str, object] = {}

    def fake_run_sql_steps(date, project_id, sql_files, skip_data_check, granularity, dataset):
        seen["dataset"] = dataset
        seen["granularity"] = granularity
        return "Success"

    monkeypatch.setattr(union, "_run_sql_steps", fake_run_sql_steps)
    args = ("2026-08-07", "proj", ["01.sql"], False, "metro", STAGING)
    assert union._run_sql_steps_worker(args) == "Success"
    assert seen["dataset"] == STAGING, "worker dropped the dataset argument"
    assert seen["granularity"] == "metro"


def test_worker_arity_matches_what_the_dispatcher_builds():
    """_run_parallel_sql builds the tuple; the worker unpacks it. Keep them in step."""
    import inspect

    src = inspect.getsource(union._run_parallel_sql)
    built = src.split("worker_args = [", 1)[1].split("]", 1)[0]
    # the tuple the dispatcher constructs, e.g. (date, project_id, ..., dataset)
    tuple_src = built.split("(", 1)[1].split(")", 1)[0]
    n_built = len([p for p in tuple_src.split(",") if p.strip()])

    unpack = inspect.getsource(union._run_sql_steps_worker).split("= args", 1)[0]
    n_unpacked = len([p for p in unpack.split("=", 1)[0].split(",") if p.strip()])
    assert n_built == n_unpacked, f"dispatcher builds {n_built}, worker unpacks {n_unpacked}"


# ---------------------------------------------------------------------------
# Resume guard: a benign NULL is not a second regime.
# ---------------------------------------------------------------------------
class _FakeResult:
    def __init__(self, rows):
        self._rows = [type("R", (), {"granularity": g})() for g in rows]
        self.total_rows = len(rows)

    def __iter__(self):
        return iter(self._rows)


class _FakeClient:
    def __init__(self, rows):
        self._rows = rows

    def query(self, *a, **k):
        return type("J", (), {"result": lambda _s: _FakeResult(self._rows)})()


def _guard(monkeypatch, rows, requested="metro"):
    from google.cloud import bigquery

    monkeypatch.setattr(bigquery, "Client", lambda *a, **k: _FakeClient(rows))
    return union.step_already_done("p", "t", "2026-08-07", requested)


def test_resume_accepts_metro_partition_with_benign_giga_nulls(monkeypatch):
    """The regression: 03 leaves NULL for unmatched giga by design, and counting
    it as a regime made a finished metro date permanently un-rerunnable."""
    assert _guard(monkeypatch, ["metro", "<NULL>"]) is True


def test_resume_accepts_a_cleanly_labelled_partition(monkeypatch):
    assert _guard(monkeypatch, ["metro"]) is True


def test_resume_reports_not_done_for_an_empty_date(monkeypatch):
    assert _guard(monkeypatch, []) is False


def test_resume_still_refuses_a_conflicting_regime(monkeypatch):
    with pytest.raises(RuntimeError, match="refusing to append metro"):
        _guard(monkeypatch, ["maxmind_city", "<NULL>"])


def test_resume_still_refuses_an_entirely_unlabelled_partition(monkeypatch):
    """All-NULL means pre-migration data, which genuinely is an unknown regime."""
    with pytest.raises(RuntimeError, match="no detection_granularity at all"):
        _guard(monkeypatch, ["<NULL>"])

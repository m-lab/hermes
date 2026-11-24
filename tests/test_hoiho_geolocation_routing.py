"""``hermes.geolocation`` must follow --target, like the metro polygons do.

Re-enabled HOIHO writes ~2.5M hostnames into this table and step 04 prefers it
over RIPE IPmap, so the change has to be exercised in staging first. Staging
could not do that while step 04 hard-coded the production table and enrichment
never ran for a non-production target.
"""

from __future__ import annotations

from datetime import date
from unittest import mock

from hermes.pipeline import init_staging, union
from hermes.sql import loader

PROD = union.DEFAULT_DATASET
STAGING = union.STAGING_DATASET


def test_geolocation_follows_the_target():
    assert union.geolocation_for(PROD) == f"{union.PROJECT}.hermes.geolocation"
    assert union.geolocation_for(STAGING) == f"{union.PROJECT}.hermes_staging.geolocation"


def test_default_geolocation_param_is_production():
    assert loader.DEFAULT_PARAMS["GEOLOCATION"] == union.geolocation_for(PROD)


def test_step04_reads_geolocation_through_the_param():
    sql = loader.load_query(
        "04_mapping_union.sql", {"DS": STAGING, "GEOLOCATION": "p.hermes_staging.geolocation"}
    )
    assert "`p.hermes_staging.geolocation`" in sql
    assert ".hermes.geolocation`" not in sql


def test_run_sql_steps_passes_the_targets_geolocation(monkeypatch):
    captured: list[dict] = []

    def fake_load_query(name, params):
        captured.append(dict(params))
        return "SELECT 1"

    monkeypatch.setattr(union.loader, "load_query", fake_load_query)
    monkeypatch.setattr(union, "execute_query", lambda *a, **k: None)
    monkeypatch.setattr(union, "step_already_done", lambda *a, **k: False)

    union._run_sql_steps(date(2026, 11, 1), "p", ["04_mapping_union.sql"], True, "metro", STAGING)

    assert captured[0]["GEOLOCATION"] == union.geolocation_for(STAGING)


def test_metro_pass_rewrites_the_table_it_was_given():
    sql = loader.load_query("enrich_geolocation_add_metro.sql", {"GEOLOCATION": "p.d.geolocation"})
    assert "CREATE OR REPLACE TABLE `p.d.geolocation`" in sql
    assert "FROM `p.d.geolocation`" in sql
    assert ".hermes.geolocation`" not in sql


def test_staging_reference_copy_includes_geolocation():
    client = mock.MagicMock()
    init_staging.refresh_reference_tables(client)
    sqls = [c.args[0] for c in client.query.call_args_list]
    assert any(
        s.startswith(f"CREATE OR REPLACE TABLE `{union.PROJECT}.hermes_staging.geolocation`")
        and f"FROM `{union.PROJECT}.hermes.geolocation`" in s
        for s in sqls
    )

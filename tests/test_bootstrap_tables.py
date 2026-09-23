from unittest.mock import MagicMock

from hermes.pipeline import bootstrap_tables
from hermes.sql import loader


def _statements(sql: str) -> str:
    """The SQL with `--` comment lines dropped.

    These files carry long header comments that legitimately name the very
    tables and parameters a test asserts are absent from the statements.
    """
    return "\n".join(line for line in sql.splitlines() if not line.lstrip().startswith("--"))


def test_ddl_files_listed():
    assert "create_correlation_hyperedges_tomography_v2.sql" in bootstrap_tables.DDL_FILES
    assert "add_client_geo_source_columns.sql" in bootstrap_tables.DDL_FILES
    assert "create_events_enriched.sql" in bootstrap_tables.DDL_FILES
    assert bootstrap_tables.DDL_FILES.index(
        "add_client_geo_source_columns.sql"
    ) < bootstrap_tables.DDL_FILES.index("create_events_enriched.sql")


def test_n_baseline_migration_is_bootstrapped_after_the_create():
    """Step 07 names n_baseline in its INSERT list, so the column must exist first.

    CREATE TABLE IF NOT EXISTS is a no-op on the already-created production table
    and will never add the column, so without this migration in the list the
    nightly run fails on the INSERT rather than degrading.
    """
    assert "add_n_baseline_column.sql" in bootstrap_tables.DDL_FILES
    assert bootstrap_tables.DDL_FILES.index(
        "create_events_explained_daily.sql"
    ) < bootstrap_tables.DDL_FILES.index("add_n_baseline_column.sql")


def test_upload_migration_is_bootstrapped_before_the_public_view():
    assert "add_upload_anomaly_columns.sql" in bootstrap_tables.DDL_FILES
    assert bootstrap_tables.DDL_FILES.index(
        "create_events_explained_daily.sql"
    ) < bootstrap_tables.DDL_FILES.index("add_upload_anomaly_columns.sql")
    assert bootstrap_tables.DDL_FILES.index(
        "add_upload_anomaly_columns.sql"
    ) < bootstrap_tables.DDL_FILES.index("create_events_enriched.sql")


def test_bootstrap_runs_each_ddl(monkeypatch):
    loaded = []
    monkeypatch.setattr(
        bootstrap_tables.loader,
        "load_query",
        lambda name, params=None: loaded.append((name, params)) or "SELECT 1",
    )
    client = MagicMock()
    bootstrap_tables.bootstrap(client)
    assert {name for name, _ in loaded} == set(bootstrap_tables.DDL_FILES)
    assert client.query.call_count == len(bootstrap_tables.DDL_FILES)


def test_bootstrap_parameterizes_every_ddl_with_one_dataset(monkeypatch):
    """One knob, not two.

    The canonical view used to take a separate PUBLISHED_DS, so a staging
    bootstrap could publish a staging-named view over production data -- or, as
    it actually behaved, leave ${PUBLISHED_DS} unsubstituted in the submitted
    SQL. DS alone now decides both what is read and where the view lands.
    """
    loaded = {}

    def capture(name, params=None):
        loaded[name] = params
        return "SELECT 1"

    monkeypatch.setattr(bootstrap_tables.loader, "load_query", capture)
    bootstrap_tables.bootstrap(MagicMock(), source_dataset="hermes_staging")

    assert loaded["create_events_enriched.sql"] == {"DS": "hermes_staging"}
    assert loaded["add_client_geo_source_columns.sql"] == {"DS": "hermes_staging"}
    for name, params in loaded.items():
        assert params == {"DS": "hermes_staging"}, name


def test_canonical_view_is_published_in_the_union_dataset():
    """events_enriched lives beside the table it reads, not in `hermes`."""
    assert not hasattr(bootstrap_tables, "DEFAULT_PUBLISHED_DATASET")
    assert bootstrap_tables.DEFAULT_SOURCE_DATASET == "hermes_union"

    sql = _statements(loader.load_query("create_events_enriched.sql", {"DS": "hermes_union"}))
    assert "`mlab-collaboration.hermes_union.events_enriched`" in sql
    assert "`mlab-collaboration.hermes.events_enriched`" not in sql


def test_partition_filter_requirement_is_bootstrapped_last():
    """It ALTERs every table in the dataset, so they must all exist first."""
    assert bootstrap_tables.DDL_FILES[-1] == "require_partition_filter.sql"


def test_partition_filter_ddl_derives_its_table_list():
    """A hard-coded list would silently omit any table added later."""
    statements = _statements(
        loader.load_query("require_partition_filter.sql", {"DS": "hermes_union"})
    )
    assert "INFORMATION_SCHEMA.COLUMNS" in statements
    assert "is_partitioning_column = 'YES'" in statements
    assert "require_partition_filter = TRUE" in statements
    # The only ALTER is the dynamic one; no table is named literally.
    assert statements.count("ALTER TABLE") == 1
    assert "%s` SET OPTIONS (require_partition_filter = TRUE)" in statements

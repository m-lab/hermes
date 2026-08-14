from unittest.mock import MagicMock

from hermes.pipeline import bootstrap_tables


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


def test_bootstrap_parameterizes_canonical_view(monkeypatch):
    loaded = {}

    def capture(name, params=None):
        loaded[name] = params
        return "SELECT 1"

    monkeypatch.setattr(bootstrap_tables.loader, "load_query", capture)
    bootstrap_tables.bootstrap(
        MagicMock(),
        source_dataset="hermes_staging",
        published_dataset="hermes_staging",
    )

    assert loaded["create_events_enriched.sql"] == {
        "DS": "hermes_staging",
        "PUBLISHED_DS": "hermes_staging",
    }
    assert loaded["add_client_geo_source_columns.sql"] == {"DS": "hermes_staging"}
    for name, params in loaded.items():
        assert params["DS"] == "hermes_staging", name

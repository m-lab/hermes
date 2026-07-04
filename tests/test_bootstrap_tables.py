from unittest.mock import MagicMock

from hermes.pipeline import bootstrap_tables


def test_ddl_files_listed():
    assert "create_correlation_hyperedges_tomography_v2.sql" in bootstrap_tables.DDL_FILES


def test_bootstrap_runs_each_ddl(monkeypatch):
    loaded = []
    monkeypatch.setattr(
        bootstrap_tables.loader,
        "load_query",
        lambda name, params=None: loaded.append(name) or "SELECT 1",
    )
    client = MagicMock()
    bootstrap_tables.bootstrap(client)
    assert set(loaded) == set(bootstrap_tables.DDL_FILES)
    assert client.query.call_count == len(bootstrap_tables.DDL_FILES)

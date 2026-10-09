from hermes.pipeline import bootstrap_tables
from hermes.sql import loader

STEP = "03_build_transient_events_union.sql"


def _sql() -> str:
    return loader.load_query(STEP, {"DAY": "2026-10-06", "ONE_WEEK_EARLIER": "2026-09-29"})


def test_revtr_joins_on_both_connections_of_a_test():
    sql = _sql()
    assert "TestConnections AS (" in sql
    assert "upload_id" in sql
    assert "t.raw.uuid = c.connection_id" in sql
    assert "ON t2.test_id = ag.id" in sql
    # the old download-only key must be gone
    assert "ON t2.raw.uuid = ag.id" not in sql


def test_revtr_prefers_reaching_then_download_connection():
    sql = _sql()
    order = sql[sql.index("PARTITION BY test_id") :]
    assert order.index("REACHES") < order.index("connection_pref") < order.index("raw.date DESC")


def test_upload_id_migration_is_bootstrapped():
    assert "add_upload_id_column.sql" in bootstrap_tables.DDL_FILES
    assert bootstrap_tables.DDL_FILES.index(
        "add_upload_id_column.sql"
    ) < bootstrap_tables.DDL_FILES.index("require_partition_filter.sql")
    ddl = loader.load_query("add_upload_id_column.sql", {"DS": "hermes_staging"})
    assert "merged_download_upload" in ddl and "ADD COLUMN IF NOT EXISTS upload_id" in ddl

"""Tests for the public-format union step (events_explained_daily)."""

from __future__ import annotations

from hermes.sql import loader, paths

STEP = "07_translating_to_public_format_union.sql"


def test_step_file_exists():
    assert paths.query_path(STEP).is_file()


def test_step_templates_and_targets_union_table():
    sql = loader.load_query(STEP, {"DAY": "2026-06-01"})
    # Template fully resolved
    assert "${DAY}" not in sql
    assert "2026-06-01" in sql
    # Writes the shortened union target table
    assert "mlab-collaboration.hermes_union.events_explained_daily" in sql
    # Reads union inputs
    assert "mlab-collaboration.hermes_union.events_with_as_and_geoloc" in sql
    assert "mlab-collaboration.hermes_union.correlation_hyperedges_tomography" in sql


def test_step_has_no_legacy_references():
    sql = loader.load_query(STEP, {"DAY": "2026-06-01"})
    # No legacy hermes-dataset event/tomography tables
    assert "hermes.transient_events" not in sql
    assert "correlation_hyperedges_tomography_with_forward_only_included" not in sql
    assert "events_explained_daily_with_forward_only_included" not in sql
    # No legacy hardcoded metadata reference date
    assert "2025-06-09" not in sql
    # Shared metadata table is still allowed
    assert "hermes.as_metadata" in sql


DDL = "create_events_explained_daily.sql"


def test_ddl_file_exists_and_is_idempotent_create():
    sql = loader.load_query(DDL, {})
    assert "CREATE TABLE IF NOT EXISTS" in sql
    assert "mlab-collaboration.hermes_union.events_explained_daily" in sql
    assert "PARTITION BY partition_date" in sql
    # Key columns present with expected types
    assert "observed_ips ARRAY<STRING>" in sql
    assert "partition_date DATE" in sql
    assert "src_asn INT64" in sql


from hermes.pipeline import union  # noqa: E402

PUBLIC_TABLE = "mlab-collaboration.hermes_union.events_explained_daily"


def test_public_step_registered_in_resume_mapping():
    assert STEP in union.SQL_FILES_PUBLIC
    assert union.SQL_FILE_TO_OUTPUT_TABLE[STEP] == PUBLIC_TABLE


def test_final_output_table_is_public_table():
    # main()'s "already processed" check must key off the true last step.
    assert union.FINAL_OUTPUT_TABLE == PUBLIC_TABLE


def test_public_table_in_output_tables_for_deletion():
    assert PUBLIC_TABLE in union.OUTPUT_TABLES


def test_dry_run_logs_public_step(caplog):
    import datetime as dt

    with caplog.at_level("INFO"):
        union.run_dates(
            [dt.date(2026, 6, 1)],
            project_id="mlab-collaboration",
            max_workers=1,
            skip_data_check=True,
            dry_run=True,
        )
    assert any(STEP in m for m in caplog.messages)


def test_06_cuts_over_to_v2_and_surfaces_tiers():
    from hermes.sql import loader

    sql = loader.load_query("07_translating_to_public_format_union.sql", {"DAY": "2026-06-19"})
    assert "correlation_hyperedges_tomography_v2" in sql
    assert "attribution_method" in sql and "confidence_tier" in sql


def test_ddl_has_tier_columns():
    from hermes.sql import loader

    sql = loader.load_query("create_events_explained_daily.sql", {})
    assert "attribution_method" in sql
    assert "confidence_tier" in sql


import os  # noqa: E402

import pytest  # noqa: E402


def _bq_creds() -> bool:
    return bool(
        os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") or os.environ.get("HERMES_BQ_TEST")
    )


@pytest.mark.bq
def test_step_produces_rows_with_root_cause_fields():
    if not _bq_creds():
        pytest.skip("BigQuery creds not configured (on-demand test)")
    from google.cloud import bigquery

    c = bigquery.Client(project="mlab-collaboration")
    q = """
    SELECT
      COUNT(*) AS n_rows,
      COUNTIF(information_source IS NOT NULL) AS with_info_source
    FROM `mlab-collaboration.hermes_union.events_explained_daily`
    WHERE partition_date = '2026-06-01'
    """
    row = list(c.query(q).result())[0]
    assert row["n_rows"] > 0
    assert row["with_info_source"] > 0

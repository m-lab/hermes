"""Guards on which tables the pipeline deletes and which steps resume.

These two concerns used to share one list (``OUTPUT_TABLES``), zipped
positionally against ``SQL_FILES``. That made the delete list structurally
incapable of holding a table with no 1:1 SQL file — which is exactly what the
Phase-D correlation tables are. Every re-run therefore needed a manual
pre-delete or it appended a second copy of that date's rows.
"""

from datetime import date
from types import SimpleNamespace

from hermes.pipeline import union


def test_every_sql_file_has_a_resume_target():
    """A step with no resume target would never be skipped by step_already_done."""
    for sql_file in union.SQL_FILES:
        assert sql_file in union.SQL_FILE_TO_OUTPUT_TABLE, (
            f"{sql_file} has no entry in SQL_FILE_TO_OUTPUT_TABLE"
        )


def test_resume_map_has_no_extra_entries():
    """A mapping for a file that is not run is dead config and drifts silently."""
    assert set(union.SQL_FILE_TO_OUTPUT_TABLE) == set(union.SQL_FILES)


def test_delete_covers_all_append_only_phase_d_tables():
    """The regression: these are append-only, so a re-run without deleting duplicates.

    correlation_tomography writes them with insert_rows_json, which has no
    self-delete. They are not the resume target of any SQL file, so they can only
    reach the delete list via DERIVED_OUTPUT_TABLES.
    """
    for table in (
        "correlation_hyperedges_tomography_v2",
        "correlation_culprits_multigranularity",
        "correlation_entity_stats_multigranularity",
        "temporal_path_verdicts",
    ):
        assert any(t.endswith(table) for t in union.DELETE_TABLES), (
            f"{table} is written by Phase D but --delete-first would not clear it"
        )


def test_delete_covers_every_resume_target():
    """Anything gating a step's resume must also be clearable, or re-runs no-op."""
    assert set(union.OUTPUT_TABLES) <= set(union.DELETE_TABLES)


def test_giga_is_excluded_from_delete_by_design():
    """giga_meter_measurements accumulates history under first-writer-wins.

    A delete not followed by a successful 04 loses traces permanently, and part
    of its history was recovered by hand. Clearing it stays a deliberate manual
    act. This test exists so adding it becomes a conscious decision.
    """
    assert not any("giga_meter_measurements" in t for t in union.DELETE_TABLES)


def test_metro_conversion_can_explicitly_clear_giga(monkeypatch):
    queries = []
    query_job = SimpleNamespace(result=lambda: None)
    client = SimpleNamespace(query=lambda query: queries.append(query) or query_job)
    monkeypatch.setattr(union.bigquery, "Client", lambda **kwargs: client)

    union.delete_dates(
        "billing-project",
        [date(2026, 8, 7)],
        include_giga=True,
    )

    assert any(union.GIGA_OUTPUT_TABLE in query for query in queries)


def test_delete_tables_are_unique():
    """A duplicate would issue the same DELETE twice — harmless but a sign of drift."""
    assert len(union.DELETE_TABLES) == len(set(union.DELETE_TABLES))

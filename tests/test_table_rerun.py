from __future__ import annotations

import datetime as dt
import sys

import pytest

from hermes.pipeline import table_rerun


def test_process_date_substitutes_dataset(monkeypatch, tmp_path):
    captured: dict[str, object] = {}

    def fake_load_sql(path: str, params: dict) -> str:
        captured["path"] = path
        captured["params"] = params
        return "SELECT 1"

    monkeypatch.setattr(table_rerun, "load_sql", fake_load_sql)
    monkeypatch.setattr(
        table_rerun,
        "execute_query",
        lambda query, project_id, description="": captured.update(
            query=query, project_id=project_id, description=description
        ),
    )

    result = table_rerun.process_date_with_sql(
        dt.date(2026, 8, 1),
        "mlab-collaboration",
        "hermes_union",
        str(tmp_path),
        "07_translating_to_public_format_union.sql",
    )

    assert result == "Success: 2026-08-01"
    assert captured["params"] == {
        "ONE_WEEK_EARLIER": "2026-07-25",
        "DAY": "2026-08-01",
        "DS": "hermes_union",
        "DETECTION_GRANULARITY": "metro",
    }


@pytest.mark.parametrize(
    "table_name",
    ["table", "dataset.table", "project.dataset.table.extra", "project..table"],
)
def test_validate_table_name_rejects_non_fully_qualified_names(table_name):
    assert not table_rerun.validate_table_name(table_name)


def test_parallel_failure_exits_nonzero(monkeypatch, tmp_path):
    class FakePool:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, traceback):
            return False

        def map(self, function, args):
            return [f"Error: {item[0]} - rejected" for item in args]

    class FakeContext:
        def Pool(self, processes):
            return FakePool()

    monkeypatch.setattr(table_rerun, "print_active_credentials", lambda: None)
    monkeypatch.setattr(table_rerun, "get_existing_dates", lambda project, table: set())
    monkeypatch.setattr(table_rerun.mp, "get_context", lambda method: FakeContext())
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "hermes-table-rerun",
            "--table",
            "mlab-collaboration.hermes_union.events_explained_daily",
            "--dates",
            "2026-08-01",
            "2026-08-02",
            "--sql-file",
            "07_translating_to_public_format_union.sql",
            "--sql-folder",
            str(tmp_path),
            "--max-workers",
            "1",
        ],
    )

    with pytest.raises(SystemExit) as error:
        table_rerun.main()

    assert error.value.code == 1

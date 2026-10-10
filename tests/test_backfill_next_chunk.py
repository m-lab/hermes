"""The backfill chunk picker must only issue queries hermes_union accepts."""

import importlib.util
import re
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "backfill_next_chunk.py"


def _load():
    spec = importlib.util.spec_from_file_location("backfill_next_chunk", SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class _Client:
    def __init__(self):
        self.sql: list[str] = []

    def query(self, sql):
        self.sql.append(sql)
        return self

    def result(self):
        return []


def test_granularity_query_filters_on_partition_date():
    """hermes_union requires a partition filter; without one the walker aborts daily."""
    client = _Client()
    _load().granularity_blocked(client, "metro")
    (sql,) = client.sql
    assert "anomaly_counts_union" in sql
    assert re.search(r"WHERE\s+partition_date\s*>=", sql)

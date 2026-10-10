"""Both path directions are eligible for every anomaly type (RTT, download, upload)."""

import re

import pytest

from hermes.sql import loader

FILES = [
    "05_temporal_edge_prevalences_union.sql",
    "06_correlation_tomography_prepare_union.sql",
    "06_correlation_tomography_unexplained_hops_union.sql",
]


def _gate(sql: str, name: str) -> str:
    """The parenthesised expression immediately before ``AS <name>``."""
    m = re.search(r"\)\s*AS " + name + r"\b", sql)
    assert m, name
    end, depth = m.start(), 0
    for k in range(end, -1, -1):
        depth += {")": 1, "(": -1}.get(sql[k], 0)
        if depth == 0:
            return " ".join(sql[k + 1 : end].split())
    raise AssertionError(name)


@pytest.mark.parametrize("step", FILES)
def test_forward_and_reverse_gates_are_identical(step):
    sql = loader.load_query(step, {"DAY": "2026-10-06"})
    fwd = _gate(sql, "is_forward_anomaly")
    rev = _gate(sql, "is_reverse_anomaly")
    assert fwd == rev
    for signal in (
        "anomaly_ratio_rtt",
        "anomaly_ratio_throughput",
        "anomaly_ratio_upload_throughput",
    ):
        assert signal in fwd


def test_public_format_direction_guard_applies_only_to_legacy_keys():
    sql = loader.load_query("07_translating_to_public_format_union.sql", {"DAY": "2026-10-06"})
    # both mirrored guards are bypassed for four-component (ip_version) keys
    assert sql.count("ocd.pair_ip_version IS NOT NULL") == 2

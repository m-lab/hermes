"""Step 02 tests each metric on its own samples (fix #9a).

The candidate gate used to require > 2 RTT samples on both sides for EVERY test, so
a group whose download RTT was missing never had its throughput, upload or loss
tested. Step 06 measured 58,643 rows/day with NULL ndt_rtt.
"""

from __future__ import annotations

import os
import re

import pytest

from hermes.sql import loader

STEP = "02_detect_anomalies_union.sql"


def _sql() -> str:
    return loader.load_query(
        STEP,
        {"DAY": "2026-10-14", "ONE_WEEK_EARLIER": "2026-10-07", "DETECTION_GRANULARITY": "metro"},
    )


def _block(sql: str, name: str) -> str:
    start = sql.index(f"\n{name} AS (") + 1
    return sql[start : sql.index("\n),", start)]


# ------------------------------------------------------------------ string level


def test_candidate_gate_is_any_metric_not_rtt_only():
    cand = _block(_sql(), "CandidateGroups")
    where = cand[cand.rindex("WHERE") :]
    for metric in ("rtt", "throughput", "upload_throughput", "loss"):
        assert f"ARRAY_LENGTH(current_{metric}_array), 0) > 2" in where, metric
        assert f"ARRAY_LENGTH(baseline_{metric}_array), 0) > 2" in where, metric
    assert where.count(" OR ") == 3


def test_each_test_and_ratio_is_gated_on_its_own_metric():
    st = _block(_sql(), "StatisticalTestsResults")
    gates = {
        "rtt_testable": ["AS mann_whitney,", "AS t_test_result,", "AS anomaly_ratio_rtt"],
        "throughput_testable": [
            "AS mann_whitney_throughput",
            "AS t_test_result_throughput",
            "AS wasserstein_throughput_result",
            "AS anomaly_ratio_throughput",
        ],
        "upload_testable": [
            "AS mann_whitney_upload_throughput",
            "AS t_test_result_upload_throughput",
            "AS wasserstein_upload_throughput_result",
            "AS anomaly_ratio_upload_throughput",
        ],
        "loss_testable": ["AS z_loss_occurrence", "AS mann_whitney_loss_severity"],
    }
    # split the select list into top-level items and check which gate each uses
    for gate, aliases in gates.items():
        for alias in aliases:
            i = st.index(alias)
            item = st[st.rindex(",\n\n", 0, i) : i] if ",\n\n" in st[:i] else st[:i]
            assert gate in item, (gate, alias)
            others = set(gates) - {gate}
            assert not any(o in item for o in others), (alias, item)


# ------------------------------------------------------------- literal rows on BQ


def _arr(xs) -> str:
    return "ARRAY<FLOAT64>[" + ", ".join(repr(float(x)) for x in xs) + "]"


def _group(name: str, rtt_c, rtt_b, tput_c, tput_b, n_loss=(20, 30)) -> str:
    loss_c, loss_b = [0.0] * n_loss[0], [0.0] * n_loss[1]
    return (
        f"SELECT 1 AS src_asn, '{name}' AS src_city, 'site01' AS dst_site, 'v4' AS ip_version, "
        f"{_arr(rtt_c)} AS current_rtt_array, {_arr(tput_c)} AS current_throughput_array, "
        f"{_arr([])} AS current_upload_throughput_array, {_arr(loss_c)} AS current_loss_array, "
        f"{_arr([])} AS current_positive_loss_array, 0 AS current_lossy_count, "
        f"{len(loss_c)} AS current_loss_total, {_arr(rtt_b)} AS baseline_rtt_array, "
        f"{_arr(tput_b)} AS baseline_throughput_array, "
        f"{_arr([])} AS baseline_upload_throughput_array, {_arr(loss_b)} AS baseline_loss_array, "
        f"{_arr([])} AS baseline_positive_loss_array, 1.0 AS current_rtt_stddev, "
        f"(SELECT PERCENTILE_CONT(v, 0.5) OVER () FROM UNNEST({_arr(rtt_b)}) v LIMIT 1) "
        "AS baseline_median_rtt, "
        f"(SELECT PERCENTILE_CONT(v, 0.5) OVER () FROM UNNEST({_arr(tput_b)}) v LIMIT 1) "
        "AS baseline_median_throughput, CAST(NULL AS FLOAT64) AS baseline_median_upload_throughput"
    )


def _query() -> str:
    sql = _sql()
    functions = sql[: sql.index("ASSERT ")]
    start = sql.index("\nCandidateGroups AS (") + 1
    body = sql[start : sql.index("\n-- One set of descriptive attributes")].rstrip().rstrip(",")
    slow = [10.0 + k for k in range(20)]  # day-of throughput collapsed
    fast = [100.0 + k for k in range(30)]
    rtt_c, rtt_b = [80.0 + k for k in range(20)], [20.0 + k for k in range(30)]
    combined = " UNION ALL ".join(
        [
            _group("no_rtt", [], [], slow, fast),  # RTT missing: throughput must be tested
            _group("no_tput", rtt_c, rtt_b, [], []),  # throughput missing: RTT still tested
            _group("too_few", [1.0, 2.0], rtt_b, [1.0], fast, (2, 30)),  # nothing testable
        ]
    )
    return (
        functions
        + f"WITH CombinedRTTs AS ({combined}),\n{body}\n"
        + "SELECT src_city, mann_whitney.p_value AS mw_rtt, t_test_result.p_value AS t_rtt, "
        "anomaly_ratio_rtt, mann_whitney_throughput.p_value AS mw_tput, "
        "t_test_result_throughput.p_value AS t_tput, "
        "wasserstein_throughput_result.p_value AS w_tput, anomaly_ratio_throughput, "
        "mann_whitney_upload_throughput IS NULL AS upload_null, z_loss_occurrence "
        "FROM StatisticalTestsResults"
    )


@pytest.mark.bq
def test_throughput_is_tested_when_rtt_is_missing():
    """Runs 02's real CandidateGroups + StatisticalTestsResults on literal arrays (0 bytes)."""
    if not (os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") or os.environ.get("HERMES_BQ_TEST")):
        pytest.skip("BigQuery creds not configured (on-demand test)")
    from google.cloud import bigquery

    rows = {
        r["src_city"]: r
        for r in bigquery.Client(project="mlab-collaboration").query(_query()).result()
    }
    assert set(rows) == {"no_rtt", "no_tput"}  # too_few is not a candidate at all
    g = rows["no_rtt"]
    assert g["mw_rtt"] is None and g["t_rtt"] is None and g["anomaly_ratio_rtt"] is None
    assert g["mw_tput"] < 0.05 and g["t_tput"] < 0.05 and g["w_tput"] < 0.05
    assert g["anomaly_ratio_throughput"] == 1.0
    assert g["upload_null"] is True  # no upload samples: upload untested
    assert g["z_loss_occurrence"] is None  # no lossy tests on either side: z undefined
    h = rows["no_tput"]
    assert h["mw_rtt"] < 0.05 and h["anomaly_ratio_rtt"] == 1.0
    assert h["mw_tput"] is None and h["w_tput"] is None and h["anomaly_ratio_throughput"] is None


def test_query_builder_is_self_consistent():
    q = _query()
    assert re.search(r"WITH CombinedRTTs AS \(SELECT 1 AS src_asn", q)
    assert "StatisticalTestsResults AS (" in q


# ------------------------------------------------ step 06 distance-vs-RTT filter (fix #9b)


def test_prepare_distance_filter_keeps_rows_without_an_rtt():
    """`NOT distance/100 > ndt_rtt` is NULL for a NULL ndt_rtt, and WHERE NULL dropped
    58,643 rows/day (648 throughput-anomalous) before 06 ever saw them."""
    sql = loader.load_query("06_correlation_tomography_prepare_union.sql", {"DAY": "2026-10-14"})
    block = sql[sql.index("CREATE TEMP TABLE final_results") : sql.index("-- 3. Extract")]
    assert "NOT IFNULL(forward_distance / 100 > ndt_rtt, FALSE)" in block
    assert "NOT IFNULL(reverse_distance / 100 > ndt_rtt, FALSE)" in block
    assert not re.search(r"NOT \w+_distance / 100 > ndt_rtt", block)
    # no other step carries the bare (NULL-dropping) form
    for step in (
        "05_temporal_edge_prevalences_union.sql",
        "05_temporal_tomography_union.sql",
        "06_correlation_tomography_all_edges_union.sql",
        "06_correlation_tomography_unexplained_hops_union.sql",
        "07_translating_to_public_format_union.sql",
    ):
        text = loader.load_query(step, {"DAY": "2026-10-14"})
        assert not re.search(r"NOT\s+\w*\.?\w+_distance\s*/\s*100\s*>", text), step

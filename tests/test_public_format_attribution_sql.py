"""Step 07: a correlation attribution is never deleted by AS-name lookups (fix #7).

In Oct 2026 prod, 10 hyperedge rows carried to_asn values such as 'Vũng Tàu' (a
region name containing ' - ' was split as if it were the node delimiter). `resolved`
INNER JOINed as_metadata on to_asn, so the attribution was dropped, and `unresolved`
excludes every pair with a correlation row, so the pair vanished from the output.
"""

from __future__ import annotations

import os
import re

import pytest

from hermes.sql import loader

STEP = "07_translating_to_public_format_union.sql"


def _sql() -> str:
    return loader.load_query(STEP, {"DAY": "2026-10-14"})


def _block(sql: str, name: str) -> str:
    start = sql.index(f"\n  {name} AS (") + 1
    return sql[start : sql.index("\n  ),", start)]


# ------------------------------------------------------------------ string level


def test_resolved_joins_as_names_with_left_joins_on_edge_derived_asns():
    resolved = _block(_sql(), "resolved")
    assert "LEFT JOIN closest_metadata metadata_left" in resolved
    assert "LEFT JOIN closest_metadata metadata_right" in resolved
    assert "ocd.edge_from_asn = CAST(metadata_left.asn AS STRING)" in resolved
    assert "ocd.edge_to_asn = CAST(metadata_right.asn AS STRING)" in resolved
    assert not re.search(r"\n\s+JOIN closest_metadata", resolved)
    assert "WHERE ocd.partition_date = '2026-10-14'" in resolved


def test_src_and_dst_name_lookups_cannot_drop_rows():
    sql = _sql()
    assert "LEFT JOIN closest_metadata metadata_src_asn" in sql
    assert "LEFT JOIN closest_metadata metadata_dst_asn" in sql
    assert not re.search(r"\n\s+JOIN closest_metadata", sql)


def test_source_events_rewrites_only_the_node_boundary():
    sql = _sql()
    assert "SPLIT(source_events, ' - ')" not in sql


# ------------------------------------------------------------- literal rows on BQ


def _bq():
    if not (os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") or os.environ.get("HERMES_BQ_TEST")):
        pytest.skip("BigQuery creds not configured (on-demand test)")
    from google.cloud import bigquery

    return bigquery.Client(project="mlab-collaboration")


VUNG_TAU_EDGE = "7552-Ho Chi Minh City-Hồ Chí Minh city-VN - 45899-Vung Tau-Bà Rịa - Vũng Tàu-VN"
RABAT_EDGE = (
    "36903-Kenitra-Gharb - Chrarda - Béni Hssen-MA - 6713-Rabat-Rabat - Salé - Zemmour - Zaer-MA"
)

# (pair, edge, stored from_asn, stored to_asn): the stored to_asn is what the
# pre-fix Python wrote; 07 must not depend on it.
_HYPEREDGES = [
    ("7552 - HCMC-HCM-VN - sgn01 - v4", VUNG_TAU_EDGE, "7552", "Vũng Tàu"),
    ("36903 - Kenitra-MA - rba01 - v4", RABAT_EDGE, "36903", "Salé"),
]
# eligible anomalous pairs (src_asn, label, site, ipv); the third has no correlation row
_ELIGIBLE = [
    (7552, "HCMC-HCM-VN", "sgn01", "v4"),
    (36903, "Kenitra-MA", "rba01", "v4"),
    (3320, "Berlin-Berlin-DE", "fra01", "v4"),
]
# as_metadata knows 7552 and 36903 only -- 45899 and 6713 have no name
_NAMES = {7552: "VIETEL", 36903: "MT-MPLS", 3320: "DTAG"}


def _literal_inputs() -> str:
    hx = " UNION ALL ".join(
        f"SELECT DATE '2026-10-14' AS partition_date, ['{p}'] AS anomalous_src_dst_pairs_impacted, "
        f"'{e}' AS edge_asn_metro, '{f}' AS from_asn, '{t}' AS to_asn, "
        "'forward' AS information_source, 'interdomain' AS is_interdomain, "
        "2.0 AS ratio_anomaly, 0.5 AS fraction_anomalies_explained_by_edge, "
        "1 AS cumulative_anomalies_explained, 2 AS iteration_number, "
        "'correlation' AS attribution_method, 'high' AS confidence_tier"
        for p, e, f, t in _HYPEREDGES
    )
    ta_cols = (
        "CAST(NULL AS STRING) AS detection_granularity, CAST(NULL AS STRING) AS src_city, "
        "CAST(NULL AS STRING) AS src_metro, 20 AS n_dayof, 50 AS n_baseline, "
        "CAST(NULL AS STRING) AS src_state, CAST(NULL AS STRING) AS src_country, "
        "1 AS dst_asn, CAST(NULL AS STRING) AS dst_city, CAST(NULL AS STRING) AS dst_country, "
        "['1.2.3.4'] AS observed_ips, 1.0 AS baseline_median_rtt, "
        "1.0 AS baseline_median_throughput, 1.0 AS baseline_median_upload_throughput, "
        "1.0 AS median_daily_rtt, 1.0 AS median_daily_throughput, "
        "1.0 AS median_daily_upload_throughput, 1.0 AS mean_daily_rtt, "
        "1.0 AS mean_daily_throughput, 1.0 AS mean_daily_upload_throughput, "
        "1.0 AS anomaly_ratio_rtt, 1.0 AS anomaly_ratio_throughput, "
        "1.0 AS anomaly_ratio_upload_throughput, ['latency'] AS anomaly_signals"
    )
    ta = " UNION ALL ".join(
        f"SELECT '{a} - {lab} - {s} - {v}' AS src_dst_pair, {a} AS src_asn, "
        f"'{lab}' AS src_group_label, '{s}' AS dst_site, '{v}' AS ip_version, {ta_cols}"
        for a, lab, s, v in _ELIGIBLE
    )
    meta = " UNION ALL ".join(f"SELECT {a} AS asn, '{n}' AS asnName" for a, n in _NAMES.items())
    return (
        f"total_anomalous_src_dst_pairs AS ({ta}),\nclosest_metadata AS ({meta}),\nhx AS ({hx}),\n"
    )


def _query() -> str:
    sql = _sql()
    start = sql.index("\n  correlation_pair_parts AS (") + 1
    body = sql[start : sql.index("\nanomalies AS (")].rstrip().rstrip(",")
    body = body.replace(
        "`mlab-collaboration.hermes_union.correlation_hyperedges_tomography_v2`", "hx"
    )
    return (
        "WITH " + _literal_inputs() + body + "\n"
        "SELECT 'resolved' AS branch, CAST(src_asn AS INT64) AS src_asn, source_events, "
        "source_events_org FROM resolved\n"
        "UNION ALL SELECT 'unresolved', CAST(src_asn AS INT64), source_events, "
        "source_events_org FROM unresolved"
    )


@pytest.mark.bq
def test_every_eligible_pair_lands_in_exactly_one_branch():
    """Runs 07's real correlation_pair_parts..unresolved CTEs on literal rows (0 bytes)."""
    rows = list(_bq().query(_query()).result())
    branches: dict[int, list[str]] = {}
    for r in rows:
        branches.setdefault(r["src_asn"], []).append(r["branch"])
    assert branches == {7552: ["resolved"], 36903: ["resolved"], 3320: ["unresolved"]}
    got = {r["src_asn"]: r for r in rows}
    # the stored to_asn ('Vũng Tàu') is ignored: the ASN comes from the edge string,
    # and an AS without a name keeps its number
    assert got[7552]["source_events"] == VUNG_TAU_EDGE
    assert got[7552]["source_events_org"] == "VIETEL --- 45899"
    assert got[36903]["source_events_org"] == "MT-MPLS --- 6713"


def _source_events_expr() -> str:
    m = re.search(r"\n\s+(IFNULL\(REGEXP_REPLACE\(source_events.*?\)) AS source_events,", _sql())
    assert m, "source_events expression not found"
    return m.group(1)


@pytest.mark.bq
def test_source_events_separator_keeps_place_names_whole():
    expr = _source_events_expr()
    q = (
        f"SELECT {expr} AS out FROM UNNEST(["
        f"'{VUNG_TAU_EDGE}', '{RABAT_EDGE}', '3356-Miami-Florida-US - 174-Atlanta-Georgia-US', "
        "'3356-Miami-Florida-US - *', CAST(NULL AS STRING)]) AS source_events WITH OFFSET o "
        "ORDER BY o"
    )
    got = [r["out"] for r in _bq().query(q).result()]
    assert got == [
        "7552-Ho Chi Minh City-Hồ Chí Minh city-VN --- 45899-Vung Tau-Bà Rịa - Vũng Tàu-VN",
        "36903-Kenitra-Gharb - Chrarda - Béni Hssen-MA --- "
        "6713-Rabat-Rabat - Salé - Zemmour - Zaer-MA",
        "3356-Miami-Florida-US --- 174-Atlanta-Georgia-US",
        "3356-Miami-Florida-US --- *",
        "",  # unresolved rows: '' as before (final_result keys the 'source' case on it)
    ]

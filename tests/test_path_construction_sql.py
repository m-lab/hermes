"""Step 03 path construction: positions, destination, no-reply traces, last router.

Position convention (both sources): position 1 is the synthetic server row; a router
sits at its distance + 1. scamper1 keys a hop on the probe TTL of its outgoing link
(= distance + 1); scamper2 lists hops at their real TTL and is shifted by one.

Four defects confirmed on 2026-10-14 and fixed here:
1. scamper2's first router was overwritten by the server row (no +1 shift);
2. the destination was listed twice when it already was a hop row (2.4% of traces);
3. scamper1 traces with no replies produced no row, so their tests vanished (7.1%);
4. the deepest router of a non-reaching scamper1 trace (a link far end only) never
   got a hop row (97% of non-reaching traces).
"""

import re

import pytest

from hermes.sql import loader


def _sql():
    return loader.load_query("03_build_transient_events_union.sql", {"DAY": "2026-10-14"})


def _block(sql, name):
    start = sql.index(f"\n{name} AS (") + 1
    return sql[start : sql.index("\n),", start)]


# ---------------------------------------------------------------- string level


def test_scamper2_hops_take_the_scamper1_position_convention():
    s2 = _block(_sql(), "Scamper2WithoutDstNodeExtraction")
    assert "SAFE_CAST(hop.probe_ttl AS INT64) + 1 AS probe_ttl" in s2


def test_destination_is_appended_only_when_not_already_a_kept_hop():
    sql = _sql()
    final = _block(sql, "FinalNodeExtraction")
    assert re.search(
        r"LOGICAL_OR\(r\.ttl_rn = 1 AND r\.addr = r\.dst\) OVER \(PARTITION BY r\.id\)", final
    )
    assert "IFNULL(" in final  # a NULL dst must not suppress every append
    node = _block(sql, "NodeExtraction")
    assert re.search(r"WHERE add_dst = TRUE\s+AND NOT dst_is_hop", node)
    # the hop kept per position is still chosen exactly as before
    assert re.search(
        r"PARTITION BY ne\.id, ne\.probe_ttl\s*\n\s*ORDER BY ne\.far_rtt ASC, ne\.addr ASC", final
    )
    assert "WHERE ttl_rn = 1" in node


def test_no_reply_trace_keeps_a_server_only_placeholder():
    sql = _sql()
    w = _block(sql, "WithoutDstNodeExtraction")
    assert "WHERE bf.best_flow_id IS NULL" in w
    assert "CROSS JOIN UNNEST(bf.hops) AS h" not in w  # the old form dropped such traces
    # the placeholder is not scamper1 data: scamper2 may still fill that test
    assert "WHERE flow_id IS NOT NULL" in _block(sql, "Scamper1IDs")


def test_far_end_only_router_gets_a_row_with_its_own_reply():
    w = _block(_sql(), "WithoutDstNodeExtraction")
    assert "f.probe_ttl + 1              AS probe_ttl" in w
    assert "f.link_addr != bf.dst" in w  # the destination keeps its own append rule
    assert re.search(r"o\.addr = f\.link_addr OR o\.probe_ttl = f\.probe_ttl \+ 1", w)
    assert "MIN(f.far_rtt)               AS rtts" in w


# ------------------------------------------------- literal traces on BigQuery

# Every column the path CTEs carry through, in one order for both sources (the
# merge is a positional UNION ALL).
_COLS = [
    ("src_city", "STRING"), ("src_asn", "INT64"), ("dst_site", "STRING"),
    ("dst_city", "STRING"), ("dst_asn", "INT64"), ("dst_country", "STRING"),
    ("src_country", "STRING"), ("src_asn_name", "STRING"), ("client_name", "STRING"),
    ("src_lat", "FLOAT64"), ("src_state", "STRING"), ("src_lon", "FLOAT64"),
    ("dst_lat", "FLOAT64"), ("dst_lon", "FLOAT64"), ("is_consistent", "BOOL"),
    ("window_start", "TIMESTAMP"), ("total_windows", "INT64"), ("ndt_rtt", "FLOAT64"),
    ("ndt_throughput", "FLOAT64"), ("ndt_loss_rate", "FLOAT64"),
    ("detection_granularity", "STRING"), ("src_group_label", "STRING"),
    ("client_geo_source", "STRING"), ("start_sec", "INT64"),
    ("number_of_measurements_baseline", "FLOAT64"),
    ("number_of_unique_src_ips_baseline", "FLOAT64"),
    ("unique_ip_count_per_site", "FLOAT64"), ("measurement_count_per_site", "FLOAT64"),
    ("baseline_median_rtt", "FLOAT64"), ("baseline_median_throughput", "FLOAT64"),
    ("baseline_median_upload_throughput", "FLOAT64"), ("baseline_median_loss", "FLOAT64"),
    ("anomaly_ratio_rtt", "FLOAT64"), ("anomaly_rtt_count", "INT64"),
    ("anomaly_ratio_throughput", "FLOAT64"), ("anomaly_throughput_count", "INT64"),
    ("anomaly_ratio_upload_throughput", "FLOAT64"),
    ("anomaly_upload_throughput_count", "INT64"), ("anomaly_loss_ratio", "FLOAT64"),
    ("difference_latency", "FLOAT64"), ("difference_throughput", "FLOAT64"),
    ("difference_upload_throughput", "FLOAT64"), ("wasserstein_throughput_result", "FLOAT64"),
    ("wasserstein_upload_throughput_result", "FLOAT64"), ("mann_whitney_latency", "FLOAT64"),
    ("mann_whitney_throughput", "FLOAT64"), ("mann_whitney_upload_throughput", "FLOAT64"),
    ("t_test_latency", "FLOAT64"), ("ip_version", "STRING"),
]  # fmt: skip

# scamper1 traces: id -> links (near, far end, flow, probe TTL, reply RTT); dst 'D'.
_S1 = {
    # stops short of D: R3 answers on R2 -> R3 but has no outgoing link
    "s1_short": [("R1", "R2", 1, 2, 10.0), ("R2", "R3", 1, 3, 20.0)],
    # reaches D, which is a far end only: appended once, after the path
    "s1_reach": [("R1", "R2", 1, 2, 10.0), ("R2", "D", 1, 3, 30.0)],
    "s1_noreply": [],  # no replies at all
    "s1_noreply_s2": [],  # no replies, but a scamper2 trace for the same test
}
# scamper2 traces: id -> hops (addr, real TTL, rtt)
_S2 = {
    # first router at TTL 1, destination answers as a hop
    "s2_reach": [("R1", 1, 1.0), ("R2", 2, 2.0), ("D", 3, 3.0)],
    "s1_noreply_s2": [("Q1", 1, 5.0), ("Q2", 2, 6.0)],
}

# id -> (node_details as (position, addr, rtt), reach_dest, traceroute_rtt)
_EXPECTED = {
    "s1_short": ([(1, "S", 0.0), (2, "R1", None), (3, "R2", 10.0), (4, "R3", 20.0)], False, 20.0),
    "s1_reach": ([(1, "S", 0.0), (2, "R1", None), (3, "R2", 10.0), (4, "D", 30.0)], True, 30.0),
    "s1_noreply": ([(1, "S", 0.0)], False, None),
    "s2_reach": ([(1, "S", 0.0), (2, "R1", 1.0), (3, "R2", 2.0), (4, "D", 3.0)], True, 3.0),
    "s1_noreply_s2": ([(1, "S", 0.0), (2, "Q1", 5.0), (3, "Q2", 6.0)], False, 6.0),
}  # fmt: skip


def _nodes(links) -> str:
    by_near: dict[str, list] = {}
    for near, far, flow, ttl, rtt in links:
        by_near.setdefault(near, []).append((far, flow, ttl, rtt))
    nodes = []
    for addr, items in by_near.items():
        probes = ", ".join(
            f"STRUCT('{far}' AS addr, [STRUCT({flow} AS Flowid, {ttl} AS TTL, "
            f"[STRUCT({rtt} AS RTT, 64 AS TTL)] AS Replies)] AS Probes)"
            for far, flow, ttl, rtt in items
        )
        nodes.append(
            f"STRUCT('{addr}' AS addr, CAST(NULL AS STRING) AS name, "
            f"[STRUCT([{probes}] AS Links)] AS links)"
        )
    if not nodes:  # an empty array of the same element type
        return (
            f"ARRAY(SELECT n FROM UNNEST([{_nodes([('X', 'Y', 1, 2, 1.0)])[1:-1]}]) n WHERE FALSE)"
        )
    return f"[{', '.join(nodes)}]"


def _hops(hops) -> str:
    return (
        "["
        + ", ".join(
            f"STRUCT('{a}' AS addr, CAST(NULL AS STRING) AS name, {r} AS rtt, {t} AS probe_ttl)"
            for a, t, r in hops
        )
        + "]"
    )


def _common(tid: str) -> str:
    cols = ", ".join(f"CAST(NULL AS {t}) AS {c}" for c, t in _COLS)
    return f"'{tid}' AS id, 'S' AS src, 'D' AS dst, {cols}"


def _path_query() -> str:
    sql = _sql()
    start = sql.index("\nScamperFlowHops AS (") + 1
    body = sql[start : sql.index("\nAggregatedData AS (")].rstrip().rstrip(",")
    s1 = " UNION ALL ".join(
        f"SELECT CAST(NULL AS STRING) AS raw, {_nodes(links)} AS nodes, {_common(tid)}"
        for tid, links in _S1.items()
    )
    s2 = " UNION ALL ".join(f"SELECT {_hops(h)} AS hops, {_common(tid)}" for tid, h in _S2.items())
    return (
        f"WITH FilteredData AS ({s1}),\nScamper2FilteredData AS ({s2}),\n{body}\n"
        "SELECT id, reach_dest, traceroute_rtt, expanded_details AS node_details "
        "FROM ExpandedNodeDetails"
    )


@pytest.mark.bq
def test_paths_on_hand_built_traces():
    """Runs step 03's real path CTEs (ScamperFlowHops .. ExpandedNodeDetails) on literal
    scamper1 and scamper2 traces (bills 0 bytes)."""
    from google.cloud import bigquery

    rows = bigquery.Client(project="mlab-collaboration").query(_path_query()).result()
    got = {
        r.id: (
            [(h["ttl"], h["addr"], h["rtts"]) for h in r.node_details],
            r.reach_dest,
            r.traceroute_rtt,
        )
        for r in rows
    }
    assert got == _EXPECTED

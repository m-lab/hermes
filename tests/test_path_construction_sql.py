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

Fixed 2026-10-31:
5. scamper1 hop rows were the link's NEAR node at position = probe TTL. On a
   "compressed" link (node.links has more TTL steps than node.linkc: it crosses an
   unresponsive or per-packet load-balanced hop) the near node was copied onto every
   TTL of the span, its real position became '*', the routers answering inside the
   span were lost, and A..B..A loops were manufactured (18.5% / 3.7% of rows on
   2026-10-14). Hop rows are now the RESPONDERS, at probe TTL + 1;
6. a scamper2 trace with hops = [] produced no row, so its test vanished;
7. a listed Giga school IP running an ordinary client, in a group with no
   anomaly-count row, was dropped before step 04 could write it to the giga subset.
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
    assert "LOGICAL_OR(flow_id IS NOT NULL) AS has_data" in _block(sql, "Scamper1IDs")


def test_hop_rows_are_responders_at_probe_ttl_plus_one():
    hops = _block(_sql(), "ScamperFlowHops")
    assert "probe.TTL + 1        AS probe_ttl" in hops
    assert "link_item.addr       AS addr" in hops
    assert "WHERE link_item.addr != '*'" in hops
    # the near node is no longer copied onto the TTLs of its outgoing link; only the
    # first router (whose TTL-1 reply is not stored) is placed from nodes[]
    assert not re.search(r"(?<![_\w])node\.addr", hops)
    assert "IF(first_row, 2, r.probe_ttl)" in hops
    assert "IF(first_row, fd.nodes[SAFE_OFFSET(0)].addr, r.addr) AS addr" in hops
    assert "r.rn_in_flow = 1 AND r.min_ttl > 2 AND fd.nodes[SAFE_OFFSET(0)].addr != '*'" in hops
    # every responder has its own row, so the far-end-only branch is gone
    w = _block(_sql(), "WithoutDstNodeExtraction")
    assert "f.probe_ttl + 1" not in w and "f.link_addr" not in w


def test_reply_unnests_name_a_reply_field():
    # An UNNEST(probe.Replies) that references no field of the reply makes BigQuery
    # read every reply field: the first draft of this fix billed 274 GiB per day
    # instead of 162 GiB because the first-router branch only counted replies.
    # A second pass over the replies also ran out of memory on a full day, so the
    # replies are unnested exactly once.
    hops = _block(_sql(), "ScamperFlowHops")
    assert len(re.findall(r"UNNEST\(probe\.Replies\)\s+AS replies", hops)) == 1
    assert hops.count("replies.RTT IS NOT NULL") == 1
    assert "ARRAY_LENGTH(probe.Replies)" not in hops


def test_scamper2_trace_without_hops_keeps_a_server_only_placeholder():
    s2 = _block(_sql(), "Scamper2WithoutDstNodeExtraction")
    assert re.search(
        r"WHERE NOT EXISTS \(\s*SELECT 1 FROM UNNEST\(fd\.hops\) AS hop\s*"
        r"WHERE SAFE_CAST\(hop\.probe_ttl AS INT64\) IS NOT NULL",
        s2,
    )
    assert "CAST(NULL AS INT64)                     AS flow_id" in s2
    merged = _block(_sql(), "MergedNodeExpansion")
    # a scamper2 placeholder never doubles a scamper1 one
    assert re.search(
        r"WHERE s1\.id IS NULL\s+OR \(NOT s1\.has_data AND s2\.flow_id IS NOT NULL\)", merged
    )


def test_giga_school_ips_survive_the_group_filter_in_both_branches():
    sql = _sql()
    sql04 = loader.load_query("04_mapping_union.sql", {"DAY": "2026-10-14"})
    school = sql04[sql04.index("SELECT ip_address\n", sql04.index("OR src IN (")) :]
    school = school[
        : school.index("AND ip_address IS NOT NULL") + len("AND ip_address IS NOT NULL")
    ]
    norm = lambda x: " ".join(x.split())  # noqa: E731
    for cte in ("ScamperDataIntermediary", "Scamper2DataIntermediary"):
        block = _block(sql, cte)
        assert "OR ndt.client_ip IN (" in block
        assert norm(school) in norm(block), cte


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


# scamper1 traces in the raw shape of measurement-lab.ndt.scamper1:
#   node = (addr, [step, ...]): node.links[], one entry per TTL step of its outgoing
#          link(s); more steps than linkc means a compressed link
#   step = [(responder addr, [(flow, probe TTL, [reply RTTs])]), ...]: the inner
#          Links[]; '*' = unanswered (no probes)
# A reply to a probe with TTL t comes from the responder, at position t + 1. The
# first node is the first router (TTL 1, reply not stored). dst is 'D'.
def _simple(links):
    """(near, far end, flow, probe TTL, reply RTT) links -> raw nodes, one step each."""
    by_near: dict[str, list] = {}
    for near, far, flow, ttl, rtt in links:
        by_near.setdefault(near, []).append((far, [(flow, ttl, [rtt])]))
    return [(addr, [items]) for addr, items in by_near.items()]


_STAR = ("*", [])
_S1 = {
    # stops short of D: R3 answers on R2 -> R3 but has no outgoing link
    "s1_short": _simple([("R1", "R2", 1, 2, 10.0), ("R2", "R3", 1, 3, 20.0)]),
    # reaches D, which is a far end only: appended once, after the path
    "s1_reach": _simple([("R1", "R2", 1, 2, 10.0), ("R2", "D", 1, 3, 30.0)]),
    "s1_noreply": [],  # no replies at all
    "s1_noreply_s2": [],  # no replies, but a scamper2 trace for the same test
    "s1_noreply_s2empty": [],  # no replies, and a scamper2 trace without hops
    # sv7dl shape (ndt-sv7dl_1791352187_00000000000D26D5, 2026-10-14): B has linkc 1
    # but three steps: TTL 3 unanswered, TTL 4 answered by C (plus '*'), TTL 5 by E.
    # main put B at 4 and 5 and '*' at 3, and lost C.
    "s1_compressed": [
        ("A1", [[("B", [(f, 2, [0.4 + f]) for f in (1, 2, 3)])]]),
        ("B", [
            [_STAR],
            [("C", [(1, 4, [2.6]), (2, 4, [1.5]), (3, 4, [2.4])]), _STAR],
            [("E", [(1, 5, [0.8]), (2, 5, [0.9]), (3, 5, [1.1])])],
        ]),
        ("E", [[("D", [(1, 6, [5.0]), (2, 6, [5.2])])]]),
        ("D", [[_STAR], [_STAR], [_STAR]]),
    ],
    # d846q / mcgtx shape: G's compressed link crosses X (answering at TTL 3, next
    # to an unanswered probe) and an unanswered TTL 4 before H; X is a node too.
    # main copied G onto TTLs 3 and 5 around X: A1 G X G H, an A..B..A loop.
    "s1_loop": [
        ("A1", [[("G", [(1, 2, [1.0])])]]),
        ("G", [
            [("X", [(1, 3, [2.0]), (1, 3, [])]), _STAR],  # one probe answered, one lost
            [_STAR],
            [("H", [(1, 5, [4.0])])],
        ]),
        ("X", [[("Y", [(1, 4, [3.0])])]]),
        ("H", [[_STAR], [_STAR], [_STAR]]),
    ],
    # per-packet load balancing: L and M both answer TTL 2 on flow 1 with the same
    # RTT (lowest address wins), and two replies from L (fastest kept)
    "s1_lb": [
        ("A1", [[("M", [(1, 2, [2.0])]), ("L", [(1, 2, [2.0, 2.5])])]]),
        ("L", [[("D", [(1, 3, [5.0])])]]),
        ("M", [[("D", [(1, 3, [5.0])])]]),
    ],
    # the first hop did not answer (nodes[0] is '*'): position 2 is a gap
    "s1_firststar": [
        ("*", [[("R2", [(1, 2, [1.0])])]]),
        ("R2", [[("D", [(1, 3, [2.0])])]]),
    ],
}  # fmt: skip
# scamper2 traces: id -> hops (addr, real TTL, rtt)
_S2 = {
    # first router at TTL 1, destination answers as a hop
    "s2_reach": [("R1", 1, 1.0), ("R2", 2, 2.0), ("D", 3, 3.0)],
    "s1_noreply_s2": [("Q1", 1, 5.0), ("Q2", 2, 6.0)],
    "s2_empty": [],  # hops = []: the test must survive as a server-only path
    "s1_noreply_s2empty": [],
}

# id -> (node_details as (position, addr, rtt), reach_dest, traceroute_rtt)
_EXPECTED = {
    "s1_short": ([(1, "S", 0.0), (2, "R1", None), (3, "R2", 10.0), (4, "R3", 20.0)], False, 20.0),
    "s1_reach": ([(1, "S", 0.0), (2, "R1", None), (3, "R2", 10.0), (4, "D", 30.0)], True, 30.0),
    "s1_noreply": ([(1, "S", 0.0)], False, None),
    "s2_reach": ([(1, "S", 0.0), (2, "R1", 1.0), (3, "R2", 2.0), (4, "D", 3.0)], True, 3.0),
    "s1_noreply_s2": ([(1, "S", 0.0), (2, "Q1", 5.0), (3, "Q2", 6.0)], False, 6.0),
    "s1_noreply_s2empty": ([(1, "S", 0.0)], False, None),
    "s2_empty": ([(1, "S", 0.0)], False, None),
    "s1_compressed": (
        [(1, "S", 0.0), (2, "A1", None), (3, "B", 1.4), (4, "*", -1.0), (5, "C", 2.6),
         (6, "E", 0.8), (7, "D", 5.0)],
        True, 5.0,
    ),
    "s1_loop": (
        [(1, "S", 0.0), (2, "A1", None), (3, "G", 1.0), (4, "X", 2.0), (5, "Y", 3.0),
         (6, "H", 4.0)],
        False, 4.0,
    ),
    "s1_lb": ([(1, "S", 0.0), (2, "A1", None), (3, "L", 2.0), (4, "D", 5.0)], True, 5.0),
    "s1_firststar": ([(1, "S", 0.0), (2, "*", -1.0), (3, "R2", 1.0), (4, "D", 2.0)], True, 2.0),
}  # fmt: skip

_REPLY = "STRUCT<RTT FLOAT64, TTL INT64>"
_PROBE = f"STRUCT<Flowid INT64, TTL INT64, Replies ARRAY<{_REPLY}>>"
_ITEM = f"STRUCT<addr STRING, Probes ARRAY<{_PROBE}>>"
_STEP = f"STRUCT<Links ARRAY<{_ITEM}>>"
_NODE = f"STRUCT<addr STRING, name STRING, links ARRAY<{_STEP}>>"


def _nodes(nodes) -> str:
    def probe(flow, ttl, rtts):
        replies = ", ".join(f"STRUCT({r} AS RTT, 64 AS TTL)" for r in rtts)
        return f"STRUCT({flow} AS Flowid, {ttl} AS TTL, ARRAY<{_REPLY}>[{replies}] AS Replies)"

    def item(addr, probes):
        ps = ", ".join(probe(*p) for p in probes)
        return f"STRUCT('{addr}' AS addr, ARRAY<{_PROBE}>[{ps}] AS Probes)"

    def node(addr, steps):
        st = ", ".join(f"STRUCT([{', '.join(item(*i) for i in step)}] AS Links)" for step in steps)
        return (
            f"STRUCT('{addr}' AS addr, CAST(NULL AS STRING) AS name, ARRAY<{_STEP}>[{st}] AS links)"
        )

    return f"ARRAY<{_NODE}>[{', '.join(node(*n) for n in nodes)}]"


def _hops(hops) -> str:
    return (
        "ARRAY<STRUCT<addr STRING, name STRING, rtt FLOAT64, probe_ttl INT64>>["
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

    rows = list(bigquery.Client(project="mlab-collaboration").query(_path_query()).result())
    assert sorted(r.id for r in rows) == sorted(_EXPECTED)  # one path per test, none lost
    got = {
        r.id: (
            [(h["ttl"], h["addr"], h["rtts"]) for h in r.node_details],
            r.reach_dest,
            r.traceroute_rtt,
        )
        for r in rows
    }
    assert got == _EXPECTED

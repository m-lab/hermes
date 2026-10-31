"""Step 03 must attach each scamper1 hop's OWN RTT, not the next hop's.

scamper stores probes on links: the probes on a link node -> link_item were answered
by link_item (the far end). Before 2026-10 step 03 keyed the hop on node.addr but
took the RTT from that link, pairing every router with the next one's RTT. Verified
on raw scamper1 (2026-10-14): fixed output 6,210/6,210 router RTTs are replies sent
by that address; the old output 64/8,045.

Since 2026-10-31 a hop row is the replying address itself, at probe TTL + 1, so its
RTT is simply its own reply on that flow at that TTL (the earlier fix matched a
near-node row to a reply from the same address one TTL lower on the same flow; the
result is the same wherever that row sat at its real position).
"""

import re

import pytest

from hermes.sql import loader


def _sql():
    return loader.load_query("03_build_transient_events_union.sql", {"DAY": "2026-10-14"})


def _block(sql, name):
    start = sql.index(f"\n{name} AS (") + 1
    return sql[start : sql.index("\n),", start)]


def test_own_rtt_is_the_responders_reply_at_that_ttl():
    hops = _block(_sql(), "ScamperFlowHops")
    # one row per (flow, position, responder); its RTT is that responder's fastest
    # reply on that flow at that TTL. No cross-flow lookup, no near/far matching.
    assert re.search(
        r"probe\.Flowid\s+AS flow_id,\s+probe\.TTL \+ 1\s+AS probe_ttl,.*\n"
        r"\s+link_item\.addr\s+AS addr,\s+MIN\(replies\.RTT\)\s+AS rtt,",
        hops,
    )
    assert "GROUP BY flow_id, probe_ttl, addr" in hops
    assert ") AS sf" not in hops and "prev_ttl" not in hops  # the old matching is gone
    # the first router's reply (TTL 1) is not stored: NULL, never the next hop's
    assert re.search(r"2\s+AS probe_ttl,(?:.*\n){4}\s+CAST\(NULL AS FLOAT64\)\s+AS rtts,", hops)


def test_far_rtt_is_the_own_reply_and_drives_hop_choice_and_end_to_end_rtt():
    sql = _sql()
    hops = _block(sql, "ScamperFlowHops")
    assert "r.rtt         AS rtts," in hops and "r.rtt         AS far_rtt," in hops
    # traceroute_rtt is the furthest reply's RTT
    assert (
        "FIRST_VALUE(far_rtt) OVER (PARTITION BY w.id ORDER BY probe_ttl DESC, far_rtt ASC)" in sql
    )
    # several responders at one position: fastest reply, then lowest address
    # (ranked in FinalNodeExtraction, kept in NodeExtraction)
    assert re.search(
        r"PARTITION BY ne\.id, ne\.probe_ttl\s*\n\s*ORDER BY ne\.far_rtt ASC, ne\.addr ASC",
        _block(sql, "FinalNodeExtraction"),
    )
    assert "WHERE ttl_rn = 1" in _block(sql, "NodeExtraction")


def test_scamper2_hops_are_not_shifted():
    s2 = _block(_sql(), "Scamper2WithoutDstNodeExtraction")
    assert "SAFE_CAST(hop.rtt AS FLOAT64)         AS rtts" in s2
    assert "SAFE_CAST(hop.rtt AS FLOAT64)         AS far_rtt" in s2


def test_answered_hop_without_its_own_rtt_stays_null_not_minus_one():
    # -1 would read as "faster than light" to step 04's distance check, and 05/06/07
    # drop every measurement with such a hop.
    expanded = _block(_sql(), "ExpandedRows")
    assert "WHEN n.addr IS NULL OR n.addr = '*' THEN -1" in expanded
    assert "COALESCE(n.rtts, -1)" not in expanded


# (near, far end, flow, probe TTL, reply RTT): the probe sent with that TTL was
# answered by the far end, which therefore sits at position TTL + 1 with that RTT.
# R1 (the first node) is the first router: its TTL-1 reply is not stored, so it sits at
# position 2 on every flow with a NULL RTT. Flow 2 loops (R2 answers at TTL 2 and 4).
# Flow 5: X answers at TTL 4 and has two outgoing links (to Y, W). Until 2026-10-31
# rows were the NEAR node at position = probe TTL (R1 at 2, R2 at 3, ...), with the
# own RTT looked up from the link into it; the responder rows below replace that.
_LINKS = [
    ("R1", "R2", 1, 2, 10.0), ("R2", "R3", 1, 3, 20.0), ("R3", "D", 1, 4, 30.0),
    ("R1", "R2", 2, 2, 11.0), ("R2", "R3", 2, 3, 21.0), ("R3", "R2", 2, 4, 35.0),
    ("R2", "D", 2, 5, 40.0),
    ("R3", "D", 3, 4, 31.0),
    ("R5", "R3", 4, 5, 99.0),
    ("P", "X", 5, 4, 4.0), ("X", "Y", 5, 5, 5.0), ("X", "W", 5, 6, 6.5),
    ("Q", "X", 6, 6, 60.0),
    ("R2", "R3", 1, 3, 19.5),  # a second reply from R3 on flow 1 at TTL 3: fastest kept
]  # fmt: skip

# (flow, position, responder) -> (own rtt, far_rtt)
_EXPECTED = {
    **{(f, 2, "R1"): (None, None) for f in (1, 2, 3, 4, 5, 6)},
    (1, 3, "R2"): (10.0, 10.0),
    (1, 4, "R3"): (19.5, 19.5),
    (1, 5, "D"): (30.0, 30.0),
    (2, 3, "R2"): (11.0, 11.0),
    (2, 4, "R3"): (21.0, 21.0),
    (2, 5, "R2"): (35.0, 35.0),  # the loop: R2 really is at 3 and 5 on flow 2
    (2, 6, "D"): (40.0, 40.0),
    (3, 5, "D"): (31.0, 31.0),
    (4, 6, "R3"): (99.0, 99.0),
    (5, 5, "X"): (4.0, 4.0),
    (5, 6, "Y"): (5.0, 5.0),
    (5, 7, "W"): (6.5, 6.5),
    (6, 7, "X"): (60.0, 60.0),
}


def _fixture_nodes() -> str:
    by_near: dict[str, list] = {}
    for near, far, flow, ttl, rtt in _LINKS:
        by_near.setdefault(near, []).append((far, flow, ttl, rtt))
    nodes = []
    for addr, links in by_near.items():
        items = ", ".join(
            f"STRUCT('{far}' AS addr, [STRUCT({flow} AS Flowid, {ttl} AS TTL, "
            f"[STRUCT({rtt} AS RTT, 64 AS TTL)] AS Replies)] AS Probes)"
            for far, flow, ttl, rtt in links
        )
        nodes.append(
            f"STRUCT('{addr}' AS addr, CAST(NULL AS STRING) AS name, "
            f"[STRUCT([{items}] AS Links)] AS links)"
        )
    return ", ".join(nodes)


@pytest.mark.bq
def test_hop_rtts_on_a_hand_built_trace():
    """Runs step 03's actual ScamperFlowHops expression on literal data (bills 0 bytes)."""
    from google.cloud import bigquery

    sql = _sql()
    start = sql.index("ScamperFlowHops AS (")
    expr = sql[
        sql.index("    ARRAY(\n", start) : sql.index("    ) AS hops\n", start) + len("    )")
    ]
    query = (
        f"SELECT h.* FROM (SELECT {expr} AS hops "
        f"FROM (SELECT [{_fixture_nodes()}] AS nodes) AS fd), UNNEST(hops) h"
    )
    rows = bigquery.Client(project="mlab-collaboration").query(query).result()
    got = {(r.flow_id, r.probe_ttl, r.addr): (r.rtts, r.far_rtt) for r in rows}
    assert got == _EXPECTED


def test_distance_check_does_not_pass_a_hop_without_its_own_rtt():
    sql = loader.load_query("04_mapping_union.sql", {"DAY": "2026-10-14"})
    fwd = sql[sql.index("\ndistance_rtt_checks AS (") :]
    check = fwd[: fwd.index("END AS distance_rtt_check")]
    assert "WHEN rtts IS NULL THEN NULL" in check

"""Step 03 must attach each scamper1 hop's OWN RTT, not the next hop's.

scamper stores probes on links: the probes on a link node -> link_item were answered
by link_item (the far end). Before 2026-10 step 03 keyed the hop on node.addr but
took the RTT from that link, pairing every router with the next one's RTT. Verified
on raw scamper1 (2026-10-14): fixed output 6,210/6,210 router RTTs are replies sent
by that address; the old output 64/8,045.
"""

import re

import pytest

from hermes.sql import loader


def _sql():
    return loader.load_query("03_build_transient_events_union.sql", {"DAY": "2026-10-14"})


def _block(sql, name):
    start = sql.index(f"\n{name} AS (") + 1
    return sql[start : sql.index("\n),", start)]


def test_own_rtt_comes_from_links_whose_far_end_is_the_hop():
    hops = _block(_sql(), "ScamperFlowHops")
    # same flow only, keyed on the link's far end, and bounded to the TTLs between the
    # previous answered position and this one, so a repeated address cannot reuse
    # the reply of an earlier occurrence
    assert re.search(
        r"ON sf\.flow_id = h\.flow_id\s+AND sf\.addr = h\.addr\s+"
        r"AND sf\.probe_ttl < h\.probe_ttl\s+AND sf\.probe_ttl >= IFNULL\(h\.prev_ttl, 0\)",
        hops,
    )
    assert "RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING" in hops
    assert ") AS af" not in hops and "af.rtt" not in hops  # no cross-flow fallback
    assert "ARRAY_AGG(sf.rtt IGNORE NULLS ORDER BY sf.probe_ttl DESC LIMIT 1)" in hops


def test_previous_value_kept_only_for_hop_choice_and_end_to_end_rtt():
    sql = _sql()
    assert "MIN(replies.RTT)             AS far_rtt" in _block(sql, "ScamperFlowHops")
    # traceroute_rtt is the furthest reply's RTT, as before the fix
    assert (
        "FIRST_VALUE(far_rtt) OVER (PARTITION BY w.id ORDER BY probe_ttl DESC, far_rtt ASC)" in sql
    )
    # the hop kept per TTL is chosen exactly as before, so paths do not change
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


# (near, far end, flow, probe TTL, reply RTT). TTL-1 probes are never stored, so R1
# has no own RTT. Flow 2 loops (R2 at TTL 2 and 4); flow 3 has no reply from R3 on
# its own flow (other flows are not used); flow 4 sees R3 again at TTL 5 on another
# arm. Flow 5: X answers at TTL 4 and sits at positions 5 and 6 (two outgoing links);
# its position-6 occurrence has no reply of its own on flow 5 (flow 6 sees X at TTL 6).
_LINKS = [
    ("R1", "R2", 1, 2, 10.0), ("R2", "R3", 1, 3, 20.0), ("R3", "D", 1, 4, 30.0),
    ("R1", "R2", 2, 2, 11.0), ("R2", "R3", 2, 3, 21.0), ("R3", "R2", 2, 4, 35.0),
    ("R2", "D", 2, 5, 40.0),
    ("R3", "D", 3, 4, 31.0),
    ("R5", "R3", 4, 5, 99.0),
    ("P", "X", 5, 4, 4.0), ("X", "Y", 5, 5, 5.0), ("X", "W", 5, 6, 6.5),
    ("Q", "X", 6, 6, 60.0),
]  # fmt: skip

# (flow, row probe_ttl, near addr) -> (own rtt, far_rtt)
_EXPECTED = {
    (1, 2, "R1"): (None, 10.0),
    (1, 3, "R2"): (10.0, 20.0),
    (1, 4, "R3"): (20.0, 30.0),
    (2, 2, "R1"): (None, 11.0),
    (2, 3, "R2"): (11.0, 21.0),
    (2, 4, "R3"): (21.0, 35.0),
    (2, 5, "R2"): (35.0, 40.0),  # the loop's TTL-4 reply, not the earlier 11
    (3, 4, "R3"): (None, 31.0),  # no reply from R3 on flow 3; other flows not used
    (4, 5, "R5"): (None, 99.0),  # never a far end
    (5, 4, "P"): (None, 4.0),
    (5, 5, "X"): (4.0, 5.0),
    (5, 6, "X"): (None, 6.5),  # must not reuse the position-5 occurrence's TTL-4 reply
    (6, 6, "Q"): (None, 60.0),
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

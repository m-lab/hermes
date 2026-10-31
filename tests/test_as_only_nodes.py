"""'<asn>-unknown' nodes are AS-level evidence only (fix #12).

Step 04 nulls the location of a hop whose location was refuted (or never known)
but keeps its AS "for AS-level analysis". Step 06 used to turn such a hop into '*',
so the AS-level cover lost that AS on ~7% of anomalous measurements. 06 now names
it '<asn>-unknown'; because that label can merge several routers of the AS, the
covers may use it only for AS candidates -- never node, metro, IXP or edge.
"""

from __future__ import annotations

import pandas as pd

from hermes.pipeline import correlation_tomography as ct
from hermes.pipeline.edge_format import edge_has_unknown_location, is_unknown_location

COLS = ["id", "src_dst_pair", "information_source", "path_type", "canonical_edge"]


def test_unknown_location_helpers():
    assert is_unknown_location("500-unknown")
    assert not is_unknown_location("500-Unknownville-Texas-US")
    assert not is_unknown_location("*")
    assert edge_has_unknown_location("174-Miami-Florida-US - 500-unknown")
    assert edge_has_unknown_location("500-unknown - 174-Miami-Florida-US")
    assert not edge_has_unknown_location("174-Miami-Florida-US - 500-Dallas-Texas-US")


def _edges() -> pd.DataFrame:
    """AS 500 is the culprit. Pairs p0-p3 cross it at two located routers; p4-p5
    cross it only at hops step 04 could not locate. Healthy traffic avoids it."""
    rows = []
    via = {
        "p0": "500-Dallas-Texas-US",
        "p1": "500-Dallas-Texas-US",
        "p2": "500-Denver-Colorado-US",
        "p3": "500-Denver-Colorado-US",
        "p4": "500-unknown",
        "p5": "500-unknown",
    }
    for pair, hop in via.items():
        for t in range(3):
            tid = f"{pair}-{t}"
            for e in (f"174-Miami-Florida-US - {hop}", f"{hop} - 3356-Chicago-Illinois-US"):
                rows.append((tid, pair, "forward", "anomalous", e))
    for k in range(40):
        tid = f"h{k}"
        for e in ("174-Miami-Florida-US - 7018-Atlanta-Georgia-US",
                  "7018-Atlanta-Georgia-US - 3356-Chicago-Illinois-US"):  # fmt: skip
            rows.append((tid, f"q{k}", "forward", "non_anomalous", e))
    return pd.DataFrame(rows, columns=COLS)


def _all_edges() -> pd.DataFrame:
    return pd.DataFrame(columns=["from_asn_metro", "from_ixp", "to_asn_metro", "to_ixp"])


def test_as_only_node_counts_at_as_level_and_nowhere_else():
    culprits, stats = ct.run_mixed_granularity_cover(_edges(), _all_edges(), "2026-10-14")
    st = {(s["granularity"], s["entity"]): s for s in stats}
    # AS level: all 18 anomalous measurements, incl. the 6 seen only as '500-unknown'
    assert st[("AS", "500")]["support_anomalous"] == 18
    # never a node, metro, IXP or edge entity
    for (gran, ent), _ in st.items():
        if gran != "AS":
            assert "unknown" not in ent, (gran, ent)
    assert ("node", "500-Dallas-Texas-US") in st and ("node", "500-unknown") not in st
    assert ("metro", "unknown") not in st
    # the AS is the culprit and explains the unlocated-only pairs too
    (c,) = [c for c in culprits if c["granularity"] == "AS"]
    assert c["entity"] == "500"
    assert set(c["anomalous_src_dst_pairs_impacted"]) == {f"p{k}" for k in range(6)}
    for c in culprits:
        assert c["granularity"] == "AS" or "unknown" not in c["entity"], c


def test_as_seen_only_unlocated_is_not_promoted_by_the_distinctness_guard():
    """No located node vouches for it, so the coarse AS pick is dropped (conservative)."""
    e = _edges()
    e = e[e["src_dst_pair"].isin(["p4", "p5"]) | (e["path_type"] == "non_anomalous")]
    culprits, stats = ct.run_mixed_granularity_cover(e, _all_edges(), "2026-10-14")
    assert any(s["granularity"] == "AS" and s["entity"] == "500" for s in stats)
    assert not any("500" in c["entity"] for c in culprits)


def test_greedy_set_cover_never_picks_an_edge_with_an_unknown_endpoint():
    e = _edges().assign(is_interdomain="interdomain")
    e["edge"] = e["canonical_edge"]
    culprits, total = ct.run_greedy_set_cover(e, day_str="2026-10-14")
    assert total == 6
    picked = {c["canonical_edge"] for c in culprits}
    assert picked  # the located 500 edges are still found
    assert not any(edge_has_unknown_location(e) or "unknown" in e for e in picked), picked
    assert "174-Miami-Florida-US - 500-Dallas-Texas-US" in picked

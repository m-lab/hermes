"""Edge strings whose place names contain the ' - ' node delimiter (fix #7).

Real culprit edges from hermes_staging (Oct 2026). Before the fix, 06 split them at
the first ' - ' and stored e.g. to_asn = 'Vũng Tàu'; 07 then dropped the attribution.
"""

from __future__ import annotations

import pandas as pd
import pytest

from hermes.pipeline import correlation_tomography as ct
from hermes.pipeline import temporal_verdict as tv
from hermes.pipeline.edge_format import node_asn, split_edge

VN = "7552-Ho Chi Minh City-Hồ Chí Minh city-VN"
VUNG_TAU = "45899-Vung Tau-Bà Rịa - Vũng Tàu-VN"
KENITRA = "36903-Kenitra-Gharb - Chrarda - Béni Hssen-MA"
RABAT = "6713-Rabat-Rabat - Salé - Zemmour - Zaer-MA"
GOOSE_BAY = "855-Happy Valley - Goose Bay-Newfoundland and Labrador-CA"
MONCTON = "855-Moncton-New Brunswick-CA"


@pytest.mark.parametrize(
    "a, b",
    [
        (VN, VUNG_TAU),
        (VUNG_TAU, VN),
        (KENITRA, RABAT),
        (RABAT, KENITRA),
        (MONCTON, GOOSE_BAY),
        ("4766-Busan-26-KR", "4766-Seoul-11-KR"),  # numeric region codes
        ("*", "3356-Miami-Florida-US"),
        ("3356-Miami-Florida-US", "*"),
        ("100-M1-Region1-C1", "700"),  # bare-ASN node (synthetic phase-4 fixture)
    ],
)
def test_split_edge_finds_the_node_boundary(a, b):
    assert split_edge(f"{a} - {b}") == (a, b)


def test_split_edge_rejects_non_edges():
    assert split_edge(None) is None
    assert split_edge(float("nan")) is None
    assert split_edge("3356-Miami-Florida-US") is None
    assert split_edge("Bà Rịa - Vũng Tàu") is None


def test_node_asn():
    assert node_asn(VUNG_TAU) == "45899"
    assert node_asn("*") == "*"


# --------------------------------------------------------- compute_hyperedges


class _InsertClient:
    def __init__(self):
        self.rows: list[dict] = []

    def insert_rows_json(self, table, rows):
        self.rows.extend(rows)
        return []


def _all_edges(pairs):
    rows = []
    for a, b in pairs:
        fa, fb = a.split("-"), b.split("-")
        rows.append(
            {
                "from_asn": fa[0],
                "from_metro": fa[1].strip(),
                "from_asn_metro": f"{fa[0]}-{fa[1].strip()}",
                "from_ixp": "None",
                "to_asn": fb[0],
                "to_metro": fb[1].strip(),
                "to_asn_metro": f"{fb[0]}-{fb[1].strip()}",
                "to_ixp": "None",
            }
        )
    return pd.DataFrame(rows)


def _culprit(edge: str) -> dict:
    keys = [
        "day",
        "information_source",
        "is_interdomain",
        "src_dst_pairs_impacted",
        "anomalous_src_dst_pairs_impacted",
        "paths",
        "max_fraction_anomalous",
        "max_fraction_src_dst_pair_anomalous",
        "max_fraction_non_anomalous",
        "ratio_anomaly",
        "max_fraction_src_dst_pair_non_anomalous",
        "fraction_anomalous_paths",
        "partition_date",
        "iteration_number",
        "anomalies_explained_by_edge",
        "fraction_anomalies_explained_by_edge",
        "cumulative_anomalies_explained",
        "cumulative_fraction_anomalies_explained_so_far",
    ]
    row = {k: None for k in keys}
    row.update(canonical_edge=edge, day="2026-10-14", partition_date="2026-10-14", paths=[])
    return row


def test_compute_hyperedges_parses_region_names_with_the_delimiter():
    client = _InsertClient()
    edges = [f"{VN} - {VUNG_TAU}", f"{KENITRA} - {RABAT}", f"{MONCTON} - {GOOSE_BAY}"]
    ct.compute_hyperedges(
        client,
        [_culprit(e) for e in edges],
        "2026-10-14",
        all_edges=_all_edges([(VN, VUNG_TAU), (KENITRA, RABAT), (MONCTON, GOOSE_BAY)]),
    )
    got = {r["edge_asn_metro"]: r for r in client.rows}
    assert got[edges[0]]["from_asn"] == "7552" and got[edges[0]]["to_asn"] == "45899"
    assert got[edges[0]]["right_part"] == VUNG_TAU
    assert got[edges[0]]["to_asn_metro"] == "45899-Vung Tau"
    assert got[edges[1]]["from_asn"] == "36903" and got[edges[1]]["to_asn"] == "6713"
    assert got[edges[1]]["left_part"] == KENITRA and got[edges[1]]["right_part"] == RABAT
    assert got[edges[2]]["to_asn"] == "855" and got[edges[2]]["to_metro"] == "Happy Valley"
    for r in client.rows:
        assert r["from_asn"].isdigit() and r["to_asn"].isdigit(), r
        # the to-node totals are found again (key parsed the same way as all_edges)
        assert r["to_total_edges_asn"] >= 1


# --------------------------------------------------------- mixed-granularity cover


def test_mixed_cover_nodes_are_whole_nodes():
    rows = []
    for k in range(6):  # anomalous: through the Vietnamese and Moroccan edges
        for e in (f"{VN} - {VUNG_TAU}", f"{KENITRA} - {RABAT}"):
            rows.append((f"a{k}", f"p{k}", "forward", "anomalous", e))
    for k in range(6):  # healthy: elsewhere
        rows.append((f"h{k}", f"q{k}", "forward", "non_anomalous", "3356-A-B-US - 174-C-D-US"))
    edges = pd.DataFrame(
        rows, columns=["id", "src_dst_pair", "information_source", "path_type", "canonical_edge"]
    )
    all_edges = pd.DataFrame(columns=["from_asn_metro", "from_ixp", "to_asn_metro", "to_ixp"])
    _, stats = ct.run_mixed_granularity_cover(edges, all_edges, "2026-10-14")
    by_gran: dict[str, set[str]] = {}
    for s in stats:
        by_gran.setdefault(s["granularity"], set()).add(s["entity"])
    assert {VN, VUNG_TAU, KENITRA, RABAT} <= by_gran["node"]
    assert by_gran["AS"] >= {"7552", "45899", "36903", "6713"}
    assert all(a.isdigit() for a in by_gran["AS"]), by_gran["AS"]
    assert "Vũng Tàu-VN" not in by_gran["node"] | by_gran["metro"]
    assert "Bà Rịa - Vũng Tàu-VN" not in by_gran["metro"]  # metro = location after the ASN
    assert "Vung Tau-Bà Rịa - Vũng Tàu-VN" in by_gran["metro"]


# --------------------------------------------------------- temporal verdict coupling


class _Job:
    def __init__(self, rows):
        self._rows = rows

    def result(self):
        return self._rows


class _HyperedgeClient:
    def __init__(self, rows):
        self.rows = rows

    def query(self, sql):
        return _Job(self.rows)


def test_temporal_culprit_edges_keep_region_delimiter_edges():
    rows = [{"pairs": ["P"], "edge_asn_metro": f"{VN} - {VUNG_TAU}"}]
    got = tv._culprit_edges(_HyperedgeClient(rows), "2026-10-14")
    assert got == {"P": {f"{VN}-{VUNG_TAU}", f"{VUNG_TAU}-{VN}"}}

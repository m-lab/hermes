"""The temporal verdict (05) and the correlation culprits (06) must share one key
and one node naming, or `agrees_with_culprit` is silently FALSE everywhere.

The expressions are read out of the SQL files and evaluated here, so neither the
05 nor the 06 format is hard-coded in the tests.
"""

import re

import pandas as pd

from hermes.pipeline import temporal_verdict as tv
from hermes.sql import paths

PREV = "05_temporal_edge_prevalences_union.sql"
PREPARE = "06_correlation_tomography_prepare_union.sql"
HOPS = "06_correlation_tomography_unexplained_hops_union.sql"

_PAIR_RE = re.compile(r"(CONCAT\([^()]*\))\s+AS\s+src_dst_pair", re.S)
_NODE_RE = re.compile(r"(IFNULL\(CONCAT\(\w+\.associated_asn,.*?\), '\*'\))", re.S)


def _sql(name: str) -> str:
    return paths.query_path(name).read_text()


def _norm(expr: str) -> str:
    """Drop table aliases (fr., n., fwd., rwd.) and whitespace differences."""
    expr = re.sub(r"\b(?:fr|n|fwd|rwd)\.", "", expr)
    return re.sub(r"\s+", " ", expr).strip()


def _pair_exprs(name: str) -> set[str]:
    return {_norm(e) for e in _PAIR_RE.findall(_sql(name))}


def _node_exprs(name: str) -> set[str]:
    return {_norm(e) for e in _NODE_RE.findall(_sql(name))}


def _split_args(inner: str) -> list[str]:
    out, depth, cur, quoted = [], 0, "", False
    for ch in inner:
        if ch == "'":
            quoted = not quoted
        if not quoted and ch == "(":
            depth += 1
        if not quoted and ch == ")":
            depth -= 1
        if ch == "," and depth == 0 and not quoted:
            out.append(cur.strip())
            cur = ""
        else:
            cur += ch
    out.append(cur.strip())
    return out


def _eval(expr: str, row: dict):
    """Evaluate the small SQL subset the key/node expressions use."""
    expr = expr.strip()
    if expr.startswith("'"):
        return expr[1:-1]
    m = re.fullmatch(r"(\w+)\((.*)\)", expr, re.S)
    if m:
        fn, args = m.group(1).upper(), [_eval(a, row) for a in _split_args(m.group(2))]
        if fn == "CONCAT":
            return None if any(a is None for a in args) else "".join(str(a) for a in args)
        if fn == "COALESCE":
            return next((a for a in args if a is not None), None)
        if fn == "IFNULL":
            return args[0] if args[0] is not None else args[1]
        raise AssertionError(f"unsupported function {fn}")
    return row[expr.split(".")[-1]]


def _key(name: str, row: dict) -> str:
    (expr,) = _pair_exprs(name)
    return _eval(expr, row)


def _node(name: str, hop: dict) -> str:
    (expr,) = _node_exprs(name)
    return _eval(expr, hop)


# --- string-level contract --------------------------------------------------


def test_05_and_06_pair_key_expressions_are_identical():
    prev, prep, hops = _pair_exprs(PREV), _pair_exprs(PREPARE), _pair_exprs(HOPS)
    assert len(prev) == 1 and len(prep) == 1 and len(hops) == 1
    assert prev == prep == hops
    assert "ip_version" in next(iter(prev))


def test_05_and_06_node_expressions_are_identical():
    prev, prep, hops = _node_exprs(PREV), _node_exprs(PREPARE), _node_exprs(HOPS)
    assert len(prev) == 1 and len(prep) == 1 and len(hops) == 1
    assert prev == prep == hops
    # 05 must join the same canonical-metro table 06 does
    join = re.compile(r"place_canonical_metro`\s+al\s+ON\s+al\.place\s*=\s*\w+\.place")
    assert len(join.findall(_sql(PREV))) == 2  # forward + reverse
    assert len(join.findall(_sql(PREPARE))) == 2


# --- end-to-end through compute_temporal_verdicts ---------------------------

GROUP = {"src_asn": "10000", "src_group_label": "Sasebo-Nagasaki-JP", "dst_site": "icn03"}
# metro (lat/lon-derived) disagrees with the place string on purpose: 06 prefers metro
HOP_A = {
    "associated_asn": "3257",
    "metro": "Miami-Florida-US",
    "canon_metro": None,
    "place": "Miami-FL-US",
}
HOP_B = {
    "associated_asn": "174",
    "metro": "Miami-Florida-US",
    "canon_metro": None,
    "place": "Miami-FL-US",
}
HOP_C = {
    "associated_asn": "174",
    "metro": "Atlanta-Georgia-US",
    "canon_metro": None,
    "place": "Atlanta-GA-US",
}


class _Job:
    def __init__(self, rows):
        self._rows = rows

    def result(self):
        return self._rows


class _Client:
    """Serves correlation_hyperedges_tomography_v2 rows to the real _culprit_edges."""

    def __init__(self, rows):
        self.rows = rows

    def query(self, sql):
        assert "correlation_hyperedges_tomography_v2" in sql
        return _Job(self.rows)


def _culprit_row(ip_version: str, hop_x: dict, hop_y: dict) -> dict:
    """A hyperedge row as 06 + compute_hyperedges write it."""
    pair = _key(PREPARE, {**GROUP, "ip_version": ip_version})
    a, b = _node(PREPARE, hop_x), _node(PREPARE, hop_y)
    # canonical_edge orders the two nodes by ASN string (prepare SQL)
    lo, hi = sorted([a, b], key=lambda n: n.split("-")[0])
    return {"pairs": [pair], "edge_asn_metro": f"{lo} - {hi}"}


def _verdict_df(ip_version: str) -> pd.DataFrame:
    """05 prevalence rows for one group: forward route moves A-B -> A-C (reroute)."""
    pair = _key(PREV, {**GROUP, "ip_version": ip_version})
    a, b, c = (_node(PREV, h) for h in (HOP_A, HOP_B, HOP_C))
    common = dict(
        src_dst_pair=pair,
        ip_version=ip_version,
        direction="forward",
        healthy_n=20,
        dayof_n=20,
        base_hop_rtt=10,
        day_hop_rtt=10,
    )
    return pd.DataFrame(
        [
            {**common, "edge": f"{a}-{b}", "prev_u": 1.0, "prev_d": 0.0},
            {**common, "edge": f"{a}-{c}", "prev_u": 0.0, "prev_d": 1.0},
        ]
    )


def _run(monkeypatch, df, culprit_rows):
    monkeypatch.setattr(tv, "_read_prevalences", lambda c, d, **kw: df)
    return tv.compute_temporal_verdicts(_Client(culprit_rows), "2026-10-14")


def test_agrees_when_culprit_is_the_changed_edge(monkeypatch):
    (row,) = _run(monkeypatch, _verdict_df("v4"), [_culprit_row("v4", HOP_C, HOP_A)])
    assert row["verdict"] == "reroute"
    assert row["changed_segment"] == "3257-Miami-Florida-US-174-Atlanta-Georgia-US"
    assert row["agrees_with_culprit"] is True
    # the stored column keeps its 3-field form; ip_version has its own column
    assert row["src_dst_pair"] == "10000 - Sasebo-Nagasaki-JP - icn03"
    assert row["ip_version"] == "v4"


def test_disagrees_when_culprit_is_another_edge(monkeypatch):
    (row,) = _run(monkeypatch, _verdict_df("v4"), [_culprit_row("v4", HOP_A, HOP_B)])
    assert row["agrees_with_culprit"] is False


def test_disagrees_when_culprit_is_for_the_other_ip_version(monkeypatch):
    (row,) = _run(monkeypatch, _verdict_df("v4"), [_culprit_row("v6", HOP_C, HOP_A)])
    assert row["agrees_with_culprit"] is False

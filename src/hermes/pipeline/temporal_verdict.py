"""Reroute-vs-congestion verdict math for temporal tomography (pure, testable).

U = the group's usual (healthy) route edge prevalences; D = its day-of anomalous
route edge prevalences. Divergence is the share of day-of traffic that moved onto
edges the usual route did not use.
"""


def divergence(prev_u: dict[str, float], prev_d: dict[str, float]) -> float:
    edges = set(prev_u) | set(prev_d)
    return float(sum(max(0.0, prev_d.get(e, 0.0) - prev_u.get(e, 0.0)) for e in edges))


def label_edges(
    prev_u: dict[str, float], prev_d: dict[str, float], delta: float = 0.5
) -> dict[str, str]:
    out: dict[str, str] = {}
    for e in set(prev_u) | set(prev_d):
        shift = prev_d.get(e, 0.0) - prev_u.get(e, 0.0)
        out[e] = "diverted" if shift >= delta else "abandoned" if shift <= -delta else "stable"
    return out


def classify(
    div_forward: float,
    div_reverse: float,
    healthy_n: int,
    dayof_n: int,
    tau: float = 0.3,
    s_min: int = 5,
) -> tuple[str, str | None]:
    if healthy_n < s_min or dayof_n < s_min:
        return ("indeterminate", None)
    change_dir = "forward" if div_forward >= div_reverse else "reverse"
    if max(div_forward, div_reverse) >= tau:
        return ("reroute", change_dir)
    return ("congestion_in_place", change_dir)


# ---------------------------------------------------------------------------
# Orchestration: per-group verdict computation + correlation-culprit coupling
# ---------------------------------------------------------------------------

from hermes.pipeline.correlation_tomography import _read_df  # noqa: E402
from hermes.sql import loader  # noqa: E402

_PREV_SQL = "05_temporal_edge_prevalences_union.sql"
_V2 = "mlab-collaboration.hermes_union.correlation_hyperedges_tomography_v2"


def _read_prevalences(client, day_str: str):
    return _read_df(client.query(loader.load_query(_PREV_SQL, {"DAY": day_str})))


def _culprit_edges(client, day_str: str) -> dict[str, set[str]]:
    """Return src_dst_pair -> set of culprit edge strings, in the SAME
    `<full-node>-<full-node>` format the prevalence SQL emits.

    Build from the full `edge_asn_metro` ("nodeA - nodeB", full metros) rather than
    `from_asn_metro`/`to_asn_metro`, which truncate the place to its first hyphen
    component ("2907-Tokyo") and therefore never match the prevalence SQL's full-metro
    nodes ("2907-Tokyo-Tokyo-JP"). Emit both directed orderings. Edges whose metro
    contains the " - " node delimiter (rare region names) can't be split and are
    skipped.
    """
    sql = (
        f"SELECT anomalous_src_dst_pairs_impacted AS pairs, edge_asn_metro "
        f"FROM `{_V2}` WHERE partition_date = '{day_str}'"
    )
    out: dict[str, set[str]] = {}
    for r in client.query(sql).result():
        parts = (r["edge_asn_metro"] or "").split(" - ")
        if len(parts) != 2:
            continue
        a, b = parts[0].strip(), parts[1].strip()
        for p in r["pairs"] or []:
            out.setdefault(p, set()).update((f"{a}-{b}", f"{b}-{a}"))
    return out


def compute_temporal_verdicts(
    client, day_str: str, tau: float = 1.0, delta: float = 0.5, s_min: int = 5
) -> list[dict]:
    # tau=1.0 validated on staged days (2026-06-13..19): divergence is unbounded
    # "new-edge mass" (median ~0.6, p90 ~3.3), so tau=1.0 (~one full new edge) gives a
    # balanced reroute/congestion split; tau=0.3 over-calls reroute.
    df = _read_prevalences(client, day_str)
    if df is None or len(df) == 0:
        return []
    culprits = _culprit_edges(client, day_str)
    rows: list[dict] = []
    for (pair, ipv), g in df.groupby(["src_dst_pair", "ip_version"]):
        per_dir: dict[str, tuple] = {}
        for d in ("forward", "reverse"):
            sub = g[g["direction"] == d]
            u = {r.edge: (r.prev_u or 0.0) for r in sub.itertuples()}
            dd = {r.edge: (r.prev_d or 0.0) for r in sub.itertuples()}
            per_dir[d] = (u, dd, sub)
        div_f = divergence(*per_dir["forward"][:2])
        div_r = divergence(*per_dir["reverse"][:2])
        healthy_n = int(g["healthy_n"].max() or 0)
        dayof_n = int(g["dayof_n"].max() or 0)
        verdict, change_dir = classify(div_f, div_r, healthy_n, dayof_n, tau, s_min)
        changed_segment = congested_segment = None
        if change_dir is not None:
            u, dd, sub = per_dir[change_dir]
            if verdict == "reroute":
                # the most-diverted edge on the changed direction (largest positive
                # prev_d-prev_u). Don't require the per-edge `delta` label: a diffuse
                # reroute spread over several edges can have div>=tau while no single
                # edge crosses delta, yet we still want to name the dominant new edge.
                cand = [
                    (dd.get(e, 0.0) - u.get(e, 0.0), e)
                    for e in dd
                    if dd.get(e, 0.0) - u.get(e, 0.0) > 0
                ]
                changed_segment = max(cand)[1] if cand else None
            elif verdict == "congestion_in_place":
                labels = label_edges(u, dd, delta)
                stable = sub[sub["edge"].map(lambda e: labels.get(e) == "stable")]  # noqa: B023
                stable = stable.assign(
                    jump=stable["day_hop_rtt"].fillna(0) - stable["base_hop_rtt"].fillna(0)
                )
                congested_segment = (
                    stable.sort_values("jump", ascending=False)["edge"].iloc[0]
                    if len(stable)
                    else None
                )
        seg = changed_segment or congested_segment
        agrees = bool(seg is not None and seg in culprits.get(pair, set()))
        rows.append(
            {
                "partition_date": day_str,
                "src_dst_pair": pair,
                "ip_version": ipv,
                "verdict": verdict,
                "change_dir": change_dir,
                "div_forward": round(div_f, 4),
                "div_reverse": round(div_r, 4),
                "changed_segment": changed_segment,
                "congested_segment": congested_segment,
                "agrees_with_culprit": agrees,
            }
        )
    return rows


_VERDICTS = "mlab-collaboration.hermes_union.temporal_path_verdicts"


def write_verdicts(client, rows: list[dict]) -> None:
    """Stream per-pair verdicts to temporal_path_verdicts."""
    if not rows:
        return
    errors = client.insert_rows_json(_VERDICTS, rows)
    if errors:
        raise RuntimeError(f"temporal_path_verdicts insert failed: {errors[:3]}")

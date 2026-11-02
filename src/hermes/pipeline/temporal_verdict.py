"""Reroute-vs-congestion verdict math for temporal tomography (pure, testable).

U = the group's usual (healthy) route edge prevalences; D = its day-of anomalous
route edge prevalences. Divergence is the share of day-of traffic that moved onto
edges the usual route did not use.
"""


def divergence(prev_u: dict[str, float], prev_d: dict[str, float]) -> float:
    edges = sorted(set(prev_u) | set(prev_d))
    return float(sum(max(0.0, prev_d.get(e, 0.0) - prev_u.get(e, 0.0)) for e in edges))


def label_edges(
    prev_u: dict[str, float], prev_d: dict[str, float], delta: float = 0.5
) -> dict[str, str]:
    out: dict[str, str] = {}
    for e in sorted(set(prev_u) | set(prev_d)):
        shift = prev_d.get(e, 0.0) - prev_u.get(e, 0.0)
        out[e] = "diverted" if shift >= delta else "abandoned" if shift <= -delta else "stable"
    return out


_DIRECTIONS = ("forward", "reverse")


def classify_directions(
    div: dict[str, float],
    support: dict[str, tuple[int, int]],
    tau: float = 0.3,
    s_min: int = 5,
) -> tuple[str, str | None]:
    """Verdict from the directions that have enough support of their OWN.

    ``support[d]`` is ``(healthy_n, dayof_n)`` for direction ``d``. A direction
    decides only if both counts are >= ``s_min``; its divergence is never judged
    against another direction's sample sizes. No qualifying direction ->
    ``('indeterminate', None)``; one -> that direction alone; both -> the larger
    divergence (forward on a tie), as before.
    """
    ok = [d for d in _DIRECTIONS if min(support.get(d, (0, 0))) >= s_min]
    if not ok:
        return ("indeterminate", None)
    change_dir = ok[0] if len(ok) == 1 else max(ok, key=lambda d: (div[d], d == "forward"))
    if div[change_dir] >= tau:
        return ("reroute", change_dir)
    return ("congestion_in_place", change_dir)


def classify(
    div_forward: float,
    div_reverse: float,
    healthy_n: int,
    dayof_n: int,
    tau: float = 0.3,
    s_min: int = 5,
) -> tuple[str, str | None]:
    """:func:`classify_directions` with the same support for both directions."""
    n = (healthy_n, dayof_n)
    return classify_directions(
        {"forward": div_forward, "reverse": div_reverse}, {"forward": n, "reverse": n}, tau, s_min
    )


def _count(sub, col: str) -> int:
    """A direction's healthy_n / dayof_n (one value per direction; 0 if absent)."""
    v = sub[col].max() if len(sub) else None
    return int(v) if v is not None and v == v else 0


# ---------------------------------------------------------------------------
# Orchestration: per-group verdict computation + correlation-culprit coupling
# ---------------------------------------------------------------------------

from hermes.pipeline.correlation_tomography import (  # noqa: E402
    DEFAULT_DATASET,
    PROJECT_ID,
    _dataset_table,
    _load_dataset_query,
    _read_df,
)
from hermes.pipeline.edge_format import split_edge  # noqa: E402

_PREV_SQL = "05_temporal_edge_prevalences_union.sql"


def _stored_pair(key: str, ip_version) -> str:
    """The `temporal_path_verdicts.src_dst_pair` value for a 05/06 group key.

    The prevalence SQL emits the same 4-field key as the correlation SQL
    (`asn - group - site - ip_version`) so culprits can be looked up by it. The
    table keeps its established 3-field form plus a separate `ip_version` column,
    which the dashboard joins on, so the ip_version suffix is stripped on write.
    """
    suffix = f" - {ip_version}"
    return key[: -len(suffix)] if ip_version is not None and key.endswith(suffix) else key


def _read_prevalences(
    client,
    day_str: str,
    *,
    project_id: str = PROJECT_ID,
    dataset: str = DEFAULT_DATASET,
):
    sql = _load_dataset_query(_PREV_SQL, {"DAY": day_str}, project_id=project_id, dataset=dataset)
    return _read_df(client.query(sql))


def _culprit_edges(
    client,
    day_str: str,
    *,
    project_id: str = PROJECT_ID,
    dataset: str = DEFAULT_DATASET,
) -> dict[str, set[str]]:
    """Return src_dst_pair -> set of culprit edge strings, in the SAME
    `<full-node>-<full-node>` format the prevalence SQL emits.

    Keys are the correlation SQL's 4-field `asn - group - site - ip_version`
    strings, which the prevalence SQL reproduces byte for byte. Matching ignores
    direction and `information_source`: both orderings are emitted, so a culprit
    found on the reverse path also matches the same AS-metro edge on the forward
    path.

    Build from the full `edge_asn_metro` ("nodeA - nodeB", full metros) rather than
    `from_asn_metro`/`to_asn_metro`, which truncate the place to its first hyphen
    component ("2907-Tokyo") and therefore never match the prevalence SQL's full-metro
    nodes ("2907-Tokyo-Tokyo-JP"). Emit both directed orderings. Region names may
    contain the " - " node delimiter ("Bà Rịa - Vũng Tàu"); split_edge splits only
    at the boundary that precedes the second node's ASN, so those edges match too.
    """
    table = _dataset_table(
        "correlation_hyperedges_tomography_v2", project_id=project_id, dataset=dataset
    )
    sql = (
        f"SELECT anomalous_src_dst_pairs_impacted AS pairs, edge_asn_metro "
        f"FROM `{table}` WHERE partition_date = '{day_str}'"
    )
    out: dict[str, set[str]] = {}
    for r in client.query(sql).result():
        parts = split_edge(r["edge_asn_metro"])
        if parts is None:
            continue
        a, b = parts
        for p in r["pairs"] or []:
            out.setdefault(p, set()).update((f"{a}-{b}", f"{b}-{a}"))
    return out


def compute_temporal_verdicts(
    client,
    day_str: str,
    tau: float = 1.0,
    delta: float = 0.5,
    s_min: int = 5,
    *,
    project_id: str = PROJECT_ID,
    dataset: str = DEFAULT_DATASET,
) -> list[dict]:
    # tau=1.0 validated on staged days (2026-06-13..19): divergence is unbounded
    # "new-edge mass" (median ~0.6, p90 ~3.3), so tau=1.0 (~one full new edge) gives a
    # balanced reroute/congestion split; tau=0.3 over-calls reroute.
    df = _read_prevalences(client, day_str, project_id=project_id, dataset=dataset)
    if df is None or len(df) == 0:
        return []
    culprits = _culprit_edges(client, day_str, project_id=project_id, dataset=dataset)
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
        # Support per direction. Taking max() across directions let a direction
        # with 1 healthy measurement decide on the other direction's 20.
        support = {
            d: (_count(per_dir[d][2], "healthy_n"), _count(per_dir[d][2], "dayof_n"))
            for d in _DIRECTIONS
        }
        verdict, change_dir = classify_directions(
            {"forward": div_f, "reverse": div_r}, support, tau, s_min
        )
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
                # A jump needs an RTT on both sides. Filling a missing side with 0
                # made an edge with no baseline RTT "jump" by its whole day-of RTT,
                # so it tended to be named the congested segment.
                stable = stable.dropna(subset=["day_hop_rtt", "base_hop_rtt"])
                stable = stable.assign(jump=stable["day_hop_rtt"] - stable["base_hop_rtt"])
                congested_segment = (
                    stable.sort_values(["jump", "edge"], ascending=[False, True], kind="mergesort")[
                        "edge"
                    ].iloc[0]
                    if len(stable)
                    else None
                )
        seg = changed_segment or congested_segment
        agrees = bool(seg is not None and seg in culprits.get(pair, set()))
        rows.append(
            {
                "partition_date": day_str,
                "src_dst_pair": _stored_pair(pair, ipv),
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


def write_verdicts(
    client,
    rows: list[dict],
    *,
    project_id: str = PROJECT_ID,
    dataset: str = DEFAULT_DATASET,
) -> None:
    """Stream per-pair verdicts to temporal_path_verdicts."""
    if not rows:
        return
    table = _dataset_table("temporal_path_verdicts", project_id=project_id, dataset=dataset)
    errors = client.insert_rows_json(table, rows)
    if errors:
        raise RuntimeError(f"temporal_path_verdicts insert failed: {errors[:3]}")


def verdicts_exist(
    client,
    day_str: str,
    *,
    project_id: str = PROJECT_ID,
    dataset: str = DEFAULT_DATASET,
) -> bool:
    """True if temporal_path_verdicts already has rows for day_str."""
    from hermes.pipeline.correlation_tomography import step_already_done

    table = _dataset_table("temporal_path_verdicts", project_id=project_id, dataset=dataset)
    return step_already_done(client, table, day_str)

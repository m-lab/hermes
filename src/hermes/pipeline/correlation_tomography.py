"""
Hybrid Python+SQL correlation tomography.

Phase 1 (SQL): Extract edges from events → download to pandas
Phase 2 (Python): Greedy set-cover loop
Phase 3 (SQL): Download all_edges_per_node → compute hyperedges in Python → upload

No intermediate BigQuery tables — only reads from events_with_as_and_geoloc
and writes to correlation_hyperedges_tomography_v2.

Usage:
    python correlation_tomography.py --date 2026-05-15
    python correlation_tomography.py --start-date 2026-05-14 --end-date 2026-05-16
    python correlation_tomography.py --date 2026-05-15 --max-iterations 50
"""

import argparse
import datetime as _dt
import logging
import math
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from google.cloud import bigquery
from scipy.stats import fisher_exact

from hermes.sql import loader

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

PROJECT_ID = "mlab-collaboration"


def _read_df(query_job) -> pd.DataFrame:
    """Materialize a query job to a DataFrame via the Arrow-based BigQuery Storage
    Read API, which is far faster than the REST/paginated path for the multi-million
    row edge/hop downloads. Falls back to REST if the storage client is unavailable.
    """
    try:
        from google.cloud import bigquery_storage  # type: ignore[attr-defined]

        return query_job.to_dataframe(bqstorage_client=bigquery_storage.BigQueryReadClient())
    except Exception as exc:  # pragma: no cover - REST fallback path
        logger.warning("  Storage Read API unavailable (%s); using REST download", exc)
        return query_job.to_dataframe()


# Confidence-tier cutoffs (Phase 1 defaults; tune in Phase 4 validation).
# A culprit is only "strong" when it is statistically significant AND the effect
# size is meaningful (odds_ratio): with large N, tiny over-representations (odds~1.2)
# are highly significant but are not real bottlenecks, so p-value alone is not enough.
_STRONG_P = 0.01
_STRONG_MIN_SUPPORT = 3
_STRONG_MIN_ODDS = 2.0


def edge_significance(
    anom_through: int, anom_total: int, healthy_through: int, healthy_total: int
) -> tuple[float, float]:
    """One-sided Fisher's exact that an edge is over-represented on anomalous paths.

    Returns (p_value, odds_ratio). odds_ratio may be math.inf when healthy_through == 0.
    """
    a = anom_through
    b = max(anom_total - anom_through, 0)
    c = healthy_through
    d = max(healthy_total - healthy_through, 0)
    odds, p = fisher_exact([[a, b], [c, d]], alternative="greater")
    return float(p), float(odds)


def confidence_tier(
    p_value: float | None, support_anom: int, method: str, odds_ratio: float | None = None
) -> str:
    """Map significance + support + effect size + method to a display tier.

    "strong" requires statistical significance (p), adequate support, AND a
    meaningful effect size (odds_ratio >= _STRONG_MIN_ODDS) so that large-N but
    barely-over-represented edges are not over-trusted. odds_ratio may be inf
    (perfectly discriminative: zero healthy paths), which counts as strong.
    """
    if method == "path_local":
        return "path_local"
    if (
        p_value is not None
        and p_value <= _STRONG_P
        and support_anom >= _STRONG_MIN_SUPPORT
        and odds_ratio is not None
        and odds_ratio >= _STRONG_MIN_ODDS
    ):
        return "strong"
    return "weak"


def _sanitize_for_json(obj):
    """Replace NaN/Inf floats with None recursively — BigQuery rejects NaN in JSON."""
    if isinstance(obj, float):
        return None if (math.isnan(obj) or math.isinf(obj)) else obj
    if isinstance(obj, dict):
        return {k: _sanitize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize_for_json(v) for v in obj]
    return obj


OUTPUT_TABLE = f"{PROJECT_ID}.hermes_union.correlation_hyperedges_tomography_v2"


def attribute_unexplained(
    client, day_str: str, explained_pairs: set[str], anomalous_pairs: set[str]
) -> list[dict]:
    """Path-local culprits for ANOMALOUS src_dst_pairs not covered by set-cover.

    `anomalous_pairs` bounds attribution to the day's anomalous groups only — without
    it, the hops query (which carries every measurement's path) would emit a culprit
    for tens of thousands of non-anomalous pairs.
    """
    from hermes.pipeline.path_local_attribution import localize_on_path

    targets = anomalous_pairs - explained_pairs
    if not targets:
        return []
    sql = loader.load_query(
        "06_correlation_tomography_unexplained_hops_union.sql", {"DAY": day_str}
    )
    hops_df = _read_df(client.query(sql))
    if hops_df.empty:
        return []
    rows: list[dict] = []
    for sdp, g in hops_df.groupby("src_dst_pair"):
        if sdp not in targets:
            continue
        # prefer forward; fall back to reverse
        for src in ("forward", "reverse"):
            hops = g[g["information_source"] == src].to_dict("records")
            seg = localize_on_path(hops)
            if seg:
                rows.append(
                    _sanitize_for_json(
                        {
                            "canonical_edge": f"{seg['from_node']} - {seg['to_node']}",
                            "day": day_str,
                            "partition_date": day_str,
                            "information_source": src,
                            "is_interdomain": "undetermined",
                            "attribution_method": "path_local",
                            "confidence_tier": "path_local",
                            "reason": seg["reason"],
                            "anomalous_src_dst_pairs_impacted": [sdp],
                            "src_dst_pairs_impacted": [sdp],
                            "anomalies_explained_by_edge": 1,
                            "paths": [],
                        }
                    )
                )
                break
    return rows


def step_already_done(client: bigquery.Client, table_name: str, day_str: str) -> bool:
    """Return ``True`` if *table_name* already has rows for *day_str*.

    Parameters
    ----------
    client
        BigQuery client to use for the check query.
    table_name
        Fully-qualified BigQuery table name.
    day_str
        Date string in ``YYYY-MM-DD`` format.

    Returns
    -------
    bool
        ``True`` when at least one row exists for the given date.
    """
    query = f"SELECT 1 FROM `{table_name}` WHERE DATE(partition_date) = '{day_str}' LIMIT 1"
    return client.query(query).result().total_rows > 0


# =========================================================================
# Phase 1: Extract edges via SQL, download to pandas
# =========================================================================


def download_edges(client: bigquery.Client, day_str: str) -> pd.DataFrame:
    """Run the prepare SQL script and download the resulting edge rows.

    Parameters
    ----------
    client
        BigQuery client.
    day_str
        Date string in ``YYYY-MM-DD`` format; substituted as ``${DAY}``
        in ``06_correlation_tomography_prepare_union.sql``.

    Returns
    -------
    pandas.DataFrame
        Edge rows with columns including ``edge``, ``information_source``,
        ``canonical_edge``, ``is_interdomain``, ``path_type``, ``src_dst_pair``,
        and ``id``.
    """
    logger.info("  Phase 1: extracting edges from BigQuery...")
    params: dict[str, object] = {"DAY": day_str}
    sql = loader.load_query("06_correlation_tomography_prepare_union.sql", params)
    df = _read_df(client.query(sql))
    logger.info(f"  Downloaded {len(df):,} edge rows")
    return df


# =========================================================================
# Phase 2: Greedy set-cover in pandas
# =========================================================================


def run_greedy_set_cover(
    edges_df: pd.DataFrame,
    day_str: str | None = None,
    max_iterations: int = 200,
    no_progress_limit: int = 5,
    min_marginal_gain: float = 0.01,
    marginal_window: int = 10,
    alpha: float = 0.01,
    purity_threshold: float = 0.0,
    min_support: int = 2,
    min_odds: float = 1.5,
) -> tuple[list[dict], int]:
    """Run the iterative greedy set-cover loop entirely in memory.

    Selects the edge that explains the greatest number of unexplained anomalous
    ``src_dst_pair`` groups at each iteration, stopping early when one of
    several convergence criteria is met.

    Parameters
    ----------
    edges_df
        Edge rows as returned by :func:`download_edges`.
    day_str
        Date label embedded in culprit rows (``YYYY-MM-DD``).
    max_iterations
        Hard upper bound on the number of iterations.
    no_progress_limit
        Stop after this many consecutive iterations with zero new anomalies
        explained.
    min_marginal_gain
        Stop when the last ``marginal_window`` iterations together explained
        less than this fraction of total anomalies.
    marginal_window
        Number of recent iterations used for the marginal-gain check.
    alpha
        Fisher's-exact p-value threshold; candidate edges with ``p > alpha``
        are excluded from selection.
    purity_threshold
        Minimum ``fraction_anomalous_paths`` required to pass the HAVING gate
        (0.0 means no purity floor).
    min_support
        Minimum number of anomalous-path observations required for a candidate
        edge (replaces the hard-coded ``anom_edge_count >= 2`` filter).

    Returns
    -------
    culprit_rows : list of dict
        One dict per selected culprit edge, ready for upload to BigQuery.
    total_anomalies : int
        Total number of anomalous ``src_dst_pair`` groups found in the input.
    """

    # Derive anomalies from edges (src_dst_pairs with any anomalous edge)
    all_anomalies = set(edges_df.loc[edges_df["path_type"] == "anomalous", "src_dst_pair"].unique())
    total_anomalies = len(all_anomalies)
    logger.info(f"  {total_anomalies} anomalous src_dst_pairs")

    if total_anomalies == 0 or len(edges_df) == 0:
        logger.info("  No anomalies or edges — skipping loop")
        return [], total_anomalies

    explained: set[str] = set()
    culprit_rows: list[dict] = []
    cumulative_explained = 0
    cumul_fraction = 0.0
    no_progress_count = 0
    recent_gains: list[int] = []  # track last N iterations' gains for marginal check

    # Pre-build a mapping: for each (edge, information_source), which src_dst_pairs
    # are anomalous? This avoids recomputing per iteration.
    # Also pre-build edge→canonical_edge and edge→is_interdomain lookups.
    edge_meta = (
        edges_df.groupby(["edge", "information_source"])
        .agg(
            canonical_edge=("canonical_edge", "first"),
            is_interdomain=("is_interdomain", "first"),
        )
        .reset_index()
    )
    edge_meta_dict = {
        (r["edge"], r["information_source"]): (r["canonical_edge"], r["is_interdomain"])
        for _, r in edge_meta.iterrows()
    }

    # Pre-build edge → set of src_dst_pairs per path_type
    # This is the expensive part — do it ONCE, then filter per iteration.
    logger.info("  Pre-building edge→src_dst_pair index...")
    edge_sdp_anom = (
        edges_df[edges_df["path_type"] == "anomalous"]
        .groupby(["edge", "information_source"])["src_dst_pair"]
        .apply(set)
        .to_dict()
    )
    edge_sdp_non = (
        edges_df[edges_df["path_type"] == "non_anomalous"]
        .groupby(["edge", "information_source"])["src_dst_pair"]
        .apply(set)
        .to_dict()
    )

    # Pre-count: edge → count per (path_type, edge, info_source)
    _edge_id_counts = (
        edges_df.groupby(["path_type", "edge", "information_source"])["id"]
        .agg(["count", "nunique"])
        .rename(columns={"count": "edge_count", "nunique": "unique_ids"})
        .reset_index()
    )

    logger.info("  Index built. Starting iterations...")

    for iteration in range(2, max_iterations + 1):
        remaining_anomalies = all_anomalies - explained
        if not remaining_anomalies:
            logger.info(f"  Iteration {iteration}: all anomalies explained")
            break

        # --- Recompute counts excluding explained src_dst_pairs ---
        # Filter the pre-built sets rather than the full DataFrame
        remaining_mask = edges_df["src_dst_pair"].isin(explained)

        # Fast totals per path_type (excluding explained)
        active_ids_anom = edges_df.loc[
            (~remaining_mask) & (edges_df["path_type"] == "anomalous"), "id"
        ].nunique()
        active_ids_non = edges_df.loc[
            (~remaining_mask) & (edges_df["path_type"] == "non_anomalous"), "id"
        ].nunique()
        active_sdps_anom = len(
            set(
                edges_df.loc[
                    (~remaining_mask) & (edges_df["path_type"] == "anomalous"), "src_dst_pair"
                ].unique()
            )
        )
        active_sdps_non = len(
            set(
                edges_df.loc[
                    (~remaining_mask) & (edges_df["path_type"] == "non_anomalous"), "src_dst_pair"
                ].unique()
            )
        )

        if active_ids_anom == 0:
            break

        # Edge counts excluding explained (vectorized groupby)
        active = edges_df[~remaining_mask]
        ec = (
            active.groupby(["path_type", "edge", "information_source"])
            .agg(edge_count=("id", "size"), sdps=("src_dst_pair", "nunique"))
            .reset_index()
        )

        # Fractions
        ec["total_paths"] = np.where(
            ec["path_type"] == "anomalous", active_ids_anom, active_ids_non
        )
        ec["total_sdps"] = np.where(
            ec["path_type"] == "anomalous", active_sdps_anom, active_sdps_non
        )
        ec["fraction"] = ec["edge_count"] / ec["total_paths"]
        ec["fraction_src_dst_pair"] = ec["sdps"] / ec["total_sdps"]

        # Pivot anomalous vs non-anomalous per (edge, information_source)
        anom = ec[ec["path_type"] == "anomalous"].set_index(["edge", "information_source"])
        non_anom = ec[ec["path_type"] == "non_anomalous"].set_index(["edge", "information_source"])

        # Build candidates from anomalous edges
        candidates = anom[["edge_count", "fraction", "fraction_src_dst_pair", "sdps"]].copy()
        candidates.columns = ["anom_edge_count", "anom_fraction", "anom_frac_sdp", "anom_sdps"]

        # Join non-anomalous
        if len(non_anom):
            non_cols = non_anom[["edge_count", "fraction", "fraction_src_dst_pair"]].copy()
            non_cols.columns = ["non_edge_count", "non_fraction", "non_frac_sdp"]
            candidates = candidates.join(non_cols, how="left")
        else:
            candidates["non_edge_count"] = 0
            candidates["non_fraction"] = 0.0
            candidates["non_frac_sdp"] = 0.0
        candidates = candidates.fillna({"non_edge_count": 0, "non_fraction": 0, "non_frac_sdp": 0})

        candidates["total_edge_count"] = (
            candidates["anom_edge_count"] + candidates["non_edge_count"]
        )
        candidates["fraction_anomalous_paths"] = (
            candidates["anom_edge_count"] / candidates["total_edge_count"]
        )
        candidates["ratio_anomaly"] = np.where(
            candidates["non_fraction"] > 0,
            candidates["anom_fraction"] / candidates["non_fraction"],
            float("inf"),
        )

        # Count anomalies explained per edge (using pre-built sets, minus explained)
        candidates = candidates.reset_index()
        anomalies_explained = []
        all_sdps_impacted = []
        anom_sdps_impacted = []
        for _, r in candidates.iterrows():
            key = (r["edge"], r["information_source"])
            # All src_dst_pairs for this edge (anom + non-anom), minus explained
            a_sdps = edge_sdp_anom.get(key, set()) - explained
            n_sdps = edge_sdp_non.get(key, set()) - explained
            all_sdps = a_sdps | n_sdps
            anom_in_remaining = a_sdps & remaining_anomalies
            anomalies_explained.append(len(anom_in_remaining))
            all_sdps_impacted.append(list(all_sdps))
            anom_sdps_impacted.append(list(anom_in_remaining))

        candidates["anomalies_explained_by_edge"] = anomalies_explained
        candidates["src_dst_pairs_impacted"] = all_sdps_impacted
        candidates["anomalous_src_dst_pairs_impacted"] = anom_sdps_impacted

        # Add metadata
        candidates["canonical_edge"] = candidates.apply(
            lambda r: edge_meta_dict.get((r["edge"], r["information_source"]), ("", ""))[0], axis=1
        )
        candidates["is_interdomain"] = candidates.apply(
            lambda r: edge_meta_dict.get((r["edge"], r["information_source"]), ("", ""))[1], axis=1
        )

        # --- significance per candidate (Fisher's exact on the 2x2) ---
        candidates["support_anomalous"] = candidates["anom_edge_count"]
        candidates["support_healthy"] = candidates["non_edge_count"]
        # Path-level 2x2: (paths through edge) vs (all paths), anomalous vs healthy.
        # active_ids_anom / active_ids_non are already computed above this block.
        _anom = int(active_ids_anom)
        _non = int(active_ids_non)
        sig = candidates.apply(
            lambda r, _a=_anom, _n=_non: edge_significance(
                int(r["anom_edge_count"]), _a, int(r["non_edge_count"]), _n
            ),
            axis=1,
            result_type="expand",
        )
        candidates["p_value"] = sig[0]
        candidates["odds_ratio"] = sig[1]

        # HAVING (parameterized): minimum support, optional purity floor,
        # significance gate, a discriminative effect-size floor (odds_ratio) so
        # near-1.0 edges that merely happen to be common are not attributed, and
        # exclude unresolved (*) hops.
        candidates = candidates[
            (candidates["anom_edge_count"] >= min_support)
            & (candidates["fraction_anomalous_paths"] >= purity_threshold)
            & (candidates["p_value"] <= alpha)
            & (candidates["odds_ratio"] >= min_odds)
            & (~candidates["edge"].str.contains(r"\*", na=False))
        ]
        if len(candidates) == 0:
            break

        # Rank coverage-first, significance as tie-break
        candidates = candidates.sort_values(
            ["anomalies_explained_by_edge", "p_value", "edge"], ascending=[False, True, True]
        )
        top_n = max(1, int(100 / iteration) + 1)
        top = candidates.head(top_n)

        # Update explained set
        new_pairs = set()
        for pairs in top["anomalous_src_dst_pairs_impacted"]:
            new_pairs.update(pairs)
        new_explained = len(new_pairs - explained)
        explained.update(new_pairs)
        cumulative_explained += new_explained
        cumul_fraction = cumulative_explained / total_anomalies

        # Collect culprit rows (only for the top edges — small number)
        for _, row in top.iterrows():
            # Build the paths array for this edge
            key = (row["edge"], row["information_source"])
            paths_list = []
            for pt in ["anomalous", "non_anomalous"]:
                ec_row = ec[
                    (ec["edge"] == row["edge"])
                    & (ec["information_source"] == row["information_source"])
                    & (ec["path_type"] == pt)
                ]
                if len(ec_row):
                    r2 = ec_row.iloc[0]
                    paths_list.append(
                        {
                            "path_type": pt,
                            "edge_count": int(r2["edge_count"]),
                            "fraction": float(r2["fraction"]),
                            "total_paths": int(r2["total_paths"]),
                            "fraction_src_dst_pair": float(r2["fraction_src_dst_pair"]),
                            "total_src_dst_pairs_in_window": int(r2["total_sdps"]),
                        }
                    )

            culprit_rows.append(
                _sanitize_for_json(
                    {
                        "edge": row["edge"],
                        "canonical_edge": row["canonical_edge"],
                        "day": day_str,
                        "information_source": row["information_source"],
                        "is_interdomain": row["is_interdomain"],
                        "src_dst_pairs_impacted": row["src_dst_pairs_impacted"],
                        "anomalous_src_dst_pairs_impacted": row["anomalous_src_dst_pairs_impacted"],
                        "paths": paths_list,
                        "max_fraction_anomalous": float(row["anom_fraction"]),
                        "max_fraction_src_dst_pair_anomalous": float(row["anom_frac_sdp"]),
                        "max_fraction_non_anomalous": float(row["non_fraction"]),
                        "ratio_anomaly": float(row["ratio_anomaly"]),
                        "max_fraction_src_dst_pair_non_anomalous": float(row["non_frac_sdp"]),
                        "fraction_anomalous_paths": float(row["fraction_anomalous_paths"]),
                        "partition_date": day_str,
                        "iteration_number": iteration,
                        "anomalies_explained_by_edge": int(row["anomalies_explained_by_edge"]),
                        "fraction_anomalies_explained_by_edge": (
                            row["anomalies_explained_by_edge"] / len(remaining_anomalies)
                            if remaining_anomalies
                            else 0
                        ),
                        "cumulative_anomalies_explained": cumulative_explained,
                        "cumulative_fraction_anomalies_explained_so_far": cumul_fraction,
                        "attribution_method": "correlation",
                        "p_value": float(row["p_value"]),
                        "odds_ratio": (
                            None if row["odds_ratio"] == float("inf") else float(row["odds_ratio"])
                        ),
                        "support_anomalous": int(row["support_anomalous"]),
                        "support_healthy": int(row["support_healthy"]),
                        "confidence_tier": confidence_tier(
                            float(row["p_value"]),
                            int(row["support_anomalous"]),
                            "correlation",
                            float(row["odds_ratio"]),
                        ),
                    }
                )
            )

        logger.info(
            f"  Iter {iteration:>3}: +{new_explained} explained, "
            f"cumul={cumulative_explained}/{total_anomalies} ({cumul_fraction:.3f}), "
            f"{len(top)} edges selected"
        )

        # Stopping criteria
        if new_explained == 0:
            no_progress_count += 1
        else:
            no_progress_count = 0

        recent_gains.append(new_explained)

        if no_progress_count >= no_progress_limit:
            logger.info(f"  Stopping: no progress for {no_progress_limit} iterations")
            break
        if cumul_fraction >= 0.95:
            logger.info("  Stopping: 95% anomalies explained")
            break
        if len(recent_gains) >= marginal_window:
            window_gain = sum(recent_gains[-marginal_window:])
            window_frac = window_gain / total_anomalies if total_anomalies else 0
            if window_frac < min_marginal_gain:
                logger.info(
                    f"  Stopping: last {marginal_window} iterations explained "
                    f"{window_gain} anomalies ({window_frac:.4f} of total) "
                    f"< {min_marginal_gain:.3f} threshold"
                )
                break

    logger.info(
        f"  Set-cover done: {len(culprit_rows)} culprit edges, "
        f"{cumul_fraction:.3f} fraction explained"
    )
    return culprit_rows, total_anomalies


# =========================================================================
# Phase 3: Download all_edges_per_node, compute hyperedges, upload
# =========================================================================


def compute_hyperedges(
    client: bigquery.Client,
    culprit_rows: list[dict],
    day_str: str,
    all_edges: pd.DataFrame | None = None,
) -> None:
    """Download all edges, compute node-level culprit fractions, and upload.

    Phase 3 of the correlation tomography pipeline:

    1. Downloads the ``all_edges_per_node`` view via
       ``06_correlation_tomography_all_edges_union.sql``.
    2. Computes per-node culprit fractions at ASN-metro, ASN, and metro
       granularities.
    3. Uploads the final hyperedge rows to :data:`OUTPUT_TABLE`.

    Parameters
    ----------
    client
        BigQuery client.
    culprit_rows
        Culprit edge dicts as returned by :func:`run_greedy_set_cover`.
    day_str
        Date string in ``YYYY-MM-DD`` format; used as the ``${DAY}``
        parameter and embedded in every uploaded row.
    """
    if not culprit_rows:
        logger.info("  No culprits — skipping hyperedge computation")
        return

    # Download all_edges_per_node (unless a preloaded frame was passed in)
    if all_edges is None:
        logger.info("  Phase 3: downloading all_edges_per_node...")
        sql = loader.load_query("06_correlation_tomography_all_edges_union.sql", {"DAY": day_str})
        all_edges = _read_df(client.query(sql))
        logger.info(f"  Downloaded {len(all_edges):,} node-edge rows")

    # --- Node counts at 3 granularities ---
    def node_counts(df, from_col, to_col, name):
        frm = df.groupby(from_col).size().reset_index(name="edge_count")
        frm.columns = [name, "edge_count"]
        to = df.groupby(to_col).size().reset_index(name="edge_count")
        to.columns = [name, "edge_count"]
        return pd.concat([frm, to]).groupby(name)["edge_count"].sum().reset_index()

    all_nc_am = node_counts(all_edges, "from_asn_metro", "to_asn_metro", "node_asn_metro")
    all_nc_asn = node_counts(all_edges, "from_asn", "to_asn", "node_asn")
    all_nc_metro = node_counts(all_edges, "from_metro", "to_metro", "node_metro")
    # IXP granularity: associated_ixp per endpoint ('None' when the hop is not at an
    # IXP). Drop 'None' so the placeholder isn't treated as a node.
    all_nc_ixp = node_counts(all_edges, "from_ixp", "to_ixp", "node_ixp")
    all_nc_ixp = all_nc_ixp[all_nc_ixp["node_ixp"] != "None"]

    # Map each ⟨AS,metro⟩ node to its IXP (most common non-'None' association) so the
    # set-cover culprits — which are keyed on ⟨AS,metro⟩ — can be attributed to an IXP.
    _ixp_pairs = pd.concat(
        [
            all_edges[["from_asn_metro", "from_ixp"]].rename(
                columns={"from_asn_metro": "asn_metro", "from_ixp": "ixp"}
            ),
            all_edges[["to_asn_metro", "to_ixp"]].rename(
                columns={"to_asn_metro": "asn_metro", "to_ixp": "ixp"}
            ),
        ]
    )
    _ixp_pairs = _ixp_pairs[_ixp_pairs["ixp"] != "None"]
    asn_metro_to_ixp = (
        _ixp_pairs.groupby("asn_metro")["ixp"].agg(lambda s: s.value_counts().idxmax()).to_dict()
        if len(_ixp_pairs)
        else {}
    )

    # --- Parse culprit edges into from/to ---
    culprits_df = pd.DataFrame(culprit_rows)
    parts = culprits_df["canonical_edge"].str.split(" - ", n=1, expand=True)
    culprits_df["left_part"] = parts[0]
    culprits_df["right_part"] = parts[1]
    culprits_df["from_asn"] = culprits_df["left_part"].str.split("-").str[0].str.strip()
    culprits_df["from_metro"] = culprits_df["left_part"].str.split("-").str[1].str.strip()
    culprits_df["from_asn_metro"] = culprits_df["from_asn"] + "-" + culprits_df["from_metro"]
    culprits_df["to_asn"] = culprits_df["right_part"].str.split("-").str[0].str.strip()
    culprits_df["to_metro"] = culprits_df["right_part"].str.split("-").str[1].str.strip()
    culprits_df["to_asn_metro"] = culprits_df["to_asn"] + "-" + culprits_df["to_metro"]
    culprits_df["from_ixp"] = culprits_df["from_asn_metro"].map(asn_metro_to_ixp).fillna("None")
    culprits_df["to_ixp"] = culprits_df["to_asn_metro"].map(asn_metro_to_ixp).fillna("None")

    # --- Culprit node counts ---
    culp_nc_am = node_counts(culprits_df, "from_asn_metro", "to_asn_metro", "node_asn_metro")
    culp_nc_asn = node_counts(culprits_df, "from_asn", "to_asn", "node_asn")
    culp_nc_metro = node_counts(culprits_df, "from_metro", "to_metro", "node_metro")
    culp_nc_ixp = node_counts(culprits_df, "from_ixp", "to_ixp", "node_ixp")
    culp_nc_ixp = culp_nc_ixp[culp_nc_ixp["node_ixp"] != "None"]

    # --- Joined fractions ---
    def join_fractions(all_nc, culp_nc, key, prefix):
        merged = all_nc.merge(culp_nc, on=key, how="left", suffixes=("_total", "_culprit"))
        merged["edge_count_culprit"] = merged["edge_count_culprit"].fillna(0).astype(int)
        merged[f"fraction_culprit_{prefix}"] = np.where(
            merged["edge_count_total"] > 0,
            merged["edge_count_culprit"] / merged["edge_count_total"],
            0,
        )
        merged.rename(
            columns={
                "edge_count_total": f"total_edges_for_node_{prefix}",
                "edge_count_culprit": f"culprit_edges_for_node_{prefix}",
            },
            inplace=True,
        )
        return merged

    j_am = join_fractions(all_nc_am, culp_nc_am, "node_asn_metro", "asn_metro")
    j_am["node_asn"] = j_am["node_asn_metro"].str.split("-").str[0]
    j_am["node_metro"] = j_am["node_asn_metro"].str.split("-").str[1]

    j_asn = join_fractions(all_nc_asn, culp_nc_asn, "node_asn", "asn")
    j_metro = join_fractions(all_nc_metro, culp_nc_metro, "node_metro", "metro")
    j_ixp = join_fractions(all_nc_ixp, culp_nc_ixp, "node_ixp", "ixp")

    # --- Build final rows (one per culprit edge) ---
    output_rows = []
    for _, c in culprits_df.iterrows():
        row = {
            "edge_asn_metro": c["canonical_edge"],
            **{
                k: c[k]
                for k in [
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
                    "left_part",
                    "right_part",
                    "from_asn",
                    "from_metro",
                    "from_asn_metro",
                    "from_ixp",
                    "to_asn",
                    "to_metro",
                    "to_asn_metro",
                    "to_ixp",
                ]
            },
            **{
                k: c.get(k)
                for k in [
                    "attribution_method",
                    "confidence_tier",
                    "p_value",
                    "odds_ratio",
                    "support_anomalous",
                    "support_healthy",
                    "reason",
                ]
            },
        }

        # From-node stats
        for _prefix, j_df, _key_col, key_val in [
            ("from", j_am, "node_asn_metro", c["from_asn_metro"]),
        ]:
            match = j_df[j_df["node_asn_metro"] == key_val]
            if len(match):
                m = match.iloc[0]
                row["from_total_edges_asn_metro"] = int(m["total_edges_for_node_asn_metro"])
                row["from_culprit_edges_asn_metro"] = int(m["culprit_edges_for_node_asn_metro"])
                row["from_fraction_culprit_asn_metro"] = m["fraction_culprit_asn_metro"]

        match_asn = j_asn[j_asn["node_asn"] == c["from_asn"]]
        if len(match_asn):
            m = match_asn.iloc[0]
            row["from_total_edges_asn"] = int(m["total_edges_for_node_asn"])
            row["from_culprit_edges_asn"] = int(m["culprit_edges_for_node_asn"])
            row["from_fraction_culprit_asn"] = m["fraction_culprit_asn"]

        match_metro = j_metro[j_metro["node_metro"] == c["from_metro"]]
        if len(match_metro):
            m = match_metro.iloc[0]
            row["from_total_edges_metro"] = int(m["total_edges_for_node_metro"])
            row["from_culprit_edges_metro"] = int(m["culprit_edges_for_node_metro"])
            row["from_fraction_culprit_metro"] = m["fraction_culprit_metro"]

        if c["from_ixp"] != "None":
            match_ixp = j_ixp[j_ixp["node_ixp"] == c["from_ixp"]]
            if len(match_ixp):
                m = match_ixp.iloc[0]
                row["from_total_edges_ixp"] = int(m["total_edges_for_node_ixp"])
                row["from_culprit_edges_ixp"] = int(m["culprit_edges_for_node_ixp"])
                row["from_fraction_culprit_ixp"] = m["fraction_culprit_ixp"]

        # To-node stats
        match_am = j_am[j_am["node_asn_metro"] == c["to_asn_metro"]]
        if len(match_am):
            m = match_am.iloc[0]
            row["to_total_edges_asn_metro"] = int(m["total_edges_for_node_asn_metro"])
            row["to_culprit_edges_asn_metro"] = int(m["culprit_edges_for_node_asn_metro"])
            row["to_fraction_culprit_asn_metro"] = m["fraction_culprit_asn_metro"]

        match_asn = j_asn[j_asn["node_asn"] == c["to_asn"]]
        if len(match_asn):
            m = match_asn.iloc[0]
            row["to_total_edges_asn"] = int(m["total_edges_for_node_asn"])
            row["to_culprit_edges_asn"] = int(m["culprit_edges_for_node_asn"])
            row["to_fraction_culprit_asn"] = m["fraction_culprit_asn"]

        match_metro = j_metro[j_metro["node_metro"] == c["to_metro"]]
        if len(match_metro):
            m = match_metro.iloc[0]
            row["to_total_edges_metro"] = int(m["total_edges_for_node_metro"])
            row["to_culprit_edges_metro"] = int(m["culprit_edges_for_node_metro"])
            row["to_fraction_culprit_metro"] = m["fraction_culprit_metro"]

        if c["to_ixp"] != "None":
            match_ixp = j_ixp[j_ixp["node_ixp"] == c["to_ixp"]]
            if len(match_ixp):
                m = match_ixp.iloc[0]
                row["to_total_edges_ixp"] = int(m["total_edges_for_node_ixp"])
                row["to_culprit_edges_ixp"] = int(m["culprit_edges_for_node_ixp"])
                row["to_fraction_culprit_ixp"] = m["fraction_culprit_ixp"]

        output_rows.append(_sanitize_for_json(row))

    # Upload to BigQuery
    logger.info(f"  Uploading {len(output_rows)} rows to {OUTPUT_TABLE}...")
    errors = client.insert_rows_json(OUTPUT_TABLE, output_rows)
    if errors:
        logger.error(f"  Upload errors: {errors[:3]}")
        raise RuntimeError(f"Failed to upload: {errors[:3]}")

    logger.info("  Upload complete")


# =========================================================================
# Multi-granularity cover (§4.3.2 aggregation: edge → node → AS → metro → IXP)
# =========================================================================

MULTIGRAN_TABLE = f"{PROJECT_ID}.hermes_union.correlation_culprits_multigranularity"
ENTITY_STATS_TABLE = f"{PROJECT_ID}.hermes_union.correlation_entity_stats_multigranularity"
_GRAN_RANK = {"edge": 0, "node": 1, "AS": 2, "metro": 2, "IXP": 2}  # 0/1 = fine, 2 = coarse


def _node_ixp_map(all_edges: pd.DataFrame) -> dict:
    """Map each ⟨AS,metro⟩ node to its most common non-'None' IXP."""
    m: dict = {}
    for cn, ci in [("from_asn_metro", "from_ixp"), ("to_asn_metro", "to_ixp")]:
        s = (
            all_edges[all_edges[ci] != "None"]
            .groupby(cn)[ci]
            .agg(lambda x: x.value_counts().idxmax())
        )
        m.update(s.to_dict())
    return m


def run_mixed_granularity_cover(
    edges_df: pd.DataFrame,
    all_edges: pd.DataFrame,
    day_str: str,
    *,
    alpha: float = 0.01,
    min_support: int = 2,
    min_odds: float = 1.5,
    purity_floor: float = 0.1,
    distinct_min: int = 2,
    accuracy_keep: float = 0.5,
    coverage_stop: float = 0.98,
    max_iterations: int = 500,
) -> tuple[list[dict], list[dict]]:
    """Single greedy set-cover over a mixed pool of entities at five granularities.

    Each anomalous src–dst path "votes" for every entity it traverses at the
    edge / node(⟨AS,metro⟩) / AS / metro / IXP level. Precision (Fisher's-exact
    ``p`` + odds ratio over all paths) is computed once per entity and used as an
    eligibility gate; the greedy then repeatedly picks the eligible entity that
    explains the most still-unexplained anomalies (coverage-first). Because a
    coarse entity covers a superset of its finer parts, coverage-first naturally
    prefers it *when it is precise* — and the precision gate keeps diluted transit
    ASes out. Each anomalous pair is attributed to exactly one culprit.

    Distinctness guard: a coarse pick (AS/metro/IXP) that, among the pairs it would
    explain, subsumes fewer than ``distinct_min`` distinct finer ⟨AS,metro⟩ nodes is
    demoted to its single dominant node — so "AS seen as one ⟨AS,metro⟩" reports the
    node, not the whole AS.
    """
    node_to_ixp = _node_ixp_map(all_edges)

    # --- explode edges into per-(id,sdp,info,path_type) node rows ---
    parts = edges_df["canonical_edge"].str.split(" - ", n=1, expand=True)
    e = edges_df.assign(_a=parts[0], _b=parts[1])
    base = ["id", "src_dst_pair", "information_source", "path_type"]
    nodes = pd.concat(
        [
            e[base + ["_a"]].rename(columns={"_a": "node"}),
            e[base + ["_b"]].rename(columns={"_b": "node"}),
        ]
    ).dropna(subset=["node"])
    nodes = nodes[~nodes["node"].str.contains(r"\*", na=False)]
    nodes["asn"] = nodes["node"].str.split("-").str[0]
    nodes["metro"] = nodes["node"].str.split("-", n=1).str[1]
    nodes["ixp"] = nodes["node"].map(node_to_ixp)

    # --- build the candidate rows at all granularities (long form) ---
    def lvl(df, col, gran):
        out = df[base].copy()
        out["gran"] = gran
        out["entity"] = df[col].astype(str)
        return out[df[col].notna() & ~out["entity"].isin(["", "None", "nan"])]

    edge_rows = e[base].copy()
    edge_rows["gran"] = "edge"
    edge_rows["entity"] = e["canonical_edge"]
    edge_rows = edge_rows[~edge_rows["entity"].str.contains(r"\*", na=False)]
    units = pd.concat(
        [
            edge_rows,
            lvl(nodes, "node", "node"),
            lvl(nodes, "asn", "AS"),
            lvl(nodes, "metro", "metro"),
            lvl(nodes[nodes["ixp"].notna()], "ixp", "IXP"),
        ],
        ignore_index=True,
    ).drop_duplicates(["id", "src_dst_pair", "information_source", "gran", "entity"])

    # int-encode src_dst_pair to keep the per-candidate membership sets lean
    sdp_cat = edges_df["src_dst_pair"].astype("category")
    sdp_to_code = {v: i for i, v in enumerate(sdp_cat.cat.categories)}
    units["sdp_code"] = units["src_dst_pair"].map(sdp_to_code)

    total_anom_ids = int(edges_df.loc[edges_df["path_type"] == "anomalous", "id"].nunique())
    total_non_ids = int(edges_df.loc[edges_df["path_type"] == "non_anomalous", "id"].nunique())
    all_anom_codes = set(units.loc[units["path_type"] == "anomalous", "sdp_code"].unique().tolist())
    total_anomalies = len(all_anom_codes)
    logger.info(
        f"  [multigran] {total_anomalies} anomalous pairs, "
        f"{units[['gran', 'entity', 'information_source']].drop_duplicates().shape[0]:,} candidates"
    )
    if total_anomalies == 0:
        return [], []

    # --- per-candidate static stats ---
    g = units.groupby(["gran", "entity", "information_source", "path_type"])
    idc = g["id"].nunique().unstack("path_type").fillna(0)
    a_ids = idc.get("anomalous", pd.Series(0, index=idc.index)).astype(int)
    n_ids = idc.get("non_anomalous", pd.Series(0, index=idc.index)).astype(int)
    anom_sets = (
        units[units["path_type"] == "anomalous"]
        .groupby(["gran", "entity", "information_source"])["sdp_code"]
        .apply(set)
    )

    # coarse entity -> its finer ⟨AS,metro⟩ node children (for distinctness + purity)
    coarse_children: dict = {}
    nn = nodes.dropna(subset=["node"])
    for _, r in (
        nn[["node", "asn", "metro", "ixp", "information_source"]].drop_duplicates().iterrows()
    ):
        for gr, ent in (("AS", r["asn"]), ("metro", r["metro"]), ("IXP", r["ixp"])):
            if pd.isna(ent) or ent in ("", "None", "nan"):
                continue
            coarse_children.setdefault((gr, ent, r["information_source"]), set()).add(
                (str(r["node"]), r["information_source"])
            )

    # Candidate pool: static precision pre-filter (purity floor, Fisher, odds) bounds the
    # set; purity is then re-evaluated DYNAMICALLY on the unexplained pairs each iteration.
    pool = {}
    for key in anom_sets.index:
        ai, ni = int(a_ids.get(key, 0)), int(n_ids.get(key, 0))
        if ai < min_support or (ai + ni) == 0 or ai / (ai + ni) < purity_floor:
            continue
        p, odds = edge_significance(ai, total_anom_ids, ni, total_non_ids)
        if p > alpha or odds < min_odds:
            continue
        pool[key] = {"anom": anom_sets[key], "p": p, "odds": odds}
    logger.info(f"  [multigran] {len(pool)} candidates after static precision pre-filter")

    # Running (dynamic-on-remaining) path counts per pooled candidate + an inverse index
    # sdp -> [(candidate, d_anom, d_non)] used to decrement counts when a group is explained.
    pool_df = pd.DataFrame(list(pool.keys()), columns=["gran", "entity", "information_source"])
    psub = units.merge(pool_df, on=["gran", "entity", "information_source"], how="inner")
    csc = (
        psub.groupby(["gran", "entity", "information_source", "sdp_code", "path_type"])["id"]
        .nunique()
        .reset_index(name="cnt")
    )
    a_run = {k: 0 for k in pool}
    n_run = {k: 0 for k in pool}
    inverse: dict = {}
    for gr, ent, inf, sdp_c, pt, cnt in csc.itertuples(index=False):
        k = (gr, ent, inf)
        if pt == "anomalous":
            a_run[k] += cnt
            da, dn = cnt, 0
        else:
            n_run[k] += cnt
            da, dn = 0, cnt
        inverse.setdefault(sdp_c, []).append((k, da, dn))
    gt = (
        edges_df.groupby(["src_dst_pair", "path_type"])["id"]
        .nunique()
        .unstack("path_type")
        .fillna(0)
    )
    ganom = {
        sdp_to_code[s]: int(gt.loc[s].get("anomalous", 0)) for s in gt.index if s in sdp_to_code
    }
    gnon = {
        sdp_to_code[s]: int(gt.loc[s].get("non_anomalous", 0)) for s in gt.index if s in sdp_to_code
    }
    tot_a, tot_n = total_anom_ids, total_non_ids

    def purity_of(k):
        a, n = a_run[k], n_run[k]
        return a / (a + n) if (a + n) else 0.0

    # --- greedy: coverage-first, DYNAMIC purity gate, distinctness + accuracy demotion ---
    remaining = set(all_anom_codes)
    culprits: list[dict] = []
    code_to_sdp = {i: v for v, i in sdp_to_code.items()}
    for it in range(1, max_iterations + 1):
        if not remaining or len(remaining) <= (1 - coverage_stop) * total_anomalies:
            break
        best, best_cov, best_pur = None, 0, 0.0
        for k, d in pool.items():
            if a_run[k] < min_support or purity_of(k) < purity_floor:
                continue
            cov = len(d["anom"] & remaining)
            if cov == 0:
                continue
            pur = purity_of(k)
            if cov > best_cov or (cov == best_cov and pur > best_pur):
                best, best_cov, best_pur = k, cov, pur
        if best is None or best_cov < min_support:
            break
        gran, entity, info = best
        covered = pool[best]["anom"] & remaining
        # Coarsening control on the CURRENT (unexplained) view: keep a coarse pick only if
        # it subsumes >= distinct_min eligible finer nodes AND its dynamic purity is within
        # accuracy_keep of its purest such child. Otherwise demote to the most explanatory
        # child; if no finer node is individually plausible on the residual, drop the coarse
        # pick (it is no longer an accurate explanation) rather than over-attributing.
        demoted_from = None
        if _GRAN_RANK[gran] == 2:
            child_info = []
            for cent, cinf in coarse_children.get(best, set()):
                ck = ("node", cent, cinf)
                if ck in pool and a_run[ck] >= min_support and purity_of(ck) >= purity_floor:
                    ccov = len(pool[ck]["anom"] & covered)
                    if ccov > 0:
                        child_info.append((ccov, purity_of(ck), ck))
            keep_coarse = len(child_info) >= distinct_min and purity_of(
                best
            ) >= accuracy_keep * max(cp for _, cp, _ in child_info)
            if not keep_coarse:
                if child_info:
                    _, _, domk = max(child_info, key=lambda t: (t[0], t[1]))
                    demoted_from = f"{gran}:{entity}"
                    gran, entity, info = domk
                    covered = pool[domk]["anom"] & remaining
                else:
                    del pool[best]
                    continue
        sk = (gran, entity, info)
        a_sel, n_sel = a_run[sk], n_run[sk]
        psel, osel = edge_significance(a_sel, tot_a, n_sel, tot_n)
        rsel = ((a_sel / tot_a) / (n_sel / tot_n)) if (n_sel and tot_a and tot_n) else float("inf")
        culprits.append(
            {
                "day": day_str,
                "partition_date": day_str,
                "information_source": info,
                "granularity": gran,
                "entity": entity,
                "attribution_method": "correlation",
                "demoted_from": demoted_from,
                "iteration_number": it,
                "anomalies_explained": len(covered),
                "ratio_anomaly": rsel,
                "p_value": psel,
                "odds_ratio": osel,
                "support_anomalous": a_sel,
                "support_healthy": n_sel,
                "anomalous_src_dst_pairs_impacted": [code_to_sdp[c] for c in covered],
            }
        )
        # explain the covered groups: drop them from the universe and decrement running counts
        for s in covered:
            for ck, da, dn in inverse.get(s, ()):
                a_run[ck] -= da
                n_run[ck] -= dn
            tot_a -= ganom.get(s, 0)
            tot_n -= gnon.get(s, 0)
        remaining -= covered

    explained = total_anomalies - len(remaining)
    logger.info(
        f"  [multigran] {len(culprits)} culprits, "
        f"{explained}/{total_anomalies} ({explained / total_anomalies:.3f}) explained"
    )
    # cumulative bookkeeping
    cum = 0
    for c in culprits:
        cum += c["anomalies_explained"]
        c["cumulative_anomalies_explained"] = cum
        c["cumulative_fraction_explained"] = cum / total_anomalies

    # Per-entity stats for EVERY candidate the cover evaluated (winners + non-winners),
    # so the dashboard can zoom to any granularity, not just the chosen culprits. Stats
    # are over all event-day paths (the entity's overall daily footprint). is_culprit
    # marks the entities the cover selected (a demoted node is the culprit; the coarse
    # entity it was demoted from is present here too, with is_culprit=False).
    winners = {(c["granularity"], c["entity"], c["information_source"]) for c in culprits}
    entity_stats = []
    for key in a_ids.index:
        ai, ni = int(a_ids.get(key, 0)), int(n_ids.get(key, 0))
        if ai < min_support:
            continue
        gr, ent, inf = key
        p, odds = edge_significance(ai, total_anom_ids, ni, total_non_ids)
        ratio = ((ai / total_anom_ids) / (ni / total_non_ids)) if ni else float("inf")
        entity_stats.append(
            {
                "partition_date": day_str,
                "information_source": inf,
                "granularity": gr,
                "entity": ent,
                "support_anomalous": ai,
                "support_healthy": ni,
                "ratio_anomaly": ratio,
                "p_value": p,
                "odds_ratio": odds,
                "is_culprit": key in winners,
            }
        )
    logger.info(f"  [multigran] {len(entity_stats)} entity-stat rows (winners + non-winners)")
    return culprits, entity_stats


def upload_multigranularity(client: bigquery.Client, culprits: list[dict], day_str: str) -> None:
    """Replace the day's rows in MULTIGRAN_TABLE with the mixed-granularity culprits."""
    client.query(
        f"DELETE FROM `{MULTIGRAN_TABLE}` WHERE DATE(partition_date) = '{day_str}'"
    ).result()
    if not culprits:
        logger.info("  [multigran] no culprits to upload")
        return
    rows = [_sanitize_for_json(c) for c in culprits]
    errors = client.insert_rows_json(MULTIGRAN_TABLE, rows)
    if errors:
        logger.error(f"  [multigran] upload errors: {errors[:3]}")
        raise RuntimeError(f"multigran upload failed: {errors[:3]}")
    logger.info(f"  [multigran] uploaded {len(rows)} culprits")


def upload_entity_stats(client: bigquery.Client, entity_stats: list[dict], day_str: str) -> None:
    """Replace the day's partition in ENTITY_STATS_TABLE via a load job.

    Holds one row per (date, information_source, granularity, entity) for every
    candidate the cover evaluated (winners + non-winners), enabling the dashboard to
    look up any entity's anomaly stats at any granularity. A load job to the partition
    decorator (table$YYYYMMDD, WRITE_TRUNCATE) is idempotent and avoids the streaming
    buffer (the row count is too large for insert_rows_json + DELETE).
    """
    schema = [
        bigquery.SchemaField("partition_date", "DATE"),
        bigquery.SchemaField("information_source", "STRING"),
        bigquery.SchemaField("granularity", "STRING"),
        bigquery.SchemaField("entity", "STRING"),
        bigquery.SchemaField("support_anomalous", "INT64"),
        bigquery.SchemaField("support_healthy", "INT64"),
        bigquery.SchemaField("ratio_anomaly", "FLOAT64"),
        bigquery.SchemaField("p_value", "FLOAT64"),
        bigquery.SchemaField("odds_ratio", "FLOAT64"),
        bigquery.SchemaField("is_culprit", "BOOL"),
    ]
    rows = [_sanitize_for_json(r) for r in entity_stats]
    if not rows:
        client.query(
            f"DELETE FROM `{ENTITY_STATS_TABLE}` WHERE DATE(partition_date) = '{day_str}'"
        ).result()
        return
    dest = f"{ENTITY_STATS_TABLE}${day_str.replace('-', '')}"
    job = client.load_table_from_json(
        rows,
        dest,
        job_config=bigquery.LoadJobConfig(
            schema=schema, write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        ),
    )
    job.result()
    logger.info(f"  [multigran] uploaded {len(rows)} entity-stat rows")


# =========================================================================
# Orchestrator
# =========================================================================


def run_correlation_tomography(
    date: _dt.date,
    project_id: str = PROJECT_ID,
    max_iterations: int = 200,
    no_progress_limit: int = 5,
    write_multigranularity: bool = False,
) -> None:
    """Run the full hybrid correlation tomography pipeline for one date.

    Orchestrates the three-phase process:

    1. **Phase 1 (SQL)** — Extract edges via ``06_correlation_tomography_prepare_union.sql``
       and download to a pandas DataFrame (:func:`download_edges`).
    2. **Phase 2 (Python)** — Greedy set-cover in memory
       (:func:`run_greedy_set_cover`).
    3. **Phase 3 (SQL + Python)** — Compute node-level hyperedge fractions and
       upload results to :data:`OUTPUT_TABLE` (:func:`compute_hyperedges`).

    If the output table already contains rows for ``date``, the function
    returns immediately without reprocessing.

    Parameters
    ----------
    date
        The day to process.
    project_id
        GCP project ID (default: ``"mlab-collaboration"``).
    max_iterations
        Maximum number of greedy set-cover iterations.
    no_progress_limit
        Stop early after this many consecutive iterations with zero new
        anomalies explained.
    write_multigranularity
        also compute/write the out-of-scope multigranularity tables (default
        False; requires their DDLs to exist).
    """
    day_str = date.strftime("%Y-%m-%d")
    client = bigquery.Client(project=project_id)

    if step_already_done(client, OUTPUT_TABLE, day_str):
        logger.info(f"[{day_str}] Skipping — already done")
        return

    logger.info(f"[{day_str}] Starting correlation tomography")

    # Phase 1: SQL → download edges
    edges_df = download_edges(client, day_str)

    # Phase 2: Python set-cover
    logger.info(f"[{day_str}] Phase 2: greedy set-cover...")
    culprits, total_anomalies = run_greedy_set_cover(
        edges_df,
        day_str=day_str,
        max_iterations=max_iterations,
        no_progress_limit=no_progress_limit,
    )

    # Path-local pass: attribute the anomalous groups the set-cover couldn't explain.
    anomalous_pairs = set(
        edges_df.loc[edges_df["path_type"] == "anomalous", "src_dst_pair"].unique()
    )
    explained = {p for c in culprits for p in c.get("anomalous_src_dst_pairs_impacted", [])}
    path_local = attribute_unexplained(client, day_str, explained, anomalous_pairs)
    logger.info(f"[{day_str}] Path-local attributions: {len(path_local)}")
    culprits = culprits + path_local

    # Phase 3: hyperedges computation + upload. Download all_edges_per_node ONCE
    # and reuse it for both the edge-level hyperedge stats and the
    # multi-granularity cover (avoids a second multi-GB scan/download).
    logger.info(f"[{day_str}] Phase 3: downloading all_edges_per_node...")
    all_edges = _read_df(
        client.query(
            loader.load_query("06_correlation_tomography_all_edges_union.sql", {"DAY": day_str})
        )
    )
    logger.info(f"  Downloaded {len(all_edges):,} node-edge rows")

    logger.info(f"[{day_str}] Phase 3: hyperedge summary...")
    compute_hyperedges(client, culprits, day_str, all_edges=all_edges)

    # Phase 4: multi-granularity cover (edge→node→AS→metro→IXP) + path-local tail.
    if write_multigranularity:
        logger.info(f"[{day_str}] Phase 4: multi-granularity cover...")
        multigran, entity_stats = run_mixed_granularity_cover(edges_df, all_edges, day_str)
        upload_entity_stats(client, entity_stats, day_str)
        # Fold in path-local attribution for anomalies the correlation cover left
        # unexplained (singletons), so the table is the complete attribution.
        explained_mg = {p for c in multigran for p in c.get("anomalous_src_dst_pairs_impacted", [])}
        pl_mg = attribute_unexplained(client, day_str, explained_mg, anomalous_pairs)
        for r in pl_mg:
            impacted = r.get("anomalous_src_dst_pairs_impacted", [])
            multigran.append(
                {
                    "day": day_str,
                    "partition_date": day_str,
                    "information_source": r.get("information_source"),
                    "granularity": "edge",
                    "entity": r.get("canonical_edge"),
                    "attribution_method": "path_local",
                    "demoted_from": None,
                    "iteration_number": None,
                    "anomalies_explained": len(impacted),
                    "ratio_anomaly": None,
                    "p_value": None,
                    "odds_ratio": None,
                    "support_anomalous": None,
                    "support_healthy": None,
                    "anomalous_src_dst_pairs_impacted": impacted,
                }
            )
        # recompute cumulative over the combined list
        cum = 0
        total = len(anomalous_pairs) or 1
        for c in multigran:
            cum += c.get("anomalies_explained", 0)
            c["cumulative_anomalies_explained"] = cum
            c["cumulative_fraction_explained"] = cum / total
        logger.info(
            f"[{day_str}] Phase 4: {len(multigran)} culprits "
            f"({len(pl_mg)} path-local), {cum}/{total} explained"
        )
        upload_multigranularity(client, multigran, day_str)

    logger.info(f"[{day_str}] Done")


def main() -> None:
    """CLI entry point for the standalone correlation tomography script.

    Parses ``--date`` or ``--start-date``/``--end-date`` and calls
    :func:`run_correlation_tomography` for each date in the range.
    """
    parser = argparse.ArgumentParser(description="Hybrid correlation tomography")
    parser.add_argument("--date", help="Single date (YYYY-MM-DD)")
    parser.add_argument("--start-date", help="Start date for range")
    parser.add_argument("--end-date", help="End date for range")
    parser.add_argument("--max-iterations", type=int, default=200)
    parser.add_argument(
        "--no-progress-limit",
        type=int,
        default=5,
        help="Stop after N iterations with no new anomalies explained",
    )
    parser.add_argument("--project", default=PROJECT_ID)
    args = parser.parse_args()

    if args.date:
        dates = [datetime.strptime(args.date, "%Y-%m-%d").date()]
    elif args.start_date and args.end_date:
        start = datetime.strptime(args.start_date, "%Y-%m-%d").date()
        end = datetime.strptime(args.end_date, "%Y-%m-%d").date()
        dates = []
        d = start
        while d <= end:
            dates.append(d)
            d += timedelta(days=1)
    else:
        dates = [(datetime.today() - timedelta(days=1)).date()]

    for date in dates:
        run_correlation_tomography(
            date,
            project_id=args.project,
            max_iterations=args.max_iterations,
            no_progress_limit=args.no_progress_limit,
        )


if __name__ == "__main__":
    main()

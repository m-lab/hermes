"""
Hybrid Python+SQL correlation tomography.

Phase 1 (SQL): Extract edges from events → download to pandas
Phase 2 (Python): Greedy set-cover loop
Phase 3 (SQL): Download all_edges_per_node → compute hyperedges in Python → upload

No intermediate BigQuery tables — only reads from events_with_as_and_geoloc
and writes to correlation_hyperedges_tomography.

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

from hermes.sql import loader

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

PROJECT_ID = "mlab-collaboration"


def _sanitize_for_json(obj):
    """Replace NaN/Inf floats with None recursively — BigQuery rejects NaN in JSON."""
    if isinstance(obj, float):
        return None if (math.isnan(obj) or math.isinf(obj)) else obj
    if isinstance(obj, dict):
        return {k: _sanitize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize_for_json(v) for v in obj]
    return obj


OUTPUT_TABLE = f"{PROJECT_ID}.hermes_union.correlation_hyperedges_tomography"


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
    df = client.query(sql).to_dataframe()
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

        # HAVING filters
        candidates = candidates[
            (candidates["anom_edge_count"] >= 2)
            & (candidates["fraction_anomalous_paths"] >= 0.7)
            & (~candidates["edge"].str.contains(r"\*", na=False))
        ]
        if len(candidates) == 0:
            break

        # Rank and pick top
        candidates = candidates.sort_values(
            ["ratio_anomaly", "is_interdomain"], ascending=[False, False]
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


def compute_hyperedges(client: bigquery.Client, culprit_rows: list[dict], day_str: str) -> None:
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

    # Download all_edges_per_node
    logger.info("  Phase 3: downloading all_edges_per_node...")
    params: dict[str, object] = {"DAY": day_str}
    sql = loader.load_query("06_correlation_tomography_all_edges_union.sql", params)
    all_edges = client.query(sql).to_dataframe()
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

    # --- Culprit node counts ---
    culp_nc_am = node_counts(culprits_df, "from_asn_metro", "to_asn_metro", "node_asn_metro")
    culp_nc_asn = node_counts(culprits_df, "from_asn", "to_asn", "node_asn")
    culp_nc_metro = node_counts(culprits_df, "from_metro", "to_metro", "node_metro")

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
                    "to_asn",
                    "to_metro",
                    "to_asn_metro",
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

        output_rows.append(_sanitize_for_json(row))

    # Upload to BigQuery
    logger.info(f"  Uploading {len(output_rows)} rows to {OUTPUT_TABLE}...")
    errors = client.insert_rows_json(OUTPUT_TABLE, output_rows)
    if errors:
        logger.error(f"  Upload errors: {errors[:3]}")
        raise RuntimeError(f"Failed to upload: {errors[:3]}")

    logger.info("  Upload complete")


# =========================================================================
# Orchestrator
# =========================================================================


def run_correlation_tomography(
    date: _dt.date,
    project_id: str = PROJECT_ID,
    max_iterations: int = 200,
    no_progress_limit: int = 5,
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

    # Phase 3: hyperedges computation + upload
    logger.info(f"[{day_str}] Phase 3: hyperedge summary...")
    compute_hyperedges(client, culprits, day_str)

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

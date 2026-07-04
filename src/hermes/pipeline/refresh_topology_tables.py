#!/usr/bin/env python3
"""Refresh the topology/metadata tables the union pipeline reads but does not write.

`hermes_pipeline_union.py` runs a *reduced* enrichment (geolocation + rDNS + HOIHO
only). The BGP IP→AS tables, the IXP membership tables, and the AS-metadata table
are NOT refreshed by it — step 04 just reads whatever rows already exist. This
script refreshes those tables on the cadence you actually want:

    BGP (RouteViews)         →  run per day (RouteViews has dated archives → uses --date)
    IXP membership           →  run per day (live ≈current snapshot; ingested with its
                                 own true date — NOT backfilled to --date)
    AS metadata              →  run ~monthly

For as_metadata, this script also GENERATES the required CAIDA + PeeringDB input
files by invoking the generators in the missing-peering-links repo
(get_as_rank_data.py + PeeringDB_Crawler.py), then uploads the augmented metadata.
Generation is skipped automatically when the files already exist for the date.

Examples
--------
    # BGP + IXP for one day (IPv4 only — the default)
    python refresh_topology_tables.py --date 2025-06-09

    # BGP + IXP for one day, both IPv4 and IPv6
    python refresh_topology_tables.py --date 2025-06-09 --ip-version both

    # IPv6 only (e.g. to add v6 after v4 already ran — avoids duplicate v4 rows)
    python refresh_topology_tables.py --date 2025-06-09 --ip-version ipv6

    # Everything together: daily BGP+IXP and monthly as_metadata (generate + upload)
    python refresh_topology_tables.py --date 2025-06-01 --as-metadata

    # as_metadata only, reusing already-generated input files
    python refresh_topology_tables.py --date 2025-06-01 --as-metadata \
        --skip-bgp --skip-ixp --skip-as-metadata-generation

    # Only one component
    python refresh_topology_tables.py --date 2025-06-09 --skip-ixp        # BGP only
    python refresh_topology_tables.py --date 2025-06-09 --skip-bgp        # IXP only
"""

import argparse
import glob
import logging
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from hermes.enrichment.as_metadata.enricher import update_as_metadata
from hermes.enrichment.peeringdb_ixp.ixp_collector import IXPCollector
from hermes.enrichment.peeringdb_ixp.ixp_collector_ipv6 import IXPCollectorIPv6
from hermes.enrichment.routeviews import RouteViewsEnricher
from hermes.enrichment.routeviews.enricher_ipv6 import RouteViewsEnricherIPv6

# Default location of the companion repo that generates the as_metadata inputs
# (CAIDA AS-Rank JSON + PeeringDB footprint/type CSVs).
DEFAULT_MPL_REPO = os.path.expanduser("~/Documents/GitHub/missing-peering-links")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("refresh_topology_tables")


def refresh_bgp(date: str, project_id: str, do_v4: bool, do_v6: bool) -> None:
    """Refresh hermes.unified_ip_to_as[_ipv6] from RouteViews for `date`."""
    if do_v4:
        logger.info("[BGP] Refreshing unified_ip_to_as (IPv4) for %s", date)
        RouteViewsEnricher(project_id).process_date(date)

    if do_v6:
        logger.info("[BGP] Refreshing unified_ip_to_as_ipv6 for %s", date)
        RouteViewsEnricherIPv6(project_id).process_date(date)


def _latest_merged_members_file(output_dir: str, ipv6: bool) -> str | None:
    """Newest merged-members snapshot file in `output_dir`, or None.

    IXP membership is a live (≈current) snapshot — the underlying wrapper.py only
    ever generates "today-1" and ignores any requested date — so we ingest the
    most recent file and let the collector stamp its true date from the filename.
    """
    pattern = os.path.join(output_dir, "merged-members-gen-*.txt")
    candidates = glob.glob(pattern)
    if ipv6:
        candidates = [c for c in candidates if c.endswith("_ipv6.txt")]
    else:
        candidates = [c for c in candidates if not c.endswith("_ipv6.txt")]
    # Filenames embed YYYYMMDD in fixed-width form, so lexicographic == chronological.
    return max(candidates) if candidates else None


def _ingest_ixp_snapshot(collector, ipv6: bool) -> None:
    """Parse the latest merged-members file and load it into the IXP tables."""
    label = "IPv6" if ipv6 else "IPv4"
    path = _latest_merged_members_file(collector.output_dir, ipv6)
    if path is None:
        raise RuntimeError(
            f"{label} IXP: no merged-members-gen-*.txt found in {collector.output_dir} "
            "(run with --refresh-ixp-snapshot to generate one)"
        )
    logger.info(
        "[IXP] Ingesting %s snapshot %s (partition_date taken from filename)",
        label,
        os.path.basename(path),
    )
    members_rows, unified_rows = collector.process_data_file(path)
    if not members_rows or not unified_rows:
        raise RuntimeError(f"{label} IXP: no rows parsed from {path}")
    if not collector.insert_to_bigquery(members_rows, unified_rows):
        raise RuntimeError(f"{label} IXP: BigQuery insert failed")


def refresh_ixp(do_v4: bool, do_v6: bool, refresh_snapshot: bool) -> None:
    """Load the current IXP membership snapshot into the IXP tables.

    By default this ingests the latest already-generated merged-members file with
    its true (filename) date — no backfill, no date coupling to the BGP --date.
    With `refresh_snapshot`, regenerate a fresh snapshot via wrapper.py first
    (one run produces both the IPv4 and IPv6 files; this can take ~an hour).
    """
    v4 = IXPCollector()

    # wrapper.py produces both the IPv4 and IPv6 snapshot files in one run, so
    # generation is keyed on the IPv4 file regardless of which versions we ingest.
    have_snapshot = _latest_merged_members_file(v4.output_dir, ipv6=False) is not None
    if refresh_snapshot or not have_snapshot:
        reason = "forced refresh" if refresh_snapshot else "no existing snapshot found"
        logger.info(
            "[IXP] Generating fresh IXP snapshot via wrapper.py (%s) — may take ~an hour", reason
        )
        if not v4.run_wrapper_script():
            raise RuntimeError("IXP wrapper.py failed to generate a snapshot")

    if do_v4:
        _ingest_ixp_snapshot(v4, ipv6=False)
    if do_v6:
        _ingest_ixp_snapshot(IXPCollectorIPv6(), ipv6=True)


def _as_metadata_input_paths(date: str, mpl_repo: str) -> dict[str, Path]:
    """The three local files hermes.as_metadata is built from, for `date`.

    Naming must match hermes_enrichment/as_metadata/enricher.py::update_as_metadata.
    """
    repo = Path(mpl_repo)
    yyyymmdd = date.replace("-", "")
    year, month, _ = date.split("-")
    return {
        "caida": repo / "data" / "BGP_data" / f"ASNS-{yyyymmdd}.json",
        "footprint": repo
        / "scripts"
        / "data"
        / "PeeringDB"
        / f"AS_footprint_info_{year}-{month}.csv",
        "as_type": repo / "scripts" / "data" / "PeeringDB" / f"AS_Type{year}-{month}.csv",
    }


def generate_as_metadata_inputs(
    date: str, mpl_repo: str, python_exe: str, regenerate: bool
) -> None:
    """Generate the CAIDA + PeeringDB input files in the missing-peering-links repo.

    Runs the same generators the companion repo uses:

    - ``scripts/get_as_rank_data.py``
      → ``data/BGP_data/ASNS-<YYYYMMDD>.json`` (CAIDA AS-Rank)
    - ``scripts/PeeringDB_Crawler.py``
      → ``scripts/data/PeeringDB/AS_{footprint_info,Type}-<YYYY-MM>.csv``

    Neither script needs an API key. Both are run under ``python_exe``
    (which must have ``graphqlclient``, ``requests``, and ``pandas`` available).
    Skips generation when all files already exist unless ``regenerate`` is set.

    Parameters
    ----------
    date
        Target date as ``YYYY-MM-DD``.
    mpl_repo
        Absolute path to the ``missing-peering-links`` companion repository.
    python_exe
        Python interpreter used to run the generator scripts.
    regenerate
        When ``True``, regenerate inputs even if they already exist.

    Raises
    ------
    RuntimeError
        If ``mpl_repo/scripts`` is not found, a generator script fails, or
        any expected output file is missing after generation.
    """
    repo = Path(mpl_repo)
    scripts_dir = repo / "scripts"
    if not scripts_dir.is_dir():
        raise RuntimeError(f"missing-peering-links scripts dir not found: {scripts_dir}")

    paths = _as_metadata_input_paths(date, mpl_repo)
    if not regenerate and all(p.exists() for p in paths.values()):
        logger.info("[AS_METADATA] Input files already present for %s — skipping generation", date)
        return

    yyyymmdd = date.replace("-", "")
    paths["caida"].parent.mkdir(parents=True, exist_ok=True)

    # 1. CAIDA AS-Rank: ASNS / ORGS / LINKS JSON
    asns = paths["caida"]
    orgs = asns.with_name(f"ORGS-{yyyymmdd}.json")
    links = asns.with_name(f"LINKS-{yyyymmdd}.json")
    logger.info("[AS_METADATA] Generating CAIDA AS-Rank data → %s", asns)
    _run(
        [python_exe, "get_as_rank_data.py", "-a", str(asns), "-o", str(orgs), "-l", str(links)],
        cwd=scripts_dir,
    )

    # 2. PeeringDB footprint + AS-type CSVs (written under scripts/data/PeeringDB/)
    logger.info("[AS_METADATA] Generating PeeringDB footprint/type CSVs (DATE=%s)", yyyymmdd)
    _run([python_exe, "PeeringDB_Crawler.py", "--DATE", yyyymmdd], cwd=scripts_dir)

    missing = [str(p) for p in paths.values() if not p.exists()]
    if missing:
        raise RuntimeError("as_metadata input generation did not produce: " + ", ".join(missing))


def _run(cmd: list[str], cwd: Path) -> None:
    """Run a subprocess, streaming failure output into our log."""
    logger.info("[AS_METADATA] $ %s  (cwd=%s)", " ".join(cmd), cwd)
    result = subprocess.run(cmd, cwd=str(cwd), capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"command failed ({result.returncode}): {' '.join(cmd)}\n"
            f"stderr:\n{result.stderr.strip()}"
        )


def refresh_as_metadata(
    date: str, mpl_repo: str, python_exe: str, regenerate: bool, skip_generation: bool
) -> None:
    """Refresh hermes.as_metadata for `date` (monthly cadence).

    Generates the CAIDA/PeeringDB input files via the missing-peering-links repo
    (unless --skip-as-metadata-generation), then uploads the augmented metadata.
    """
    if skip_generation:
        paths = _as_metadata_input_paths(date, mpl_repo)
        missing = [str(p) for p in paths.values() if not p.exists()]
        if missing:
            raise RuntimeError("as_metadata input files missing: " + ", ".join(missing))
        logger.info("[AS_METADATA] Using existing input files (generation skipped)")
    else:
        generate_as_metadata_inputs(date, mpl_repo, python_exe, regenerate)

    logger.info("[AS_METADATA] Uploading as_metadata for %s", date)
    if not update_as_metadata(date):
        raise RuntimeError(f"as_metadata upload failed for {date}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--date", required=True, help="Target date (YYYY-MM-DD)")
    parser.add_argument("--project", default="mlab-collaboration", help="GCP project ID")
    parser.add_argument(
        "--ip-version",
        choices=["ipv4", "ipv6", "both"],
        default="ipv4",
        help="Which IP version(s) of the BGP/IXP tables to refresh "
        "(default: ipv4). as_metadata is version-agnostic.",
    )
    parser.add_argument(
        "--ipv6", action="store_true", help=argparse.SUPPRESS
    )  # deprecated alias for --ip-version both
    parser.add_argument(
        "--as-metadata",
        action="store_true",
        help="Also refresh hermes.as_metadata (run ~monthly; generates inputs + uploads)",
    )
    parser.add_argument("--skip-bgp", action="store_true", help="Skip the RouteViews/BGP refresh")
    parser.add_argument("--skip-ixp", action="store_true", help="Skip the IXP refresh")
    parser.add_argument(
        "--refresh-ixp-snapshot",
        action="store_true",
        help="Regenerate a fresh IXP snapshot via wrapper.py before ingesting "
        "(slow, ~an hour). Default: ingest the latest existing snapshot.",
    )
    parser.add_argument(
        "--mpl-repo",
        default=DEFAULT_MPL_REPO,
        help="Path to the missing-peering-links repo (generates as_metadata inputs)",
    )
    parser.add_argument(
        "--mpl-python",
        default=sys.executable,
        help="Python interpreter used to run the as_metadata input generators "
        "(default: this interpreter)",
    )
    parser.add_argument(
        "--regenerate-as-metadata-inputs",
        action="store_true",
        help="Force regeneration of as_metadata inputs even if the files exist",
    )
    parser.add_argument(
        "--skip-as-metadata-generation",
        action="store_true",
        help="Do not generate as_metadata inputs; require the files to already exist",
    )
    args = parser.parse_args()

    # Validate the date early so we fail before any network/BigQuery work.
    try:
        datetime.strptime(args.date, "%Y-%m-%d")
    except ValueError as err:
        raise SystemExit(f"--date must be YYYY-MM-DD, got {args.date!r}") from err

    # Resolve which IP versions to process (--ipv6 is a deprecated alias for "both").
    ip_version = "both" if args.ipv6 else args.ip_version
    do_v4 = ip_version in ("ipv4", "both")
    do_v6 = ip_version in ("ipv6", "both")

    failures: list[str] = []

    if not args.skip_bgp:
        try:
            refresh_bgp(args.date, args.project, do_v4, do_v6)
        except Exception as err:
            logger.error("BGP refresh failed: %s", err)
            failures.append("bgp")

    if not args.skip_ixp:
        try:
            refresh_ixp(do_v4, do_v6, args.refresh_ixp_snapshot)
        except Exception as err:
            logger.error("IXP refresh failed: %s", err)
            failures.append("ixp")

    if args.as_metadata:
        try:
            refresh_as_metadata(
                args.date,
                args.mpl_repo,
                args.mpl_python,
                regenerate=args.regenerate_as_metadata_inputs,
                skip_generation=args.skip_as_metadata_generation,
            )
        except Exception as err:
            logger.error("as_metadata refresh failed: %s", err)
            failures.append("as_metadata")

    if failures:
        logger.error("Completed with failures: %s", ", ".join(failures))
        return 1

    logger.info("All requested topology-table refreshes completed successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

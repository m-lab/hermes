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
import sys
from datetime import datetime

from hermes.enrichment.as_metadata.enricher import update_as_metadata
from hermes.enrichment.as_metadata.paths import as_metadata_input_paths
from hermes.enrichment.as_metadata.sources import generate_as_metadata_inputs_native
from hermes.enrichment.peeringdb_ixp.ixp_collector import IXPCollector
from hermes.enrichment.peeringdb_ixp.ixp_collector_ipv6 import IXPCollectorIPv6
from hermes.enrichment.peeringdb_ixp.snapshot import download_peeringdb_dump, generate_snapshot
from hermes.enrichment.routeviews import RouteViewsEnricher
from hermes.enrichment.routeviews.enricher_ipv6 import RouteViewsEnricherIPv6

# Default location of the companion repo that generates the as_metadata inputs
# (CAIDA AS-Rank JSON + PeeringDB footprint/type CSVs).
DEFAULT_MPL_REPO = os.path.expanduser("~/Documents/GitHub/missing-peering-links")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("refresh_topology_tables")


def refresh_bgp(date: str, project_id: str, do_v4: bool, do_v6: bool, force: bool = False) -> None:
    """Refresh hermes.unified_ip_to_as[_ipv6] from RouteViews for `date`.

    Raises:
        RuntimeError: if a requested address family did not refresh. Previously
            a failure here was only logged, so the caller exited 0 and reported
            "All requested topology-table refreshes completed successfully" --
            which is how the IPv6 table silently stopped updating for months
            while the scheduled job looked healthy every time.
    """
    failed = []

    if do_v4:
        logger.info("[BGP] Refreshing unified_ip_to_as (IPv4) for %s", date)
        if not RouteViewsEnricher(project_id).process_date(date, force=force):
            failed.append("IPv4")

    if do_v6:
        logger.info("[BGP] Refreshing unified_ip_to_as_ipv6 for %s", date)
        if not RouteViewsEnricherIPv6(project_id).process_date(date, force=force):
            failed.append("IPv6")

    if failed:
        raise RuntimeError(f"BGP refresh failed for {', '.join(failed)} on {date}")


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


def refresh_ixp(date: str, do_v4: bool, do_v6: bool, refresh_snapshot: bool) -> None:
    """Load the current IXP membership snapshot into the IXP tables.

    By default this ingests the latest already-generated merged-members file with
    its true (filename) date. With `refresh_snapshot`, generate a fresh snapshot
    first.

    Generation is in-process (``peeringdb_ixp.snapshot.generate_snapshot``), not
    the old ``wrapper.py`` subprocess: PeeringDB comes from CAIDA's archived
    daily bulk dump and PCH from its public API, so this needs no companion repo,
    no separate interpreter and no laptop-specific paths -- which is what kept
    IXP membership from ever being refreshed on the VM. It still takes roughly an
    hour, almost entirely PCH's per-IXP rate limit.
    """
    v4 = IXPCollector()

    # One generation run produces both the IPv4 and IPv6 files, so the check is
    # keyed on the IPv4 file regardless of which versions we ingest.
    have_snapshot = _latest_merged_members_file(v4.output_dir, ipv6=False) is not None
    if refresh_snapshot or not have_snapshot:
        reason = "forced refresh" if refresh_snapshot else "no existing snapshot found"
        logger.info(
            "[IXP] Generating fresh IXP snapshot for %s (%s) — may take ~an hour", date, reason
        )
        if generate_snapshot(date, v4.output_dir) is None:
            raise RuntimeError(f"IXP snapshot generation failed for {date}")

    if do_v4:
        _ingest_ixp_snapshot(v4, ipv6=False)
    if do_v6:
        _ingest_ixp_snapshot(IXPCollectorIPv6(), ipv6=True)


def generate_as_metadata_inputs(
    date: str, mpl_repo: str, python_exe: str, regenerate: bool
) -> None:
    """Generate the CAIDA AS Rank + PeeringDB input files for `date`.

    Built in-process (``as_metadata.sources``) rather than by shelling out to
    ``get_as_rank_data.py`` and ``PeeringDB_Crawler.py`` in the private
    missing-peering-links repo. Those could not run on hermes-ec2 -- the repo is
    not there -- and the old precondition only checked that ``<repo>/scripts``
    was a directory, which an empty leftover tree on the VM satisfies, so the
    check passed and the first subprocess died instead.

    AS Rank comes from api.asrank.caida.org; the two PeeringDB CSVs come from the
    same CAIDA archived dump the IXP snapshot downloads, so no new source and no
    geocoding.

    Args:
        date: Target date, ``YYYY-MM-DD``.
        mpl_repo: Base directory for the input files.
        python_exe: Unused; kept so the CLI flag stays accepted.
        regenerate: Rebuild even when all three files already exist.
    """
    del python_exe  # no subprocess to run under an interpreter any more

    paths = as_metadata_input_paths(date, mpl_repo)
    if not regenerate and all(p.exists() for p in paths.values()):
        logger.info("[AS_METADATA] Input files already present for %s — skipping generation", date)
        return

    cache_dir = os.path.join(os.path.expanduser("~"), ".cache", "hermes", "peeringdb")
    downloaded = download_peeringdb_dump(date, cache_dir)
    if downloaded is None:
        raise RuntimeError(f"no PeeringDB dump available for {date} or nearby dates")
    dump_path, actual = downloaded
    if actual != date:
        logger.warning("[AS_METADATA] Using PeeringDB dump from %s (requested %s)", actual, date)

    logger.info("[AS_METADATA] Generating inputs for %s", date)
    generate_as_metadata_inputs_native(date, paths, dump_path)


def refresh_as_metadata(
    date: str, mpl_repo: str, python_exe: str, regenerate: bool, skip_generation: bool
) -> None:
    """Refresh hermes.as_metadata for `date` (monthly cadence).

    Generates the CAIDA/PeeringDB input files via the missing-peering-links repo
    (unless --skip-as-metadata-generation), then uploads the augmented metadata.
    """
    if skip_generation:
        paths = as_metadata_input_paths(date, mpl_repo)
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
    parser.add_argument(
        "--force-bgp",
        action="store_true",
        help="Re-upload BGP rows even if the resolved date already has them. The "
        "enrichers APPEND, so use this only after deleting the existing rows for "
        "that date -- otherwise it doubles the snapshot.",
    )
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
            refresh_bgp(args.date, args.project, do_v4, do_v6, force=args.force_bgp)
        except Exception as err:
            logger.error("BGP refresh failed: %s", err)
            failures.append("bgp")

    if not args.skip_ixp:
        try:
            refresh_ixp(args.date, do_v4, do_v6, args.refresh_ixp_snapshot)
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

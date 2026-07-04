"""
Check BigQuery quota usage for today (or a given date range).

Uses the BigQuery Jobs API (client.list_jobs) to report:
  - Total bytes billed today
  - Estimated cost (on-demand pricing: $6.25 per TiB)
  - Breakdown by dataset
  - Top 10 most expensive queries

Usage:
    python check_quota.py                          # today's usage
    python check_quota.py --date 2025-05-20        # specific date
    python check_quota.py --start 2025-05-01 --end 2025-05-23  # date range
    python check_quota.py --project my-project     # different project
"""

import argparse
import logging
from collections import defaultdict
from datetime import UTC, datetime

from google.auth import default
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# BigQuery on-demand pricing: $6.25 per TiB
COST_PER_TIB = 6.25
BYTES_PER_TIB = 1024**4


def get_client(project_id: str | None) -> bigquery.Client:
    """Create an authenticated BigQuery client for *project_id*.

    Parameters
    ----------
    project_id
        GCP project ID.  If ``None``, the project from Application Default
        Credentials is used.

    Returns
    -------
    google.cloud.bigquery.Client
        Authenticated client targeting the resolved project.
    """
    creds, default_project = default()
    proj = project_id or default_project
    account = getattr(creds, "service_account_email", None)
    if account:
        logger.info(f"Authenticated as service account: {account}")
    else:
        logger.info("Authenticated as user (ADC)")
    logger.info(f"Project: {proj}")
    return bigquery.Client(project=proj)


def bytes_to_human(b: float) -> str:
    """Format a byte count as a human-readable string with appropriate units.

    Parameters
    ----------
    b
        Number of bytes.

    Returns
    -------
    str
        E.g. ``"1.23 GiB"`` or ``"456.78 MiB"``.
    """
    for unit in ["B", "KiB", "MiB", "GiB", "TiB"]:
        if abs(b) < 1024:
            return f"{b:.2f} {unit}"
        b /= 1024
    return f"{b:.2f} PiB"


def estimate_cost(total_bytes: float) -> float:
    """Estimate BigQuery on-demand query cost from bytes billed.

    Parameters
    ----------
    total_bytes
        Total bytes billed.

    Returns
    -------
    float
        Estimated cost in USD at ``$6.25 / TiB``.
    """
    return (total_bytes / BYTES_PER_TIB) * COST_PER_TIB


def collect_jobs(
    client: bigquery.Client,
    start_dt: datetime,
    end_dt: datetime,
) -> list[bigquery.QueryJob]:
    """Fetch all query jobs in the given time window.

    Parameters
    ----------
    client
        BigQuery client.
    start_dt
        Inclusive start of the window (timezone-aware).
    end_dt
        Inclusive end of the window (timezone-aware).

    Returns
    -------
    list of google.cloud.bigquery.QueryJob
        All ``QueryJob`` instances created in the window (up to 10 000).
    """
    jobs = []
    logger.info(f"Fetching jobs from {start_dt} to {end_dt} ...")
    for job in client.list_jobs(
        min_creation_time=start_dt,
        max_results=10000,
        all_users=False,
    ):
        # Filter to the end date
        if job.created and job.created > end_dt:
            continue
        # Only count query jobs
        if not isinstance(job, bigquery.QueryJob):
            continue
        jobs.append(job)
    logger.info(f"Found {len(jobs)} query jobs")
    return jobs


def print_report(
    jobs: list[bigquery.QueryJob],
    client: bigquery.Client,
    start_date: str,
    end_date: str,
) -> None:
    """Print a formatted BigQuery usage report to stdout.

    Parameters
    ----------
    jobs
        Query jobs to summarise (typically from :func:`collect_jobs`).
    client
        BigQuery client (used for the project name in the report header).
    start_date
        Report start date as ``YYYY-MM-DD`` string.
    end_date
        Report end date as ``YYYY-MM-DD`` string.
    """
    print("=" * 70)
    print(f"  BigQuery Usage Report: {start_date} to {end_date}")
    print(f"  Project: {client.project}")
    print("=" * 70)

    # --- Overall summary ---
    total_jobs = len(jobs)
    successful = sum(1 for j in jobs if j.state == "DONE" and j.errors is None)
    failed = sum(1 for j in jobs if j.state == "DONE" and j.errors is not None)
    total_billed = sum(j.total_bytes_billed or 0 for j in jobs)
    total_processed = sum(j.total_bytes_processed or 0 for j in jobs)
    cost = estimate_cost(total_billed)

    print(f"\n{'SUMMARY':=^70}")
    print(f"  Total queries:      {total_jobs}")
    print(f"  Successful:         {successful}")
    print(f"  Failed:             {failed}")
    print(f"  Bytes processed:    {bytes_to_human(total_processed)}")
    print(f"  Bytes billed:       {bytes_to_human(total_billed)}")
    print(f"  Estimated cost:     ${cost:.4f}")

    total_slot_ms = sum(getattr(j, "slot_millis", 0) or 0 for j in jobs)
    if total_slot_ms:
        slot_hours = total_slot_ms / (1000 * 3600)
        print(f"  Total slot time:    {slot_hours:.2f} slot-hours")

    # --- By dataset (from referenced tables) ---
    dataset_stats: defaultdict[str, dict[str, int]] = defaultdict(
        lambda: {"queries": 0, "bytes_billed": 0}
    )
    for j in jobs:
        refs = getattr(j, "referenced_tables", None) or []
        billed = j.total_bytes_billed or 0
        seen = set()
        for ref in refs:
            key = f"{ref.project}.{ref.dataset_id}"
            if key not in seen:
                seen.add(key)
                dataset_stats[key]["queries"] += 1
                dataset_stats[key]["bytes_billed"] += billed

    if dataset_stats:
        sorted_ds = sorted(dataset_stats.items(), key=lambda x: x[1]["bytes_billed"], reverse=True)
        print(f"\n{'BY DATASET':=^70}")
        print(f"  {'Project.Dataset':<45} {'Queries':>8} {'Billed':>14}")
        print(f"  {'-' * 45} {'-' * 8} {'-' * 14}")
        for label, stats in sorted_ds[:20]:
            print(
                f"  {label:<45} {stats['queries']:>8} {bytes_to_human(stats['bytes_billed']):>14}"
            )

    # --- Top queries ---
    billed_jobs = [j for j in jobs if (j.total_bytes_billed or 0) > 0]
    billed_jobs.sort(key=lambda j: j.total_bytes_billed or 0, reverse=True)
    top = billed_jobs[:10]

    if top:
        print(f"\n{'TOP QUERIES BY BYTES BILLED':=^70}")
        for i, j in enumerate(top, 1):
            billed = j.total_bytes_billed or 0
            dur = ""
            if j.started and j.ended:
                dur = f"{(j.ended - j.started).total_seconds():.0f}s"
            query_preview = (j.query or "")[:120].replace("\n", " ")
            print(f"\n  #{i}  {bytes_to_human(billed)} billed  |  {dur}  |  {j.user_email}")
            print(f"      {query_preview}")

    print("\n" + "=" * 70)
    print(f"  On-demand pricing: ${COST_PER_TIB}/TiB")
    print("=" * 70)


def main() -> None:
    """CLI entry point for the BigQuery budget checker.

    Parses ``--project``, ``--date``, ``--start``, and ``--end`` arguments,
    fetches query jobs for the resolved window, and prints the usage report.
    """
    parser = argparse.ArgumentParser(
        description="Check BigQuery quota usage for today or a date range."
    )
    parser.add_argument(
        "--project",
        default="mlab-collaboration",
        help="GCP project ID (default: mlab-collaboration)",
    )
    parser.add_argument(
        "--date",
        help="Single date to check (YYYY-MM-DD). Defaults to today.",
    )
    parser.add_argument(
        "--start",
        help="Start date for range (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--end",
        help="End date for range (YYYY-MM-DD).",
    )
    args = parser.parse_args()

    if args.start and args.end:
        start_date = args.start
        end_date = args.end
    elif args.date:
        start_date = args.date
        end_date = args.date
    else:
        today = datetime.now(UTC).strftime("%Y-%m-%d")
        start_date = today
        end_date = today

    start_dt = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=UTC)
    end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(
        hour=23, minute=59, second=59, tzinfo=UTC
    )

    client = get_client(args.project)
    jobs = collect_jobs(client, start_dt, end_dt)
    print_report(jobs, client, start_date, end_date)


if __name__ == "__main__":
    main()

import argparse
import logging
import multiprocessing as mp
from datetime import date, datetime, timedelta

from google.auth import default
from google.cloud import bigquery

from hermes.enrichment.main import HermesEnrichment
from hermes.pipeline.tomography import run_tomography
from hermes.sql import loader

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# SQL files executed sequentially for each date.
# 01-03 run first, then a Python enrichment step geolocates new topology IPs,
# then 04 runs the hop-level mapping against the freshly-updated geo tables.
SQL_FILES_PRE_ENRICHMENT = [
    "01_merge_upload_download_union.sql",
    "02_detect_anomalies_union.sql",
    "03_build_transient_events_union.sql",
]

SQL_FILES_POST_ENRICHMENT = [
    "04_mapping_union.sql",
    # correlation tomography now runs as a Python v2 step (Phase D)
    "05_temporal_tomography_union.sql",
]

SQL_FILES = SQL_FILES_PRE_ENRICHMENT + SQL_FILES_POST_ENRICHMENT

OUTPUT_TABLES = [
    "mlab-collaboration.hermes_union.merged_download_upload",
    "mlab-collaboration.hermes_union.anomaly_counts_union",
    "mlab-collaboration.hermes_union.transient_events_union",
    # 04_mapping_union.sql now writes both events_with_as_and_geoloc
    # AND giga_meter_measurements from a single computation (no separate step 05).
    "mlab-collaboration.hermes_union.events_with_as_and_geoloc",
    "mlab-collaboration.hermes_union.temporal_correlations",
]

# Maps each SQL file to the output table it writes to.
# Used for per-step resume: if the table already has data for a date, skip that step.
SQL_FILE_TO_OUTPUT_TABLE = dict(zip(SQL_FILES, OUTPUT_TABLES, strict=True))

FINAL_OUTPUT_TABLE = "mlab-collaboration.hermes_union.correlation_hyperedges_tomography_v2"


def print_active_credentials() -> None:
    """Log the currently active Google Cloud credentials.

    Logs whether authentication is via a service account (showing the email)
    or user Application Default Credentials.
    """
    creds, project = default()
    account_info = getattr(creds, "service_account_email", None)
    if account_info:
        logger.info(f"Authenticated as service account: {account_info}")
    else:
        logger.info(f"Authenticated as user: {creds}")


def get_existing_dates(project_id: str, table_name: str) -> set[date]:
    """Fetch the set of dates already present in a BigQuery table.

    Parameters
    ----------
    project_id
        GCP project ID used for the BigQuery client.
    table_name
        Fully-qualified BigQuery table name (``project.dataset.table``).

    Returns
    -------
    set of datetime.date
        Distinct dates found in the table's ``partition_date`` column.
    """
    client = bigquery.Client(project=project_id)
    query = f"""
        SELECT DISTINCT DATE(partition_date) AS date
        FROM `{table_name}`
    """
    query_job = client.query(query)
    results = query_job.result()
    return {row.date for row in results}


def check_input_data(project_id: str, day: date) -> set[date]:
    """Check that the NDT source has data for the 7-day window ending on ``day``.

    Queries the raw measurement source (``measurement-lab.ndt.ndt7_union``) — the
    table step 01 ingests — rather than the pipeline's own output. This reflects
    real input availability, so dates that simply have not been processed yet are
    not reported as missing.

    Parameters
    ----------
    project_id
        GCP project the BigQuery job is billed to (the source table is public).
    day
        The target date (inclusive upper bound of the window).

    Returns
    -------
    set of datetime.date
        Dates in ``[day-7, day]`` that have no rows in the NDT source.
        An empty set means all input data is present.

    Notes
    -----
    Reads only the ``date`` column over the partitions in range, but scanning a
    large public partitioned table still bills bytes — the pipeline's
    ``--skip-data-check`` flag bypasses this check when availability is already known.
    """
    client = bigquery.Client(project=project_id)
    one_week_earlier = day - timedelta(days=7)
    query = f"""
        SELECT DISTINCT date
        FROM `measurement-lab.ndt.ndt7_union`
        WHERE date BETWEEN '{one_week_earlier}' AND '{day}'
    """
    results = client.query(query).result()
    available = {row.date for row in results}
    expected = {one_week_earlier + timedelta(days=i) for i in range(8)}
    return expected - available


def dates_missing_baseline(
    dates: list[date], present_in_source: set[date], window_days: int = 7
) -> dict[date, int]:
    """Count, per target date, how many of its preceding days lack baseline data.

    Anomaly detection (step 02) compares each date against a trailing baseline
    aggregated from ``merged_download_upload`` over the preceding ``window_days``.
    A preceding day is only usable if it is *already present* in that table — a
    date scheduled earlier in the same multi-date run does NOT count, because
    Phase A runs steps 01-03 for all dates in parallel (no ordering guarantee).

    Parameters
    ----------
    dates
        Target dates being processed.
    present_in_source
        Dates already present in ``merged_download_upload``.
    window_days
        Size of the trailing baseline window (default 7).

    Returns
    -------
    dict
        Target date -> number of baseline days (of ``window_days``) that are
        unavailable. ``0`` = full baseline; ``window_days`` = no baseline at all.
    """
    missing: dict[date, int] = {}
    for day in dates:
        window = {day - timedelta(days=i) for i in range(1, window_days + 1)}
        missing[day] = len(window - present_in_source)
    return missing


def baseline_fill_dates(
    dates: list[date], present_in_source: set[date], window_days: int = 7
) -> list[date]:
    """Return the days whose step-01 output must exist for the run to be correct.

    That is every target date plus each target's preceding ``window_days`` window,
    minus whatever is already present in ``merged_download_upload``. Step 01 (the
    only prior-day pipeline output the detection depends on — steps 02/03 read
    ``merged_download_upload`` over the trailing week; traceroutes come from the raw
    ``scamper`` tables; tomography uses the target day's own events) is run for
    these before detection.

    Parameters
    ----------
    dates
        Target dates being processed.
    present_in_source
        Dates already present in ``merged_download_upload``.
    window_days
        Size of the trailing baseline window (default 7).

    Returns
    -------
    list of datetime.date
        Sorted dates needing step 01, absent from the source.
    """
    needed: set[date] = set(dates)
    for day in dates:
        needed |= {day - timedelta(days=i) for i in range(1, window_days + 1)}
    return sorted(needed - present_in_source)


def _present_in_merged(project_id: str, lo: date, hi: date) -> set[date]:
    """Return distinct dates present in ``merged_download_upload`` in ``[lo, hi]``."""
    client = bigquery.Client(project=project_id)
    query = f"""
        SELECT DISTINCT DATE(partition_date) AS date
        FROM `mlab-collaboration.hermes_union.merged_download_upload`
        WHERE partition_date BETWEEN '{lo.strftime("%Y-%m-%d")}' AND '{hi.strftime("%Y-%m-%d")}'
    """
    return {row.date for row in client.query(query).result()}


def ensure_baseline(
    project_id: str, dates: list[date], max_workers: int | None, window_days: int = 7
) -> None:
    """Auto-fill missing baseline days by running step 01 for them.

    Makes an isolated/sparse run self-sufficient: e.g. processing a single recent
    date will first populate ``merged_download_upload`` (step 01 only) for any of
    its preceding ``window_days`` that are missing, so anomaly detection has a real
    baseline. Runs to completion (barrier) before the caller starts detection, which
    also removes the multi-date Phase-A race (all step 01 commit before any step 02).
    """
    if not dates:
        return
    present = _present_in_merged(project_id, min(dates) - timedelta(days=window_days), max(dates))
    to_fill = baseline_fill_dates(dates, present, window_days)
    if not to_fill:
        return
    logger.info(
        f"Auto-baseline: running step 01 for {len(to_fill)} day(s) missing from "
        f"merged_download_upload: {', '.join(d.strftime('%Y-%m-%d') for d in to_fill)}"
    )
    merge_step = [SQL_FILES_PRE_ENRICHMENT[0]]  # 01_merge_upload_download_union.sql only
    results = _run_parallel_sql(to_fill, project_id, merge_step, max_workers, skip_data_check=True)
    for r in results:
        if not r.startswith("Success:"):
            logger.warning(f"Auto-baseline: {r}")


def warn_thin_baselines(project_id: str, dates: list[date], window_days: int = 7) -> None:
    """Log a warning for dates whose anomaly-detection baseline window is empty/thin.

    Prevents a silent "0 anomalies" result (from a missing baseline) being mistaken
    for a genuinely quiet day — e.g. when an isolated recent date is processed
    without its preceding week present in ``merged_download_upload``.
    """
    if not dates:
        return
    present = _present_in_merged(project_id, min(dates) - timedelta(days=window_days), max(dates))
    for day, n_missing in sorted(dates_missing_baseline(dates, present, window_days).items()):
        day_str = day.strftime("%Y-%m-%d")
        if n_missing >= window_days:
            logger.warning(
                f"[{day_str}] EMPTY baseline: 0/{window_days} preceding days present in "
                "merged_download_upload — anomaly detection will produce NO anomalies for this "
                "date. Process the preceding week first (sequentially, ascending dates)."
            )
        elif n_missing > 0:
            logger.warning(
                f"[{day_str}] THIN baseline: only {window_days - n_missing}/{window_days} "
                "preceding days present in merged_download_upload — anomaly detection may "
                "under-report. Consider backfilling the preceding week first."
            )


def delete_dates(project_id: str, dates: list[date]) -> None:
    """Delete rows for specific dates from all union pipeline output tables.

    Parameters
    ----------
    project_id
        GCP project ID.
    dates
        Dates to delete from every table in :data:`OUTPUT_TABLES`.
    """
    client = bigquery.Client(project=project_id)

    date_strings = [f"'{date.strftime('%Y-%m-%d')}'" for date in dates]
    date_list = ", ".join(date_strings)

    for table in OUTPUT_TABLES:
        logger.info(
            f"Deleting entries for dates: {', '.join(d.strftime('%Y-%m-%d') for d in dates)} from table: {table}"
        )
        query = f"""
            DELETE FROM `{table}`
            WHERE DATE(partition_date) IN ({date_list})
        """
        try:
            query_job = client.query(query)
            query_job.result()
            logger.info(f"Successfully deleted from {table}")
        except Exception as e:
            logger.error(f"Error deleting from {table}: {str(e)}")

    logger.info("Deletion completed for all tables")


def step_already_done(project_id: str, table_name: str, day_str: str) -> bool:
    """Return ``True`` if *table_name* already contains rows for *day_str*.

    Parameters
    ----------
    project_id
        GCP project ID.
    table_name
        Fully-qualified BigQuery table name.
    day_str
        Date string in ``YYYY-MM-DD`` format.

    Returns
    -------
    bool
        ``True`` when at least one row with ``partition_date`` equal to
        ``day_str`` exists; ``False`` otherwise.
    """
    client = bigquery.Client(project=project_id)
    query = f"""
        SELECT 1
        FROM `{table_name}`
        WHERE DATE(partition_date) = '{day_str}'
        LIMIT 1
    """
    return client.query(query).result().total_rows > 0


def execute_query(query: str, project_id: str, description: str = "") -> bigquery.QueryJob:
    """Execute a single BigQuery SQL query or multi-statement script.

    Parameters
    ----------
    query
        SQL text to execute.
    project_id
        GCP project ID.
    description
        Human-readable label logged alongside the job ID.

    Returns
    -------
    google.cloud.bigquery.QueryJob
        The completed query job.
    """
    client = bigquery.Client(project_id)
    query_job = client.query(query)
    query_job.result()
    logger.info(f"Query executed successfully. Job ID: {query_job.job_id} - {description}")
    billed_bytes = query_job.total_bytes_billed or 0
    billed_gb = billed_bytes / (1024**3)
    logger.info(f"Total bytes billed: {billed_bytes:,} bytes ({billed_gb:.2f} GB)")
    return query_job


def run_enrichment(date_str: str, project_id: str) -> None:
    """Geolocate new topology IPs found in transient_events_union.

    Runs between SQL steps 03 and 04 so that step 04's hop-level mapping
    has fresh geolocation data.  Reuses :class:`HermesEnrichment` but overrides
    the ``transient_events`` table to point at the union table.

    Parameters
    ----------
    date_str
        Target date in ``YYYY-MM-DD`` format.  The 30-day IPInfo/RIPE-IPMap
        lookback window means this covers IPs from earlier dates in the batch.
    project_id
        GCP project ID.
    """
    union_transient_table = "mlab-collaboration.hermes_union.transient_events_union"

    for ipv6 in (False, True):
        label = "IPv6" if ipv6 else "IPv4"
        logger.info(f"[enrichment] Starting {label} enrichment for {date_str}")

        enricher = HermesEnrichment(project_id=project_id, ipv6=ipv6)
        # Override the transient_events table to the union version
        # (must also propagate to child enrichers that have their own tables dict)
        enricher.tables["transient_events"] = union_transient_table
        enricher.zdns.tables["transient_events"] = union_transient_table

        # 1. Geolocate new IPs (IPInfo + RIPE IPMap) → unified_ip_to_geoloc
        enricher.process_geolocation(date_str)

        # 2 & 3. rDNS + HOIHO — skip for dates >90 days in the past
        # (lookups would not return the hostnames that were valid then)
        cutoff_str = (datetime.today() - timedelta(days=90)).strftime("%Y-%m-%d")
        if date_str >= cutoff_str:
            if ipv6:
                logger.info(
                    f"[enrichment] Skipping rDNS/HOIHO for {date_str} (IPv6 data is too large for lookups to be useful)"
                )
            else:
                enricher.zdns.process_rdns(date_str)
            enricher.process_hoiho_geolocation(date_str)
        else:
            logger.info(f"[enrichment] Skipping rDNS/HOIHO for {date_str} (>90 days in the past)")

        logger.info(f"[enrichment] Finished {label} enrichment for {date_str}")


def _run_sql_steps(date, project_id, sql_files, skip_data_check=False):
    """Run a list of SQL steps for a single date, skipping steps already done."""
    day_str = date.strftime("%Y-%m-%d")
    params = {
        "ONE_WEEK_EARLIER": (date - timedelta(days=7)).strftime("%Y-%m-%d"),
        "DAY": day_str,
    }

    # Input data availability check (only needed for pre-enrichment steps)
    if not skip_data_check and sql_files is SQL_FILES_PRE_ENRICHMENT:
        missing = check_input_data(project_id, date)
        if missing:
            missing_str = ", ".join(sorted(d.strftime("%Y-%m-%d") for d in missing))
            logger.warning(f"Missing input data for dates: {missing_str}. Skipping {day_str}.")
            return f"Skipped: {day_str} (missing input data: {missing_str})"

    for sql_file in sql_files:
        output_table = SQL_FILE_TO_OUTPUT_TABLE[sql_file]
        if step_already_done(project_id, output_table, day_str):
            logger.info(f"[{day_str}] Skipping {sql_file} — {output_table} already has data")
            continue
        logger.info(f"[{day_str}] Executing {sql_file}...")
        query = loader.load_query(sql_file, params)
        execute_query(query, project_id, f"{sql_file} for {day_str}")

    return f"Success: {day_str}"


def _run_sql_steps_worker(args):
    """Worker function for parallel SQL step execution."""
    date, project_id, sql_files, skip_data_check = args
    try:
        return _run_sql_steps(date, project_id, sql_files, skip_data_check)
    except Exception as e:
        day_str = date.strftime("%Y-%m-%d")
        logger.error(f"Error processing {day_str}: {str(e)}")
        return f"Error: {day_str} - {str(e)}"


def _run_tomography_worker(args):
    """Worker: correlation v2 then temporal v2 for one date (parallel-safe)."""
    date, project_id, backend = args
    day_str = date.strftime("%Y-%m-%d")
    try:
        run_tomography(
            date, backend=backend, project_id=project_id
        )  # → correlation_hyperedges_tomography_v2
        from hermes.pipeline import temporal_verdict

        client = bigquery.Client(project=project_id)
        if not temporal_verdict.verdicts_exist(client, day_str):
            rows = temporal_verdict.compute_temporal_verdicts(client, day_str)
            temporal_verdict.write_verdicts(client, rows)
        return f"Success: {day_str}"
    except Exception as e:
        logger.error(f"Error in Phase D (correlation+temporal) for {day_str}: {e}")
        return f"Error: {day_str} - {e}"


def _run_parallel_sql(dates, project_id, sql_files, max_workers, skip_data_check):
    """Run SQL steps for multiple dates in parallel. Returns list of result strings."""
    if not dates:
        return []

    effective_workers = max_workers or min(mp.cpu_count(), len(dates))
    worker_args = [(date, project_id, sql_files, skip_data_check) for date in dates]

    if len(dates) == 1:
        return [_run_sql_steps_worker(worker_args[0])]

    with mp.Pool(processes=effective_workers) as pool:
        return pool.map(_run_sql_steps_worker, worker_args)


def generate_date_range(start_date: date, end_date: date, interval_days: int) -> list[date]:
    """Generate a list of dates separated by a fixed interval.

    Parameters
    ----------
    start_date
        First date in the range (inclusive).
    end_date
        Last date in the range (inclusive).
    interval_days
        Step size in days between consecutive dates.

    Returns
    -------
    list of datetime.date
        Dates from ``start_date`` to ``end_date`` (inclusive) at the given
        interval, in ascending order.
    """
    dates = []
    current_date = start_date
    while current_date <= end_date:
        dates.append(current_date)
        current_date += timedelta(days=interval_days)
    return dates


def run_dates(
    dates: list[date],
    project_id: str,
    max_workers: int | None,
    skip_data_check: bool,
    dry_run: bool,
    tomography_backend: str = "python",
    auto_baseline: bool = True,
) -> None:
    """Run the full union pipeline for a batch of dates.

    Executes four phases:

    - **Phase A** — SQL steps 01-03 for all dates in parallel.
    - **Phase B** — Enrichment once (geolocation + rDNS for topology IPs,
      covering all dates via the 30-day lookback window).
    - **Phase C** — SQL steps 04 + temporal tomography for all dates in parallel.
    - **Phase D** — Python v2 correlation tomography for all dates.

    Parameters
    ----------
    dates
        Ordered list of dates to process.
    project_id
        GCP project ID.
    max_workers
        Maximum parallel workers for phases A/C.  ``None`` defaults to the
        CPU count capped by the number of dates.
    skip_data_check
        When ``True``, skip the Phase A input-data availability check.
    dry_run
        When ``True``, log what would run without executing any queries.
    tomography_backend
        Correlation tomography backend (python v2 hybrid).
    """
    if not dates:
        logger.info("No dates to process.")
        return

    if dry_run:
        for date in dates:
            day_str = date.strftime("%Y-%m-%d")
            for sql_file in SQL_FILES:
                logger.info(f"[DRY RUN] Would execute: {sql_file} with DAY={day_str}")
            logger.info(f"[DRY RUN] Would run enrichment for DAY={day_str}")
            logger.info(
                f"[DRY RUN] Would run correlation + temporal tomography (python v2) for DAY={day_str}"
            )
        return

    # Pre-flight: ensure every date's anomaly-detection baseline window exists.
    # By default, auto-fill missing baseline days (step 01 only); otherwise just warn.
    if auto_baseline:
        ensure_baseline(project_id, dates, max_workers)
    else:
        warn_thin_baselines(project_id, dates)

    # ── Phase A: steps 01-03 in parallel ──────────────────────────────────
    logger.info(f"═══ Phase A: Running steps 01-03 for {len(dates)} date(s) ═══")
    results_a = _run_parallel_sql(
        dates, project_id, SQL_FILES_PRE_ENRICHMENT, max_workers, skip_data_check
    )

    # Determine which dates succeeded phase A (eligible for enrichment + phase C)
    successful_dates = []
    for date, result in zip(dates, results_a, strict=True):
        if result.startswith("Success:"):
            successful_dates.append(date)
        else:
            logger.info(f"  {result}")

    if not successful_dates:
        logger.info("No dates completed phase A. Nothing to enrich or map.")
        return

    # ── Phase B: enrichment (single pass, covers all dates) ───────────────
    # Use the latest date as the enrichment target — the 30-day lookback
    # window will cover all IPs from earlier dates too.
    enrichment_date = max(successful_dates).strftime("%Y-%m-%d")
    logger.info(f"═══ Phase B: Running enrichment once (date={enrichment_date}) ═══")
    run_enrichment(enrichment_date, project_id)

    # ── Phase C: steps 04 + temporal tomography in parallel ─────────────
    logger.info(
        f"═══ Phase C: Running post-enrichment SQL (04 + temporal) for {len(successful_dates)} date(s) ═══"
    )
    results_c = _run_parallel_sql(
        successful_dates,
        project_id,
        SQL_FILES_POST_ENRICHMENT,
        max_workers,
        skip_data_check=True,  # no data check needed for step 04
    )

    # ── Phase D: Python v2 correlation + temporal tomography (parallel across dates) ──
    logger.info(
        f"═══ Phase D: Running correlation + temporal tomography for {len(successful_dates)} date(s) ═══"
    )
    effective_workers = max_workers or min(mp.cpu_count(), len(successful_dates))
    worker_args = [(date, project_id, tomography_backend) for date in successful_dates]

    if len(successful_dates) == 1:
        results_d = [_run_tomography_worker(worker_args[0])]
    else:
        with mp.Pool(processes=effective_workers) as pool:
            results_d = pool.map(_run_tomography_worker, worker_args)

    # ── Summary ───────────────────────────────────────────────────────────
    all_results = results_a + results_c + results_d
    successful = [r for r in all_results if r.startswith("Success:")]
    skipped = [r for r in all_results if r.startswith("Skipped:")]
    failed = [r for r in all_results if r.startswith("Error:")]

    logger.info("Pipeline completed:")
    logger.info(f"  Phase A+C successful: {len(successful)}")
    logger.info(f"  Skipped: {len(skipped)}")
    logger.info(f"  Failed: {len(failed)}")
    if failed:
        logger.warning("Failed steps:")
        for f in failed:
            logger.warning(f"  {f}")


def main() -> None:
    """CLI entry point for the Hermes Union Pipeline.

    Parses command-line arguments, resolves the date range to process (skipping
    already-completed dates unless ``--force-rerun`` is set), and delegates to
    :func:`run_dates`.
    """
    parser = argparse.ArgumentParser(description="Hermes Union Pipeline (IPv4+IPv6)")
    parser.add_argument("--start-date", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, help="End date (YYYY-MM-DD)")
    parser.add_argument("--interval", type=int, default=1, help="Interval between dates in days")
    parser.add_argument("--force-rerun", action="store_true", help="Force rerun for all dates")
    parser.add_argument(
        "--rerun-dates", type=str, nargs="+", help="Specific dates to rerun (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--delete-first", action="store_true", help="Delete existing entries before processing"
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=None,
        help="Maximum number of parallel workers (default: number of CPU cores)",
    )
    parser.add_argument(
        "--skip-data-check", action="store_true", help="Skip input data availability check"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Show what would run without executing queries"
    )
    parser.add_argument(
        "--tomography-backend",
        choices=["python"],
        default="python",
        help="Correlation tomography backend (python v2 hybrid)",
    )
    parser.add_argument(
        "--no-auto-baseline",
        action="store_true",
        help="Disable auto-filling missing baseline days (step 01 for the preceding "
        "7 days) before detection; warn instead.",
    )

    args = parser.parse_args()

    project_id = "mlab-collaboration"

    print_active_credentials()

    # Parse dates
    if args.start_date:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
    else:
        start_date = (datetime.today() - timedelta(days=2)).date()

    if args.end_date:
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
    else:
        end_date = (datetime.today() - timedelta(days=1)).date()

    # Handle specific dates to rerun
    if args.rerun_dates:
        rerun_dates = sorted(datetime.strptime(d, "%Y-%m-%d").date() for d in args.rerun_dates)
        if args.delete_first:
            delete_dates(project_id, rerun_dates)
        run_dates(
            rerun_dates,
            project_id,
            args.max_workers,
            args.skip_data_check,
            args.dry_run,
            tomography_backend=args.tomography_backend,
            auto_baseline=not args.no_auto_baseline,
        )
        return

    # Get existing dates from the final output table
    if not args.force_rerun and not args.dry_run:
        try:
            existing_dates = get_existing_dates(project_id, FINAL_OUTPUT_TABLE)
            logger.info(f"Found {len(existing_dates)} existing dates in {FINAL_OUTPUT_TABLE}")
        except Exception as e:
            logger.warning(f"Could not check existing dates ({e}). Proceeding with all dates.")
            existing_dates = set()
    else:
        existing_dates = set()

    # Generate dates with interval
    dates_to_process = generate_date_range(start_date, end_date, args.interval)

    # Delete existing entries if requested
    # if args.delete_first:
    #     delete_dates(project_id, dates_to_process)

    # Filter out already-processed dates
    dates_to_actually_process = []
    for current_date in dates_to_process:
        if current_date in existing_dates and not args.force_rerun:
            logger.info(f"Skipping date {current_date.strftime('%Y-%m-%d')} (already processed).")
        else:
            dates_to_actually_process.append(current_date)

    if not dates_to_actually_process:
        logger.info("No dates to process (all dates already exist and force-rerun not specified)")
        return

    run_dates(
        dates_to_actually_process,
        project_id,
        args.max_workers,
        args.skip_data_check,
        args.dry_run,
        tomography_backend=args.tomography_backend,
        auto_baseline=not args.no_auto_baseline,
    )


if __name__ == "__main__":
    main()

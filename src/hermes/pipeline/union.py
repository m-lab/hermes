import argparse
import concurrent.futures as cf
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

# Phase E: aggregate + root-cause join into the public-events table. Runs AFTER
# Phase D because it reads correlation_hyperedges_tomography_v2 (a Phase-D output).
SQL_FILES_PUBLIC = [
    "07_translating_to_public_format_union.sql",
]

SQL_FILES = SQL_FILES_PRE_ENRICHMENT + SQL_FILES_POST_ENRICHMENT + SQL_FILES_PUBLIC

OUTPUT_TABLES = [
    "mlab-collaboration.hermes_union.merged_download_upload",
    "mlab-collaboration.hermes_union.anomaly_counts_union",
    "mlab-collaboration.hermes_union.transient_events_union",
    # 04_mapping_union.sql now writes both events_with_as_and_geoloc
    # AND giga_meter_measurements from a single computation (no separate step 05).
    "mlab-collaboration.hermes_union.events_with_as_and_geoloc",
    "mlab-collaboration.hermes_union.temporal_correlations",
    # Phase E public-events table.
    "mlab-collaboration.hermes_union.events_explained_daily",
]

# Maps each SQL file to the output table it writes to.
# Used for per-step resume: if the table already has data for a date, skip that step.
SQL_FILE_TO_OUTPUT_TABLE = dict(zip(SQL_FILES, OUTPUT_TABLES, strict=True))

# The pipeline's true final output. main() uses this for the "already processed"
# resume check, so it must be the Phase-E table — not the tomography table.
FINAL_OUTPUT_TABLE = "mlab-collaboration.hermes_union.events_explained_daily"


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


def check_input_data(project_id: str, day: date, window_days: int = 0) -> set[date]:
    """Check that the NDT source has data for ``day`` (and optionally days before it).

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
    window_days
        How many days *before* ``day`` to check as well. Defaults to 0, i.e. the
        target day only — see the cost note below.

    Returns
    -------
    set of datetime.date
        Dates in ``[day - window_days, day]`` that have no rows in the NDT source.
        An empty set means all input data is present.

    Notes
    -----
    **This check is expensive and scales linearly with the window.** Measured by
    dry run against the live view: ~34 GiB for one day, ~264 GiB for eight,
    ~884 GiB for thirty — about 33 GiB per day of window. Partition pruning does
    work; the cost is simply reading the ``date`` column of a very large table.
    It is also run *per date*, so a 15-date backfill paid it 15 times.

    ``window_days`` therefore defaults to 0. The previous behaviour checked a
    7-day lookback, which cost ~230 GiB per date more and was **redundant**:
    a caller that fails this check merely *skips* the target date, whereas the
    trailing week is already handled properly by :func:`baseline_fill_dates` and
    :func:`dates_missing_baseline`, which check ``merged_download_upload``, fill
    missing step-01 days, and warn about a thin baseline. Pass ``window_days=7``
    to restore the old behaviour.

    ``--skip-data-check`` bypasses this entirely when availability is already known.

    The source is a VIEW over tables in another project, so its partition
    metadata is not readable from here — ``INFORMATION_SCHEMA.PARTITIONS`` cannot
    be used to answer this for free.
    """
    client = bigquery.Client(project=project_id)
    earliest = day - timedelta(days=window_days)
    query = f"""
        SELECT DISTINCT date
        FROM `measurement-lab.ndt.ndt7_union`
        WHERE date BETWEEN '{earliest}' AND '{day}'
    """
    results = client.query(query).result()
    available = {row.date for row in results}
    expected = {earliest + timedelta(days=i) for i in range(window_days + 1)}
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


def run_enrichment(date_str: str, project_id: str, lookback_days: int = 30) -> None:
    """Geolocate new topology IPs found in transient_events_union.

    Runs between SQL steps 03 and 04 so that step 04's hop-level mapping
    has fresh geolocation data.  Reuses :class:`HermesEnrichment` but overrides
    the ``transient_events`` table to point at the union table.

    Parameters
    ----------
    date_str
        Target date in ``YYYY-MM-DD`` format — the LATEST date of the batch.
    project_id
        GCP project ID.
    lookback_days
        How far back to collect candidate IPs. Enrichment runs once per batch
        while step 04 maps every date, so this must span the whole batch:
        callers should pass ``(max(dates) - min(dates)).days``. It is the
        dominant cost of enrichment — measured ~3.3 GiB for a 1-day window,
        ~20.5 GiB for 7 days and ~86.4 GiB for 30 — so a single-date nightly
        should not pay for 30. Defaults to 30 to preserve the old behaviour for
        any caller that does not size it.
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
        enricher.process_geolocation(date_str, lookback_days=lookback_days)

        # 2 & 3. rDNS + HOIHO — skip for dates >90 days in the past
        # (lookups would not return the hostnames that were valid then)
        cutoff_str = (datetime.today() - timedelta(days=90)).strftime("%Y-%m-%d")
        if date_str >= cutoff_str:
            if ipv6:
                logger.info(
                    f"[enrichment] Skipping rDNS/HOIHO for {date_str} (IPv6 data is too large for lookups to be useful)"
                )
            else:
                enricher.zdns.process_rdns(date_str, lookback_days=lookback_days)
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


def get_populated_dates(project_id: str, table_name: str) -> set[date]:
    """Dates that have a NON-EMPTY partition in ``table_name``.

    Reads ``INFORMATION_SCHEMA.PARTITIONS`` rather than scanning the table, so
    this is free metadata regardless of table size — unlike
    :func:`get_existing_dates`, whose ``SELECT DISTINCT`` scans the whole
    ``partition_date`` column (fine for the small public table, expensive if
    pointed at a multi-TiB one).

    Partitions with ``total_rows = 0`` are treated as absent: deleting a date's
    rows leaves the partition metadata behind, and such a date still needs
    re-processing. Non-date partition ids (``__NULL__``, ``__UNPARTITIONED__``)
    are ignored.

    Parameters
    ----------
    project_id
        GCP project ID used for the BigQuery client (and billed for the query).
    table_name
        Fully-qualified table name (``project.dataset.table``).

    Returns
    -------
    set of datetime.date
        Dates whose partition exists and holds at least one row.
    """
    table_project, dataset, table = table_name.split(".")
    client = bigquery.Client(project=project_id)
    query = f"""
        SELECT partition_id
        FROM `{table_project}.{dataset}.INFORMATION_SCHEMA.PARTITIONS`
        WHERE table_name = @table
          AND total_rows > 0
          AND REGEXP_CONTAINS(partition_id, r'^[0-9]{{8}}$')
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("table", "STRING", table)]
    )
    rows = client.query(query, job_config=job_config).result()
    return {datetime.strptime(row.partition_id, "%Y%m%d").date() for row in rows}


def resolve_missing_dates(
    project_id: str,
    candidate_dates: list[date],
    table_name: str,
    include_unattributed: bool = True,
) -> list[date]:
    """Narrow ``candidate_dates`` to those not properly present in ``table_name``.

    A date counts as missing when its partition is absent or empty. When
    ``table_name`` is the public events table and ``include_unattributed`` is
    set, partitions that exist but are 100% NULL ``attribution_method`` also
    count as missing — those are the silently-degraded partitions a failed
    Phase D used to produce, and they are exactly what a repair run should
    target. See :func:`find_unattributed_partitions`.

    Parameters
    ----------
    project_id
        GCP project ID used for the BigQuery client.
    candidate_dates
        Dates under consideration (typically an expanded ``--start-date`` /
        ``--end-date`` range).
    table_name
        Fully-qualified table whose coverage defines "missing".
    include_unattributed
        Also treat fully-unattributed partitions as missing. Only meaningful
        for the public events table; ignored for any other table.

    Returns
    -------
    list of datetime.date
        The subset needing processing, ascending.
    """
    populated = get_populated_dates(project_id, table_name)
    missing = {day for day in candidate_dates if day not in populated}

    if include_unattributed and table_name == FINAL_OUTPUT_TABLE:
        present = [day for day in candidate_dates if day in populated]
        degraded = find_unattributed_partitions(project_id, present)
        if degraded:
            logger.info(
                "%d date(s) present but 100%% unattributed — treating as missing: %s",
                len(degraded),
                ", ".join(day.strftime("%Y-%m-%d") for day in degraded),
            )
            missing.update(degraded)

    return sorted(missing)


def _result_is_success_for(result: str, days: set[date]) -> bool:
    """True if ``result`` is a ``Success:`` line for a date in ``days``."""
    if not result.startswith("Success: "):
        return False
    try:
        parsed = datetime.strptime(result[len("Success: ") :].strip(), "%Y-%m-%d").date()
    except ValueError:
        return False
    return parsed in days


def find_unattributed_partitions(project_id: str, dates: list[date]) -> list[date]:
    """Return dates whose ``events_explained_daily`` partition has NO attribution.

    A partition where *every* row has ``attribution_method IS NULL`` means the
    correlation tomography output was missing when ``06`` ran: its "unresolved"
    branch then matches every anomalous pair and emits a normal-sized partition
    with all root-cause columns NULL. Row counts look healthy, so this is only
    detectable by checking the attribution columns.

    Safe to call with an arbitrary date list, which makes it usable as a
    standalone audit over history, not just over a single run.

    Parameters
    ----------
    project_id
        GCP project ID to bill the check to.
    dates
        Dates to check. An empty list short-circuits without querying.

    Returns
    -------
    list[date]
        Dates whose partition is fully unattributed, ascending.
    """
    if not dates:
        return []

    client = bigquery.Client(project=project_id)
    query = f"""
        SELECT partition_date
        FROM `{FINAL_OUTPUT_TABLE}`
        WHERE partition_date IN UNNEST(@days)
        GROUP BY partition_date
        HAVING COUNTIF(attribution_method IS NOT NULL) = 0
        ORDER BY partition_date
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("days", "DATE", list(dates))]
    )
    try:
        return [row.partition_date for row in client.query(query, job_config=job_config).result()]
    except Exception as e:
        # A failed audit must not mask the pipeline's own result.
        logger.error(f"Could not run unattributed-partition check: {e}")
        return []


def _run_tomography_worker(args):
    """Worker: correlation v2 then temporal v2 for one date (parallel-safe)."""
    date, project_id, backend = args
    day_str = date.strftime("%Y-%m-%d")
    try:
        # write_multigranularity=True matches deployed behaviour on hermes-ec2,
        # which populates correlation_culprits_multigranularity and
        # correlation_entity_stats_multigranularity on every run. The default is
        # False (the parameter was added to gate an out-of-scope phase), so
        # leaving it unset here would silently stop writing both tables.
        # Their DDLs are bootstrapped via bootstrap_tables.DDL_FILES.
        run_tomography(
            date,
            backend=backend,
            project_id=project_id,
            write_multigranularity=True,
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


def _run_phase_d_tomography(worker_args, tomo_workers):
    """Run Phase-D correlation tomography with crash-resilient parallelism.

    Phase-D workers are memory-hungry: a single date loads millions of
    node-edge rows and can spike to ~20 GB RSS, so a worker can be SIGKILLed by
    the cgroup OOM-killer. ``multiprocessing.Pool.map`` deadlocks permanently in
    that case — the killed task's result is never delivered and ``map()`` waits
    forever (this wedged the April backfill and the July 10/11 dates).

    We use :class:`concurrent.futures.ProcessPoolExecutor` instead, which raises
    :class:`BrokenProcessPool` when a worker dies rather than hanging. Settings:

    * ``max_tasks_per_child=1`` — a fresh process per date, so memory never
      accumulates across dates.
    * a killed worker (OOM) surfaces as ``BrokenProcessPool``; the dates that
      did not complete are retried once, serialized at 1 worker to avoid a
      repeat OOM, so a single bad date can't take the whole batch down.

    Returns results in the same order as ``worker_args`` (list of ``"Success:"``
    / ``"Error:"`` strings), matching the ``Pool.map`` contract.
    """
    ctx = mp.get_context("spawn")
    results_by_date: dict[date, str] = {}
    remaining = list(worker_args)
    workers = max(1, tomo_workers)

    while remaining:
        batch = remaining
        remaining = []
        n = min(workers, len(batch))
        try:
            with cf.ProcessPoolExecutor(max_workers=n, mp_context=ctx, max_tasks_per_child=1) as ex:
                fut_to_arg = {ex.submit(_run_tomography_worker, wa): wa for wa in batch}
                for fut in cf.as_completed(fut_to_arg):
                    d = fut_to_arg[fut][0]
                    try:
                        # Handled errors come back as an "Error:" string (final);
                        # only a hard worker death (OOM/segfault) raises here.
                        results_by_date[d] = fut.result()
                    except cf.process.BrokenProcessPool:
                        pass  # leave unset → retried below
                    except Exception as e:  # noqa: BLE001 - defensive
                        results_by_date[d] = f"Error: {d.strftime('%Y-%m-%d')} - {e}"
        except cf.process.BrokenProcessPool as e:
            logger.error(f"Phase D pool broke (worker died, likely OOM): {e}")

        # Any date without a result had its worker killed — retry, but serialized.
        not_done = [wa for wa in batch if wa[0] not in results_by_date]
        if not_done and workers > 1:
            logger.warning(
                f"Retrying {len(not_done)} Phase-D date(s) at 1 worker after a "
                f"worker death: {[wa[0].strftime('%Y-%m-%d') for wa in not_done]}"
            )
            remaining = not_done
            workers = 1
        elif not_done:
            # Already at 1 worker and it still died — the date itself OOMs even
            # in isolation. Record a loud error instead of looping forever.
            for wa in not_done:
                d = wa[0]
                results_by_date[d] = (
                    f"Error: {d.strftime('%Y-%m-%d')} - tomography worker died "
                    f"(likely OOM) even at 1 worker; needs a lower memory footprint"
                )

    return [results_by_date[wa[0]] for wa in worker_args]


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
    tomography_workers: int | None = None,
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
            logger.info(f"[DRY RUN] Would run public-format step (Phase E) for DAY={day_str}")
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
    # Size the candidate-collection window to the actual batch rather than a
    # fixed 30 days. Enrichment runs once but step 04 maps every date, so the
    # window must span the batch — and only the batch. A single-date nightly
    # previously scanned 30 days of transient_events (~86 GiB) to collect IPs
    # for one day (~3 GiB).
    enrichment_lookback = (max(successful_dates) - min(successful_dates)).days
    logger.info(
        f"═══ Phase B: Running enrichment once (date={enrichment_date}, "
        f"lookback={enrichment_lookback}d) ═══"
    )
    run_enrichment(enrichment_date, project_id, lookback_days=enrichment_lookback)

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
    # Phase D is throttled INDEPENDENTLY of max_workers and defaults to a SINGLE
    # worker: one date can spike to ~20 GB RSS, so 2+ workers OOM the container
    # cgroup. Do not reuse max_workers here — the scan-bound SQL phases can
    # safely run 3-wide, Phase D cannot. The runner is crash-resilient
    # (ProcessPoolExecutor, not Pool.map) so an OOM fails loudly instead of
    # deadlocking — see _run_phase_d_tomography.
    tomo_workers = tomography_workers or 1
    logger.info(f"  Phase D parallelism: {tomo_workers} worker(s)")
    worker_args = [(date, project_id, tomography_backend) for date in successful_dates]

    if len(successful_dates) == 1:
        results_d = [_run_tomography_worker(worker_args[0])]
    else:
        results_d = _run_phase_d_tomography(worker_args, tomo_workers)

    # ── Phase E: public-format aggregation (parallel across dates) ──────
    # Runs after Phase D: reads correlation_hyperedges_tomography_v2 to attach
    # root-cause entities, writing the public events_explained_daily table.
    #
    # Phase E MUST be gated on Phase D succeeding for the same date. 06's
    # "unresolved" branch selects every anomalous pair NOT IN the correlation
    # table, so when Phase D produced nothing that branch matches *everything*
    # and the step happily writes a normal-sized partition with every
    # attribution column NULL. That looks complete to the dashboard and to
    # step_already_done(), so the bad data is sticky and invisible. Leaving the
    # partition absent instead is strictly better: absent is visible and
    # re-runnable. (Silently produced 11 fully-unattributed days before this
    # gate existed.)
    tomography_ok = {
        day
        for day, result in zip(successful_dates, results_d, strict=True)
        if result.startswith("Success:")
    }
    dates_for_e = [day for day in successful_dates if day in tomography_ok]
    tomography_failed = [day for day in successful_dates if day not in tomography_ok]
    if tomography_failed:
        logger.error(
            "Phase E SKIPPED for %d date(s) whose Phase D failed — writing the public "
            "table for these would silently produce 100%% NULL attribution: %s",
            len(tomography_failed),
            ", ".join(day.strftime("%Y-%m-%d") for day in tomography_failed),
        )

    logger.info(f"═══ Phase E: Building public events table for {len(dates_for_e)} date(s) ═══")
    results_e = _run_parallel_sql(
        dates_for_e,
        project_id,
        SQL_FILES_PUBLIC,
        max_workers,
        skip_data_check=True,
    )
    results_e += [f"Error: {day} - Phase D failed, Phase E skipped" for day in tomography_failed]

    # ── Post-Phase-E integrity check ──────────────────────────────────────
    # Belt-and-braces for the case the gate above cannot catch: Phase D
    # "succeeds" but writes zero hyperedges, so 06 still emits an
    # all-unattributed partition. Verify what actually landed.
    degraded = find_unattributed_partitions(project_id, dates_for_e)
    if degraded:
        logger.error(
            "INTEGRITY: %d partition(s) written by Phase E have 100%% NULL "
            "attribution_method (tomography output was empty). These need Phase D+E "
            "re-run and should NOT be trusted: %s",
            len(degraded),
            ", ".join(day.strftime("%Y-%m-%d") for day in degraded),
        )
        degraded_set = set(degraded)
        results_e = [r for r in results_e if not _result_is_success_for(r, degraded_set)]
        results_e += [f"Error: {day} - Phase E wrote 100% NULL attribution" for day in degraded]

    # ── Summary ───────────────────────────────────────────────────────────
    all_results = results_a + results_c + results_d + results_e
    successful = [r for r in all_results if r.startswith("Success:")]
    skipped = [r for r in all_results if r.startswith("Skipped:")]
    failed = [r for r in all_results if r.startswith("Error:")]

    logger.info("Pipeline completed:")
    logger.info(f"  Pipeline steps successful: {len(successful)}")
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
    parser.add_argument(
        "--tomography-workers",
        type=int,
        default=None,
        help="Maximum parallel workers for Phase D correlation tomography "
        "(default: 1). Throttled separately from --max-workers: a single date can "
        "spike to ~20 GB RSS, so 2+ workers OOM the container; only raise once the "
        "cover's memory footprint is reduced.",
    )
    parser.add_argument(
        "--fill-missing",
        action="store_true",
        help="Between --start-date and --end-date, process ONLY the dates whose "
        "partition is absent or empty in the reference table (see "
        "--fill-missing-table), i.e. fill the gaps. For the public events table, "
        "partitions that exist but are 100%% unattributed also count as missing. "
        "Coverage is read from INFORMATION_SCHEMA.PARTITIONS, so the check is free "
        "regardless of table size. Unlike the implicit default, this is honoured "
        "under --dry-run, so the preview matches the real run.",
    )
    parser.add_argument(
        "--fill-missing-table",
        type=str,
        default=FINAL_OUTPUT_TABLE,
        help=f"Table whose coverage defines 'missing' for --fill-missing "
        f"(default: {FINAL_OUTPUT_TABLE}).",
    )

    args = parser.parse_args()

    if args.fill_missing and args.force_rerun:
        parser.error(
            "--fill-missing and --force-rerun are mutually exclusive: the first "
            "processes only absent dates, the second reprocesses every date."
        )
    if args.fill_missing and args.rerun_dates:
        parser.error(
            "--fill-missing operates on a --start-date/--end-date range; "
            "it cannot be combined with --rerun-dates."
        )

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
            tomography_workers=args.tomography_workers,
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

    # --fill-missing: run only the gaps in the reference table. Explicit
    # counterpart to the implicit skip below, and unlike it this is applied
    # under --dry-run too, so the preview matches what a real run would do.
    if args.fill_missing:
        missing = resolve_missing_dates(project_id, dates_to_process, args.fill_missing_table)
        if not missing:
            logger.info(
                "--fill-missing: no gaps in %s between %s and %s — nothing to do.",
                args.fill_missing_table,
                start_date.strftime("%Y-%m-%d"),
                end_date.strftime("%Y-%m-%d"),
            )
            return
        logger.info(
            "--fill-missing: %d of %d date(s) missing from %s — processing only these: %s",
            len(missing),
            len(dates_to_process),
            args.fill_missing_table,
            ", ".join(day.strftime("%Y-%m-%d") for day in missing),
        )
        run_dates(
            missing,
            project_id,
            args.max_workers,
            args.skip_data_check,
            args.dry_run,
            tomography_backend=args.tomography_backend,
            tomography_workers=args.tomography_workers,
            auto_baseline=not args.no_auto_baseline,
        )
        return

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
        tomography_workers=args.tomography_workers,
        auto_baseline=not args.no_auto_baseline,
    )


if __name__ == "__main__":
    main()

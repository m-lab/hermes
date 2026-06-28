#!/usr/bin/env python3
"""
Table Rerun Script for Hermes Pipeline

This script allows you to rerun results for a specific table only.
It can delete specific dates from the table and then run the corresponding SQL
to regenerate the data for those dates.

Usage:
    python table_rerun.py --table TABLE_NAME --dates 2024-01-01 2024-01-02 --sql-file legacy_detecting_events.sql
    python table_rerun.py --table TABLE_NAME --start-date 2024-01-01 --end-date 2024-01-07 --sql-file legacy_mapping_events.sql
"""

import argparse
import datetime as _dt
import logging
import multiprocessing as mp
import os
import sys
from datetime import datetime, timedelta
from string import Template

from google.auth import default
from google.cloud import bigquery

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def print_active_credentials() -> None:
    """Log the currently active Google Cloud credentials."""
    creds, project = default()
    account_info = getattr(creds, "service_account_email", None)
    if account_info:
        logger.info(f"Authenticated as service account: {account_info}")
    else:
        logger.info(f"Authenticated as user: {creds}")


def load_sql(file_path: str, params: dict) -> str:
    """Load an SQL file and substitute ``${...}`` placeholders with parameters.

    Parameters
    ----------
    file_path
        Absolute or relative path to the ``.sql`` file.
    params
        Mapping used for ``Template.safe_substitute``.

    Returns
    -------
    str
        SQL text with placeholders substituted.
    """
    try:
        with open(file_path, encoding="utf-8") as f:
            sql_template = Template(f.read())
        return sql_template.safe_substitute(params)
    except FileNotFoundError:
        logger.error(f"SQL file not found: {file_path}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error loading SQL file {file_path}: {str(e)}")
        sys.exit(1)


def get_existing_dates(project_id: str, table_name: str) -> set[_dt.date]:
    """Fetch the set of dates already present in a BigQuery table.

    Parameters
    ----------
    project_id
        GCP project ID.
    table_name
        Fully-qualified BigQuery table name.

    Returns
    -------
    set of datetime.date
        Distinct dates found in the table's ``partition_date`` column.
        Returns an empty set on error (logged at ``ERROR`` level).
    """
    client = bigquery.Client(project=project_id)
    query = f"""
        SELECT DISTINCT DATE(partition_date) AS date
        FROM `{table_name}`
        ORDER BY date
    """
    try:
        query_job = client.query(query)
        results = query_job.result()
        return {row.date for row in results}
    except Exception as e:
        logger.error(f"Error fetching existing dates from {table_name}: {str(e)}")
        return set()


def delete_dates_from_table(project_id: str, table_name: str, dates: list[_dt.date]) -> None:
    """Delete rows for specific dates from the specified BigQuery table.

    Parameters
    ----------
    project_id
        GCP project ID.
    table_name
        Fully-qualified BigQuery table name.
    dates
        Dates to delete (matched against the ``partition_date`` column).
    """
    if not dates:
        logger.warning("No dates provided for deletion")
        return

    client = bigquery.Client(project=project_id)

    # Convert dates to strings for the query
    date_strings = [f"'{date.strftime('%Y-%m-%d')}'" for date in dates]
    date_list = ", ".join(date_strings)

    logger.info(
        f"Deleting entries for dates: {', '.join(date.strftime('%Y-%m-%d') for date in dates)} from table: {table_name}"
    )

    query = f"""
        DELETE FROM `{table_name}`
        WHERE DATE(partition_date) IN ({date_list})
    """

    try:
        query_job = client.query(query)
        query_job.result()
        logger.info(f"Successfully deleted from {table_name}")
    except Exception as e:
        logger.error(f"Error deleting from {table_name}: {str(e)}")
        sys.exit(1)


def execute_query(query: str, project_id: str, description: str = "") -> bigquery.QueryJob:
    """Execute a single BigQuery SQL query.

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

    logger.info(f"Executing query: {description}")
    query_job = client.query(query)
    query_job.result()

    logger.info(f"Query executed successfully. Job ID: {query_job.job_id}")
    billed_bytes = query_job.total_bytes_billed
    billed_gb = billed_bytes / (1024**3)
    logger.info(f"Total bytes billed: {billed_bytes:,} bytes ({billed_gb:.2f} GB)")

    return query_job


def process_date_with_sql(date: _dt.date, project_id: str, sql_folder: str, sql_file: str) -> str:
    """Process a single date with the specified SQL file.

    Parameters
    ----------
    date
        The date to process; substituted as ``${DAY}`` and ``${ONE_WEEK_EARLIER}``.
    project_id
        GCP project ID.
    sql_folder
        Directory containing the SQL file.
    sql_file
        SQL filename (relative to ``sql_folder``).

    Returns
    -------
    str
        ``"Success: YYYY-MM-DD"`` on completion.
    """
    # Set up logging for this process
    process_logger = logging.getLogger(f"process_{date.strftime('%Y-%m-%d')}")
    process_logger.setLevel(logging.INFO)

    # Create console handler if it doesn't exist
    if not process_logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        )
        process_logger.addHandler(handler)

    params = {
        "ONE_WEEK_EARLIER": (date - timedelta(days=7)).strftime("%Y-%m-%d"),
        "DAY": date.strftime("%Y-%m-%d"),
    }

    process_logger.info(f"Processing for date: {params['DAY']} with SQL file: {sql_file}")

    sql_path = os.path.join(sql_folder, sql_file)
    process_logger.info(f"Loading SQL from {sql_path}...")

    query = load_sql(sql_path, params)
    execute_query(query, project_id, f"Processing {sql_file} for {params['DAY']}")

    process_logger.info(f"Completed processing for date: {params['DAY']}")
    return f"Success: {params['DAY']}"


def process_date_worker(args: tuple) -> str:
    """Worker function for multiprocessing — processes a single date."""
    date, project_id, sql_folder, sql_file = args
    try:
        return process_date_with_sql(date, project_id, sql_folder, sql_file)
    except Exception as e:
        logger.error(f"Error processing date {date.strftime('%Y-%m-%d')}: {str(e)}")
        return f"Error: {date.strftime('%Y-%m-%d')} - {str(e)}"


def generate_date_range(
    start_date: _dt.date, end_date: _dt.date, interval_days: int = 1
) -> list[_dt.date]:
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
        Dates from ``start_date`` to ``end_date`` at the given interval.
    """
    dates = []
    current_date = start_date
    while current_date <= end_date:
        dates.append(current_date)
        current_date += timedelta(days=interval_days)
    return dates


def validate_table_name(table_name: str) -> bool:
    """Validate that *table_name* follows the ``project.dataset.table`` format.

    Parameters
    ----------
    table_name
        Table name to validate.

    Returns
    -------
    bool
        ``True`` if the name contains at least one dot; ``False`` otherwise.
    """
    if not table_name or "." not in table_name:
        logger.error("Table name must be in format: project.dataset.table")
        return False
    return True


def main() -> None:
    """CLI entry point for the table-rerun script.

    Parses arguments, optionally deletes existing rows, and processes the
    specified dates (sequentially for one date, in parallel for many).
    """
    # Set up argument parser
    parser = argparse.ArgumentParser(description="Hermes Table Rerun Script")
    parser.add_argument(
        "--table", type=str, required=True, help="Full table name (project.dataset.table)"
    )
    parser.add_argument(
        "--sql-file",
        type=str,
        required=True,
        help="SQL file name to execute (e.g., legacy_detecting_events.sql)",
    )
    parser.add_argument("--start-date", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, help="End date (YYYY-MM-DD)")
    parser.add_argument(
        "--dates", type=str, nargs="+", help="Specific dates to process (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--interval", type=int, default=1, help="Interval between dates in days (default: 1)"
    )
    parser.add_argument(
        "--delete-first", action="store_true", help="Delete existing entries before processing"
    )
    parser.add_argument(
        "--sql-folder",
        type=str,
        default="../sql_queries/hermes_core",
        help="Path to SQL files folder (default: ../sql_queries/hermes_core)",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Show what would be done without executing"
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=None,
        help="Maximum number of parallel workers (default: number of CPU cores)",
    )

    args = parser.parse_args()

    # Validate table name
    if not validate_table_name(args.table):
        sys.exit(1)

    # Set up project ID
    project_id = args.table.split(".")[0]

    # Print credentials
    print_active_credentials()

    # Parse dates
    if args.dates:
        # Specific dates provided
        dates_to_process = [datetime.strptime(date, "%Y-%m-%d").date() for date in args.dates]
    elif args.start_date and args.end_date:
        # Date range provided
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
        dates_to_process = generate_date_range(start_date, end_date, args.interval)
    else:
        logger.error("Either --dates or both --start-date and --end-date must be provided")
        sys.exit(1)

    if not dates_to_process:
        logger.error("No dates to process")
        sys.exit(1)

    # Sort dates for consistent processing
    dates_to_process.sort()

    logger.info(f"Table: {args.table}")
    logger.info(f"SQL file: {args.sql_file}")
    logger.info(f"Dates to process: {[d.strftime('%Y-%m-%d') for d in dates_to_process]}")
    logger.info(f"Delete first: {args.delete_first}")
    logger.info(f"Dry run: {args.dry_run}")

    if args.dry_run:
        logger.info("DRY RUN - No actual operations will be performed")
        if args.delete_first:
            logger.info(
                f"Would delete dates from {args.table}: {[d.strftime('%Y-%m-%d') for d in dates_to_process]}"
            )

        if len(dates_to_process) == 1:
            logger.info(
                f"Would process single date {dates_to_process[0].strftime('%Y-%m-%d')} with {args.sql_file}"
            )
        else:
            max_workers = args.max_workers or min(mp.cpu_count(), len(dates_to_process))
            logger.info(
                f"Would process {len(dates_to_process)} dates in parallel with {max_workers} workers:"
            )
            for date in dates_to_process:
                logger.info(
                    f"  Would process date {date.strftime('%Y-%m-%d')} with {args.sql_file}"
                )
        return

    # Get existing dates from the table
    existing_dates = get_existing_dates(project_id, args.table)
    logger.info(f"Existing dates in {args.table}: {sorted(existing_dates)}")

    # Delete existing entries if requested
    if args.delete_first:
        delete_dates_from_table(project_id, args.table, dates_to_process)

    # Process dates in parallel
    if len(dates_to_process) == 1:
        # Single date - process sequentially
        logger.info(f"Processing single date: {dates_to_process[0].strftime('%Y-%m-%d')}")
        process_date_with_sql(dates_to_process[0], project_id, args.sql_folder, args.sql_file)
    else:
        # Multiple dates - process in parallel
        max_workers = args.max_workers or min(mp.cpu_count(), len(dates_to_process))
        logger.info(
            f"Processing {len(dates_to_process)} dates in parallel with {max_workers} workers"
        )

        # Prepare arguments for each worker
        worker_args = [
            (date, project_id, args.sql_folder, args.sql_file) for date in dates_to_process
        ]

        # Process in parallel
        with mp.Pool(processes=max_workers) as pool:
            results = pool.map(process_date_worker, worker_args)

        # Log results
        successful = [r for r in results if r.startswith("Success:")]
        failed = [r for r in results if r.startswith("Error:")]

        logger.info("Parallel processing completed:")
        logger.info(f"  Successful: {len(successful)} dates")
        logger.info(f"  Failed: {len(failed)} dates")

        if failed:
            logger.warning("Failed dates:")
            for failure in failed:
                logger.warning(f"  {failure}")

    logger.info("Table rerun completed successfully!")


if __name__ == "__main__":
    main()

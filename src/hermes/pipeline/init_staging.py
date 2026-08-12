"""Create the staging twins of the pipeline's operational tables.

``--target staging`` writes with ``INSERT INTO``, which requires the destination to
exist, so the staging dataset has to be provisioned before the first run. Every
table is created **empty** with the production schema, partitioning and clustering
copied, so a staging run exercises the same DDL constraints production does.

    python -m hermes.pipeline.init_staging                 # show the plan
    python -m hermes.pipeline.init_staging --create        # create what is missing
    python -m hermes.pipeline.init_staging --copy-inputs \
        --start 2026-08-01 --end 2026-08-08                # seed merged_download_upload

Reference datasets (``hermes``, ``measurement-lab``) are deliberately shared with
production and are never copied: a staging run must resolve the same geolocation,
AS metadata and IXP data, or the comparison against production is meaningless.

``--copy-inputs`` exists because step 02's baseline reads a trailing week of
``merged_download_upload``. Copying those partitions from production is far cheaper
than re-running step 01 (which re-scans the raw NDT tables) and keeps the staging
run comparable to production by construction.
"""

from __future__ import annotations

import argparse
import logging
from datetime import date, timedelta

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from hermes.pipeline.union import (
    DEFAULT_DATASET,
    PROJECT,
    STAGING_DATASET,
    delete_tables,
    metro_polygons_for,
)

logger = logging.getLogger(__name__)

#: Read by step 02's baseline window; seedable with --copy-inputs.
INPUT_TABLE = "merged_download_upload"


def staging_targets() -> list[str]:
    """Every operational table a staging run reads or writes, GIGA included."""
    return sorted(set(delete_tables(STAGING_DATASET, include_giga=True)))


def _exists(client: bigquery.Client, table: str) -> bool:
    try:
        client.get_table(table)
        return True
    except NotFound:
        return False


def plan(client: bigquery.Client) -> tuple[list[str], list[str]]:
    """Split the staging tables into (missing, present)."""
    missing: list[str] = []
    present: list[str] = []
    for table in staging_targets():
        (present if _exists(client, table) else missing).append(table)
    return missing, present


def create_missing(client: bigquery.Client, dry_run: bool = False) -> list[str]:
    """Create each missing staging table empty, mirroring the production schema.

    ``CREATE TABLE LIKE`` copies schema, partitioning and clustering without any
    rows, which is exactly the property wanted here: identical DDL, no data.
    """
    missing, _ = plan(client)
    created = []
    for target in missing:
        source = target.replace(f".{STAGING_DATASET}.", f".{DEFAULT_DATASET}.")
        if not _exists(client, source):
            logger.warning("skipping %s — production source %s does not exist", target, source)
            continue
        sql = f"CREATE TABLE IF NOT EXISTS `{target}` LIKE `{source}`"
        if dry_run:
            logger.info("[dry-run] %s", sql)
        else:
            client.query(sql).result()
            logger.info("created %s", target)
        created.append(target)
    return created


def copy_input_partitions(
    client: bigquery.Client, start: date, end: date, dry_run: bool = False
) -> int:
    """Copy ``merged_download_upload`` partitions from production into staging.

    Overwrites the same date range in staging so re-running is idempotent.
    """
    src = f"{PROJECT}.{DEFAULT_DATASET}.{INPUT_TABLE}"
    dst = f"{PROJECT}.{STAGING_DATASET}.{INPUT_TABLE}"
    delete = f"""
        DELETE FROM `{dst}`
        WHERE partition_date BETWEEN DATE '{start:%Y-%m-%d}' AND DATE '{end:%Y-%m-%d}'
    """
    insert = f"""
        INSERT INTO `{dst}`
        SELECT * FROM `{src}`
        WHERE partition_date BETWEEN DATE '{start:%Y-%m-%d}' AND DATE '{end:%Y-%m-%d}'
    """
    if dry_run:
        for sql in (delete, insert):
            job = client.query(sql, job_config=bigquery.QueryJobConfig(dry_run=True))
            gib = job.total_bytes_processed / 2**30
            logger.info("[dry-run] %.2f GiB ~= $%.3f", gib, gib / 1024 * 6.25)
        return 0
    client.query(delete).result()
    job = client.query(insert)
    job.result()
    logger.info(
        "copied %s..%s of %s into staging (%d rows, %.2f GiB billed)",
        start,
        end,
        INPUT_TABLE,
        job.num_dml_affected_rows or 0,
        job.total_bytes_billed / 2**30,
    )
    return job.num_dml_affected_rows or 0


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--project-id", default=PROJECT, help="billing/job project")
    ap.add_argument("--create", action="store_true", help="create missing staging tables")
    ap.add_argument("--copy-inputs", action="store_true", help=f"seed staging {INPUT_TABLE}")
    ap.add_argument("--start", help="first date for --copy-inputs (YYYY-MM-DD)")
    ap.add_argument("--end", help="last date for --copy-inputs (YYYY-MM-DD)")
    ap.add_argument(
        "--baseline-days",
        type=int,
        default=7,
        help="extra trailing days to copy before --start, for step 02's baseline "
        "window (default: 7)",
    )
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    client = bigquery.Client(project=args.project_id)

    missing, present = plan(client)
    print(f"staging dataset: {PROJECT}.{STAGING_DATASET}")
    print(f"  metro polygons: {metro_polygons_for(STAGING_DATASET)}")
    print(f"  present: {len(present)}   missing: {len(missing)}")
    for t in missing:
        print(f"    MISSING  {t.split('.')[-1]}")

    if args.create:
        created = create_missing(client, dry_run=args.dry_run)
        print(f"  created {len(created)} table(s)")

    if args.copy_inputs:
        if not (args.start and args.end):
            ap.error("--copy-inputs requires --start and --end")
        start = date.fromisoformat(args.start) - timedelta(days=args.baseline_days)
        end = date.fromisoformat(args.end)
        print(
            f"  copying {INPUT_TABLE} {start}..{end} (includes {args.baseline_days} baseline days)"
        )
        copy_input_partitions(client, start, end, dry_run=args.dry_run)


if __name__ == "__main__":
    main()

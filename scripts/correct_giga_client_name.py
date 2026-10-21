#!/usr/bin/env python3
"""Correct giga_meter_measurements for the group-level client_name bug (PR #61).

Before #61, step 03 gave every test its group's MIN(client_name), and step 04 put a
test in giga_meter_measurements when that label was 'giga-meter' or its IP was on the
month's school list. So, for analysis dates written before the fix, the GIGA table
both misses Giga Meter tests and holds tests from other clients.

Per analysis date D, oldest first, in one transaction:

1. DELETE rows whose own client_name (step 01, merged_download_upload) is not
   'giga-meter' and whose IP is not on the month's school list.
2. UPDATE client_name on the remaining rows to each test's own name.
3. INSERT (only for D >= --insert-from) every trace in events_with_as_and_geoloc on D
   whose own name is 'giga-meter' or whose IP is listed, unless the GIGA table already
   holds that trace on any date D-7..D+7. That is step 04's one-row-per-trace rule
   (it checks D-7..D), widened forward because later dates were also written before
   this correction. The row is copied whole; only client_name becomes the test's own.

The month's list is read as it is now, so a corrected date matches what the fixed
step 04 would write today -- including school IPs filled in after the fact.

events_with_as_and_geoloc and transient_events_union keep their group-level
client_name: nothing downstream reads it, and rewriting them costs ~127 GiB per date.

Planned production run (dry-run priced 2026-10-21): DELETE + UPDATE for 2025-11-01..
2026-10-19 (~4.5 GiB/date), INSERT from 2026-08-01, where misses reach 42-52% by
September-October: ~8.4 TiB in total, of which INSERT for Aug 1..Oct 19 is 6.79 TiB.
June-July misses (5-11%) are left as they are.

Re-running a date is a no-op (nothing left to delete, update or insert), so the run is
resumable; --log records each finished date and later runs skip it.

    python scripts/correct_giga_client_name.py --dataset hermes_staging \\
        --giga-table zz_surg_giga --start 2026-09-10 --end 2026-09-10 \\
        --insert-from 2026-09-10 --plan                     # counts only
    python scripts/correct_giga_client_name.py --start 2025-11-01 --end 2026-10-19 \\
        --insert-from 2026-08-01 --log correction.jsonl --max-tib-today 8
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import date, timedelta
from pathlib import Path

from google.cloud import bigquery

PROJECT = "mlab-collaboration"
#: merged_download_upload partitions that can hold the tests of one analysis date:
#: step 03 attaches traces over a 7-day lookback, plus one day of slack.
OWN_LOOKBACK_DAYS = 8
#: Per-request timeout and polling interval while a date's transaction runs.
REQUEST_TIMEOUT_S = 60
POLL_S = 30


def own_names(ds: str) -> str:
    """Each test's own client_name, from step 01, for the window ending @d."""
    return f"""(
      SELECT id, ANY_VALUE(client_name) AS own
      FROM `{PROJECT}.{ds}.merged_download_upload`
      WHERE partition_date BETWEEN DATE_SUB(@d, INTERVAL {OWN_LOOKBACK_DAYS} DAY) AND @d
      GROUP BY id)"""


def month_list(ds: str) -> str:
    return f"""(
      SELECT ip_address FROM `{PROJECT}.{ds}.giga_school_ips`
      WHERE month_start = DATE_TRUNC(@d, MONTH) AND ip_address IS NOT NULL)"""


def delete_sql(ds: str, giga: str) -> str:
    return f"""
DELETE FROM `{PROJECT}.{ds}.{giga}` g
WHERE g.partition_date = @d
  AND g.id NOT IN (SELECT id FROM {own_names(ds)} WHERE own = 'giga-meter')
  AND (g.src IS NULL OR g.src NOT IN {month_list(ds)})"""


def update_sql(ds: str, giga: str) -> str:
    return f"""
UPDATE `{PROJECT}.{ds}.{giga}` g
SET client_name = o.own
FROM {own_names(ds)} o
WHERE g.partition_date = @d AND g.id = o.id
  AND g.client_name IS DISTINCT FROM o.own"""


def missing_traces(ds: str, giga: str, events: str) -> str:
    """Rows of events on @d that belong in the GIGA table but have no row there.

    One per trace, keyed like step 04's NOT EXISTS; a copy on any date D-7..D+7 counts.
    """
    return f"""
FROM `{PROJECT}.{ds}.{events}` e
LEFT JOIN {own_names(ds)} o ON o.id = e.id
WHERE e.partition_date = @d
  AND (o.own = 'giga-meter' OR e.src IN {month_list(ds)})
  AND NOT EXISTS (
    SELECT 1 FROM `{PROJECT}.{ds}.{giga}` g
    WHERE g.partition_date BETWEEN DATE_SUB(@d, INTERVAL 7 DAY)
                               AND DATE_ADD(@d, INTERVAL 7 DAY)
      AND g.id = e.id AND g.src_asn = e.src_asn AND g.dst_site = e.dst_site
      AND (g.src_group_label = e.src_group_label OR g.src_group_label IS NULL))
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY e.id, e.src_asn, e.src_group_label, e.dst_site ORDER BY e.window_start) = 1"""


def insert_sql(ds: str, giga: str, events: str, columns: list[str]) -> str:
    select = ",\n  ".join(
        "o.own AS client_name" if c == "client_name" else f"e.{c}" for c in columns
    )
    return f"""
INSERT INTO `{PROJECT}.{ds}.{giga}` ({", ".join(columns)})
SELECT
  {select}{missing_traces(ds, giga, events)}"""


def plan_sql(ds: str, giga: str, events: str, with_insert: bool) -> str:
    """Counts what one date's correction would do, without writing."""
    insert_count = (
        f"(SELECT COUNT(*) FROM (SELECT e.id {missing_traces(ds, giga, events)}))"
        if with_insert
        else "NULL"
    )
    return f"""
SELECT
  (SELECT COUNT(*) FROM `{PROJECT}.{ds}.{giga}` g WHERE g.partition_date = @d) AS rows_before,
  (SELECT COUNT(*) FROM `{PROJECT}.{ds}.{giga}` g WHERE g.partition_date = @d
     AND g.id NOT IN (SELECT id FROM {own_names(ds)} WHERE own = 'giga-meter')
     AND (g.src IS NULL OR g.src NOT IN {month_list(ds)})) AS to_delete,
  {insert_count} AS to_insert"""


def wait_for(client: bigquery.Client, job: bigquery.QueryJob) -> bigquery.QueryJob:
    """Poll a job to completion, each request with its own timeout.

    job.result() can block forever on a dropped connection: a staging run sat 4 h
    on a dead socket after its job had finished. A fresh get_job per poll cannot.
    """
    while True:
        try:
            job = client.get_job(job.job_id, location=job.location, timeout=REQUEST_TIMEOUT_S)
        except Exception as e:  # transient network error: keep polling
            print(f"  poll of {job.job_id} failed ({type(e).__name__}); retrying", flush=True)
        else:
            if job.state == "DONE":
                if job.error_result:
                    raise RuntimeError(f"{job.job_id}: {job.error_result}")
                return job
        time.sleep(POLL_S)


def tib_used_today(client: bigquery.Client) -> float:
    sql = """SELECT IFNULL(SUM(total_bytes_billed), 0) / POW(1024, 4)
      FROM `mlab-collaboration.region-us.INFORMATION_SCHEMA.JOBS_BY_USER`
      WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
        AND job_type = 'QUERY'
        AND DATE(creation_time, 'America/Los_Angeles') = CURRENT_DATE('America/Los_Angeles')"""
    return float(list(wait_for(client, client.query(sql)).result(timeout=REQUEST_TIMEOUT_S))[0][0])


def done_dates(log: Path | None) -> set[str]:
    if not log or not log.exists():
        return set()
    return {json.loads(line)["date"] for line in log.read_text().splitlines() if line.strip()}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--dataset", default="hermes_union")
    ap.add_argument("--giga-table", default="giga_meter_measurements")
    ap.add_argument("--events-table", default="events_with_as_and_geoloc")
    ap.add_argument("--start", type=date.fromisoformat, required=True)
    ap.add_argument("--end", type=date.fromisoformat, required=True)
    ap.add_argument(
        "--insert-from",
        type=date.fromisoformat,
        help="first date that also gets the INSERT step (default: none)",
    )
    ap.add_argument("--plan", action="store_true", help="count only; write nothing")
    ap.add_argument("--log", type=Path, help="JSONL of finished dates; resumes past them")
    ap.add_argument(
        "--max-tib-today",
        type=float,
        default=None,
        help="stop before a date once this identity has billed this many TiB "
        "in the current Pacific day (leave room for the nightly)",
    )
    args = ap.parse_args(argv)
    if args.dataset == "hermes_union" and not (args.plan or args.log):
        ap.error("production writes need --log so the run is auditable and resumable")

    client = bigquery.Client(project=PROJECT)
    columns = [
        f.name for f in client.get_table(f"{PROJECT}.{args.dataset}.{args.giga_table}").schema
    ]
    events_cols = {
        f.name for f in client.get_table(f"{PROJECT}.{args.dataset}.{args.events_table}").schema
    }
    missing = [c for c in columns if c not in events_cols]
    if missing:
        print(f"events table lacks GIGA columns {missing}; refusing", file=sys.stderr)
        return 2

    skip = done_dates(args.log)
    d = args.start
    while d <= args.end:
        ds = d.isoformat()
        if ds in skip:
            d += timedelta(days=1)
            continue
        with_insert = args.insert_from is not None and d >= args.insert_from
        param = [bigquery.ScalarQueryParameter("d", "DATE", d)]
        if args.plan:
            row = dict(
                list(
                    client.query(
                        plan_sql(args.dataset, args.giga_table, args.events_table, with_insert),
                        job_config=bigquery.QueryJobConfig(query_parameters=param),
                    ).result()
                )[0]
            )
            print(json.dumps({"date": ds, **row}), flush=True)
            d += timedelta(days=1)
            continue

        if args.max_tib_today is not None:
            used = tib_used_today(client)
            if used >= args.max_tib_today:
                print(
                    f"stopping before {ds}: {used:.2f} TiB billed today "
                    f">= --max-tib-today {args.max_tib_today}",
                    flush=True,
                )
                return 0

        stmts = [
            delete_sql(args.dataset, args.giga_table),
            update_sql(args.dataset, args.giga_table),
        ]
        if with_insert:
            stmts.append(insert_sql(args.dataset, args.giga_table, args.events_table, columns))
        script = "BEGIN TRANSACTION;\n" + ";\n".join(stmts) + ";\nCOMMIT TRANSACTION;"
        job = wait_for(
            client, client.query(script, job_config=bigquery.QueryJobConfig(query_parameters=param))
        )
        children = list(client.list_jobs(parent_job=job.job_id, timeout=REQUEST_TIMEOUT_S))
        counts = {}
        for child in children:
            stmt = (child.statement_type or "").lower()
            if stmt in ("delete", "update", "insert"):
                counts[stmt] = int(child.num_dml_affected_rows or 0)
        rec = {
            "date": ds,
            "deleted": counts.get("delete", 0),
            "updated": counts.get("update", 0),
            "inserted": counts.get("insert", 0) if with_insert else None,
            "gib_billed": round((job.total_bytes_billed or 0) / 2**30, 1),
            "job_id": job.job_id,
        }
        print(json.dumps(rec), flush=True)
        if args.log:
            with args.log.open("a") as fh:
                fh.write(json.dumps(rec) + "\n")
        d += timedelta(days=1)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

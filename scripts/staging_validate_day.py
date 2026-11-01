#!/usr/bin/env python3
"""Run the pipeline's SQL and Python steps for one date in hermes_staging and keep copies.

Used to validate a change before it reaches production: the outputs are saved as
``hermes_staging.<table>_<suffix>`` so two runs (old code vs new code, or two runs of
the same code) can be compared table by table.

Stages:
  det    steps 02 and 03 twice back to back (saved as ``_<suffix>_r1`` / ``_r2``): a
         reproducibility check, since the two runs see the same inputs
  rest   steps 04, 05, 06 (+ temporal verdicts) and 07 once (saved as ``_<suffix>``)
  all    det then rest

Deliberately NOT ``hermes-pipeline --target staging``:
  * no step 01 -- staging's merged_download_upload is seeded from production with
    ``python -m hermes.pipeline.init_staging --copy-inputs`` so both see one input;
  * no enrichment, which writes the shared ``hermes.*`` reference tables.

Writes only to hermes_staging; refuses anything else. Run it from a container built
from the commit under test so the code is exactly that commit:

    docker run --rm --name hermes-validate --memory=28g \\
      -v ~/.config/gcloud/application_default_credentials.json:/app/credentials.json:ro \\
      -e GOOGLE_APPLICATION_CREDENTIALS=/app/credentials.json \\
      --entrypoint python hermes-pipeline:<sha> scripts/staging_validate_day.py \\
      --date 2026-10-14 --suffix rv_20261014 --stage all
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import time

from google.cloud import bigquery

from hermes.pipeline import correlation_tomography as ct
from hermes.pipeline import temporal_verdict, union
from hermes.sql import loader

PROJECT, DS = "mlab-collaboration", union.STAGING_DATASET
ST = f"{PROJECT}.{DS}"
#: Written with insert_rows_json: saved with CREATE TABLE AS, since a copy job
#: skips rows still in the streaming buffer.
STREAMED = (
    "correlation_hyperedges_tomography_v2",
    "correlation_culprits_multigranularity",
    "correlation_entity_stats_multigranularity",
    "temporal_path_verdicts",
)

log = logging.getLogger("staging_validate")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--date", required=True, help="analysis date (YYYY-MM-DD)")
    ap.add_argument("--suffix", required=True, help="suffix for the saved copies")
    ap.add_argument("--stage", choices=("all", "det", "rest"), default="all")
    args = ap.parse_args()
    assert DS != union.DEFAULT_DATASET

    day = dt.date.fromisoformat(args.date)
    d = day.isoformat()
    params = {
        "ONE_WEEK_EARLIER": (day - dt.timedelta(days=union.BASELINE_DAYS)).isoformat(),
        "DAY": d,
        "DETECTION_GRANULARITY": "metro",
        "DS": DS,
        "METRO_POLYGONS": union.metro_polygons_for(DS),
    }
    client = bigquery.Client(project=PROJECT)
    started = dt.datetime.now(dt.UTC)

    def q(sql: str):
        return client.query(sql).result()

    def billed_since(t0: dt.datetime) -> float:
        jobs = client.list_jobs(min_creation_time=t0, all_users=False)
        return sum((j.total_bytes_billed or 0) for j in jobs if j.job_type == "query") / 2**30

    def delete(table: str) -> None:
        q(f"DELETE FROM `{ST}.{table}` WHERE partition_date = '{d}'")

    def run_sql(name: str) -> None:
        sql = loader.load_query(name, params)
        union._assert_dataset_isolated(sql, name, DS)
        t0 = time.time()
        q(sql)
        log.info(f"  {name} done in {time.time() - t0:.0f}s")

    def save(table: str, suffix: str) -> None:
        q(f"DROP TABLE IF EXISTS `{ST}.{table}_{suffix}`")
        q(
            f"CREATE TABLE `{ST}.{table}_{suffix}` PARTITION BY partition_date AS "
            f"SELECT * FROM `{ST}.{table}` WHERE partition_date = '{d}'"
        )
        n = list(q(f"SELECT COUNT(*) n FROM `{ST}.{table}_{suffix}`"))[0].n
        log.info(f"  saved {table}_{suffix}: {n:,} rows")

    def wait_for_buffer() -> None:
        while any(client.get_table(f"{ST}.{t}").streaming_buffer for t in STREAMED):
            log.info("streaming buffer present; waiting 5 min")
            time.sleep(300)

    if args.stage in ("all", "det"):
        for r in ("r1", "r2"):
            t0 = dt.datetime.now(dt.UTC)
            delete("anomaly_counts_union")
            run_sql("02_detect_anomalies_union.sql")
            save("anomaly_counts_union", f"{args.suffix}_{r}")
            delete("transient_events_union")
            run_sql("03_build_transient_events_union.sql")
            save("transient_events_union", f"{args.suffix}_{r}")
            log.info(f"=== 02+03 {r}: {billed_since(t0):.1f} GiB billed")

    if args.stage in ("all", "rest"):
        t0 = dt.datetime.now(dt.UTC)
        delete("events_with_as_and_geoloc")
        delete("giga_meter_measurements")
        run_sql("04_mapping_union.sql")
        save("events_with_as_and_geoloc", args.suffix)
        save("giga_meter_measurements", args.suffix)
        log.info(f"=== 04: {billed_since(t0):.1f} GiB billed")

        t0 = dt.datetime.now(dt.UTC)
        delete("temporal_correlations")
        run_sql("05_temporal_tomography_union.sql")
        save("temporal_correlations", args.suffix)
        log.info(f"=== 05: {billed_since(t0):.1f} GiB billed")

        t0 = dt.datetime.now(dt.UTC)
        wait_for_buffer()
        for t in (*STREAMED, "events_explained_daily"):
            delete(t)
        ct.run_correlation_tomography(
            day, project_id=PROJECT, write_multigranularity=True, dataset=DS
        )
        rows = temporal_verdict.compute_temporal_verdicts(client, d, dataset=DS)
        temporal_verdict.write_verdicts(client, rows, dataset=DS)
        log.info(
            f"=== 06 + temporal verdicts ({len(rows)} rows): {billed_since(t0):.1f} GiB billed"
        )

        t0 = dt.datetime.now(dt.UTC)
        run_sql("07_translating_to_public_format_union.sql")
        for t in (*STREAMED, "events_explained_daily"):
            save(t, args.suffix)
        log.info(f"=== 07 + saves: {billed_since(t0):.1f} GiB billed")

    log.info(f"=== ALL DONE: {billed_since(started):.1f} GiB billed in total")


if __name__ == "__main__":
    main()

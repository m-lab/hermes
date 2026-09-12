#!/usr/bin/env python3
"""Rerun Step 02 in staging and prove its complete partition is unchanged."""

from __future__ import annotations

import argparse
import json
import re
from datetime import date, timedelta

from google.cloud import bigquery

from hermes.sql import loader

PROJECT = "mlab-collaboration"
SANDBOX_DATASET = "hermes_staging"


def _table(dataset: str, name: str) -> str:
    for value in (dataset, name):
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
            raise ValueError(f"invalid BigQuery identifier: {value!r}")
    return f"{PROJECT}.{dataset}.{name}"


def partition_fingerprint(client: bigquery.Client, table: str, day: date) -> dict[str, object]:
    sql = f"""
    WITH row_hashes AS (
      SELECT TO_HEX(MD5(TO_JSON_STRING(t))) AS row_hash
      FROM `{table}` AS t
      WHERE partition_date = @day
    ),
    decision_groups AS (
      SELECT
        src_asn, src_city, dst_site, ip_version,
        SUM(anomaly_rtt_count) AS anomaly_rtt_count,
        SUM(anomaly_throughput_count) AS anomaly_throughput_count,
        SUM(anomaly_upload_throughput_count) AS anomaly_upload_throughput_count,
        SUM(anomaly_loss_rate_count) AS anomaly_loss_rate_count
      FROM `{table}`
      WHERE partition_date = @day
      GROUP BY src_asn, src_city, dst_site, ip_version
    ),
    decision_hashes AS (
      SELECT TO_HEX(MD5(TO_JSON_STRING(d))) AS decision_hash
      FROM decision_groups AS d
    )
    SELECT
      (SELECT COUNT(*) FROM row_hashes) AS row_count,
      (SELECT TO_HEX(MD5(STRING_AGG(row_hash, '' ORDER BY row_hash))) FROM row_hashes)
        AS content_hash,
      (SELECT COUNT(*) FROM decision_hashes) AS decision_group_count,
      (SELECT TO_HEX(MD5(STRING_AGG(decision_hash, '' ORDER BY decision_hash)))
       FROM decision_hashes) AS decision_hash
    """
    config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("day", "DATE", day)]
    )
    row = next(iter(client.query(sql, job_config=config).result()))
    return {
        "row_count": row["row_count"],
        "content_hash": row["content_hash"],
        "decision_group_count": row["decision_group_count"],
        "decision_hash": row["decision_hash"],
    }


def verify(day: date, dataset: str = SANDBOX_DATASET) -> dict[str, object]:
    if dataset != SANDBOX_DATASET:
        raise ValueError("repeatability verification is restricted to hermes_staging")

    client = bigquery.Client(project=PROJECT)
    table = _table(dataset, "anomaly_counts_union")
    before = partition_fingerprint(client, table, day)
    if before["row_count"] == 0:
        raise RuntimeError(f"no detector rows exist in {dataset} for {day}")

    delete_sql = f"DELETE FROM `{table}` WHERE partition_date = @day"
    config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("day", "DATE", day)]
    )
    client.query(delete_sql, job_config=config).result()

    query = loader.load_query(
        "02_detect_anomalies_union.sql",
        {
            "DAY": day.isoformat(),
            "ONE_WEEK_EARLIER": (day - timedelta(days=7)).isoformat(),
            "DETECTION_GRANULARITY": "metro",
            "DS": dataset,
        },
    )
    client.query(query).result()
    after = partition_fingerprint(client, table, day)
    result = {"day": day.isoformat(), "dataset": dataset, "before": before, "after": after}
    repeatable = (
        before["row_count"] == after["row_count"]
        and before["decision_group_count"] == after["decision_group_count"]
        and before["decision_hash"] == after["decision_hash"]
    )
    if not repeatable:
        raise RuntimeError(json.dumps({**result, "repeatable": False}, sort_keys=True))
    return {
        **result,
        "repeatable": True,
        "diagnostic_float_content_identical": before["content_hash"] == after["content_hash"],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--day", required=True, type=date.fromisoformat)
    args = parser.parse_args()
    print(json.dumps(verify(args.day), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

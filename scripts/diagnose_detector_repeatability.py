#!/usr/bin/env python3
"""Identify which Step 02 fields differ across identical staging reruns."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import date, timedelta

from google.cloud import bigquery

from hermes.sql import loader

PROJECT = "mlab-collaboration"
DATASET = "hermes_staging"
TABLE = f"{PROJECT}.{DATASET}.anomaly_counts_union"
KEYS = ("src_asn", "src_city", "dst_site", "ip_version")
FIELDS = (
    "anomaly_rtt_count",
    "anomaly_throughput_count",
    "anomaly_upload_throughput_count",
    "anomaly_loss_rate_count",
)


def _read(client: bigquery.Client, day: date) -> dict[tuple, dict]:
    selected = ", ".join((*KEYS, *(f"SUM({name}) AS {name}" for name in FIELDS)))
    group_by = ", ".join(KEYS)
    sql = (
        f"SELECT {selected} FROM `{TABLE}` WHERE partition_date = @day "
        f"GROUP BY {group_by}"
    )
    config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("day", "DATE", day)]
    )
    rows = {}
    for row in client.query(sql, job_config=config).result():
        key = tuple(row[name] for name in KEYS)
        if key in rows:
            raise RuntimeError(f"duplicate detector key: {key!r}")
        rows[key] = {name: row[name] for name in FIELDS}
    return rows


def _rerun(client: bigquery.Client, day: date) -> None:
    config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("day", "DATE", day)]
    )
    client.query(f"DELETE FROM `{TABLE}` WHERE partition_date = @day", job_config=config).result()
    sql = loader.load_query(
        "02_detect_anomalies_union.sql",
        {
            "DAY": day.isoformat(),
            "ONE_WEEK_EARLIER": (day - timedelta(days=7)).isoformat(),
            "DETECTION_GRANULARITY": "metro",
            "DS": DATASET,
        },
    )
    client.query(sql).result()


def diagnose(day: date) -> dict:
    client = bigquery.Client(project=PROJECT)
    before = _read(client, day)
    _rerun(client, day)
    after = _read(client, day)
    changed = Counter()
    samples = []
    for key in sorted(before.keys() | after.keys()):
        if key not in before:
            changed["rows_added"] += 1
            continue
        if key not in after:
            changed["rows_removed"] += 1
            continue
        field_changes = [name for name in FIELDS if before[key][name] != after[key][name]]
        for name in field_changes:
            changed[name] += 1
        if field_changes and len(samples) < 10:
            samples.append({"key": key, "changed_fields": field_changes})
    return {
        "day": day.isoformat(),
        "before_rows": len(before),
        "after_rows": len(after),
        "changed_counts": dict(changed.most_common()),
        "sample_changed_groups": samples,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--day", required=True, type=date.fromisoformat)
    args = parser.parse_args()
    print(json.dumps(diagnose(args.day), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Clear downstream staging partitions while preserving Steps 01 and 02."""

from __future__ import annotations

import argparse
from datetime import date

from google.cloud import bigquery

PROJECT = "mlab-collaboration"
DATASET = "hermes_staging"
TABLES = (
    "transient_events_union",
    "events_with_as_and_geoloc",
    "temporal_correlations",
    "events_explained_daily",
    "correlation_hyperedges_tomography_v2",
    "correlation_culprits_multigranularity",
    "correlation_entity_stats_multigranularity",
    "temporal_path_verdicts",
    "giga_meter_measurements",
)


def clear(day: date) -> None:
    client = bigquery.Client(project=PROJECT)
    config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("day", "DATE", day)]
    )
    for name in TABLES:
        table = f"{PROJECT}.{DATASET}.{name}"
        client.query(
            f"DELETE FROM `{table}` WHERE partition_date = @day", job_config=config
        ).result()
        print(f"cleared {table} {day}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--day", required=True, type=date.fromisoformat)
    args = parser.parse_args()
    clear(args.day)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

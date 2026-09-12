#!/usr/bin/env python3
"""Compare one public-output partition between production and sandbox."""

from __future__ import annotations

import argparse
import json
from datetime import date

from google.cloud import bigquery

PROJECT = "mlab-collaboration"


def _metrics(client: bigquery.Client, dataset: str, day: date, *, upload: bool) -> dict:
    upload_expr = (
        "COUNTIF('upload' IN UNNEST(anomaly_signals))" if upload else "CAST(NULL AS INT64)"
    )
    sql = f"""
    SELECT
      COUNT(*) AS row_count,
      COUNT(DISTINCT FORMAT('%d|%s|%s|%s', src_asn, src_group_label, dst_site, ip_version))
        AS anomaly_groups,
      COUNTIF(information_source = 'forward') AS forward_rows,
      COUNTIF(information_source = 'reverse') AS reverse_rows,
      COUNTIF(information_source IS NULL) AS unresolved_rows,
      COUNTIF(attribution_method = 'correlation') AS correlation_rows,
      COUNTIF(attribution_method = 'path_local') AS path_local_rows,
      {upload_expr} AS upload_rows
    FROM `{PROJECT}.{dataset}.events_explained_daily`
    WHERE partition_date = @day
    """
    config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("day", "DATE", day)]
    )
    return dict(next(iter(client.query(sql, job_config=config).result())))


def _schema_columns(client: bigquery.Client, dataset: str) -> int:
    sql = f"""
    SELECT COUNT(*) AS columns
    FROM `{PROJECT}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'events_explained_daily'
    """
    return next(iter(client.query(sql).result()))["columns"]


def _overlap(client: bigquery.Client, day: date) -> dict:
    sql = f"""
    WITH prod AS (
      SELECT DISTINCT FORMAT('%d|%s|%s|%s', src_asn, src_group_label, dst_site, ip_version) key
      FROM `{PROJECT}.hermes_union.events_explained_daily`
      WHERE partition_date = @day
    ), sandbox AS (
      SELECT
        FORMAT('%d|%s|%s|%s', src_asn, src_group_label, dst_site, ip_version) key,
        LOGICAL_OR('upload' IN UNNEST(anomaly_signals)) AS has_upload,
        LOGICAL_OR(
          'latency' IN UNNEST(anomaly_signals) OR 'download' IN UNNEST(anomaly_signals)
        ) AS has_legacy
      FROM `{PROJECT}.hermes_staging.events_explained_daily`
      WHERE partition_date = @day
      GROUP BY key
    )
    SELECT
      COUNTIF(prod.key IS NOT NULL AND sandbox.key IS NOT NULL) AS common_groups,
      COUNTIF(prod.key IS NOT NULL AND sandbox.key IS NULL) AS production_only_groups,
      COUNTIF(prod.key IS NULL AND sandbox.key IS NOT NULL) AS sandbox_only_groups,
      COUNTIF(prod.key IS NULL AND sandbox.has_upload AND NOT sandbox.has_legacy)
        AS sandbox_only_upload_only_groups,
      COUNTIF(prod.key IS NULL AND sandbox.has_upload AND sandbox.has_legacy)
        AS sandbox_only_upload_and_legacy_groups,
      COUNTIF(prod.key IS NULL AND NOT sandbox.has_upload AND sandbox.has_legacy)
        AS sandbox_only_legacy_groups,
      COUNTIF(sandbox.has_upload) AS sandbox_upload_groups,
      COUNTIF(sandbox.has_upload AND NOT sandbox.has_legacy) AS sandbox_upload_only_groups
    FROM prod
    FULL OUTER JOIN sandbox USING (key)
    """
    config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("day", "DATE", day)]
    )
    return dict(next(iter(client.query(sql, job_config=config).result())))


def compare(day: date) -> dict:
    client = bigquery.Client(project=PROJECT)
    prod = _metrics(client, "hermes_union", day, upload=False)
    sandbox = _metrics(client, "hermes_staging", day, upload=True)
    return {
        "day": day.isoformat(),
        "production": {**prod, "schema_columns": _schema_columns(client, "hermes_union")},
        "sandbox": {**sandbox, "schema_columns": _schema_columns(client, "hermes_staging")},
        "group_overlap": _overlap(client, day),
        "delta": {
            key: sandbox[key] - prod[key]
            for key in prod
            if isinstance(prod[key], int) and isinstance(sandbox[key], int)
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--day", required=True, type=date.fromisoformat)
    args = parser.parse_args()
    print(json.dumps(compare(args.day), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

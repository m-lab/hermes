"""Refresh HOIHO router geolocation: new rDNS hostnames -> ``geolocation``.

One run:

1. selects hostnames seen in reverse DNS over the lookback window that are in
   neither the geolocation table nor the lookup cache;
2. looks them up in CAIDA HOIHO (``lookup.query_hoiho``);
3. appends every match to the geolocation table, then records every hostname
   that was successfully looked up -- hit or miss -- in the lookup cache;
4. recomputes ``metro`` for the geolocation table if anything was added.

Writes are load jobs, not streaming inserts: step 4 replaces the table with
CREATE OR REPLACE, which must not race rows still in a streaming buffer.
Geolocation is written before the cache, so a crash between the two re-queries
those hostnames next run; the candidate query then skips them via the
geolocation table, so nothing is duplicated.

IPv4 only. The IPv6 reverse-DNS table is not refreshed by the union pipeline.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

from google.cloud import bigquery

from hermes.enrichment.hoiho import lookup
from hermes.sql import loader

logger = logging.getLogger(__name__)

#: ``hermes.geolocation`` as it exists in production (metro is the last column).
GEOLOCATION_SCHEMA = [
    bigquery.SchemaField("hostname", "STRING"),
    bigquery.SchemaField("lat", "FLOAT"),
    bigquery.SchemaField("lon", "FLOAT"),
    bigquery.SchemaField("place", "STRING"),
    bigquery.SchemaField("cc", "STRING"),
    bigquery.SchemaField("state", "STRING"),
    bigquery.SchemaField("code", "STRING"),
    bigquery.SchemaField("locode", "STRING"),
    bigquery.SchemaField("domain", "STRING"),
    bigquery.SchemaField("match_strs", "STRING", mode="REPEATED"),
    bigquery.SchemaField("match_meanings", "STRING", mode="REPEATED"),
    bigquery.SchemaField("clli", "STRING"),
    bigquery.SchemaField("metro", "STRING"),
]

#: Every hostname HOIHO answered for, matched or not.
LOOKUPS_SCHEMA = [
    bigquery.SchemaField("hostname", "STRING"),
    bigquery.SchemaField("matched", "BOOLEAN"),
    bigquery.SchemaField("ruleset_date", "STRING"),
    bigquery.SchemaField("looked_up_at", "TIMESTAMP"),
]


@dataclass
class RefreshSummary:
    candidates: int = 0
    looked_up: int = 0
    matched_hostnames: int = 0
    rows_written: int = 0
    failed_batches: int = 0


def _candidate_sql(
    rdns_table: str, geolocation_table: str, lookups_table: str, day: str, lookback_days: int
) -> str:
    start = (date.fromisoformat(day) - timedelta(days=lookback_days)).isoformat()
    return f"""
WITH recent AS (
  SELECT DISTINCT LOWER(RTRIM(TRIM(hostname), '.')) AS hostname
  FROM `{rdns_table}`
  WHERE partition_date > '{start}' AND partition_date <= '{day}'
    AND hostname IS NOT NULL
)
SELECT r.hostname
FROM recent r
WHERE r.hostname != ''
  AND NOT EXISTS (SELECT 1 FROM `{lookups_table}` l WHERE l.hostname = r.hostname)
  AND NOT EXISTS (SELECT 1 FROM `{geolocation_table}` g WHERE g.hostname = r.hostname)
"""


def _append(client: bigquery.Client, rows: list[dict], table: str, schema) -> None:
    config = bigquery.LoadJobConfig(
        schema=schema, write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )
    client.load_table_from_json(rows, table, job_config=config).result()


def refresh_hoiho_geolocation(
    client: bigquery.Client,
    day: str,
    *,
    rdns_table: str,
    geolocation_table: str,
    lookups_table: str,
    lookback_days: int = 30,
    query: Callable[..., lookup.LookupResult] = lookup.query_hoiho,
) -> RefreshSummary:
    """Look up new rDNS hostnames in HOIHO and store the results.

    Parameters
    ----------
    client
        BigQuery client billed for the candidate query and the loads.
    day
        Last day (``YYYY-MM-DD``) of the reverse-DNS window.
    rdns_table, geolocation_table, lookups_table
        Fully-qualified tables. Staging passes its own geolocation and cache.
    lookback_days
        Width of the reverse-DNS window.
    query
        HOIHO client (injected in tests).

    Returns
    -------
    RefreshSummary
        Counts for logging and monitoring.
    """
    client.create_table(bigquery.Table(lookups_table, schema=LOOKUPS_SCHEMA), exists_ok=True)

    sql = _candidate_sql(rdns_table, geolocation_table, lookups_table, day, lookback_days)
    candidates = [row["hostname"] for row in client.query(sql).result()]
    summary = RefreshSummary(candidates=len(candidates))
    logger.info("HOIHO: %d new hostnames to look up", len(candidates))
    if not candidates:
        return summary

    result = query(candidates)
    summary.looked_up = len(result.looked_up)
    summary.matched_hostnames = len(result.matched)
    summary.rows_written = len(result.rows)
    summary.failed_batches = result.failed_batches

    if result.rows:
        _append(client, result.rows, geolocation_table, GEOLOCATION_SCHEMA)
    if result.looked_up:
        now = datetime.now(UTC).isoformat()
        cache_rows = [
            {
                "hostname": h,
                "matched": h in result.matched,
                "ruleset_date": result.ruleset_date,
                "looked_up_at": now,
            }
            for h in sorted(result.looked_up)
        ]
        _append(client, cache_rows, lookups_table, LOOKUPS_SCHEMA)
    if result.rows:
        metro_sql = loader.load_query(
            "enrich_geolocation_add_metro.sql", {"GEOLOCATION": geolocation_table}
        )
        client.query(metro_sql).result()

    logger.info(
        "HOIHO: looked up %d, matched %d hostnames (%d rows), %d failed batch(es), ruleset %s",
        summary.looked_up,
        summary.matched_hostnames,
        summary.rows_written,
        summary.failed_batches,
        result.ruleset_date,
    )
    return summary

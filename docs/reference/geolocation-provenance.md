# Geolocation provenance: which IPInfo dump produced a row

**Status:** current as of 2026-08-16.

HERMES geolocates client and topology IPs from IPInfo MMDB dumps held on
`hermes-ec2`. IPInfo geolocation is a *point-in-time assertion* about where an
address is, so which dump was used is part of the measurement, not an
implementation detail.

## The rule

A run uses the dump **closest in time to the date it is processing**, not the
newest one. Implemented in `closest_snapshot()`
(`src/hermes/enrichment/ipinfo/enricher.py`), threaded through
`HermesEnrichment(snapshot_date=...)` from `run_enrichment()`.

* **Nightly** — target date is ~today, so the closest dump *is* today's. Behaviour
  is unchanged from before the rule existed.
* **Backfill** — the closest dump is a historical one. Before this rule, a backfill
  silently used the newest dump, asserting present-day geolocation about year-old
  traffic. An address reallocated between countries in the interim landed in the
  wrong country for every historical measurement, with nothing in the output
  saying so.

Ties resolve to the **earlier** dump, so the choice never depends on filesystem
ordering.

## Where it is recorded

`geoloc_snapshot` (STRING) on all four geolocation tables in
`mlab-collaboration.hermes`:

| table | population |
|---|---|
| `unified_src_ip_to_geoloc` | client IPs, IPv4 |
| `unified_src_ip_to_geoloc_ipv6` | client IPs, IPv6 |
| `unified_ip_to_geoloc` | topology/hop IPs, IPv4 |
| `unified_ip_to_geoloc_ipv6` | topology/hop IPs, IPv6 |

A NULL means the row predates the column. That is not a gap in practice for
nightly-written rows: `partition_date` is the run date, and a nightly used that
day's dump, so `partition_date` is a faithful proxy. The proxy is **exactly wrong
for backfills**, which is why the column exists.

```sql
-- how stale was the geolocation behind a partition?
SELECT partition_date, geoloc_snapshot, COUNT(*) AS rows_,
       DATE_DIFF(partition_date,
                 DATE(REGEXP_EXTRACT(geoloc_snapshot, r'(\d{4}-\d{2}-\d{2})')),
                 DAY) AS dump_minus_measurement_days
FROM `mlab-collaboration.hermes.unified_src_ip_to_geoloc`
WHERE partition_date BETWEEN '2025-07-18' AND '2025-07-31'
GROUP BY 1, 2 ORDER BY 1;
```

## The dump archive

Two directories on `hermes-ec2`, now unified: the operational cache
(`~/hermes-docker-cache`, mounted into the container at `/app/cache`) and an older
archive (`~/hermes-code-union/hermes_enrichment/cache`). The archive was
**hard-linked** into the operational cache on 2026-08-16 — same filesystem, so no
extra disk. Combined span: **2025-05-13 → 2026-08-15**, 78 dumps.

Coverage is uneven. There is a gap between **2025-12-01 and 2026-06-03**, so any
backfill landing in that window will pin to a dump up to ~3 months away. Check the
gap rather than assuming it is small:

```bash
ssh hermes-ec2 'ls ~/hermes-docker-cache | grep -oE "ipinfo_[0-9-]+" | sort'
```

### Truncated dumps

The cache also holds partial downloads from interrupted `wget` runs —
`ipinfo_2025-09-09.snapshot` is 11 MB of a ~613 MB dump. These parse as a valid
date and sort normally, so a naive "closest" rule selects them on an exact date
match and geolocates an entire backfill against a stub, every lookup missing, no
error raised. Files below `MIN_SNAPSHOT_BYTES` (100 MB) are ignored with a
warning; real dumps run 585–915 MB.

## July 2025 roll-back

Dates 2025-07-18 → 07-31 were backfilled on 2026-08-16 with pinned dumps:

| date range | dump | gap |
|---|---|---|
| 2025-07-18 → ~07-22 | `ipinfo_2025-07-08` | 10–14 d |
| ~07-23 → 2025-07-31 | `ipinfo_2025-08-07` | 7–15 d |

An earlier attempt on 2026-08-15 wrote these partitions using the **2026-08-15**
dump (a ~386 day gap) before pinning existed. Those rows —
7,894,179 IPv4 and 7,306,240 IPv6 — were deleted and re-derived. If any analysis
was run against `unified_src_ip_to_geoloc` for 2025 partitions between those two
points, it used the wrong geolocation.

## What this does and does not buy

It does **not** make historical geolocation correct — no dump is contemporaneous
with the measurement. It makes the error small (days, not a year) and, more
importantly, **stated**: the gap is a column you can filter and report on, rather
than an assumption that geolocation was contemporaneous.

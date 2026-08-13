# HERMES Union Pipeline

Daily pipeline that detects internet performance anomalies (RTT, throughput, loss) from M-Lab NDT measurements, attaches forward/reverse traceroute topology, and identifies culprit network edges via iterative tomography.

Processes IPv4 and IPv6 jointly (hence "union").

## Quick start

Installing the package (`pip install .`) provides the `hermes-pipeline` console
script; the examples below use it.

```bash
# Run for yesterday (default)
hermes-pipeline

# Run for a date range (--end-date is INCLUSIVE)
hermes-pipeline --start-date 2026-05-17 --end-date 2026-05-23

# Dry run (show what would execute, no queries)
hermes-pipeline --start-date 2026-05-17 --end-date 2026-05-23 --dry-run

# Re-run specific dates, clearing their rows first (see "Resume and idempotency")
hermes-pipeline --rerun-dates 2026-05-20 2026-05-21 --delete-first

# Metro-keyed detection is the default. Existing dates require a complete delete
# so city- and metro-derived outputs can never coexist for one partition.
hermes-pipeline --rerun-dates 2026-05-20 --delete-first \
    --detection-granularity metro

# Compare against the old grouping shape using IPInfo city labels. This is not
# the historical MaxMind provider; provenance remains client_geo_source=ipinfo.
hermes-pipeline --rerun-dates 2026-05-20 --delete-first \
    --detection-granularity city

# Fill only the gaps in a range — dates absent, empty, or fully unattributed
hermes-pipeline --start-date 2026-05-01 --end-date 2026-05-23 --fill-missing

# Parallelism. --max-workers covers the scan-bound SQL phases (A/C/E).
# Phase D is throttled SEPARATELY and defaults to 1 — see the warning below.
hermes-pipeline --start-date 2026-05-01 --end-date 2026-05-23 \
    --max-workers 7 --tomography-workers 1
```

> **Do not raise `--tomography-workers`.** A single date's correlation tomography
> can spike to ~20 GB RSS, so 2+ workers OOM a typical container. It is
> deliberately *not* tied to `--max-workers`: the SQL phases are BigQuery-bound
> and parallelise safely, Phase D is memory-bound and does not.

## Prerequisites

### Credentials

- **Google Cloud ADC** for BigQuery access:
  ```bash
  gcloud auth application-default login
  ```
- The authenticated account needs:
  - `bigquery.jobs.create` on project `mlab-collaboration`
  - Read access to `measurement-lab.*` and `mlab-collaboration.*` tables
  - Write access to `mlab-collaboration.hermes_union.*` tables

### Python dependencies

```bash
pip install -r requirements.txt
```

Key packages: `google-cloud-bigquery`, `google-auth`.

The enrichment step also requires the `hermes_enrichment` module (located at `../hermes_enrichment/`), which uses IPInfo, RIPE IPMap, and the `zdns` binary for rDNS lookups.

### BigQuery UDFs

The following user-defined functions must exist in the `mlab-collaboration.hermes` dataset before running the pipeline:

| UDF | Used in | Purpose |
|-----|---------|---------|
| `hermes.mann_whitney_u_test` | Step 02 | Mann-Whitney U test on measurement arrays |
| `hermes.welchs_t_test` | Step 02 | Welch's t-test on measurement arrays |
| `hermes.compute_wasserstein_p_value` | Step 02 | Wasserstein distance with permutation p-value |

### BigQuery lookup tables

These reference tables must be populated before running step 04:

| Table | Purpose |
|-------|---------|
| `hermes.as_metadata` | ASN organization names, PeeringDB info |
| `hermes.unified_ip_to_rdns` / `_ipv6` | Reverse DNS hostnames per IP |
| `hermes.unified_ip_to_geoloc` / `_ipv6` | IP geolocation (IPInfo + RIPE IPMap) |
| `hermes.unified_ip_to_as` / `_ipv6` | IP-to-ASN mapping |
| `hermes.geolocation` | HOIHO hostname-based geolocation |
| `hermes.asn_facility_matched` | ASN-to-facility mapping |
| `hermes.site_to_state` | M-Lab site to US state mapping |
| `ix_data.ixp_members` | IXP membership (PeeringDB) |

The enrichment step (Phase B) updates `unified_ip_to_geoloc`, `unified_ip_to_rdns`, and `geolocation` automatically.

## Pipeline architecture

The orchestrator (`hermes_pipeline_union.py`) runs five phases per batch of dates:

```
Phase A ── SQL steps 01-03 (parallel across dates)
  │
Phase B ── Enrichment: geolocate + rDNS new topology IPs (once per batch)
  │
Phase C ── SQL steps 04 + temporal tomography (parallel across dates)
  │
Phase D ── Python v2 correlation tomography + temporal verdict (parallel across dates)
  │         writes: correlation_hyperedges_tomography_v2, temporal_path_verdicts
  │
Phase E ── SQL step 07: public-format aggregation (parallel across dates)
            writes: events_explained_daily
```

Multiple dates in a batch run in parallel (one worker per date). Within each date, steps run sequentially.

### Step 01: Merge upload + download measurements

**SQL:** `01_merge_upload_download_union.sql`
**Reads:** `measurement-lab.ndt.ndt7_union`, `measurement-lab.ndt_raw.ndt7`
**Writes:** `hermes_union.merged_download_upload`

Joins each NDT download test with its corresponding upload test via `access_token`. Extracts `client_name` (e.g. `giga-meter`), `metro_rank`, and computes `ip_version` (v4/v6). Produces one row per test with both download and upload metrics.

### Step 02: Detect anomalies

**SQL:** `02_detect_anomalies_union.sql`
**Reads:** `hermes_union.merged_download_upload`
**Writes:** `hermes_union.anomaly_counts_union`

For each `(client ASN, client group, server site, ip_version)` group. The client
group is selected by `--detection-granularity`: `city` uses the IPInfo
City-Region-Country label; `metro` (the default) pools every accepted
measurement assigned to the canonical metro. Both modes use IPInfo geography,
recorded independently as `client_geo_source = 'ipinfo'`. Historical rows may
retain `detection_granularity = 'maxmind_city'` and
`client_geo_source = 'maxmind'`.

1. Filters to consistent client IPs (geographic proximity + metro_rank checks).
2. Caps each IP at 40% of the group's measurements to prevent single-IP dominance.
3. Builds a 7-day baseline and a current-day sample at the selected granularity.
4. Runs three statistical tests (Mann-Whitney, Welch's t, Wasserstein) on RTT, download throughput, and upload throughput.
5. Flags anomalies when tests are significant AND the effect size exceeds a threshold (+5ms RTT, -20% throughput).

### Step 03: Build transient events (attach traceroutes)

**SQL:** `03_build_transient_events_union.sql`
**Reads:** `hermes_union.anomaly_counts_union`, `measurement-lab.ndt.scamper1`, `measurement-lab.autojoin_autoload_v2_ndt.scamper2_union`, `measurement-lab.revtr_raw.revtr1`, `hermes_union.merged_download_upload`
**Writes:** `hermes_union.transient_events_union`

Joins anomaly groups with MDA traceroutes (scamper1), standard traceroutes (scamper2), and reverse traceroutes (revtr). Each output row is one measurement with:
- Forward + reverse hop-by-hop path data (node_details arrays)
- Per-measurement RTT/throughput from the NDT test
- Group-level anomaly ratios and statistical test results
- City-level percentile summaries

### Enrichment (between steps 03 and 04)

**Code:** `hermes_pipeline_union.py:run_enrichment()`

Geolocates new topology IPs discovered in step 03's traceroutes:
1. **IPInfo + RIPE IPMap** geolocation for new IPs (both IPv4 and IPv6).
2. **rDNS** lookups via `zdns` for IPv4 IPs from the last 90 days.
3. **HOIHO** hostname-to-geolocation parsing.

Results are written to `hermes.unified_ip_to_geoloc`, `hermes.unified_ip_to_rdns`, and `hermes.geolocation`. Step 04 reads these tables to annotate hops.

### Step 04: Hop-level mapping + geolocation

**SQL:** `04_mapping_union.sql`
**Reads:** `hermes_union.transient_events_union`, plus all lookup tables listed above
**Writes:** `hermes_union.events_with_as_and_geoloc`, `hermes_union.giga_meter_measurements`

For each traceroute hop:
1. Maps IP to ASN and IXP (longest-prefix match against `unified_ip_to_as` + `hopannotation2`).
2. Geolocates via HOIHO (rDNS-based), IPInfo, or RIPE IPMap (in priority order).
3. Computes cumulative distances, speed-of-light checks, baseline consistency flags.
4. Detects AS-level loops in forward and reverse paths.

Three details worth knowing before editing this query:

- **`_rdns_geo` dedup.** The HOIHO join matches on a REGEXP-normalised hostname —
  a computed expression, not a hash key — so running it per hop-occurrence
  (~185M rows) was this step's single largest cost (~160 min / ~227 slot-hours).
  It is instead evaluated once per DISTINCT `rdns_name` (a few million) and
  equi-joined back on the raw column. Reintroducing a per-hop REGEXP join here
  would silently restore that cost.
- **Reverse-path loop cleanup.** RevTr stitches RR/atlas segments, and
  `hop_type = 4` hops have ambiguous position, so they can manufacture spurious
  AS/metro loops. The query drops `A-[4:B]-A` excursions and truncates at genuine
  re-entry, comparing against the nearest NON-NULL neighbour so unmapped hops
  don't fake an excursion.
- **Bounded `hopannotation2` scan.** The read is windowed to
  `DATE_SUB(DATE('${DAY}'), INTERVAL 1 MONTH) .. ${DAY}`. Do not replace this
  with a fixed start date — that makes the scan grow without limit as time passes.

Also writes the GIGA-meter subset: rows where `client_name = 'giga-meter'` OR the client IP appears in `hermes_union.giga_school_ips` (for older measurements before the explicit flag was adopted).

### Canonical compatibility view: `events_enriched`

**SQL:** `create_events_enriched.sql`
**Reads:** `hermes_union.events_with_as_and_geoloc`
**Publishes:** `hermes.events_enriched`

`events_enriched` is the stable downstream interface. It leaves the large
historical physical table unchanged and presents each row as nested `client`,
`server`, `performance`, `server_to_client_path`, `client_to_server_path`, and
`quality` records. No source/destination endpoint names are exposed by the
canonical contract; those names are contained at the legacy-table boundary.
Forward and reverse hops use the same canonical names; reverse-only RevTr fields
remain direction-specific. The view also materializes path summaries such as AS,
country, metro, and IXP paths, hop counts, geolocation coverage, direct geodesic
distance, and detour ratio.

The legacy names do not describe endpoint direction. Scamper's so-called forward
path is measured `server_to_client`; RevTr is measured `client_to_server`. The
view preserves each measured hop order and exposes both `direction` and
`measurement_method`, rather than reversing arrays and misrepresenting the RTT
vantage point. Segment and cumulative distance are recomputed in that exposed
order, so cumulative distance increases consistently for both paths.

Historical rows created before grouping provenance was stored are exposed as
`client.grouping_granularity = 'city'`, `client.geo_source = 'maxmind'`, with
`client.group_label` falling back to the legacy city label. New rows expose
`city` or `metro` independently from their `ipinfo` source. This is a
compatibility projection only: the physical historical rows remain NULL and are
not rewritten.

The legacy `speed_of_internet_fiber` field is a calculated lower-bound RTT, not
a speed. The canonical hop therefore exposes it as `fiber_lower_bound_rtt_ms`
and separately records the assumed propagation speed as
`propagation_speed_km_s = 200000`.

### Step 05: Temporal tomography

**SQL:** `05_temporal_tomography_union.sql`
**Reads:** `hermes_union.events_with_as_and_geoloc`
**Writes:** `hermes_union.temporal_correlations`

Runs in Phase C alongside Step 04. Single-pass before/during comparison: for each edge in the forward AS path, computes the fraction of paths traversing it during anomalies vs. during the 7-day baseline. A high ratio indicates the edge appeared disproportionately during the anomaly.

**Temporal v2 (Phase D):** `05_temporal_edge_prevalences_union.sql` + `temporal_verdict.py`
**Reads:** `hermes_union.events_with_as_and_geoloc`
**Writes:** `hermes_union.temporal_path_verdicts`

Runs in Phase D alongside Step 06 (correlation v2). Computes per-path temporal verdict scores that indicate whether a given path's edge prevalence during anomalies is statistically elevated vs. baseline. `temporal_verdict.py` orchestrates the SQL query and writes structured verdict rows to `temporal_path_verdicts`. The dashboard reads `temporal_path_verdicts` alongside `events_explained_daily`.

### Step 06: Correlation tomography

**SQL (Python hybrid backend):** `06_correlation_tomography_prepare_union.sql` + `06_correlation_tomography_all_edges_union.sql`
**SQL (path-local attribution):** `06_correlation_tomography_unexplained_hops_union.sql`
**Reads:** `hermes_union.events_with_as_and_geoloc`
**Writes:** `hermes_union.correlation_hyperedges_tomography_v2`

Runs in Phase D. Iterative greedy set-cover that identifies culprit network edges via the Python hybrid backend:

- `..._prepare_union.sql` scans the source once and returns precomputed edges; Python runs the set-cover loop; `..._all_edges_union.sql` returns all per-node edges for the final hyperedge fractions.
- `..._unexplained_hops_union.sql` performs path-local attribution for measurements not covered by the set-cover result.

Pipeline: (1) pre-compute (measurement, edge) pairs from forward/reverse AS paths; (2) iteratively select the edge explaining the most unexplained anomalous (ASN, city, site) groups by anomalous-vs-non-anomalous frequency ratio; (3) stop at 95% explained, no candidate edges, or 200 iterations; (4) build a hyperedge summary with per-node culprit fractions at ASN-metro, ASN, and metro granularities.

### Step 07: Public-format (events_explained_daily)

**SQL:** `07_translating_to_public_format_union.sql`
**Reads:** `hermes_union.events_with_as_and_geoloc`, `hermes_union.correlation_hyperedges_tomography_v2`, `hermes.as_metadata`
**Writes:** `hermes_union.events_explained_daily`

Runs in Phase E (after Phase D), **and only for dates whose Phase D succeeded.**
That gate is load-bearing, not a nicety: this query's "unresolved" branch selects
every anomalous pair `NOT IN` the correlation table, so if tomography produced
nothing for a date, the exclusion set is empty, *every* pair falls through to the
unresolved branch, and the step writes a normal-sized partition in which every
attribution column is NULL. Nothing errors and the row count looks healthy, so
such a partition is indistinguishable from a good one by size alone — and it then
looks "already processed" to the resume check. Leaving the partition absent is
strictly better: absent is visible and re-runnable. `run_dates()` therefore skips
Phase E for failed-Phase-D dates, and re-checks what it wrote via
`find_unattributed_partitions()`, downgrading any 100%-NULL partition to a
failure. `--fill-missing` also treats such partitions as missing so they get
repaired. To find them by hand:

```sql
SELECT partition_date FROM `mlab-collaboration.hermes_union.events_explained_daily`
GROUP BY partition_date HAVING COUNTIF(attribution_method IS NOT NULL) = 0
```

DELETE+INSERT per day: first deletes existing rows for the partition date, then inserts the rebuilt set. Joins the anomalous (ASN, city, site) groups from `events_with_as_and_geoloc` with the culprit-edge results from `correlation_hyperedges_tomography_v2` (resolved groups) and marks remaining groups as unresolved. Attaches AS names from `as_metadata`, computes distance extents and anomaly-site summaries, and produces the final public event rows. The dashboard reads `events_explained_daily` alongside `temporal_path_verdicts`.

## Output tables

| Table | Partitioned | Description |
|-------|-------------|-------------|
| `hermes_union.merged_download_upload` | `partition_date` | Joined upload+download NDT measurements |
| `hermes_union.anomaly_counts_union` | `partition_date` | Per-group anomaly detection results |
| `hermes_union.transient_events_union` | `partition_date` | Measurements with traceroute paths attached. Pure intermediate: consumed only by Step 04, nothing downstream reads it |
| `hermes_union.events_with_as_and_geoloc` | `partition_date` | Final enriched events with geolocated hops. Strictly 1:1 with `transient_events_union` — Step 04 enriches, it does not fan out |
| `hermes_union.giga_meter_measurements` | `partition_date` | Subset of events from GIGA school measurements |
| `hermes_union.temporal_correlations` | `partition_date` | Before/during edge frequency ratios (Step 05, Phase C) |
| `hermes_union.correlation_hyperedges_tomography_v2` | `partition_date` | Culprit edges from iterative tomography v2 (Phase D) |
| `hermes_union.correlation_culprits_multigranularity` | `partition_date` | Multi-granularity cover culprits (Phase D, phase 4) |
| `hermes_union.correlation_entity_stats_multigranularity` | `partition_date` | Per-entity stats, winners and non-winners (Phase D, phase 4) |
| `hermes_union.temporal_path_verdicts` | `partition_date` | Temporal verdict scores per path edge (Phase D; read by dashboard) |
| `hermes_union.events_explained_daily` | `partition_date` | Public event table with resolved/unresolved attribution (Phase E; read by dashboard) |
| `hermes_union.giga_school_ips` | No | School IPs for GIGA identification (loaded separately) |

## Resume and idempotency

- The pipeline checks each output table for existing data before running each step. If a date already has rows, that step is skipped.
- The `FINAL_OUTPUT_TABLE` (`events_explained_daily`) is checked at startup to skip fully-processed dates entirely.

### `--delete-first` clears the complete selected pipeline range

This is the single easiest way to get a wrong result, so it is worth stating
plainly:

| Invocation | Effect of `--delete-first` |
|---|---|
| `hermes-pipeline --rerun-dates D1 D2 …` | Clears every resumable and Phase-D output for those dates |
| `hermes-pipeline --start-date/--end-date --delete-first` | Clears every selected date before processing; use this when changing granularity |
| `hermes-table-rerun --table T --sql-file F` | Clears that one table for the given dates |

Both `--rerun-dates` and a `--start-date`/`--end-date` range support a clean
whole-pipeline re-run when paired with `--delete-first`.
For `--detection-granularity metro`, the delete also includes the normally
preserved `giga_meter_measurements` table so city- and metro-keyed rows cannot
coexist; step 04 must complete to restore that date's giga rows.
Confirm a real delete happened by looking for `Deleting entries for dates: …` /
`Successfully deleted from …`.

To re-run only the parts that are missing, prefer `--fill-missing`: it selects
which *dates* to run, while the per-step guard still decides which *steps*
execute, so a date needing only Phase E does not redo Phases A–D.

### Tables `--delete-first` does not clear

`delete_dates()` covers `OUTPUT_TABLES` only. It does **not** touch
`giga_meter_measurements` (written by Step 04 alongside `events_with_as_and_geoloc`),
`correlation_culprits_multigranularity`, `correlation_entity_stats_multigranularity`,
or `temporal_path_verdicts`. Because Step 04 and the tomography writers append,
re-running a date without clearing these yourself produces duplicate rows. Use
`hermes-table-rerun --table … --delete-first` or an explicit `DELETE`.

## Monitoring cost

```bash
hermes-budget                    # today's usage
hermes-budget --date 2026-05-20  # specific date
```

Reports total bytes billed, estimated cost ($6.25/TiB on-demand), and top queries.

**Budget for a backfill before starting one.** A single date costs roughly 560 GB
of billed SQL (step 01 ~125 GB, 02 ~8 GB, 03 ~107 GB, 04 ~300 GB, 05 ~15 GB,
07 ~5 GB) plus enrichment and tomography queries. A 30-date backfill is therefore
on the order of 19 TiB, which can exceed a per-user daily quota on its own — and
the daily pipeline consumes part of that budget too. Split large backfills across
days and leave headroom for the scheduled run, or the backfill and the nightly
will both start failing with `Custom quota exceeded ... QueryUsagePerUserPerDay`.
Note such quotas are counted on a calendar day in the project's configured
timezone, which may not be UTC — a job started late in the evening can bill
against the previous day's allowance.

To check remaining headroom directly:

```sql
SELECT DATE(creation_time) AS day,
       ROUND(SUM(total_bytes_billed)/POW(1024,4), 3) AS tib_billed,
       COUNTIF(error_result IS NOT NULL) AS errors
FROM `<project>.region-us.INFORMATION_SCHEMA.JOBS_BY_USER`
WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 DAY)
  AND job_type = 'QUERY'
GROUP BY day ORDER BY day DESC
```

(`JOBS_BY_USER` needs no special grant; `JOBS_BY_PROJECT` requires
`bigquery.jobs.listAll`.)

## SQL file reference

```
src/hermes/sql/queries/
  # Live union pipeline (numbered by execution order)
  01_merge_upload_download_union.sql              Step 01  (Phase A)
  02_detect_anomalies_union.sql                   Step 02  (Phase A)
  03_build_transient_events_union.sql             Step 03  (Phase A)
  04_mapping_union.sql                            Step 04  (Phase C; includes giga-meter output)
  05_temporal_tomography_union.sql                Step 05  (Phase C; temporal_correlations)
  05_temporal_edge_prevalences_union.sql          Step 05v2 (Phase D; temporal_path_verdicts, via temporal_verdict.py)
  06_correlation_tomography_prepare_union.sql     Step 06  (Phase D; Python hybrid, phase 1: edge extraction)
  06_correlation_tomography_all_edges_union.sql   Step 06  (Phase D; Python hybrid, phase 2: all-edges for fractions)
  06_correlation_tomography_unexplained_hops_union.sql  Step 06  (Phase D; path-local attribution for unexplained measurements)
  07_translating_to_public_format_union.sql       Step 07  (Phase E; events_explained_daily; DELETE+INSERT per day)

  # One-time bootstrap DDLs (CREATE TABLE IF NOT EXISTS; run via python -m hermes.pipeline.bootstrap_tables)
  create_correlation_hyperedges_tomography_v2.sql  creates hermes_union.correlation_hyperedges_tomography_v2
  create_temporal_path_verdicts.sql                creates hermes_union.temporal_path_verdicts
  create_events_explained_daily.sql                creates hermes_union.events_explained_daily
  create_place_canonical_metro.sql                 creates hermes_union.place_canonical_metro (optional lookup;
                                                   created empty — LEFT JOINs in 05/06 degrade safely when empty;
                                                   populate later if canonical metro overrides are desired)
  create_correlation_culprits_multigranularity.sql      creates hermes_union.correlation_culprits_multigranularity
  create_correlation_entity_stats_multigranularity.sql  creates hermes_union.correlation_entity_stats_multigranularity

  # Enrichment helpers (Phase B; run by enrichment/main.py, not numbered steps)
  enrich_geolocation_add_metro.sql                rebuilds hermes.geolocation with metro
  enrich_ip_geoloc_add_metro.sql                  rebuilds hermes.unified_ip_to_geoloc with metro

  # Legacy HERMES lineage (standalone, run via table_rerun.py --sql-file; not in the union pipeline)
  legacy_detecting_events.sql                     legacy equivalent of steps 02+03
  legacy_mapping_events.sql                       legacy equivalent of step 04

src/hermes/sql/udfs/                              persistent CREATE FUNCTIONs (one-time setup; called by Step 02)
  compute_wasserstein_p_value.sql
  mann_whitney_u_test.sql
  welchs_t_test.sql
```

# HERMES Union Pipeline

Daily pipeline that detects internet performance anomalies (RTT, throughput, loss) from M-Lab NDT measurements, attaches forward/reverse traceroute topology, and identifies culprit network edges via iterative tomography.

Processes IPv4 and IPv6 jointly (hence "union").

## Quick start

```bash
cd wrapper_automation

# Run for yesterday (default)
python hermes_pipeline_union.py

# Run for a date range
python hermes_pipeline_union.py --start-date 2026-05-17 --end-date 2026-05-23

# Dry run (show what would execute, no queries)
python hermes_pipeline_union.py --start-date 2026-05-17 --end-date 2026-05-23 --dry-run

# Re-run specific dates (skips the "already processed" check)
python hermes_pipeline_union.py --rerun-dates 2026-05-20 2026-05-21

# Parallel workers (default: CPU count)
python hermes_pipeline_union.py --start-date 2026-05-01 --end-date 2026-05-23 --max-workers 7
```

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

The orchestrator (`hermes_pipeline_union.py`) runs three phases per batch of dates:

```
Phase A ── SQL steps 01-03 (parallel across dates)
  │
Phase B ── Enrichment: geolocate + rDNS new topology IPs (once per batch)
  │
Phase C ── SQL steps 04 + tomography (parallel across dates)
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

For each (ASN, city, server-site, ip_version) group:

1. Filters to consistent client IPs (geographic proximity + metro_rank checks).
2. Caps each IP at 40% of the group's measurements to prevent single-IP dominance.
3. Builds a 7-day baseline and a current-day sample.
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

Also writes the GIGA-meter subset: rows where `client_name = 'giga-meter'` OR the client IP appears in `hermes_union.giga_school_ips` (for older measurements before the explicit flag was adopted).

### Step 05: Temporal tomography

**SQL:** `05_temporal_tomography_union.sql`
**Reads:** `hermes_union.events_with_as_and_geoloc`
**Writes:** `hermes_union.temporal_correlations`

Runs in Phase C alongside Step 04. Single-pass before/during comparison: for each edge in the forward AS path, computes the fraction of paths traversing it during anomalies vs. during the 7-day baseline. A high ratio indicates the edge appeared disproportionately during the anomaly.

### Step 06: Correlation tomography

**SQL (Python hybrid backend):** `06_correlation_tomography_prepare_union.sql` + `06_correlation_tomography_all_edges_union.sql`
**SQL (path-local attribution):** `06_correlation_tomography_unexplained_hops_union.sql`
**Reads:** `hermes_union.events_with_as_and_geoloc`
**Writes:** `hermes_union.correlation_hyperedges_tomography_v2`

Runs in Phase D. Iterative greedy set-cover that identifies culprit network edges via the Python hybrid backend:

- `..._prepare_union.sql` scans the source once and returns precomputed edges; Python runs the set-cover loop; `..._all_edges_union.sql` returns all per-node edges for the final hyperedge fractions.
- `..._unexplained_hops_union.sql` performs path-local attribution for measurements not covered by the set-cover result.

Pipeline: (1) pre-compute (measurement, edge) pairs from forward/reverse AS paths; (2) iteratively select the edge explaining the most unexplained anomalous (ASN, city, site) groups by anomalous-vs-non-anomalous frequency ratio; (3) stop at 95% explained, no candidate edges, or 200 iterations; (4) build a hyperedge summary with per-node culprit fractions at ASN-metro, ASN, and metro granularities.

## Output tables

| Table | Partitioned | Description |
|-------|-------------|-------------|
| `hermes_union.merged_download_upload` | `partition_date` | Joined upload+download NDT measurements |
| `hermes_union.anomaly_counts_union` | `partition_date` | Per-group anomaly detection results |
| `hermes_union.transient_events_union` | No | Measurements with traceroute paths attached |
| `hermes_union.events_with_as_and_geoloc` | `partition_date` | Final enriched events with geolocated hops |
| `hermes_union.giga_meter_measurements` | No | Subset of events from GIGA school measurements |
| `hermes_union.correlation_hyperedges_tomography_v2` | `partition_date` | Culprit edges from iterative tomography |
| `hermes_union.temporal_correlations` | `partition_date` | Before/during edge frequency ratios |
| `hermes_union.giga_school_ips` | No | School IPs for GIGA identification (loaded separately) |

## Resume and idempotency

- The pipeline checks each output table for existing data before running each step. If a date already has rows, that step is skipped.
- To force a re-run: use `--rerun-dates` with `--delete-first` to clear existing rows first.
- The `FINAL_OUTPUT_TABLE` (`temporal_correlations`) is checked at startup to skip fully-processed dates entirely.

## Monitoring cost

```bash
cd wrapper_automation/available_budget
python check_quota.py                    # today's usage
python check_quota.py --date 2026-05-20  # specific date
```

Reports total bytes billed, estimated cost ($6.25/TiB on-demand), and top queries.

## SQL file reference

```
src/hermes/sql/queries/
  # Live union pipeline (numbered by execution order)
  01_merge_upload_download_union.sql              Step 01  (Phase A)
  02_detect_anomalies_union.sql                   Step 02  (Phase A)
  03_build_transient_events_union.sql             Step 03  (Phase A)
  04_mapping_union.sql                            Step 04  (Phase C; includes giga-meter output)
  05_temporal_tomography_union.sql                Step 05  (Phase C)
  06_correlation_tomography_prepare_union.sql     Step 06  (Phase D; Python hybrid, phase 1: edge extraction)
  06_correlation_tomography_all_edges_union.sql   Step 06  (Phase D; Python hybrid, phase 2: all-edges for fractions)
  06_correlation_tomography_unexplained_hops_union.sql  Step 06  (Phase D; path-local attribution for unexplained measurements)

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

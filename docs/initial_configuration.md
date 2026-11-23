# Initial configuration: running HERMES in your own Google Cloud project

> **M-Lab's deployment does not need any of this.** The `mlab-collaboration`
> project already has every dataset and table below, and
> `scripts/init_project.sh` refuses to target it. This page is for running
> HERMES in another project, e.g. to reproduce the paper's pipeline.

HERMES reads M-Lab's public measurement tables and writes everything else into
one Google Cloud project, selected with `HERMES_PROJECT`. In a fresh project
those tables do not exist yet, and the pipeline does not create them itself:
it writes with `INSERT`/`MERGE`, which require the destination to exist. This
page creates them, fills the reference data the daily run does not produce,
and runs a first date range.

## 0. Prerequisites

- **M-Lab data access** and **a billing-enabled project of your own**: see
  [Data access](../README.md#data-access) in the README.
- The **Google Cloud SDK** (`gcloud`, `bq`), authenticated:
  `gcloud auth login` (for `bq`) and `gcloud auth application-default login`
  (for the Python pipeline; a service-account key works too).
- The pipeline environment: the Docker image (`docker build -t hermes-pipeline .`)
  or Python 3.14 with `pip install -e .`
- `IPINFO_TOKEN` (required) and `HOIHO_TOKEN` (optional) for enrichment.

```bash
export HERMES_PROJECT=my-project      # used by every step below
```

## 1. Create the datasets and tables

```bash
scripts/init_project.sh "$HERMES_PROJECT" --dry-run   # print the bq commands
scripts/init_project.sh "$HERMES_PROJECT"             # create them
```

The script creates three datasets in the `US` multi-region (it has to match
`measurement-lab`, because BigQuery cannot join across locations) and 27 empty
tables. It skips anything that already exists, so you can re-run it safely.
Creating empty tables costs nothing.

The table definitions are in [`scripts/schemas/`](../scripts/schemas/):
[`tables.tsv`](../scripts/schemas/tables.tsv) lists each table's partitioning,
clustering and how it is populated, and `<dataset>.<table>.json` holds its
schema. Both were exported from the live M-Lab tables on 2026-11-23, so column
order and types match production.

| Dataset | Tables | Populated by |
|---|---|---|
| `hermes_union` | `merged_download_upload`, `anomaly_counts_union`, `transient_events_union`, `events_with_as_and_geoloc`, `temporal_correlations`, `correlation_*`, `temporal_path_verdicts`, `events_explained_daily`, `giga_meter_measurements` | the pipeline itself (steps 01–07) |
| `hermes` | `unified_ip_to_geoloc`, `unified_src_ip_to_geoloc`, `unified_ip_to_rdns` (and `_ipv6` variants) | the pipeline's enrichment stage (IPInfo, RIPE IPmap, ZDNS) |
| `hermes` | `geolocation` | HOIHO router geolocation by rDNS hostname. **The current pipeline does not refresh it**: the HOIHO lookup is disabled, so in a new project it stays empty. Step 04 `LEFT JOIN`s it, so an empty table loses HOIHO router locations but drops no rows. |
| `hermes` | `unified_ip_to_as`, `as_metadata` | step 3 below |
| `ix_data` | `ixp_members`, `ixp_members_ipv6` | step 3 below |
| `hermes` | `metro_polygons_v2` | step 3 below |
| `hermes` | `site_to_state` | loaded by `init_project.sh` from [`scripts/schemas/data/`](../scripts/schemas/data/) |
| `hermes_union` | `giga_school_ips` | GIGA partner data, not public. Leave it empty: the GIGA subset then holds only tests run by the `giga-meter` client. Nothing else changes. |
| `hermes_union` | `place_canonical_metro` | optional lookup. Empty is safe. |

## 2. Bootstrap views and table options

```bash
PYTHONPATH=src python -m hermes.pipeline.bootstrap_tables
```

This runs the same idempotent DDL as production: it creates the
`hermes_union.events_enriched` view and enforces `require_partition_filter` on
every partitioned `hermes_union` table. Its `CREATE TABLE IF NOT EXISTS` and
`ADD COLUMN IF NOT EXISTS` statements are no-ops on tables from step 1.

## 3. Populate the reference tables the daily run does not write

The daily pipeline refreshes geolocation and reverse DNS itself, but only
*reads* the IP-to-AS, IXP, AS-metadata and metro-polygon tables. Fill them once
before the first run.

**IP-to-AS (RouteViews) and IXP membership (PeeringDB)**, for each date you
plan to process:

```bash
PYTHONPATH=src python -m hermes.pipeline.refresh_topology_tables \
    --date 2026-11-01 --ip-version both
```

**AS metadata** (monthly in production). It generates its inputs from CAIDA
AS-Rank and PeeringDB into `--mpl-repo` (any writable directory); the module
docstring of
[`refresh_topology_tables.py`](../src/hermes/pipeline/refresh_topology_tables.py)
lists the files.

```bash
PYTHONPATH=src python -m hermes.pipeline.refresh_topology_tables \
    --date 2026-11-01 --as-metadata --skip-bgp --skip-ixp --mpl-repo ./as-metadata-inputs
```

**Metro polygons** (`hermes.metro_polygons_v2`). **Not yet reproducible
outside M-Lab.** Step 04 inner-joins this table, so it must be populated: an
empty table drops rows. It is built by
[`cleaning_polygons/`](../cleaning_polygons/README.md) from Natural Earth and
WorldPop, but the build also seeds from two M-Lab tables that are not public
(`hermes.metro_polygons_with_population` and `hermes.state_to_iso2`). Until the
built table is published, this step cannot be completed from the repository
alone. With those inputs available, the build follows `HERMES_PROJECT` (or
`HERMES_BQ_PROJECT`):

```bash
python -m cleaning_polygons.build --build-version 2026-08-v2 --upload
```

## 4. Run the pipeline

Step 02 compares each day with a trailing 7-day baseline from
`merged_download_upload`, so for a first run ask for at least 8 days and treat
the first 7 as warm-up (the pipeline warns about thin baselines):

```bash
docker run --rm \
  -e HERMES_PROJECT \
  -e IPINFO_TOKEN -e HOIHO_TOKEN \
  -e GOOGLE_APPLICATION_CREDENTIALS=/app/credentials.json \
  -v /path/to/key.json:/app/credentials.json:ro \
  -v /data/hermes-cache:/app/cache \
  hermes-pipeline --start-date 2026-10-25 --end-date 2026-11-01
```

The log's first lines name the project in use (`BigQuery project: ...`). In
M-Lab's deployment one date reads about 450 GiB, billed to your project at
on-demand rates. Dry-run with `--dry-run` to see the plan first.

## 5. Check the output

```sql
SELECT partition_date, COUNT(*) AS events
FROM `my-project.hermes_union.events_explained_daily`
WHERE partition_date BETWEEN '2026-10-25' AND '2026-11-01'
GROUP BY partition_date ORDER BY partition_date;
```

## Known gaps

- The pipeline also reads `measurement-lab.revtr_raw.revtr1` and
  `measurement-lab.autojoin_autoload_v2_ndt.scamper2_union`. We have not
  confirmed that M-Lab Discuss membership covers them; ask on the Discuss group
  if your account cannot see them.
- `hermes.metro_polygons_v2` cannot yet be rebuilt from public inputs (see step 3).
- `hermes.geolocation` (HOIHO) is not populated by the pipeline (see the table in step 1).
- Without `giga_school_ips` the GIGA subset holds only `giga-meter` client tests.
  The rest of the pipeline is unaffected.
- If production schemas change, re-export `scripts/schemas/` so new projects
  keep matching.

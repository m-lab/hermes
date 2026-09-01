# BigQuery UDFs

Canonical definitions of the persistent BigQuery user-defined functions the
pipeline relies on. These are **persistent** routines that live in the
`mlab-collaboration.hermes` dataset; the detection queries (e.g.
`../queries/02_detect_anomalies_union.sql`) call them by their fully-qualified
name, for example:

```sql
`mlab-collaboration`.hermes.welchs_t_test(baseline, current_rtt)
```

These files exist so the definitions are **version-controlled and reviewable in
the repo** (previously they lived only in the BigQuery project). They are the
source of truth captured from `INFORMATION_SCHEMA.ROUTINES`.

| File | Function | Purpose |
|------|----------|---------|
| `welchs_t_test.sql` | `welchs_t_test(baseline ARRAY<FLOAT64>, current_rtt ARRAY<FLOAT64>)` | Welch's t-test (unequal variances) on RTT distributions. |
| `mann_whitney_u_test.sql` | `mann_whitney_u_test(...)` | Mann–Whitney U rank-sum test. |
| `compute_wasserstein_p_value.sql` | `compute_wasserstein_p_value(weekly ARRAY<FLOAT64>, daily ARRAY<FLOAT64>, num_permutations INT64)` | Self-contained deterministic 1-D Wasserstein permutation test used by Step 02. |

All three are JavaScript UDFs (`LANGUAGE js`). Welch and Mann–Whitney retain
their persistent definitions. Wasserstein is a `CREATE TEMP FUNCTION` inlined
into Step 02 by `@requires-udf`, so every image carries the exact detector it
runs and a sandbox cannot silently share a different live routine with
production. Its permutations use a stable seed derived from canonicalized
input arrays; input row order and repeated execution do not change the result.

## Deploying / recreating

The Welch and Mann–Whitney files are complete persistent `CREATE FUNCTION`
statements qualified to `mlab-collaboration.hermes`. To recreate those two:

```bash
for f in welchs_t_test mann_whitney_u_test; do
  bq query --use_legacy_sql=false --project_id=mlab-collaboration < "$f.sql"
done
```

`CREATE FUNCTION` (without `OR REPLACE`) errors if the function already exists;
add `OR REPLACE` to the DDL when intentionally updating a live function.

## Re-sourcing from BigQuery

To refresh these files from the live definitions:

```sql
SELECT routine_name, ddl
FROM `mlab-collaboration`.hermes.INFORMATION_SCHEMA.ROUTINES;
```

Write each `ddl` value to `<routine_name>.sql`.

> Step 02 declares `-- @requires-udf: compute_wasserstein_p_value`; the SQL
> loader prepends that temporary function before submitting the query. Do not
> deploy the Wasserstein file as a persistent routine.

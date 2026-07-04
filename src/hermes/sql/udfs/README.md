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
| `compute_wasserstein_p_value.sql` | `compute_wasserstein_p_value(weekly ARRAY<FLOAT64>, daily ARRAY<FLOAT64>, num_permutations INT64)` | 1-D Wasserstein distance + permutation p-value. |

All three are JavaScript UDFs (`LANGUAGE js`).

## Deploying / recreating

The `.sql` files are complete `CREATE FUNCTION` statements (fully qualified to
`mlab-collaboration.hermes`). To (re)create them in that project — or to
recreate them in a different project after editing the qualified name — run each
file once:

```bash
for f in welchs_t_test mann_whitney_u_test compute_wasserstein_p_value; do
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

> Note: a `hermes.sql.loader` exists that can prepend `-- @requires-udf: <name>`
> UDF files into a query as `CREATE TEMP FUNCTION`s for fully self-contained
> queries. The detection queries currently use the **persistent** functions
> above by qualified name, so they do not declare `@requires-udf` directives.
> The loader is available if a future query wants the inlined/self-contained form.

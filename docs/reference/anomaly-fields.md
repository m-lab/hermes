# The anomaly fields: what `_ratio` and `_count` actually mean

Internal reference. Written because `anomaly_rtt_count` reads like a count of
measurements and is not one — that misreading has cost time more than once.

## The one-line answer

`anomaly_*_count` is **not a count**. It is a per-group **0/1 statistical
verdict flag**, summed over a group that normally holds exactly one row. That is
why every predicate in this repo compares it to `>= 0.5` and never to an integer.

The field that *is* a proportion of measurements is `anomaly_ratio_*`.

## The two fields side by side

| Physical column | Question it answers | Range |
|---|---|---|
| `anomaly_ratio_rtt` | What **fraction of the individual measurements** in this window was worse than baseline? | continuous 0..1 |
| `anomaly_rtt_count` | Did **this group** pass its statistical significance gate? | 0 or 1 (see caveat) |

Measured on `hermes_union.anomaly_counts_union`, partition `2026-09-22`
(~147,602 groups):

- `anomaly_ratio_rtt` — min `0.00113`, max `1.0`, continuous.
- `anomaly_rtt_count` — min `0`, max `1`, exactly **2 distinct values**.

## Where each comes from

Both in `src/hermes/sql/queries/02_detect_anomalies_union.sql`.

**The ratio** (line ~771) is a straight proportion of samples:

```sql
SAFE_DIVIDE(
  ARRAY_LENGTH((SELECT ARRAY_AGG(rtt) FROM UNNEST(current_rtt_array) AS rtt
                WHERE rtt > baseline_median_rtt + 5)),
  ARRAY_LENGTH(current_rtt_array)
) AS anomaly_ratio_rtt
```

Download and upload use the same shape against `baseline_median_throughput` /
`baseline_median_upload_throughput`.

**The "count"** (line ~953) is a boolean, then summed (line ~1093):

```sql
IF( st.t_test_result IS NOT NULL AND st.mann_whitney IS NOT NULL
    AND (st.t_test_result.p_value < 0.05 OR st.mann_whitney.p_value < 0.05)
    AND (c.median_rtt >= b.baseline_median_rtt + 5),
    1, 0) AS anomaly_rtt
...
SUM(anomaly_rtt) AS anomaly_rtt_count
```

`AnomalyCounts` groups by the **same 18 keys** as `CurrentDayAggregated`, so the
`SUM` sees one row per group and the result is the flag itself.

### The gates are not symmetric

- **RTT**: (t-test **OR** Mann-Whitney) p < 0.05, **and** median ≥ baseline + 5 ms.
- **Download / upload**: t-test **AND** Mann-Whitney **AND** Wasserstein all
  p < 0.05, **and** a ≥ 20 % median regression. Upload additionally requires
  ≥ 10 current and ≥ 25 baseline samples.

Download is a materially stricter gate than RTT. Do not read a download flag and
an RTT flag as equally easy to trip.

## Why the predicates say `>= 0.5`

The standard event predicate is **two independent gates**, not one:

```sql
ndt_rtt > baseline_median_rtt + 5      -- the day-of median moved
AND anomaly_ratio_rtt >= 0.8           -- >=80% of samples were bad
AND anomaly_rtt_count >= 0.5           -- and the significance test fired
```

`>= 0.5` is the idiom for "the flag is set". It appears in
`05_temporal_edge_prevalences_union.sql`,
`06_correlation_tomography_unexplained_hops_union.sql`,
`07_translating_to_public_format_union.sql` and the dashboard.

## Caveat: it is not strictly 0/1

`StatisticalTestsResults` is grouped on **13 keys**:

```
src_asn, src_city, dst_site, dst_city, dst_asn, dst_country,
src_country, src_asn_name, is_consistent, src_state, dst_lat, dst_lon, ip_version
```

but is `LEFT JOIN`ed on only **4**: `src_asn, src_city, dst_site, ip_version`.

So when any of the other nine varies within one join key, the join fans out, the
copies collapse back into a single `AnomalyCounts` group, and `SUM` exceeds 1.

Observed on `2026-09-22`: 13 groups with `total_group_rows = 2`, of which 2
carried `anomaly_throughput_count = 2`. That is ~0.009 % of groups.

**Status: mechanism identified structurally, specific driving column NOT
isolated.** The key-set mismatch is plain in the SQL and the duplicate rows are
visible in the data. A first hypothesis that `dst_lat`/`dst_lon` were the cause
was **tested and disproved** — zero sites had more than one distinct server
coordinate on that date. Isolating the real column needs step 02's geo join
rebuilt to reconstruct `detection_src_city`.

Two consequences, in order of how much they matter:

1. **Detection is unaffected.** Every downstream predicate is `>= 0.5`, which
   still passes at 2.
2. **The statistics may be mis-attached, and that matters more.** A fan-out means
   two different `StatisticalTestsResults` rows attach to one group, after which
   `MIN(anomaly_ratio_rtt)` and `ARRAY_AGG(... LIMIT 1)` pick among them
   arbitrarily. This is the part worth fixing, not the count.

## The canonical view

`create_events_enriched.sql` used to propagate the misleading name as
`performance.anomaly.rtt_count`. It no longer does. The current contract is:

| View field | Type | Source |
|---|---|---|
| `performance.anomaly.rtt_anomalous_sample_fraction` | FLOAT64 | `anomaly_ratio_rtt` |
| `performance.anomaly.rtt_significant` | BOOL | `anomaly_rtt_count >= 0.5` |
| `performance.anomaly.download_anomalous_sample_fraction` | FLOAT64 | `anomaly_ratio_throughput` |
| `performance.anomaly.download_significant` | BOOL | `anomaly_throughput_count >= 0.5` |
| `performance.anomaly.upload_anomalous_sample_fraction` | FLOAT64 | `anomaly_ratio_upload_throughput` |
| `performance.anomaly.upload_significant` | BOOL | `anomaly_upload_throughput_count >= 0.5` |

The BOOL exposure states the contract every predicate was already assuming, and
makes the fan-out invisible to consumers rather than enshrining it in the public
interface. The raw integers remain on `hermes_union.anomaly_counts_union` and on
`events_with_as_and_geoloc` for diagnosis.

This rename was safe: at the time it was made, **no repo outside this one
referenced `events_enriched` at all**, and every `rtt_count` hit elsewhere was
the physical `anomaly_rtt_count`, not the view's nested field.

## Fields that genuinely are counts

If you want an actual number of measurements, use one of these instead:

| Field | Meaning |
|---|---|
| `number_of_measurements_baseline` | measurements in the baseline window |
| `number_of_unique_src_ips_baseline` | distinct client IPs in the baseline |
| `measurement_count_per_site` | measurements for the group's site |
| `unique_ip_count_per_site` | distinct client IPs for the group's site |
| `total_group_rows` | rows the `SUM` aggregated — **1 normally; >1 means the fan-out above** |
| `n_dayof` / `n_baseline` | day-of vs baseline measurement counts (step 07, dashboard) |

`total_group_rows` is the direct diagnostic for the caveat: any value above 1 is
a fanned-out group.

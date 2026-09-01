# Where upload throughput is (and is not) used

Traced from the production Docker SQL on 2026-08-27. In the production image,
upload is a **first-class detection signal** in step 02 and drives
**reverse-path** attribution in step 06, but is dropped from temporal tomography
(05) and from the public output (07). The sandbox candidate in this checkout
closes that gap while preserving the old public denominator.

## Sandbox candidate implemented

- Step 02 requires at least 10 current-day and 25 baseline upload samples before
  an upload anomaly can fire.
- The bidirectional step-05 prevalence query now uses RTT/download for the
  forward path and RTT/upload for the reverse path. The legacy forward-only
  temporal tomography query remains RTT/download-only.
- Correlation tomography already classified its main edge extraction by
  direction; its path-local fallback is now direction-aware too, so an
  upload-only anomaly cannot emit forward fallback hops.
- Correlation pair identity now includes `ip_version`. Sandbox validation found
  11 IPv4/IPv6 keys that had previously collapsed into one pair universe, which
  allowed three upload-only public rows to inherit a forward attribution from
  the other address family. Step 07 also enforces signal-to-direction eligibility
  as a compatibility guard when reading historical three-component pair keys.
- Step 07 parses correlation keys from both ends and reconstructs the complete
  middle label. Four sandbox pair references had valid source labels containing
  the historical ` - ` delimiter; a fixed-offset `SPLIT` silently misread their
  site and IP family.
- Step 07 admits upload-only anomalous pairs and publishes upload baseline,
  daily median/mean, anomaly ratio, upload-anomalous-site count, an all-signal
  site count, and `anomaly_signals`.
- Existing `total_anomalous_sites` remains RTT-or-download for longitudinal
  comparability. `total_anomalous_sites_all_signals` is the new inclusive count.

Direction convention (from `create_events_enriched.sql:246-284`):

| HERMES name | direction | measurement | NDT metric that loads it |
|---|---|---|---|
| forward path | server → client | scamper traceroute | **download** throughput |
| reverse path | client → server | reverse traceroute | **upload** throughput |

That mapping is the whole reason upload is carried: it is the only per-test signal
that stresses the reverse path.

## Step by step

**01 `01_merge_upload_download_union.sql`** — uploads come from
`measurement-lab.ndt_raw.ndt7` (`raw.Upload.*`), downloads from
`measurement-lab.ndt.ndt7_union`. They are joined on the `access_token` in
`ClientMetadata`, collapsed to one upload row per (day, token) with `ANY_VALUE`.
The join is a **LEFT JOIN from downloads**, so:

- a test with no matching upload keeps its download row with NULL upload fields;
- an upload with **no** download is dropped entirely.

Three upload fields are written: `upload_throughput_mbps`, `upload_min_rtt`,
`upload_loss_rate`. **Only throughput is ever read again** — `upload_min_rtt` and
`upload_loss_rate` are dead columns in the union path (read only by
`legacy_detecting_events.sql`). RTT and loss anomalies are download-side only.

**02 `02_detect_anomalies_union.sql`** — upload throughput gets the full parallel
battery, per (src_asn, src_group_label, dst_site, ip_version):

- baseline and current arrays: `baseline_upload_throughput_array`,
  `current_upload_throughput_array`;
- tests: `mann_whitney_upload_throughput`, Welch's t on upload,
  `wasserstein_upload_throughput_result`;
- `anomaly_upload_throughput` fires when all three p-values < 0.05 **and** the
  median dropped ≥ 20% vs baseline — the same rule as download;
- outputs: `anomaly_upload_throughput_count`, `anomaly_ratio_upload_throughput`,
  `difference_upload_throughput`, `baseline_median_upload_throughput`.

Caveat: `CandidateGroups` (lines 654-656) gates only on
`ARRAY_LENGTH(current_rtt_array) > 2` and `ARRAY_LENGTH(baseline_rtt_array) > 2`.
Because step 01's join is a LEFT JOIN from downloads, the upload arrays are
strictly shorter, so the upload tests can run on far fewer samples than the RTT
gate implies, and `anomaly_ratio_upload_throughput` is NULL when the current
upload array is empty.

**03 / 04** — pure passthrough. Neither step filters on any anomaly flag; the
upload columns ride along into `events_with_as_and_geoloc`.

**05 `05_temporal_tomography_union.sql:166-172`,
`05_temporal_edge_prevalences_union.sql:13-14`** — `is_anomaly` = RTT **or**
download only. Upload is **not** used. A group anomalous only on upload never
enters temporal tomography.

**06 correlation tomography** — this is where upload does real work
(`06_correlation_tomography_prepare_union.sql:96-107`):

- `is_forward_anomaly` = RTT or **download**;
- `is_reverse_anomaly` = RTT or **upload**;
- `is_anomaly` (overall) = RTT or download or upload.

`06_correlation_tomography_unexplained_hops_union.sql:19` uses the same three-way
`is_anomaly` as its group filter, so upload-only anomalies do reach path-local
attribution.

**07 `07_translating_to_public_format_union.sql`** — RTT and download only. The
anomalous-pair filter (lines 109-110), `is_latency_anomaly` /
`is_throughput_anomaly` (449-451), and the INSERT column list (8-19) all exclude
upload. Nothing upload-derived reaches `events_explained_daily`.

**`create_events_enriched.sql:218-244`** — exposes upload as data, not as a
verdict: `performance.upload_mbps`, `performance.baseline.upload_mbps`,
`performance.anomaly.upload_ratio` / `upload_count` / `upload_difference_mbps`.

## One-line answer

Anomaly detection is **not** download-only: upload throughput is detected with
the identical statistical rule in step 02, and it is what makes reverse-path
attribution possible in step 06. But it stops there — 05 and the public output in
07 are RTT+download only, and upload RTT/loss are collected and never used.

## Production coverage measurement

Measured on 2026-08-27 against the Dockerized union pipeline. The relevant SQL
inside `hermes-pipeline:latest` was byte-identical to this checkout, and
2026-08-26 was the latest completed partition.

Across 2026-08-19 through 2026-08-26, 21,030,349 of 39,287,651 merged download
rows had upload throughput (53.53%). Coverage on 2026-08-26 was 2,696,376 of
5,000,134 rows (53.93%). Upload RTT and loss had exactly the same row coverage as
upload throughput, so the limiting factor is whether an upload half matched, not
metric-specific nulls.

The exact step-02 grouping, IPInfo resolution, consistency filtering, trimming,
and RTT candidate gate produced 85,061 candidate groups on 2026-08-26:

| upload samples | current day | seven-day baseline |
|---|---:|---:|
| median | 7 | 41 |
| 25th percentile | 3 | 16 |
| 75th percentile | 19 | 122 |
| groups passing current >= 5 and baseline >= 25 | 35,656 (41.92%) | — |
| groups passing current >= 10 and baseline >= 25 | 25,524 (30.01%) | — |

The existing detector classified 824 groups as upload-anomalous. Of those, 694
(84.22%) were upload-only: neither RTT nor download throughput was anomalous.
An upload-specific sample gate has a large effect:

| proposed upload gate | upload-anomalous groups retained |
|---|---:|
| current >= 5, baseline >= 25 | 402 of 824 (48.79%) |
| current >= 10, baseline >= 25 | 282 of 824 (34.22%) |

The diagnostic jobs were capped and billed 15.41 GB, 5.32 GB, and 25.48 GB.
Their BigQuery job IDs were `9818d855-cca2-4e15-b2b5-2375f35065d0`,
`62f6d8d8-07eb-4869-b120-5326c075fdba`, and
`a0811192-5395-48a3-8111-fb19a60c352c`.

## Recommended incorporation

1. **Make sample sufficiency signal-specific in step 02.** Compute current and
   baseline upload counts and initially require current >= 10 and baseline >= 25
   before `anomaly_upload_throughput` can be true. The current RTT-only candidate
   gate allows more than half of the existing upload anomalies to come from groups
   below even a 5/25 upload gate.
2. **Recover upload attribution in step 07.** Add the gated upload predicate to
   the anomalous pair set and anomaly summary, and publish the upload baseline,
   daily median/mean, anomaly ratio, and upload-anomalous-site count. This recovers
   reverse-path hyperedges already computed by step 06.
3. **Preserve the old denominator.** Keep `total_anomalous_sites` RTT-or-download
   for longitudinal compatibility. Add `upload_anomaly_sites` and a new
   `total_anomalous_sites_all_signals`; do not silently redefine the existing
   field at cutover.
4. **Publish signal identity.** Add an `anomaly_signals` field containing any of
   `latency`, `download`, and `upload`. Keep it distinct from
   `information_source`, which describes the attributed path direction rather
   than the performance evidence that triggered the event.
5. **Make temporal tomography directional.** Do not simply OR upload into the
   shared `is_anomaly` in step 05. `05_temporal_tomography_union.sql` currently
   analyzes forward edges only, while `05_temporal_edge_prevalences_union.sql`
   emits both directions from one shared flag. A naive upload clause would mark
   forward paths anomalous for upload-only events. Keep the forward-only legacy
   query on RTT/download, and give the prevalence query separate forward and
   reverse anomaly flags matching step 06.
6. **Defer the FULL OUTER JOIN.** Existing paired coverage is sufficient to launch
   a conservative upload signal: 25,524 candidate groups pass 10/25 without
   recovering upload-only raw rows. Quantify upload-only raw tests separately
   before accepting the identity/geolocation complexity of changing step 01.

Upload loss remains a promising independent reverse-path signal, but it should be
a separate detector/calibration change after upload throughput is published and
validated.

## Initial upload-only sandbox validation

The candidate was built on the VM as the isolated Docker tag
`hermes-pipeline:upload-sandbox-20260827` (image
`sha256:e6cabb2d18d1d35b643d71e1a696c143984b5045481a1cf3f333520c0ee2334c`)
and run against `hermes_staging` for 2026-08-07. The production `latest` image
remained unchanged at
`sha256:0c856847081029925394d428ce078e599a99ed6e45422027ba8f848fa1057ef8`.

The detector processed the same 146,137 groups in production and staging.
Production's ungated rule emitted 1,357 upload anomalies; the new 10/25
upload-sample gate emitted 843. Corrected correlation tomography kept IPv4 and
IPv6 separate, increasing its input from 2,350 to 2,361 anomalous pair keys and
producing 1,132 hyperedges and 1,494 final culprit rows.

The final staging public partition contained 1,658 rows:

- 255 carried `upload` in `anomaly_signals`;
- 184 were upload-only, of which 18 received reverse-path attribution and 166
  remained unresolved;
- zero upload-only rows received a forward attribution;
- all 255 upload rows had complete baseline, daily median/mean, and anomaly-ratio
  metrics;
- zero rows had an empty `anomaly_signals` array;
- zero rows regressed below the legacy denominator, while 206 rows had a larger
  all-signal denominator.

The final step-07 staging job was
`51c530c4-2254-4d43-bc42-764696e7a09a` (11.84 GB billed). Local verification on
Python 3.14 passed 241 non-BigQuery tests, with two BigQuery-gated tests excluded.

## Final deterministic sandbox validation

The final VM candidate is `hermes-pipeline:upload-deterministic-sandbox-20260827`
and the stable `hermes-pipeline:sandbox` tag resolves to the same immutable image:
`sha256:476add544008c9d03a8a2267a97d7e9736f924b769615dca31183c4594277927`.
Production was not promoted and remains
`sha256:0c856847081029925394d428ce078e599a99ed6e45422027ba8f848fa1057ef8`.

The deterministic candidate additionally:

- replaces undefined baseline trimming order with a stable fingerprint order;
- inlines a canonical-input, seeded Wasserstein permutation UDF into Step 02;
- replaces Step 02 approximate percentiles with exact percentiles;
- derives lossy fractions from integer counts rather than distributed floating
  averages;
- fully breaks ties in closest-geolocation selection;
- orders public IP arrays, set-cover pair arrays, and Python tie-breaks;
- makes upload-row collapse select one deterministic metric tuple; and
- provides a sandbox-first image-promotion helper. The helper tags and verifies
  `hermes-pipeline:sandbox` before it changes `hermes-pipeline:latest`, so a
  production promotion cannot put code ahead of sandbox.

On two independent Step 02 executions from the same materialized inputs, all
142,593 logical anomaly groups had the identical decision hash
`ee102d344bb14edd9db5e0cf2f53be3b`; both runs wrote 146,138 detector rows.
The four gated values were RTT, download, upload, and loss anomaly counts.
Full-row hashes still differed because non-decision diagnostic `FLOAT64`
aggregates can vary in low bits under distributed summation; those floats are
reported by the verifier but are not treated as anomaly-decision drift.

The final downstream rebuild completed with zero failures. Step 07 job
`bd4a0d34-cb7c-42f8-8629-62a09f992469` billed 11.02 GB. Final public output for
2026-08-07 compares as follows:

| metric | production | sandbox | delta |
|---|---:|---:|---:|
| schema columns | 42 | 49 | +7 |
| public rows | 1,483 | 1,693 | +210 |
| anomaly groups | 1,189 | 1,404 | +215 |
| forward-attributed rows | 1,177 | 1,129 | -48 |
| reverse-attributed rows | 0 | 25 | +25 |
| unresolved rows | 277 | 502 | +225 |
| correlation rows | 950 | 939 | -11 |
| path-local rows | 227 | 215 | -12 |

There are 1,109 groups common to both outputs, 80 production-only groups, and
295 sandbox-only groups. Of the sandbox-only groups, 198 are upload-only, 4
combine upload with RTT/download, and 93 are legacy-signal-only differences.
Across the full sandbox output, 274 groups carry upload evidence, 200 of them
upload-only; 283 public rows are labelled with the upload signal.

Local Python 3.14 validation passes 247 tests, with two credential-gated
BigQuery tests skipped. `ruff` passes for `src`, `tests`, and the changed
operational scripts.

## Production rollout and three-date comparison

The candidate was promoted on 2026-08-31 after the existing production run for
2026-08-30 completed successfully. The production schema migration job was
`e64b9502-837f-4483-b4ce-12916b844269`; it appended the seven upload columns in
the same order as a freshly created table. The Docker tags
`hermes-pipeline:sandbox`, `hermes-pipeline:latest`, and
`hermes-pipeline:upload-deterministic-sandbox-20260827` now all resolve to
`sha256:476add544008c9d03a8a2267a97d7e9736f924b769615dca31183c4594277927`.
The sandbox tag was updated and verified before the production tag.

The exact-percentile Step 02 completed in 3m55s / 23.24 GB for 2026-08-01 and
4m20s / 23.31 GB for 2026-08-04. The two-date detector-and-downstream rebuild
completed with zero failures. Together with the independently repeated
2026-08-07 run, the final public-output comparison is:

| date | production rows | sandbox rows | delta | production groups | sandbox groups | upload-labelled sandbox rows |
|---|---:|---:|---:|---:|---:|---:|
| 2026-08-01 | 1,380 | 1,528 | +148 | 1,147 | 1,314 | 280 |
| 2026-08-04 | 1,515 | 1,714 | +199 | 1,167 | 1,366 | 272 |
| 2026-08-07 | 1,483 | 1,693 | +210 | 1,189 | 1,404 | 283 |
| **total** | **4,378** | **4,935** | **+557** | **3,503** | **4,084** | **835** |

Across the three dates there are 3,233 common groups, 270 production-only
groups, and 851 sandbox-only groups. The sandbox-only set consists of 550
upload-only groups, 15 groups combining upload with a legacy signal, and 286
legacy-only differences from the deterministic-statistics cutover. Reverse-
attributed rows increase from 130 to 198; unresolved rows increase from 836 to
1,446, principally because upload-only groups are now retained instead of being
dropped at the final join.

The separate public dashboard consumer was updated before image promotion. It
now reads the published `anomaly_signals` contract, labels download and upload
throughput separately, exposes an upload filter, and shows upload day-of versus
baseline statistics. Its 82 unit tests passed, and a live staging query returned
177 upload-filtered rows with an upload label on every row. Timestamped rollback
copies of the three deployed dashboard files use the suffix
`.bak.20260831-upload`.

Historical production partitions were not backfilled. The comparison above is
therefore intentionally old-production versus candidate-sandbox output; the
first scheduled production partition produced by the upload-aware image is the
2026-08-31 data run scheduled for 2026-09-01 15:00 UTC.

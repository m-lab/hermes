# Handover: detection-granularity work (HERMES union)

Written 2026-08-10. Companion to `2026-08-group-granularity.md` (the design doc).
This file is the operational state: what is done, what is broken, what must be
decided, and where the artifacts are.

### Implementation update — 2026-08-11

The local pipeline now has a validated `--detection-granularity
maxmind_city|metro` mode. Metro identity is resolved from
`metro_polygons_v2` before step 02 aggregation and reused by step 03; resume
checks reject mixed regimes within a date. Historical tested rows can be
labelled idempotently with `scripts/backfill_detection_granularity.sql`.
Code and static BigQuery validation are complete, but the metro-mode staging
write has not run because the currently active cloud identity cannot list or
write `hermes_staging`.

### Implementation update — 2026-08-10

The local tree now uses the final names `detection_granularity` and
`src_match_granularity`; all `02`/`03`/`07` writers have explicit column lists;
the path-local and both temporal consumers now key on `src_group_label`; the
public bootstrap DDL contains the five new fields; and Phase D accepts a dataset
override for staging. Regression coverage includes all of those paths. The
staging schema migration and the full 2026-08-01..07 rehearsal are complete;
2026-08-07 is the first city-keyed Phase D result and is recorded below.

**Nothing in production has been changed.** All work is in
`mlab-collaboration.hermes_staging`, which holds *clones* of the prod tables.

---

## 1. The problem, in one paragraph

`02_detect_anomalies_union.sql` tests anomalies on groups keyed by
`(src_asn, src_city, dst_site, ip_version)`, where `src_city` is the MaxMind
`City-Subdivision-Country` triple. `04_mapping_union.sql` then **overwrote
`src_city`** with a coarser metro-polygon label, so every stage after it
re-grouped at a granularity nobody tested at. Consequences measured on
partition 2026-08-07:

- **22.78%** of `(src_asn, dst_site, ip_version, src_city)` keys covered more
  than one tested population (worst: 136).
- `events_explained_daily` had **410 duplicate keys** (2,902 rows / 2,492 keys).
- `06` built hyperedge pair strings on the collapsed label, so **Phase D
  attributed metro-collapsed groups**.
- `07`'s `n_dayof >= 10` sufficiency gate counted **whole metros**. The
  motivating case (AS3352 → bcn01, Castello) fired on a group with **6** day-of
  measurements, which passed only because its metro totalled 38.

Scope: 319 of 345 partitions. The 26 exceptions (`2026-02-01..02-21`,
`2026-07-04..07-08`) predate the rollout and still carry MaxMind labels.

---

## 2. What the change does

`04` stops overwriting. The final column set:

| column | example | role |
|---|---|---|
| `detection_granularity` | `maxmind_city` | granularity of anomaly detection and detection-derived counts/statistics |
| `src_group_label` | `Castelló de la Plana-VC-ES` | exact key `02` grouped on; **all keying/counting uses this** |
| `src_city` | `Castelló de la Plana-Comunidad Valenciana-ES` | readable label: MaxMind city name + metro's full state + country |
| `src_state` | `Comunidad Valenciana` | same authority as the label |
| `src_metro` | `Castello-Comunidad Valenciana-ES` | metro rollup; hook for future metro-level detection |

Per table: `anomaly_counts_union` and `transient_events_union` gain
`detection_granularity` + `src_group_label`; `events_with_as_and_geoloc` and
`giga_meter_measurements` also gain `src_metro`; `events_explained_daily` gains
**five** — `detection_granularity`, `src_metro`, `src_group_label`, `n_dayof`,
`src_match_granularity` (the last added 2026-08-10; see §5 and §10.3).

`src_group_label` and `n_dayof` on the public table were added late, after it
became clear the table could not be joined back to its source group or filtered
by sample size. They are unambiguous **because** `07` now keys on the label —
one row is one tested population. (An earlier revision omitted them on the
grounds that a label would be ambiguous; that reasoning predates the re-keying.)
Verified on 2026-08-01: `min(n_dayof) = 10`, 0 rows below threshold.

`06:91` and **13 keying sites in `07`** moved from `src_city` to
`src_group_label`, so counts and the sufficiency gate match the flag.

**Why `src_city` and `src_group_label` both exist:** substituting the metro's
full state name merges **45 of 30,221** labels (0.15%) — `Kansas City-KS-US` and
`Kansas City-MO-US` both become `Kansas City-Kansas-US`. Fine for a label, fatal
for a key.

---

## 3. Verified in staging

Forward run of `02`/`03`/`04`/`07` on 2026-08-07 (447 GiB):

| check | result |
|---|---|
| identity preserved (`src_group_label` == what `03` wrote) | 30,219,175 rows, **0** mismatches |
| no positional column swap | **0** |
| collapse-induced duplicate keys | 38 → **0** (see note) |
| readable label, hyphenated city (`Vila-real`) | correct |
| `src_state` full names | correct |
| granularity tag populated | 100% |

Backfill (`UPDATE`, no pipeline) on 2026-08-01/02/03/05:

| check | result |
|---|---|
| recovered label == `transient_events_union.src_city` | 30,449,795 rows, **0** mismatches |
| distinct labels / metros vs forward run | 30,415 / 3,994 vs 30,221 / 4,007 — consistent |

**Note on duplicate keys.** An earlier reading of "410 → 0" was an artifact: the
0 came from a run whose attribution was 100% NULL, so every row came from the
`unresolved` branch, which emits one row per pair and cannot produce multi-edge
duplicates. Measured like-for-like on 2026-08-01 with attribution working: prod
has 294 extra rows, of which **256 are legitimate multi-edge attribution** (one
pair explained by several culprit edges) and **38 are collapse-induced**. After
the change: 194 extra rows, all multi-edge, 0 collapse-induced. Consumers
counting events should use `COUNT(DISTINCT src_group_label)`, not `COUNT(*)`.

---

## 4. RESOLVED — the 100% NULL attribution episode

Kept because the failure mode is easy to re-create and the diagnosis was not
obvious.

`06` was changed to build hyperedge pair strings from `src_group_label`, but
Phase D was never re-run, so `07` joined new-vocabulary pair strings against
metro-keyed hyperedges cloned from prod. Nothing matched, everything fell to the
`unresolved` branch: **100% NULL attribution**, same shape as the earlier
`reference_phase_e_null_attribution` incident.

The first fix (the `OR` on both vocabularies) matched — 15,796 pairs by metro,
0 by label — yet attribution stayed 100% NULL and row count *collapsed* to 317.
Cause: `resolved` took `src_group_label` from the pair string, so a metro-keyed
hyperedge wrote a METRO into that column and the INNER JOIN to `anomaly_summary`
(keyed on real labels) silently dropped every resolved row. Two different bugs
with an identical symptom; only the match-count diagnostic separated them.

Now `COALESCE(ta.src_group_label, SPLIT(...)[1])`. Verified 20.5% NULL on
2026-08-01 (prod 22.4%).

**Two process lessons.** Any change to `06` must be rehearsed with a Phase D
run — the `07`-only iteration loop (7 GiB, fast) re-runs the consumer without
the producer and hid this for a full cycle. And "attribution is NULL" has at
least two distinct causes that look the same; measure the join match counts
before assuming which.

---

## 5. History — DECIDED (option B, implemented)

The backfill repairs `events_with_as_and_geoloc` cheaply, but
`events_explained_daily` is derived and also needs `correlation_hyperedges_
tomography_v2`, whose stored pair strings are metro-keyed.

**Resolution: `07` accepts both vocabularies.** A hyperedge's content
(`from_asn`, `to_asn`, `edge_asn_metro`) is an intermediary **hop** pair and is
granularity-independent, so the historical corpus stays valid — only its pair
*labels* are in the old vocabulary. `07` matches on `src_group_label` **or**
`src_metro` (07:170-179), with the `unresolved` branch mirroring both. No Phase D
re-run for history.

Verified on 2026-08-01: attribution **20.5% NULL** (prod 22.4%), 2 methods.
Before the compatibility join it was 100% NULL. Pinned by
`test_07_accepts_hyperedges_keyed_either_way` and
`test_07_unresolved_mirrors_the_two_way_match`.

Result: **event** granularity is city-level everywhere; **attribution**
granularity stays metro-level on partitions whose hyperedges predate the change.

| table | backfillable? | cost for 345 partitions |
|---|---|---|
| `events_with_as_and_geoloc` | yes (`UPDATE`) | ~25 TiB (75 GiB/partition, measured) |
| `correlation_hyperedges_tomography_v2` | not needed — compatibility join | 0 |
| `events_explained_daily` | re-run `07` only | ~2.5 TiB |

**Rejected alternatives:** *A, forward-only* — leaves a discontinuity for no
saving now that B exists. *C, recent window* / *D, full history* — re-running
Phase D (~38 TiB + ~30 min compute/date) buys city-level *attribution* on old
partitions; worth revisiting only if that is wanted for its own sake.

**Known consequence of the implementation.** B was built as an unconditional
`OR` on both vocabularies rather than a partition-level regime marker. Simpler,
and it needs no marker — but the two paths then mix at row level and a reader
cannot tell whether a row's attribution came from a label match (genuine) or a
metro match (one edge spread across every city group in that metro).

**Implemented 2026-08-10** as a marker column (07:144, `unresolved` sets NULL at
07:206):

```sql
IF(SPLIT(src_dst_str,' - ')[SAFE_OFFSET(1)] = ta.src_group_label,
   'maxmind_city', 'metro') AS src_match_granularity
```

Three caveats, all from the 2026-08-10 review — see §10.3 for the full argument:

1. **Naming resolved.** The column is `src_match_granularity`: it says which
   source-label vocabulary matched stored evidence, not the culprit's topological
   granularity.
2. **It is derivable today.** Phase D runs once per partition, so a partition's
   hyperedges are uniformly one vocabulary and the value is a function of
   partition date vs cutover. Its real worth is robustness under a *partial*
   Phase D re-run (option C), not information you cannot otherwise get.
3. **It is a symptom of B, not a schema improvement.** Options A, C, D and
   "leave history alone" all yield rows that are internally consistent and need
   no marker. See the trilemma in §10.4.

**Verification gap closed 2026-08-10.** The historical control remains
2026-08-01: **1,232 of 1,232** attributed rows have
`src_match_granularity = 'metro'`. A dataset-aware Phase D run on 2026-08-07
produced 1,162 city-keyed hyperedges. Re-running `07` yielded 1,890 public rows:
1,490 exact `maxmind_city` matches, 400 unresolved, and **zero** metro matches.
The label arm is therefore exercised in staging without paying to recompute
Phase D for historical partitions.

## 6. Other open items

1. **Event volume drops 47%** (2,902 → 1,535 on 2026-08-07). The breakdown in an
   earlier revision ("410 spurious duplicates, ~957 gate") is **superseded by the
   §3 correction**: only ~38 of the extras were collapse-induced, the rest being
   legitimate multi-edge attribution. So the ≥10 gate accounts for
   correspondingly *more* of the drop, not less. Only 25.3% of 197,354 groups
   have ≥10 day-of measurements. The threshold was effectively calibrated against
   metro-level counts and now bites far harder — recompute the split against 38
   and put it through `stats-rigor` before shipping. This is the largest
   open question in the whole change: it is the one thing that alters what the
   system *claims*, as opposed to how it labels what it already claimed.
2. **Deploy order.** The local `02`/`03`/`07` writers now have explicit column
   lists. Production still requires a behaviour-only Deploy A built from those
   list changes before the schema ALTER and behavioural Deploy B.
   `ALTER TABLE ADD COLUMN` breaks the running image immediately (verified:
   `Inserted row has wrong column count; Has 50, expected 48`) **and removes the
   rollback path**. Required sequence: Deploy A (convert `02`/`03` to explicit
   column lists, no behaviour change) → ALTER → Deploy B (this change).
   **Deploy A has not been built or deployed yet.**
3. **Positional ALTER hazard.** `02`/`03`/`07` all INSERT without column lists.
   `events_explained_daily`'s appended block is STRING, STRING, STRING, INT64 —
   a `src_metro`/`src_group_label` transposition is type-valid and silent, and
   the trailing INT64 is the only anchor. Run
   `scripts/verify_group_identity.sql` (checks 2 and 4) after the first write.
4. **Backfill is NOT idempotent.** It sets `src_metro = e.src_city`, valid only
   while `src_city` still holds the metro. A second run would set `src_metro` to
   the readable label. The `ASSERT` (distinct `src_city` < 10,000) catches this
   by accident — after backfill the count is ~30,545. Make that guard explicit.
5. **26 pre-rollup partitions** need the polygon lookup path; the `ASSERT`
   currently refuses them.
6. **NULL `src_group_label` does not contaminate counts** (raised in review,
   checked). `03` leaves it NULL for giga traces with no `AnomalyCounts` match.
   `GROUP BY` buckets those NULLs together, but the bucket then fails the join —
   `dc.src_group_label = fr.src_group_label` is UNKNOWN when both are NULL — so
   those rows are *excluded*, not merged into a labelled group's `n_dayof`.
   Evidence: `null_label = 0` in the public table on 2026-08-01.
7. **Phase D dataset override implemented and exercised.** The standalone
   correlation runner accepts `--dataset hermes_staging` and retargets only
   union-dataset reads and writes. On 2026-08-07 it processed 7,199,232 edge
   rows and 12,784,571 node-edge rows, selected 413 correlation culprits plus
   749 path-local attributions, and uploaded 1,162 hyperedges.
8. **Baseline measurement count never reaches the public table.** The original
   brief asked for the group's day-of *and baseline* counts. `n_dayof` ships;
   `number_of_measurements_baseline` exists in `events_with_as_and_geoloc` and is
   never surfaced. Half of that request is still open.
9. **Resolved locally:** `src_match_granularity` and the public bootstrap schema
   are pinned by regression tests.

---

## 6b. Deployment status — NOTHING IS DEPLOYED

Verified 2026-08-10 against the running image:

```
live image  7565b57639be  (= PR #19, the bigquery-storage fallback fix)
  REPLACE (COALESCE(sm.metro, m.src_city) AS src_city) : still present
  detection_granularity                                : absent
  src_metro                                            : absent
~/hermes-build-gate  @ 0d8e90d
```

- The nightly runs the **old** code and produces old-format data. Correct — the
  ALTER cannot land until Deploy A (see §6.2), and deploying the new SQL against
  an un-altered schema would fail immediately.
- All changes are **uncommitted local working-tree edits**: five SQL files
  (`02`, `03`, `04`, `06`, `07`) plus `scripts/`, `docs/` and one test file.
  Nothing pushed, no image rebuilt, no prod table altered.
- Prod `events_explained_daily` still has its original 35 columns.

## 6c. August 2026 staging rehearsal — COMPLETE

Staging holds **2026-08-01 → 08-07** (the clone predates later partitions).
`giga_meter_measurements` is deliberately **out of scope** — it does not matter
for this audit.

| date | `events_with_as_and_geoloc` | `07` with all fixes |
|---|---|---|
| 08-01 | backfilled | done |
| 08-02 | backfilled | done |
| 08-03 | backfilled | done |
| 08-04 | backfilled | done |
| 08-05 | backfilled | done |
| 08-06 | backfilled | done |
| 08-07 | forward run + city-keyed Phase D | done |

Completed 2026-08-10: 2 backfills (60,850,280 affected rows), five historical
`07` rebuilds for 08-02..06, and one city-keyed Phase D/`07` control for 08-07.
The formal identity verifier reports zero bad rows on all seven dates; every
public row has `n_dayof >= 10`.

Note 08-07's `07` must be re-run — it was executed before the compatibility join,
the `resolved` label fix, and the `src_group_label`/`n_dayof` columns, so its
current output is the 100%-NULL version.

Adding 08-08/08-09 is **not** a simple extension: clones are whole-table, so
re-cloning would discard the backfill work. Use the forward path for later dates
instead.

## 7. Artifacts

| path | what |
|---|---|
| `docs/proposals/2026-08-group-granularity.md` | design doc (rev 3), incl. polygon appendices |
| `docs/reference/state_iso2_to_name.tsv` | 1,091 ISO→state-name pairs from the metro table (29% coverage; US/CA complete, FR has 2 of ~18) |
| `scripts/build_staging_sql.py` | rewrites operational `hermes_union` references in every numbered SQL stage; permits only the shared read-only metro lookup |
| `scripts/group_identity_alters.sql` | schema changes, `${DS}`-parameterised |
| `scripts/backfill_group_identity.sql` | `UPDATE` migration with the pre-rollup `ASSERT` |
| `scripts/backfill_detection_granularity.sql` | idempotent historical flag migration through an explicit legacy cutoff |
| `scripts/verify_group_identity.sql` | acceptance checks |

**Staging:** `mlab-collaboration.hermes_staging`, **12 clones** of prod's 16
(`cloneDefinition` verified — copy-on-write, 0 bytes billed, prod never in
scope). Drop with `DROP SCHEMA ... CASCADE` when finished.

Not cloned, deliberately: `correlation_hyperedges_tomography` (v1, superseded),
`giga_meter_measurements_old` (backup), `geoloc_flags` and
`place_canonical_metro` (read-only reference data).

The clone set was originally scoped to what 02/03/04/07 touch, which left Phase D
unable to run — it writes `correlation_culprits_multigranularity` and
`correlation_entity_stats_multigranularity`, neither of which was present. Added
2026-08-10, along with `temporal_path_verdicts` (written by `temporal_verdict.py`).
**Lesson: scope the environment to the pipeline, not to the step under test.**

Backfilled partitions: 2026-08-01, 02, 03, 05. Deferred at the quota guard:
2026-08-04, 08-06. Forward-run: 2026-08-07.

---

## 8. Quota

`mlab-collaboration` has an admin-set `QueryUsagePerUserPerDay` of ~15 TiB, **per
user per PT day**. At handover the day is at **14.68 TiB** and the backfill
stopped itself at its 14.65 guard. The PT day rolls at 07:00 UTC.

A concurrent August-2025 backfill driver (`~/aug_driver.sh` on `hermes-ec2`) is
self-pacing against a 12.5 TiB ceiling and has reached 2025-08-14.

**Measured costs** (do not re-derive): forward pipeline run 447 GiB/date;
backfill `UPDATE` **75 GiB/partition** (a read-side proxy suggested 4.5 GiB — the
DML rewrite dominates by ~16×, because the table is 75 columns including two
large `REPEATED STRUCT` hop arrays); `07` alone 7.2 GiB.

---

## 9. Gotchas worth not rediscovering

- **`City-State-CC` is not parseable from either end.** City names contain `-`
  (`Saint-Agapit`, `Vila-real`), so left-parsing truncates 4.17% of names and
  collapses `Saint-Agapit` and `Saint-Georges` both to `Saint`. Full state names
  also contain `-` (`Nordrhein-Westfalen`, `Zuid-Holland`), so right-parsing the
  readable label bleeds state into city (`Bergkamen-Nordrhein`). Only
  `src_group_label` (`City-ISO-CC`) is right-parseable, because ISO subdivision
  codes never contain `-`. Derive display names from the label, never from
  `src_city`.
- **NULL `src_group_label` rows are EXCLUDED, not merged.** `03` leaves the
  identity NULL for a giga trace with no `AnomalyCounts` match. `GROUP BY` does
  bucket those NULLs together in `dayof_counts` / `anomaly_data`, which looks
  like the metro collapse this work removed. It is not. The bucket is keyed
  `(src_asn, NULL, dst_site, ip_version)` — separate from every labelled group,
  so no real group's `n_dayof` changes — and it is then unreachable, because
  `dc.src_group_label = fr.src_group_label` is UNKNOWN when both sides are NULL
  and the join drops it. Every other keying site behaves the same way:
  `anomaly_summary`'s INNER JOIN, the `distance_extents` / `group_identity` LEFT
  JOINs, and `anomalies`, whose `CONCAT` pair string is NULL if any argument is.
  Verified 2026-08-01: `null_label = 0` in the public table while the events
  table for that period carries NULL-label giga rows.
  The exclusion is the correct outcome, but it is **implicit** — a consequence
  of NULL join semantics, not a `WHERE ... IS NOT NULL`. Giving
  `src_group_label` a scamper fallback (mirroring `src_city`'s
  `IF(a.src_asn IS NULL, ...)`) would silently admit never-tested rows into the
  >=10 gate; `test_03_carries_identity_without_synthesising_it` is what stops
  that.
- **`INFORMATION_SCHEMA.PARTITIONS.total_rows` is blind to streaming writes** —
  the `correlation_*` tables are written with `insert_rows_json`, so a fresh
  partition reads 0 there while `COUNT(*)` returns the real number.
- **Metro polygons overlap.** 111 of 63,065 points (0.2%) match two polygons;
  109 of those are genuinely different metros. A catch-all
  `Raoul Island Station / South I. remainder` polygon captures 20 mainland NZ
  points purely because `Raoul` sorts before `Tauranga`. Separate workstream;
  see Appendix B of the design doc.
- **`04`'s tiebreak is `ORDER BY metro ASC LIMIT 1`** — encodes no notion of
  correctness. Should rank by most-specific or largest population.
- **The anomaly LABEL was never metro-level, even in prod today.** `06:93` builds
  `is_anomaly` from `anomaly_ratio_rtt`, `anomaly_rtt_count` and
  `baseline_median_rtt` — all group statistics computed by `02` at MaxMind-city
  granularity and denormalized onto every row. `04` overwrote the `src_city`
  *label* but never recomputed those statistics. So every row in prod carries
  **city-level statistics under a metro label**, and a healthy city's paths are
  not labelled anomalous just because a neighbour in the same metro fired. This
  blocks the obvious false-attribution worry; see §10.5 for what metro
  aggregation *does* corrupt.
- **Tomography `support` counts PATHS, not pairs.** Measured on 2026-08-01: 1,162
  hyperedges, mean `support_anomalous` 64.2 against mean 2.28 anomalous pairs
  impacted (~28 paths per pair), `support_anomalous = 1` on **zero** edges, mean
  `p_value` 0.0009. Any reasoning about how regrouping the source population
  affects tomographic power has to start from this, and a minimum-support
  threshold is not a defence against aggregation artifacts — aggregation would
  manufacture the support rather than leave it small.

---

## 10. Review findings (2026-08-10)

An independent review re-derived the load-bearing numbers from the data rather
than from this document. Recorded here so the checks are not repeated and the
open questions are not lost.

### 10.1 What was re-verified, and held

| claim | source | re-measured | verdict |
|---|---|---|---|
| all metro polygons inverted | design doc App. A | 7,301 polygons, **0** not inverted, `ST_AREA` 4.879e14–5.101e14 m² | **holds** |
| prod duplicate keys | §1 | 2026-08-07: 2,902 rows / 2,492 keys = **410** extras; 2026-08-01: 2,504 / 2,210 = **294** | **holds** |
| post-change duplicates | §3 | staging 2026-08-01: 1,549 / 1,355 = **194** extras | **holds** |
| attribution health | §4, §5 | 317 / 1,549 NULL = **20.5%** | **holds** |
| `min(n_dayof) = 10` | §2 | confirmed, 0 rows below | **holds** |
| nothing deployed | §6b | prod columns 35 / 75 / 66 / 48, no new fields | **holds** |

The measurements in this document are sound. Where drift appeared it was in the
prose and the scripts keeping up with them, not in the analysis.

### 10.2 Actual staging state (measured, not planned)

`events_explained_daily` in staging has **345 partitions; all seven August 2026
rehearsal partitions are processed**:

| partition | state |
|---|---|
| **2026-08-01..06** | all five columns populated; historical attribution is explicitly marked `metro` |
| **2026-08-07** | 1,890 rows: 1,490 exact `maxmind_city` matches, 400 unresolved, 0 metro matches |
| 338 historical | untouched clone data |

The 338-partition historical expansion remains a separate ~25 TiB operation.
Twenty-six pre-rollup dates need the polygon-lookup migration path rather than
the rolled-up shortcut used by `scripts/backfill_group_identity.sql`.

### 10.3 There are three different "granularity" concepts — keep them apart

The single most confusing thing in this schema. They are not interchangeable:

| concept | where | values | status |
|---|---|---|---|
| **culprit topological granularity** | `correlation_culprits_multigranularity.granularity` | `edge` / `node` / `metro` / `AS` | pre-existing, unrelated to this work |
| **source aggregation granularity** | `detection_granularity` | `maxmind_city` | constant — the only form ever used |
| **which source-label form matched stored evidence** | `src_match_granularity` | `maxmind_city` / `metro` | join-compatibility artifact, renamed 2026-08-10 |

Measured values of the culprit column on 2026-08-01: `edge`/`path_local` 1,637;
`node`/`correlation` 87; `edge`/`correlation` 86; `metro`/`correlation` 8
(e.g. `Nancy-Lorraine-FR`); `AS`/`correlation` 4.

Two consequences:

- **The name collision is resolved locally.** `src_match_granularity='metro'`
  describes the evidence-label match and cannot be confused with the culprit's
  topological `granularity` column.
- **Principle 2 is now per-quantity.** Detection numbers use
  `detection_granularity`; attribution resolution is described separately by
  `src_match_granularity`.

### 10.4 The real choice for historical partitions

Re-running `07` on history buys the city-level event fixes (the ≥10 gate and the
duplicate keys). The only question is what happens to attribution:

| approach | events | attribution | row self-consistent? |
|---|---|---|---|
| re-run `07` **with** the OR (current B) | city-level, fixed | metro-resolution | **no** — needs a marker |
| re-run `07` **without** the OR | city-level, fixed | 100% NULL (§4's blocker) | yes |
| **don't** re-run `07` on history | metro events, unchanged | metro, unchanged | yes |
| re-run Phase D (C / D) | city-level | city-level | yes — ~38 TiB |

Three of four give internally consistent rows. B is the only one that does not,
and it was chosen to avoid both the NULL attribution and the 38 TiB. **Decide
whether B stays before polishing the marker column — the column is downstream of
that decision.**

### 10.5 Does source granularity harm tomography? (dilution, not contamination)

The natural worry: at metro level, an edge traversed only by a *healthy* city
inside an anomalous metro could be blamed. **This does not happen**, for the
reason in §9: anomaly labels come from city-level statistics that survived the
relabeling, so the healthy city's paths are never labelled anomalous and its
edges never accrue `support_anomalous`. A minimum-support threshold is *not*
what protects you here.

What metro aggregation does corrupt is **pair identity**:

- `anomalous_src_dst_pairs_impacted` collapses several city groups into one
  metro string, undercounting distinct anomalous populations.
- `max_fraction_src_dst_pair_anomalous` is computed over the union of the metro's
  paths, so an anomalous city's paths are **diluted** by a healthy neighbour's —
  the metro pair looks *less* anomalous than the city really is.
- `07` cannot tell which city an attribution belongs to. This is the whole bug.

So the bias runs **toward weaker evidence and missed culprits, not false ones** —
reassuring for false positives, bad for statistical power. City-level tomography
should therefore be mildly *better*, not worse: total anomalous-path support is
roughly unchanged by regrouping (support counts paths), while per-pair fractions
and pair identity get cleaner.

**Status: predicted, not measured.** An earlier draft of this argument claimed
the opposite — that city granularity would thin the evidence per hyperedge — and
that was wrong, because support counts paths rather than pairs. Settle it by
running Phase D at city granularity on one date and comparing
`max_fraction_src_dst_pair_anomalous` and edge selection against the metro run.
Gated on §6.7.

### 10.6 Answers to the original investigation brief

| question | answer |
|---|---|
| Q1 many-to-one relabel? | **confirmed** — 22.78% of keys, worst 136 |
| Q2 when introduced? | **`49902cf` 2026-06-21** ("normalize the src node to the metro polygon in 04"), consolidated by **`47aa01b` 2026-06-28**. The design doc lists this as *inferred* (~2026-07-09 from partition data); git makes it verified, with an ~11-day code→image lag consistent with the 07-04..07-08 outlier cluster. Update the doc's "Inferred, not verified" list. |
| Q3 polygons not inverted? | **refuted** — 7,301 / 7,301 inverted. The mechanism is polygon *coarseness*, not a broken join or the alphabetical tiebreak. Highest-value hypothesis in the brief, and it was wrong. |
| Q4 collisions? | **confirmed** — but the headline 410 overstates it; only ~38 are collapse-induced (§3). |
| Q5 schema addition? | **mostly delivered** — `src_group_label` (stable group id, better than lat/lon since it is the actual detection key), `n_dayof`, granularity, `src_metro`. **Baseline count still missing** (§6.8). Also note the identity is emitted by **`02`**, where the grouping happens, not by 03/04/06 as the brief assumed. |

### 10.7 Provenance of `src_match_granularity`

Added **2026-08-10 ~00:03 CDT** (mtime on `07` and `group_identity_alters.sql`),
applied and populated in staging ~00:04:42 CDT, in response to the review note
now in §5. It was **not** part of the earlier session's work. BigQuery job
history could not be consulted — the available identity lacks
`bigquery.jobs.list` on `mlab-collaboration` — so the link between the file edit
and the staging write is inferred from timestamps, not from a job record.

### 10.8 Code observations not yet tracked elsewhere

1. `ta.src_metro` (grouped by 9 columns) and `gi.src_metro` (grouped by 4) are
   independent `ANY_VALUE`s of the same quantity, so the metro used to *match* a
   hyperedge can differ from the metro *reported* on the row. Source both from
   one CTE.
2. The `group_identity` comment asserts `src_metro` / `detection_granularity` are
   "constant within this key". Appendix B's overlapping polygons suggest that is
   not guaranteed. Unverified either way — one query settles it.
3. `07` now reads `events_with_as_and_geoloc` **six times**; `dayof_counts`,
   `distance_extents` and `group_identity` share key and filter and merge into
   one CTE.
4. `user_group = CONCAT(src_city, ' - ', src_asn)` changes **value** on most rows
   (MaxMind city name replaces the metro's). Same shape, different content —
   dashboards keying on that string break silently. Belongs in the design doc's
   "what changes observably".
5. `scripts/verify_group_identity.sql` check 3 was rewritten 2026-08-10 from an
   always-*passing* assertion (`src_city = src_metro`, the superseded legacy-alias
   design) into an always-*failing* one (`src_city != src_metro`). Both are wrong:
   the two are legitimately equal whenever a city names its own metro
   (`Auckland-AUK-NZ` → `src_city` `Auckland-Auckland-NZ` = `src_metro`). It now
   asserts the invariant the bug actually violated — that `src_city` still leads
   with the MaxMind city name from `src_group_label`.
6. The focused regression suite passes (**38 passed**), with Ruff and
   `git diff --check` clean. The full suite is blocked during collection by
   pre-existing environment/code issues: system Python 3.10 lacks
   `datetime.UTC`, while the Python 3.11 virtualenv reaches the legacy
   `except ValueError, IndexError:` syntax in the IPv6 RouteViews enricher.

### 10.9 Suggested order

1. **Build and deploy A** from the implemented 02/03/07 explicit column lists —
   still reversible and behaviour-free.
2. **Done:** dataset-aware Phase D exercised the label arm on 2026-08-07.
3. **Decide whether B stays** (§10.4) before polishing the marker column.
4. **Done locally:** final provenance names adopted and pinned with tests.
5. **`stats-rigor` on the ≥10 gate** (§6.1) — the one open question that changes
   what the system claims.
6. **Done:** August staging is complete, including the 08-07 rebuild.

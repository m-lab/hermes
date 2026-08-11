# Proposal: make detection granularity explicit and consistent

Status: **staging rehearsal complete** (rev 5). Drafted 2026-08-09, revised 2026-08-10.
Rev 5 closes the cross-consumer identity gaps and adopts the final provenance
names. See
[What changed since rev 3](#what-changed-since-rev-3).
Operational state (deployment, remaining work, quota) lives in the companion
`2026-08-group-granularity-HANDOVER.md`; that file is authoritative for status.

## The bug, in one sentence

A downstream geographic enrichment silently re-grouped every stage after it at a
coarser granularity than anomaly detection actually ran at.

`02_detect_anomalies_union.sql` tests groups keyed on
`(src_asn, src_city, dst_site, ip_version)`, where `src_city` is the MaxMind
`City-Subdivision-Country` triple (02:120, 163). `04_mapping_union.sql` then
**overwrote `src_city` in place** with a metro-polygon label. Metro polygons are
far coarser (median hole 48,505 km²), so from that point on:

- **22.78%** of `(src_asn, dst_site, ip_version, src_city)` keys covered more than
  one tested population (24,742 of 108,628; worst 136).
- `events_explained_daily` carried **duplicate keys on every partition tested** —
  294 / 157 / 403 / 340 / 287 / **1,441** / 410 rows for 2026-08-01..07.
- `06_correlation_tomography_prepare_union.sql:91` built hyperedge pair strings
  from the collapsed label, so **Phase D attributes metro-collapsed groups** —
  verified: stored strings carry metro vocabulary (`9605 - Utsunomiya-Tochigi-JP
  - hnd02`, full prefecture names, not MaxMind ISO codes).
- `07`'s data-sufficiency gate (`n_dayof >= 10`) counted **whole metros**. The
  motivating Castello case fired on a group with **6** day-of measurements, which
  passed only because its metro totalled 38.

That last point is the sharp end: a 6-measurement group was not merely
mislabelled, it **should not have passed the sufficiency gate at all**.

This is **not** a polygon-matching bug — see [Appendix A](#appendix-a-polygon-investigation).

**Scope:** 319 of 345 partitions (2025-08-01 → 2026-08-07). The 26 exceptions
(`2026-02-01..02-21`, `2026-07-04..07-08`) predate the rollout and carry ~29,782
distinct labels/day against ~3,977 elsewhere.

## Principles

1. **The granularity detection ran at is recorded explicitly.** The schema can
   represent either mode, but switching modes is an algorithm change: the raw
   measurements must be regrouped and all statistics recomputed.
2. **Every quantity records the granularity at which it was computed.** Detection
   quantities use `detection_granularity`; historical attribution may separately
   record a metro-vocabulary match in `src_match_granularity`.
3. **Geographic rollup is an annotation**, never a replacement for the key.

### Metro-mode extension (2026-08-11)

The pipeline now accepts `--detection-granularity maxmind_city|metro` (default
`maxmind_city`). Metro mode resolves `metro_polygons_v2` before trimming,
baseline construction, and statistical tests; step 03 uses the same resolver
when attaching traceroutes and computing day-of summaries. Existing dates must
be deleted across the complete pipeline before changing mode, and
`scripts/backfill_detection_granularity.sql` labels historical tested rows as
`maxmind_city` without falsely labelling unmatched giga-meter traces.

## Final schema

| column | example | role |
|---|---|---|
| `detection_granularity` | `maxmind_city` | granularity of anomaly detection and detection-derived counts/statistics |
| `src_group_label` | `Castelló de la Plana-VC-ES` | the **exact key `02` grouped on**; all keying and counting uses this |
| `src_city` | `Castelló de la Plana-Comunidad Valenciana-ES` | readable label — MaxMind city name, metro's **full** state, country |
| `src_state` | `Comunidad Valenciana` | same authority as the label above |
| `src_metro` | `Castello-Comunidad Valenciana-ES` | the metro rollup; the hook for future metro-level detection |

Per table:

| table | new columns |
|---|---|
| `anomaly_counts_union` | `detection_granularity`, `src_group_label` |
| `transient_events_union` | `detection_granularity`, `src_group_label` |
| `events_with_as_and_geoloc` | + `src_metro` |
| `giga_meter_measurements` | + `src_metro` |
| `events_explained_daily` | `detection_granularity`, `src_metro`, `src_group_label`, `n_dayof`, `src_match_granularity` |

**Why `src_city` and `src_group_label` are both needed.** Substituting the metro's
full state name into the label merges **45 of 30,222** labels (0.15%) where a
city's polygon sits in a different state — `Kansas City-KS-US` and
`Kansas City-MO-US` both become `Kansas City-Kansas-US`, and `Chester-NY-US`
becomes `Chester-New Jersey-US`. That is acceptable for a *label* and fatal for a
*key*. `src_group_label` stays the raw, injective MaxMind triple; `src_city` is
readable. Everything that groups, joins or counts uses `src_group_label`.

## Change set

**`02`** — emit `detection_granularity = 'maxmind_city'` and
`src_group_label = src_city` (the value it already groups on). No regrouping, no
cost change. Appended last: this INSERT has no column list, so SELECT order must
match the table and `ALTER` appends.

**`03`** — carry both through nine sites. Deliberately **not** given the
`IF(a.src_asn IS NULL, <scamper fallback>, ...)` treatment `src_city` gets: a
giga trace with no `AnomalyCounts` match was never in a tested group, so NULL is
the truthful value.

**`04`** — stop overwriting the granularity. `src_city` becomes the readable
label (parsed from the **right**, because city and state names contain `-`:
`Saint-Agapit-QC-CA`, `Île-de-France`), `src_state` takes the metro's state so
the row does not carry a full name in one column and an ISO code in another, and
`src_metro` carries the rollup. Only the **source** location changes — `dst_*` and
hop-level `place`/`metro` are untouched.

**`06`** — pair strings key on `src_group_label` (one line, 06:91), so Phase D
attributes the tested population. The path-local unexplained-hop query uses the
same identity; its strings are intersected exactly in Python and previously
still used `src_city`, which disabled that fallback after the main key changed.

**Temporal consumers** — both the legacy temporal tomography grouping and the
v2 edge-prevalence pair strings use `src_group_label`. The source endpoint in a
topological path remains `src_metro`, because a hop location is a rollup rather
than a detection-population identifier.

**`07` — hyperedge compatibility.** Hyperedges built before this change key
their pair strings on the metro. A hyperedge's *content* (`from_asn`, `to_asn`,
`edge_asn_metro`) is an intermediary **hop** pair and is granularity-independent,
so the historical corpus stays valid — only its pair *labels* are in the old
vocabulary. `07` therefore matches on `src_group_label` **or** `src_metro`, and
the `unresolved` branch mirrors both, so no Phase D re-run is required.
Historical partitions keep metro-level *attribution* while gaining city-level
*events*, which is exactly what those partitions have always meant.

Two subtleties this exposed, both now covered by tests:

- `resolved` must take `src_group_label` from the **matched event row**, not from
  the pair string. A metro-keyed hyperedge otherwise writes a METRO into that
  column and the INNER JOIN to `anomaly_summary` silently drops every resolved
  row — indistinguishable from the join never matching.
- The display city must be parsed from `src_group_label` (`City-ISO-CC`), the
  only right-parseable form. `City-State-CC` is parseable from neither end: city
  names contain `-` (`Saint-Agapit`, `Vila-real`) and full state names do too
  (`Nordrhein-Westfalen`, `Zuid-Holland`).

**`07`** — 13 keying sites move from `src_city` to `src_group_label`:
`dayof_counts` (the ≥10 gate), `distance_extents`, `group_identity`,
`total_anomalous_src_dst_pairs`, `anomaly_data`, `anomaly_summary`, the
`resolved`/`unresolved` pair-string comparisons, and the three joins in
`combined_with_anomaly_summary`. `src_city` remains for display only.

## What changes observably

This is **not** semantics-preserving. Rev 1/2 claimed it was; that framing
assumed `src_city` stayed the metro.

- **`src_city` cardinality** ~3,977 → ~30,222 distinct/day. Anything grouping or
  counting on it changes visibly.
- **Event counts drop ~47%** (2,902 → 1,549 on a rehearsed date). The ≥10 gate
  now counts the tested population: of 3,463 groups clearing the anomaly
  conditions, only **1,308 (37.8%)** have ≥10 day-of measurements. Median
  `n_dayof` across anomalous groups is **7**. Castello's Benicàssim group (6
  day-of) no longer qualifies. **The threshold was never really binding before
  and is now doing heavy lifting — it deserves an explicit statistical review
  rather than being inherited.**
- **Duplicate-key removal is smaller than rev 3 implied.** Measured like-for-like
  on 2026-08-01: prod has 294 extra rows, of which **256 are legitimate
  multi-edge attribution** (one pair explained by several culprit edges) and only
  **38 are collapse-induced**. After the change: 194 extra rows, **all**
  multi-edge, 0 collapse-induced. The earlier "410 → 0" was an artifact — that 0
  came from a run whose attribution was 100% NULL, so every row came from the
  `unresolved` branch, which cannot produce multi-edge duplicates.
- **Phase D attribution changes** — more correct, but not a like-for-like
  comparison with historical output.
- **Historical partitions are inconsistent** until backfilled.

## Migration — no pipeline re-run

`transient_events_union.src_city` was written by `03` and never overwritten, so
the detection label is recoverable by joining on `id`. And on a rolled-up
partition `events_with_as_and_geoloc.src_city` **already holds the metro**, so
`src_metro` needs no polygon lookup.

**Measured: 75 GiB per partition ≈ 25 TiB across 345**, versus ~175 TiB to
re-run them (447-518 GiB per date). A read-side proxy suggested 4.5 GiB; the
`UPDATE` rewrite dominates. See `scripts/backfill_group_identity.sql`.

The backfill is **not idempotent** — it sets `src_metro = e.src_city`, valid only
while `src_city` still holds the metro. The `ASSERT` catches a second run by
accident (distinct `src_city` is ~30,545 afterwards, over its 10,000 threshold).
Make that guard explicit before running it at scale.

**TRAP, guarded by an `ASSERT`:** the `src_metro = src_city` shortcut is false for
the 26 pre-rollup partitions, where `src_city` is still the MaxMind label.
Applying it there would record a MaxMind city as a metro, with no error. Those
dates need the polygon lookup.

## Deployment

`02`/`03` INSERT without a column list, so `ALTER TABLE ADD COLUMN` **breaks the
running image immediately** — verified: `Inserted row has wrong column count;
Has 50, expected 48`. Worse, it removes the rollback path: after ALTER + deploy,
retagging to the previous image fails the same way.

Sequence, so every step stays reversible:

1. **Deploy A** — convert `02`/`03`/`07` to explicit column lists. No schema change,
   no output change.
2. **ALTER** — now safe; deployed code names its columns.
3. **Deploy B** — this change. Rollback to A works.

The final implementation uses explicit column lists in all three writers, so
ALTER order is no longer load-bearing. Run `scripts/verify_group_identity.sql`
after the first write to verify the public identity contract against the event
groups themselves.

## Rehearsal

`mlab-collaboration.hermes_staging` holds clones (`cloneDefinition` verified —
copy-on-write, 0 bytes billed, prod never in scope).
`scripts/build_staging_sql.py` rewrites every operational `hermes_union`
reference in 01/02/03/04/05/07 and refuses to emit if one survives. The sole
allowlisted exception is the shared, read-only `place_canonical_metro` lookup.

The 2026-08-10 rehearsal completed all 2026-08-01..07 staging partitions. A
single city-keyed Phase D control on 2026-08-07 produced 1,162 hyperedges; the
rebuilt public partition contains 1,490 exact city matches, 400 unresolved rows,
and zero metro matches. Historical dates reuse their existing hyperedges and are
explicitly marked `src_match_granularity = 'metro'` rather than paying to rerun
Phase D.

## Open questions

1. Multiple comparisons, if metro-level detection is added later: city and metro
   tests run on overlapping data, so their events are correlated.
2. Polygon coarseness and overlap are separate defects — [Appendix B](#appendix-b-polygon-coverage-parallel-workstream).
3. How far event counts drop under per-group gating — measured in rehearsal.

## What changed since rev 3

- **`07` gained a hyperedge compatibility join.** Historical metro-keyed
  hyperedges are consumed directly; no Phase D re-run for history.
- **`events_explained_daily` gained `src_group_label` and `n_dayof`.** Rev 3
  omitted a label as ambiguous; that reasoning predates the re-keying — with `07`
  keyed on the label, one row is one tested population. Verified on 2026-08-01:
  `min(n_dayof) = 10`, 0 rows below threshold.
- **The duplicate-key claim was corrected downward** (410 → 38 genuine).
- **The display-name parse moved to `src_group_label`** after right-parsing
  `src_city` was found to bleed state into city.
- **Measured costs replaced estimates**: forward run 447 GiB/date; backfill
  `UPDATE` **75 GiB/partition**, not the 5-10 extrapolated from a read-side proxy
  (the DML rewrite dominates ~16×, since the table is 75 columns with two large
  `REPEATED STRUCT` hop arrays). History via backfill ≈ 25 TiB, not 3-7.

## What changed since rev 2

- `src_city` now holds the readable detection label, not the metro. Rev 2 kept it
  as a metro alias for compatibility; that preserved the collapse in `06`/`07`.
- `src_state` moves to the metro's authority.
- `src_group_id` and `src_city_maxmind` dropped — no consumer, and redundant.
- `events_explained_daily` carries granularity + metro only; the group-count and
  label-array ideas are gone, since fixing the granularity removes the ambiguity
  they described.
- An earlier `auto` policy selecting granularity by sample size was dropped: it
  conflated geolocation precision with statistical power, and was ill-defined
  (the metro row aggregates A+B+C, not the residue of sub-threshold cities).

## Appendix A: polygon investigation

Rules out an attractive wrong explanation.

The join is `ON NOT(ST_CONTAINS(mp.polygon, ST_GEOGPOINT(lon, lat)))`. If any
polygon were not inverted, the predicate would match nearly everything and
`ARRAY_AGG(metro ORDER BY metro ASC LIMIT 1)` would pick an arbitrary
alphabetically-first metro.

Verified false: all **7,301** polygons are inverted (`ST_AREA` 4.879e14-5.101e14 m²
against Earth's 5.101e14, zero exceptions), and the five points of the motivating
case each matched **exactly one** polygon — the tiebreak did not fire *there*.
(Rev 1 generalised that into "the tiebreak is not firing", which is wrong; see
Appendix B.)

The amplifier is coarseness: median hole 48,505 km² (~220 km across), 948
polygons >100,000 km², 88 >1M km², max 22.2M km².

## Appendix B: polygon coverage (parallel workstream)

**Not part of this proposal.** The polygon set does not tile the world cleanly.
The source file is being regenerated so every `(lat, lon)` maps to exactly one
sensible metro.

Over 63,065 distinct source points, partition 2026-08-07: 62,952 (99.8%) matched
exactly one polygon, **111 (0.2%)** matched two, 2 matched none. Of the 111, only
**2** are one metro recorded twice; **109** are genuinely different metros
overlapping.

1. **Duplicate records for straddling metros.** `Kansas City` exists twice —
   `Kansas City-Kansas-US` (36,348 km²) and `Kansas City-Missouri-US` (35,873 km²).
   Either answer names the same place, so the pick is harmless for grouping.
2. **Catch-all "remainder" polygons swallowing real metros.** `Raoul Island
   Station / South I. remainder` (NZ) overlaps Auckland, Hamilton, Tauranga,
   Cambridge, Pukekohe and North Shore — Raoul Island is ~1,000 km away. Because
   `Raoul` sorts early, **20 points are labelled
   `Raoul Island Station-South I. remainder-NZ`**: a wrong location, not a coarse
   one. Auckland (44 points) and Hamilton (20) kept correct labels by alphabetical
   luck alone.

**Independent of the source-file fix, `04`'s tiebreak should not be alphabetical.**
`ORDER BY metro ASC LIMIT 1` encodes no notion of correctness; ranking by
most-specific (smallest polygon) or largest population would have chosen Tauranga
over Raoul Island regardless.

## Verification status

**Verified by query:** collision counts and `events_explained_daily` duplicate
keys; polygon inversion across all 7,301 rows; the Castello points each matching
one polygon; the 319/26 partition split across all 345 partitions;
`transient_events_union` retaining the detection label (30,222 distinct vs 4,007);
the 45-label merge from substituting the metro state (0.15%);
`(country_code, state_iso2) → state_resolved` 100% unambiguous forward across
1,091 keys, but only **29.0%** coverage (1,946 of 2,741 subdivision codes unmapped
— exported to `docs/reference/state_iso2_to_name.tsv`); Appendix B's overlap
measurements; the ALTER column-count failure mode; `06` pair strings carrying
metro vocabulary; repo and live image byte-identical for the `_src_metro` block
(md5 `fc330c0f9c19c16350bd1a3b7f68236c`).

**Inferred, not verified:** that the rollup reached the deployed image ~2026-07-09
(from the 2026-07-04..07-08 outlier cluster); that 3-7 TiB covers the backfill
(extrapolated, not dry-run at scale — prior estimates in this project have run low).

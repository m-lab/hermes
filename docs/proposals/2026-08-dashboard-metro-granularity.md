# Dashboard changes for metro granularity + metro_polygons_v2

**Audience:** whoever maintains `Burdantes/hermes-dashboard` (Flask + `/api/*` +
Plotly.js, local port 8050, reads `mlab-collaboration.hermes_union`).
**Status:** pipeline side validated in staging; **not yet in production**. Treat
this as the spec to build against, not a description of live data.
**Written:** 2026-08-11.

Two independent pipeline changes land together and both touch what the dashboard
reads.

---

## 1. What actually changed upstream

### A. `metro_polygons_v2` replaces `metro_polygons_with_population`

| | old | new |
|---|---|---|
| Table | `hermes.metro_polygons_with_population` | `hermes.metro_polygons_v2` |
| Rows | 7,301 | 8,437 (7,349 metros × 1–2 partition tiers) |
| Geometry | reads back **inverted** | ordinary **positive** polygons |
| Lookup | `NOT ST_CONTAINS(polygon, pt)` | **`ST_COVERS(polygon, pt)`** |
| Tie-break | `ORDER BY metro ASC` (alphabetical) | distance to seed, then `metro_id` |

If the dashboard queries the polygon table directly anywhere — choropleths,
geographic joins, a metro picker — **the predicate must be inverted**. Leaving
`NOT ST_CONTAINS` against v2 matches every cell except the correct one.

The old inversion was never a design choice: it was a load artifact of iGDB's
unwrapped longitudes (down to −197.5°), which BigQuery wraps on ingest so the
ring reads as its complement.

### B. `--detection-granularity metro` changes the grouping key

`src_city` becomes the canonical metro instead of the MaxMind
`City-SubdivisionISO-Country` label. Measured on 2026-08-07 in staging:

| | city | metro |
|---|---|---|
| `anomaly_counts_union` rows | 198,339 | 113,943 (−42.6 %) |
| distinct `src_city` | 30,259 | **4,003 (−86.8 %)** |

So the dashboard's source dimension collapses ~7.6×.

---

## 2. New and changed columns

### `metro_polygons_v2`

| Column | Notes |
|---|---|
| `metro_id` | **stable hash of the canonical metro identity, not of geometry.** Survives geometry rebuilds — prefer it as the join key over the `metro` string |
| `metro` | `City-COALESCE(state_resolved, state_iso2, 'NA')-CC`. **Take it from the table; never re-CONCAT it** |
| `state_code`, `state_name` | ISO 3166-2 (e.g. `US-CA`) and full ADM1 name. NULL on country-tier rows |
| `partition_tier` | `'state'` or `'country'` — see §4 |
| `seed_lat`, `seed_lon` | the metro's seed point; use for distance/tie-breaks |
| `seed_pop_max` | population of the settlement (2.75 B global) |
| `cell_population` | population **inside the cell** (7.89 B global) — the one to use for choropleth normalisation |
| `legacy_cell_population` | old table's `population_sum`, for comparison only. Computed against the **old** geometry; do not present it as describing v2 cells |
| `geometry_area_km2` | spherical area |

### Pipeline tables (`anomaly_counts_union`, `transient_events_union`, `events_with_as_and_geoloc`, `events_explained_daily`)

| Column | Notes |
|---|---|
| `detection_granularity` | `'maxmind_city'` or `'metro'`. **See the NULL caveat in §5** |
| `src_group_label` | the exact key step 02 grouped on. **Nullable by design** (§5) |

---

## 3. Required changes, by area

### 3.1 Every query gets a granularity filter — this is the big one

A date range will eventually span both regimes. Any aggregate that mixes them is
wrong: metro groups are ~7.6× coarser, so counts, ratios and rankings are not
comparable across the boundary.

```sql
-- WRONG once both regimes exist in the table
SELECT src_city, COUNT(*) FROM events_explained_daily
WHERE partition_date BETWEEN @lo AND @hi GROUP BY 1

-- RIGHT
SELECT src_city, COUNT(*) FROM events_explained_daily
WHERE partition_date BETWEEN @lo AND @hi
  AND detection_granularity = @granularity     -- never mix
GROUP BY 1
```

Recommended UX: a **single global granularity selector** (City / Metro), defaulted
to whatever the newest partition carries, applied to every panel. Not a per-panel
toggle — two panels on different regimes side by side will be read as a finding.

If a requested range spans the cutover, either clamp to the selected regime and
say so, or refuse and show which dates exist at which granularity:

```sql
SELECT detection_granularity, MIN(partition_date), MAX(partition_date), COUNT(DISTINCT partition_date)
FROM `mlab-collaboration.hermes_union.events_explained_daily`
GROUP BY 1
```

### 3.2 Group-identity change breaks saved links and caches

`metro` moves for essentially every row, from two causes:

1. **Region naming.** ~91.5 % of rows keep the same city but get a different
   region component. `Passau-NA-DE` → `Passau-Bayern-DE`. The old
   `enrich_*_add_metro.sql` used bare `state_iso2`, which put **37.85 %** of live
   metro values into the `City-NA-CC` form; that is now 0.08 %.
2. **Mojibake fixed.** Natural Earth's populated-places layer ships corrupted
   ADM1 names and those strings are in today's live keys: `Medenine-MUdenine-TN` →
   `Medenine-Médenine-TN`, `Luan Chau-Vi?n Bi-VN` → `Luan Chau-Điện Biên-VN`.
   944 region names corrected. Also collapses double spaces
   (`Washington,  D.C.` → `Washington, D.C.`).

Consequences:
- **Any URL, bookmark or saved view keyed on the `metro` string breaks.** Add a
  redirect or a "this metro was renamed" notice rather than a blank panel.
- **Invalidate every cached aggregate** keyed on `src_city` / `metro` at cutover.
- Prefer `metro_id` for internal keys and links from now on.

### 3.3 Choropleths and map layers

- Switch the predicate to `ST_COVERS`.
- Positive geometry means `ST_AREA(polygon)` is now the **cell area** (median
  ~14,600 km²), not ~510 M km². Any area-based sizing or normalisation inverts.
- Use `cell_population` for per-capita normalisation, not `seed_pop_max` (they
  differ by 2.9× globally) and not `legacy_cell_population`.
- 8,437 rows / ~33 MiB of geometry: fine to serve, but pre-simplify for the
  browser. Don't ship raw WKT per cell.
- **Do not draw a metro cell as "the city".** These are nearest-seed catchments,
  not municipal boundaries — a cell can be 100,000 km² where seeds are sparse.
  Label them as catchments in the legend.

### 3.4 Cross-border and cross-state labels disappear

The dashboard will stop showing foreign-country metros for domestic IPs. 993,552
cross-country assignments per full scan → **0**. Concretely gone: Moroccan IPs
labelled Ceuta/ES, Batam labelled Singapore, Windsor/CA for US IPs.

If any view had compensating logic — special cases, exclusion lists, a "same
country?" check — **remove it**; it will now suppress correct rows.

### 3.5 New fields worth surfacing

Two fields exist specifically to make a plausible assignment distinguishable
from an implausible one. The enrichment path writes them:

| Field | Use |
|---|---|
| `metro_distance_km` | distance from the coordinate to the metro's seed. p50 30 km, p95 95 km, p99 150 km. **Flag or footnote anything over ~150 km** |
| `metro_assignment_method` | `polygon` (98.7 %), `country_nearest_fallback` (1.24 %), `country_nearest_fallback_far` (0.02 %) |

A row resolved by `country_nearest_fallback_far` is a coordinate whose nearest
in-country metro is >100 km away — usually poor IP geolocation. Worth a subtle
marker on Event Detail rather than presenting it with the same confidence as a
containment match.

---

## 4. `partition_tier`: state vs country

Metro cells come from two tiers:

- **`state`** (7,269 cells, 2,756 ADM1 units) — built inside a single ADM1 unit,
  so a point in that cell is guaranteed to be in that state. 95.6 % of rows.
- **`country`** (1,168 cells) — territory in ADM1 units that have **no metro of
  their own**, repartitioned among all the country's seeds. A point here can get
  a metro from a neighbouring state.

Filter on `partition_tier = 'state'` if a view needs the region guarantee.

ADM1 granularity is **not comparable across countries**: US ADM1 is states
(nearly all seeded), France's is departments (96 units, only 52 seeded). So the
country-tier share varies a lot by country. Don't present "region confidence" as
a single global number.

---

## 5. NULL `detection_granularity` is expected — do not treat it as missing data

Step 03 leaves `detection_granularity` and `src_group_label` **NULL** for giga
traces whose group is absent from `anomaly_counts_union`. This is deliberate and
test-enforced: such a trace was never in a tested group, so labelling it would
assert a test that never ran.

Measured 2026-08-07 in staging: **2,147 of 32,650,613 rows (0.0066 %)**, all
`client_name = 'giga-meter'`. Their `src_city` **is** the correctly resolved metro
(verified: all 50 distinct values matched `metro_polygons_v2.metro`), so they
group correctly.

Implications:
- `detection_granularity = 'metro'` **silently drops these rows.** For giga views,
  use `COALESCE(detection_granularity, @granularity) = @granularity`, or filter on
  `partition_date` + an explicit giga predicate instead.
- `src_group_label IS NULL` means **"never part of a tested group"**, not
  "unknown". Don't render it as missing/error.
- Pre-migration partitions are **entirely** NULL for both columns. That is a
  different case — unknown regime — and should be treated as city-era data.

---

## 6. Suggested rollout for the dashboard

1. **Now:** switch any direct polygon query to `ST_COVERS` against
   `metro_polygons_v2`; add the granularity filter to every query, defaulted to
   `'maxmind_city'` so behaviour is unchanged. Ship this before the pipeline
   cutover — it is backwards compatible.
2. **Add** the granularity selector, `metro_distance_km` / `metro_assignment_method`
   surfacing, and `partition_tier` filtering.
3. **At cutover:** flip the default to `'metro'`, invalidate caches, enable the
   renamed-metro redirect.
4. **Remove** any cross-border compensation logic.

Step 1 is safe today. Steps 3–4 must wait for the pipeline to actually run metro
in production.

---

## 7. Verification the dashboard team can run

```sql
-- Which regimes exist, and over what range?
SELECT detection_granularity, MIN(partition_date) AS lo, MAX(partition_date) AS hi,
       COUNT(DISTINCT partition_date) AS days, COUNT(*) AS rows_
FROM `mlab-collaboration.hermes_union.events_explained_daily`
GROUP BY 1 ORDER BY 2;

-- The new lookup shape. ST_COVERS, country-scoped, distance tie-break.
SELECT mp.metro, mp.metro_id, mp.partition_tier, mp.state_code,
       ST_DISTANCE(ST_GEOGPOINT(@lon, @lat),
                   ST_GEOGPOINT(mp.seed_lon, mp.seed_lat)) / 1000 AS metro_distance_km
FROM `mlab-collaboration.hermes.metro_polygons_v2` AS mp
WHERE mp.country_code = @country
  AND ST_COVERS(mp.polygon, ST_GEOGPOINT(@lon, @lat))
QUALIFY ROW_NUMBER() OVER (ORDER BY metro_distance_km, mp.metro_id) = 1;

-- Sanity: cell population must total world population, not 143 B (all vintages)
SELECT ROUND(SUM(cell_population)/1e9, 3) AS billions   -- expect ~7.893
FROM `mlab-collaboration.hermes.metro_polygons_v2`;
```

Regression coordinates the old table got wrong, useful as dashboard smoke tests:

| Coordinate | old label | correct |
|---|---|---|
| Majuro, Marshall Is. | `Atafu / NZ` | `Majuro-Ratak Chain-MH` |
| Suva, Fiji | `Nukualofa / TO` | `Suva-Central-FJ` |
| Pago Pago, Am. Samoa | `Labasa / FJ` | `Pago Pago-Eastern-AS` |
| Utqiagvik, Alaska | no match | `Utqiaġvik-Alaska-US` |

---

## 8. Open items the dashboard does not need to solve

- Production `hermes_union` does not yet have `detection_granularity` /
  `src_group_label`; `scripts/group_identity_alters.sql` must be applied first.
- Phase C/D/E have not yet been compared city-vs-metro, so
  `events_explained_daily` volumes under metro are not yet known. **Do not size
  panels or pagination on the city-era numbers.**
- ~1.2 % of Antarctica is uncovered by v2 (zero population; no HERMES rows exist
  below 60 °S). Irrelevant to any dashboard view, noted for completeness.

Provenance, method and the full validation report: `cleaning_polygons/README.md`.

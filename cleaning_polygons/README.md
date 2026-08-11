# cleaning_polygons — country- and state-constrained metro tessellation

Replaces `mlab-collaboration.hermes.metro_polygons_with_population` with
`mlab-collaboration.hermes.metro_polygons_v2`: ordinary positive geometry,
queried with `ST_COVERS`, partitioned so a coordinate can only ever be given a
metro in its own country **and** — wherever the data supports it — its own
state/province.

```bash
python -m cleaning_polygons.build --build-version 2026-08-v2 --upload
```

Built and verified 2026-08-10/11. All queries dry-run first; everything is billed
to `mlab-collaboration`. Total cost was about **$1.25** of BigQuery scan,
dominated by the validation passes over `unified_ip_to_geoloc`; the polygon table
itself is 33 MiB and free to query repeatedly, and the population recompute came
to ~$0.01.

---

## Results against live HERMES data

`unified_ip_to_geoloc`, 99,437,215 rows / 91,566,517 with coordinates:

| Metric | Old table | **New table** |
|---|---|---|
| Cross-country metro assignments | 993,552 (1.084 %) | **0** |
| Coordinates matching >1 polygon | 229,705 | **0** |
| Coordinates matching no polygon | 146 | **0** |
| Rows assigned a metro | 91,566,371 | **91,566,473** |
| `City-NA-CC` share of metro strings | 37.85 % | **0.08 %** (7 rows) |
| Distinct metros used | 5,898 | 5,872 |
| Metro distance p50 / p95 / p99 | not recorded | 30.2 / 95.2 / 150.4 km |

The 44 coordinate-bearing rows still unassigned have a **blank** `country` in the
source. There is no country to constrain to, so this is a geolocation-data gap,
not a geometry gap. Every real country code present in HERMES has cells.

Assignment method breakdown: 90,409,420 rows by containment (98.7 %), 1,138,866
by the in-country nearest-seed fallback (1.24 %), 18,187 by that fallback beyond
100 km (0.02 %).

### The specific defects that are fixed

Verified through BigQuery at the real coordinates:

| Case | Old table | New table |
|---|---|---|
| Majuro, Marshall Is. | `Atafu / NZ` ✗ | `Majuro-Ratak Chain-MH` ✓ |
| Tarawa, Kiribati | `Atafu / NZ` ✗ | `Tarawa-Kiribati-KI` ✓ |
| Suva, Fiji | `Nukualofa / TO` ✗ | `Suva-Central-FJ` ✓ |
| Pago Pago, Am. Samoa | `Labasa / FJ` ✗ | `Pago Pago-Eastern-AS` ✓ |
| Utqiagvik, Alaska | no match | `Utqiaġvik-Alaska-US` ✓ |
| Longyearbyen, Svalbard | no match | `Longyearbyen-Svalbard-SJ` ✓ |
| Qaanaaq, Greenland | no match | `Qaanaaq-Qaasuitsup Kommunia-GL` ✓ |

All 18 antimeridian and Arctic regression cases resolve to the correct country;
seam probes at ±179.999 produce zero double-matches.

---

## How it works

### The invariant

For a point `x` in country `c` and ADM1 unit `s`:

```
state tier    x in S_s  ->  nearest seed among S_s' seeds       (s has >= 1 seed)
country tier  x in R_c  ->  nearest seed among c's seeds        (R_c = c minus seeded states)
```

`d` is always great-circle distance. The two tiers are disjoint and together tile
the country exactly, so the country invariant is unconditional while the state
invariant holds wherever the state has metros of its own. A state with no metros
cannot be internally consistent; those points fall back to the country tier and
`partition_tier` records it.

**Important consequence for consumers**: because state-tier cells are clipped to
their own ADM1 unit, state consistency is *structural*. The production lookup does
**not** need a region string — a point covered by a state-tier cell is necessarily
in that cell's state. `region_ip_info` is only useful for cross-checking.

### Why the geometry is exact

Great-circle distance is monotone in the dot product, so "closer to `p_i` than to
`p_j`" is the linear half-space `x · (p_i − p_j) ≥ 0`, bounded by a great circle.

Under a **gnomonic** projection about unit vector `c`, the sphere direction of a
plane point `(u, v)` is proportional to `c + u·e1 + v·e2`. Substituting into that
linear condition and dropping the positive scale factor gives

```
A·u + B·v + D >= 0        A = e1·w, B = e2·w, D = c·w,  w = p_i - p_j
```

an exact planar half-plane. So a spherical Voronoi cell is exactly a convex
polygon in the gnomonic plane, and great circles are exactly straight lines there.
Nothing is linearised. Euclidean lon/lat distance is never used, and longitude
differences are never taken as `abs(l1 - l2)`.

Cells are found by clipping half-space by half-space in order of increasing
angular distance, with an exact early exit: once the cell lies within angular
radius `R` of `p_i`, no seed farther than `2R` can reach it.

### The two coordinate hazards

**Antimeridian.** On the sphere ±180 is not a boundary at all. Each country is
tessellated in its own gnomonic frame centred on its own territory, so the seam is
never a boundary of the computation; it appears only when writing lon/lat rings,
which `antimeridian.py` handles by unwrapping longitudes as accumulated *shortest*
steps and splitting on 360° strips.

**Poles.** A pole is a single point that plate-carrée must draw as a whole
parallel, and a polar outline traverses ±180 twice. Both are format artefacts and
both vanish under a rigid rotation moving the pole to (0, 0). Polar countries are
rotated, tessellated as ordinary mid-latitude blobs, and rotated back. The
rotation is orthogonal, so it is distortion-free — Antarctica's area round-trips
to the kilometre.

---

## Verification

`validate_tessellation.py` computes the plan's invariants as set operations and
**fails the build** if any breach the tolerances in `config.TOL`. Current run:

| Check | Result |
|---|---|
| Seed self-assignment (§18.1) | **0 failures** of 7,349 |
| Cross-country cell area (§18.7) | **173 km²** of 146.7 M (1.2e-6) |
| Interior uniqueness (§18.4) | **0 multi-matches** of 1,789 points |
| Nearest-seed brute force (§18.5) | **0 mismatches** of 8,628 points, tier-aware |
| Pairwise cell overlap (§18.6) | **10.4 km²** globally |
| State-tier: points in the wrong state | **0** of 16,445 |
| State-tier: cell area outside its own state | 16.4 km² over 7,269 cells |
| Antimeridian cases (§18.8) | **0 failures**, 0 seam double-matches |
| Arctic cases (§18.9) | **0 failures** |
| Per-country area agreement | **234 of 246 within 0.01 %** (US: 16 km² on 9.45 M) |

Confirmed independently in BigQuery, whose `ST_COVERS`/`ST_AREA` are natively
spherical: 7,338 of 7,349 seeds resolve to themselves, **0 resolve to another
metro**, **0 multi-match**. The 11 that do not self-resolve sit just offshore of
Natural Earth's 10 m coastline and reach their own metro through the recorded
fallback.

### Known limitation: Antarctica

About **1.2 % of Antarctica** (~150,000 km² of ice sheet within roughly 5° of the
South Pole) is not covered by any cell. Measured by sampling 4,000 points
uniformly on the sphere inside the AQ boundary and testing them with BigQuery
`ST_COVERS`: 3,951 covered by exactly one cell, 49 by none, 0 by more than one.

Scope and why it is accepted rather than fixed:

* All **40** Antarctic research-station seeds resolve correctly to themselves.
* There is **no overlap** — the defect is a gap, never an ambiguous label.
* `unified_ip_to_geoloc` contains **zero** rows in AQ, BV, HM, TF or PN, and zero
  rows anywhere below 60 °S. Verified, not assumed.

Cause: rings that touch the pole have a degenerate longitude there, so a step of
exactly ±180 appears between consecutive vertices and `((dlon + 180) % 360) − 180`
maps +180 to −180 — the same sign ambiguity that makes a naive area sum unreliable
on meridian-cut rings. Successive fixes took this from 1.7 % to 1.2 %; closing it
fully means emitting polar cells as WKT without a Shapely round-trip, since
BigQuery can represent a pole-containing polygon and Shapely cannot.

AQ is listed in `config.SHAPELY_UNVERIFIABLE_COUNTRIES`, which **defers** its
geometric gate to the BigQuery checks in `validate_hermes_rows.verify_polar_countries`
rather than skipping it.

---

## Naming authority

One authority everywhere: `City-COALESCE(state_resolved, state_iso2, 'NA')-CC`.

* `04_mapping_union.sql` (lines 111/752/1535) and
  `scripts/backfill_group_identity.sql` already used it.
* `enrich_ip_geoloc_add_metro.sql` and `enrich_geolocation_add_metro.sql` used bare
  `state_iso2`, which put **37.85 %** of live `unified_ip_to_geoloc.metro` into the
  `City-NA-CC` form. **Both are fixed in this change.**
* No consumer should re-`CONCAT` the metro string. Take `mp.metro` from the table.

Two upstream data problems the state tier also fixed:

* **Mojibake.** Natural Earth's *populated places* layer ships corrupted ADM1
  names — `MUdenine` for Médenine, `Kasssrine` for Kassérine, `Vi?n Bi` for Điện
  Biên — and those strings are in the live metro keys today. The *admin_1* layer
  is clean and carries ISO 3166-2, so `state_resolved`/`state_iso2` are taken from
  the polygon each seed actually sits in. 944 region names corrected.
* **Double spaces.** NE names contain runs of whitespace (`Washington,  D.C.`,
  `Ft.  Worth`), which travelled into metro keys. Collapsed.

Net effect on the `-NA-` region form: 37.85 % → 7 rows.

### Population semantics — three different quantities

| Column | Meaning |
|---|---|
| `seed_pop_max` | population of the seed settlement (NE `POP_MAX`). Shanghai ≈ 14.6 M |
| `cell_population` | population inside the metro's Voronoi cell. **Populated 2026-08-11**: 8,437 of 8,437 rows, summing to **7.8931 B** — exactly the WorldPop 2020 world population. Shanghai: 31.97 M |
| `legacy_cell_population` | the old table's `population_sum`, carried for comparison only. Cell-based against the **old** geometry, so it does not describe the new cells |

The old `population_sum` was confirmed cell-based, not seed-based: it summed to
8.13 B — world population — across 7,301 rows.

The two new columns differ by design and by a lot: `seed_pop_max` sums to
**2.75 B** (named settlements only) against `cell_population`'s **7.89 B**
(everyone, since the cells tile all inhabited land). Largest cells by population
are `Muzaffarpur-Bihar-IN` 42.2 M, `Shanghai-Shanghai-CN` 32.0 M,
`Gorakhpur-Uttar Pradesh-IN` 28.0 M, `Dhaka-Dhaka-BD` 27.5 M.

217 cells have `cell_population = 0`, recorded as 0 rather than NULL because they
are genuinely unpopulated: 40 Antarctic (WorldPop does not cover AQ), 165 slivers
under 50 km², and 12 real but uninhabited Arctic/subantarctic cells — Nuussuaq GL
(172,103 km² of ice), Ennadai CA, Port-aux-Français TF, Grytviken GS.

---

## Inputs

| Source | Role |
|---|---|
| NE 10m `populated_places` (7,342) | metro seeds. See **Provenance** below — the old table came from iGDB, whose city layer is this same NE set |
| NE 10m `admin_0_map_units` (298) | country boundaries. Used instead of `admin_0_countries` because the latter folds GF, GP, MQ, RE, YT into FR and SJ into NO, while HERMES/IPInfo report them separately |
| NE 10m `admin_1_states_provinces` (4,596) | ADM1 boundaries and the clean region-name authority |
| `hermes.state_to_iso2` | `state_iso2` fallback where a seed is outside every ADM1 polygon |
| `bigquery-public-data.worldpop.population_grid_1km` | `cell_population`, 2020 vintage. **Filter `last_updated`** — 21 vintages (2000–2020), unfiltered sums to 143 B people. The filter is also what makes it cheap: 6.5 GiB instead of >100 GiB |

Archive SHA-256 prefixes are recorded in `reports/03_seed_validation.json` so a
build is reproducible against a pinned vintage.

### Provenance of the old table

The old table was **not** built from Natural Earth directly. It came from
iGDB (Anderson, Salamatian, Bischof, Dainotti, Barford, *iGDB: Connecting the
Physical and Logical Layers of the Internet*, ACM IMC 2022), via
`hermes-code/data/city_polygons_with_population.csv`:

```
iGDB.db city_polygons (7,342)  ->  + population_sum  ->  city_polygons_with_population.csv (7,342)
                               ->  BQ metro_polygons_with_population (7,301)
```

iGDB's city layer is *itself* Natural Earth 10m populated places: identical row
count (7,342 vs 7,342), identical names including NE's encoding corruption
(`MUdenine`, `Kasssrine`, `Vi?n Bi` appear verbatim in both), and coordinates
agreeing to 3 dp for 91.5 % of rows with 592 of the remaining 623 within 1 km
(median 0.285 km). Only ~31 seeds differ by more than 1 km. So seeding this build
from NE reproduces iGDB's seed set to sub-kilometre accuracy — but iGDB is the
correct attribution for where the original polygons came from.

**This explains the inversion, and it is a load artifact rather than a design
choice.** iGDB stores its Voronoi cells with *unwrapped* longitudes — the WKT runs
down to **−197.5°**. Loading that into a BigQuery `GEOGRAPHY` wraps the
out-of-range vertices and flips the ring orientation, so BigQuery reads the
polygon as its **complement**. One mechanism accounts for all three headline
defects of the old table: areas of ~510 M km² (the whole globe), the
double-covered band around ±180 (wrapped Pacific cells overlapping), and the
polar gaps. iGDB's underlying cells are not themselves at fault.

### Seed set: 7,365 seeds

* **2 country reassignments**, both corroborated by NE's own `ADM0_A3`: Atafu
  (`ISO_A2=NZ`, `ADM0_A3=TKL`) → **TK**, 2,368 km outside NZ; Alofi (`ISO_A2=NZ`,
  `ADM0_A3=NIU`) → **NU**, 1,389 km outside. Each also gave a seed to a territory
  that had a boundary and none.
* **23 additions** for inhabited territories NE omits entirely (`seed_additions.csv`):
  JE, GG, VG, AI, SX, MF, BL, MS, BQ ×3, PM, SH ×3, WF, NR, NF, PN, IO, UM ×2, TF.
  After these, **no** inhabited territory lacks a seed.
* **2 explicit exclusions** (`excluded_territories.csv`): HM and BV, uninhabited,
  no network presence.
* 27 seeds sit ≤ 25 km outside their country's coastline (Greenland fjords,
  Gibraltar, Bar Harbor). Reported, kept, resolved through the fallback. **0** are
  clearly outside. **0** invalid coordinates. **0** exact duplicate coordinates.

The Alaskan North Slope needed **no** additions. NE carries Utqiaġvik — spelled
with U+0121 `ġ`, which is why an ASCII search for "Utqiagvik" finds nothing —
plus Wainwright, Atqasuk, Prudhoe Bay, Kaktovik and Point Hope. Those places were
missing from the *old* table because its Voronoi was clipped, not because the
seeds were absent.

---

## Output

`mlab-collaboration.hermes.metro_polygons_v2` — 8,437 rows, 7,349 metros, 246
country codes, 2,756 seeded ADM1 units, clustered by `(country_code, state_code)`.
7,269 state-tier cells + 1,168 country-tier cells.

```sql
SELECT mp.metro, mp.metro_id, mp.partition_tier
FROM `mlab-collaboration.hermes.metro_polygons_v2` AS mp
WHERE mp.country_code = @country
  AND ST_COVERS(mp.polygon, ST_GEOGPOINT(@lon, @lat))
QUALIFY ROW_NUMBER() OVER (
  ORDER BY ST_DISTANCE(ST_GEOGPOINT(@lon, @lat),
                       ST_GEOGPOINT(mp.seed_lon, mp.seed_lat)),
           mp.metro_id
) = 1
```

`ST_COVERS`, not `ST_CONTAINS`: cells share boundaries, and a point exactly on one
must still match. Ties break on distance to seed then `metro_id` — **never**
alphabetical metro order, which is the bug that made Suva into Nukualofa/TO.

`sql/` holds the reference lookup, drop-in v2 enrichment templates, and the
population recompute.

---

## Layout

```
build.py                      orchestrator; --from-stage resumes
config.py                     paths, tolerances, billing project
geometry_utils.py             spherical primitives, gnomonic frame, rotation
load_country_boundaries.py    stage 02   country geometry
load_state_boundaries.py      stage 02b  ADM1 geometry + seed->state assignment
load_seeds.py                 stage 01   seeds, legacy reconciliation, naming
normalize_country_codes.py    NE <-> HERMES/IPInfo code mapping
validate_seeds.py             stage 03   plan section 4 and 5 checks
build_country_voronoi.py      stage 04   the tessellation
antimeridian.py               stage 05   +/-180 normalisation
validate_tessellation.py      stage 06   geometric gate, sections 18 and 23
export_bigquery.py            stage 07   metro_polygons_v2
validate_hermes_rows.py       stage 08   live-data comparison, section 19
seed_additions.csv            operator-maintained, one row per justified seed
country_code_overrides.csv    explicit NE -> HERMES code corrections
excluded_territories.csv      deliberate exclusions, with reasons
sql/                          reference lookup + v2 enrichment + population
reports/                      JSON and CSV written by each stage
tests/                        pytest invariants
```

Stage artifacts land in `data/stages/`; `data/raw/` holds the pinned NE archives.

---

## Rollout

Phase 1 (**done**) — `metro_polygons_v2` built, uploaded, verified.
Phase 2 (**done**) — old and new compared over all 91.6 M coordinate-bearing rows;
disagreements categorised below.
Phase 3 — `sql/recompute_cell_population.sql` **done** (2026-08-11, 45 s, ~$0.01).
Still to do: backfill a limited date range and check downstream grouping counts.
Phase 4 — point enrichment at `sql/enrich_ip_geoloc_add_metro_v2.sql`.
Phase 5 — backfill history. Keep the old table meanwhile for reproducibility.

### Disagreement categories

| Category | Rows | Share |
|---|---|---|
| `same_city_region_renamed` | 83,822,690 | 91.5 % |
| `different_city` | 6,749,818 | 7.4 % |
| `old_was_cross_country` | 993,508 | 1.1 % |
| `old_unassigned_now_assigned` | 146 | — |
| `identical` | 311 | — |

`same_city_region_renamed` is the intended naming fix (`Passau-NA-DE` →
`Passau-Bayern-DE`). `different_city` is dominated by NE vintage renames of the
same place — Bangalore → Bengaluru, Mysore → Mysuru, Hannover → Hanover — plus
genuine nearest-seed changes from the new seed set and the state tier
(`San Jose-CA-US` → `San Mateo-California-US`). `old_was_cross_country` is the
border fix (`Windsor-ON-CA` → `Detroit-Michigan-US` for US IPs).

**Group identity changes for essentially every row.** `metro` is the grouping key
for tomography and the dashboard, so Phase 3 must confirm downstream counts before
Phase 4. `metro_id` is a stable hash of the canonical identity, not of geometry, so
it survives future geometry rebuilds — prefer it as the join key.

# Metro polygon coverage — what the tessellation covers and what it misses

> **Status: this audits the OLD table.** Every defect below is fixed in
> `mlab-collaboration.hermes.metro_polygons_v2`, built by `cleaning_polygons/`
> (see its README). Cross-country assignments 993,552 → 0, overlapping matches
> 229,705 → 0, unmatched coordinates 146 → 0, `City-NA-CC` metro strings 37.85 %
> → 0.08 %. Keep this document as the audit of what was wrong and why.
>
> Two root causes stated here need correcting. First, the seed provenance: the old
> table came from **iGDB** (Anderson et al., ACM IMC 2022) via
> `hermes-code/data/city_polygons_with_population.csv`, not from Natural Earth
> directly — though iGDB's city layer is itself NE 10m populated places. Second,
> the Arctic gap was **not** missing seeds. Natural Earth does carry Utqiaġvik — spelled with U+0121 `ġ`, so an ASCII
> search for "Utqiagvik" finds nothing — along with Wainwright, Atqasuk, Prudhoe
> Bay, Kaktovik and Point Hope. The old table lacks them because its Voronoi cells
> were **clipped**, not because the upstream seeds were absent.

**Table:** `mlab-collaboration.hermes.metro_polygons_with_population`
**Measured:** 2026-08-09/10, billed to `mlab-collaboration` (~$1 total, all queries dry-run first).
**Consumers:** `enrich_ip_geoloc_add_metro.sql`, `enrich_geolocation_add_metro.sql` (Phase B),
`04_mapping_union.sql` (3 lookup sites), `scripts/backfill_group_identity.sql`.

## Short answer

The polygon set is a **global city-seeded Voronoi tessellation**, not a set of metro
outlines. It covers **100 % of the Earth's surface between 30 °S and 66.5 °N** — every
inhabited landmass — and **99.99971 % of world population**. What it misses is the
**high Arctic above ~68 °N**, **Antarctica**, and two empty stretches of **Southern
Ocean**. For HERMES's actual data, **146 of 91,416,121 geolocated IP rows** (1.6 per
million) fall outside every polygon.

The real problems are not gaps. They are (a) an **antimeridian double-coverage band**
that mislabels Pacific island nations, (b) **enormous ocean cells** that make the metro
label meaningless offshore, and (c) a **state-name regression** in the Phase-B SQL.

## Table shape

| Property | Value |
|---|---|
| Rows (polygons) | 7,301 |
| Size | 1.53 MiB |
| Distinct `country_code` | 225 |
| `population_sum` total | 8.13 B (≈ world population — the seed set accounts for everyone) |
| `population_sum` p10 / median | 30,255 / 411,611 |
| `polygon` vertex count | 5–19 — irregular convex Voronoi cells, **not** city outlines |
| Rows with `state_resolved` NULL | 88 |
| Rows with `state_iso2` NULL | 3,285 (45 %) |

**Geometry reads back INVERTED**: each `polygon` behaves as the *complement* of its
metro area. This is **not a deliberate storage convention** — it is a load
artifact. The table came from iGDB (IMC 2022) via
`hermes-code/data/city_polygons_with_population.csv`, and iGDB stores its Voronoi
cells with unwrapped longitudes reaching **−197.5°**. BigQuery wraps those
out-of-range vertices on ingest, the ring orientation flips, and the polygon is
interpreted as its complement. The same mechanism explains the ±180 double-coverage
band and the polar gaps documented below.

Operationally the consequence still holds for anyone querying this table: the match
predicate is `NOT ST_CONTAINS(polygon, point)` and
`true_area = ST_AREA(FULLGLOBE) − ST_AREA(polygon)`. `ST_AREA(FULLGLOBE)` =
510,066,073 km².

Sum of the 7,301 true areas = 510,749,903 km² vs. a 510,066,073 km² globe — i.e. the
cells tile the whole sphere with only 0.13 % excess. That excess is *not* evenly spread;
see the antimeridian section.

## Geometric coverage (2° grid, 16,200 sample points)

| Latitude band | Cells | Uncovered | % of globe area | Uncovered share of globe |
|---|---|---|---|---|
| 66.5 °N … 90 °N | 2,160 | 1,320 | 4.50 % | 1.92 % |
| 60 °N … 66.5 °N | 540 | 0 | 2.42 % | 0 |
| **30 °S … 60 °N** | **8,100** | **0** | **68.5 %** | **0** |
| 60 °S … 30 °S | 2,700 | 285 | 18.1 % | 1.63 % |
| 90 °S … 60 °S | 2,700 | 1,982 | 6.48 % | 3.88 % |

**Total uncovered: 7.43 % of Earth's surface**, all of it ocean, ice, or empty tundra.

Coverage fraction by latitude row (northern edge is ragged, not a clean cut):

```
 66.5 N 100%    70.5 N  76%    74.5 N  48%    78.5 N  24%    82.5 N   9%    86.5 N   2%
 68.5 N  93%    72.5 N  65%    76.5 N  32%    80.5 N  13%    84.5 N   5%    88.5 N   0%
```

### ASCII map (2° sampled, 4° rendered)

```
                                                   |              |              |
  +88 ..o..............oo.......................................................................
  +84 ..oo.............o#o............o..#o..............o....o....oo...........................
  +80 ..XXo............o##.....oo.oo.oo.o##ooo..........o#....oo..o#oo....oo........oo..o.......
  +76 .oXXo............o##..ooo#o.##.o.oo#####oooo#o....o#....ooo.o###o..oo#oo.....o#ooo#.oo....
  +72 oXXXXo....o.....o###oo#####o####o###################....o##o#####oo#####ooooo######.ooo.oo
  +68 XXXXX###oo##oooo#######################################oo############################XXXXX
  +64 XXXXX################################################################################XXXXX
  +60 XXXXX################################################################################XXXXX
  ...  (identical, fully covered, down to -36)
  -36 XXXXX################################################################################XXXXX
  -40 ####oo####################################################################################
  -44 ####o.o####################################################ooo.###########################
  -48 ####o...o####################################o########oooo.....###########################
  -52 ####o....o###################################..ooo##oo.........###########################
  -56 ####o.....o##################################..................###########################
  -60 ####o......o##############oo#################..................###o#########o#############
  -64 ooooo.......o##########oo...o########ooo#####..................###o########o.#############
  -68 .............o#######oo......o#####o...o####o..................###.o#######o.o###########o
  -72 .............o###ooo..........o##o......##o#o..................###..o#ooo#o..o##########o.
  -76 ..............................o#o..........oo..................o##............o#######o...
  -80 ...............................o...............................o##................o##o....
  -84 ...............................................................o##.................oo.....
  -88 .................................................................o........................
        -180 ................. -60 ................. +60 ................. +180

legend: # covered   o partial   X double-covered (ambiguous)   . NOT covered
```

The two mid-southern gaps are pure ocean:
- **lon −162 … −140, lat −39 … −60** — South Pacific between New Zealand and Chile.
- **lon 0 … 70, lat −49 … −60** — Southern Ocean south of Africa / Indian Ocean sector.

## Population coverage (WorldPop 1 km grid, latest vintage, 7.893 B)

| | People | Share |
|---|---|---|
| In covered 2° cells | 7,892,977,000 | **99.99971 %** |
| In uncovered 2° cells | **23,262** | 0.00029 % |
| In double-covered cells (ambiguous label) | 4,930,000 | 0.062 % |

Most-populated uncovered cells — all Arctic:

| lat / lon (2° cell SW corner) | Pop | What is there |
|---|---|---|
| 70, 28 | 6,405 | Finnmark coast, Norway (Kjøllefjord, Berlevåg) |
| 70, −150 | 2,759 | North Slope, Alaska (Nuiqsut, Prudhoe Bay) |
| 70, 30 | 2,731 | Murmansk-oblast coast, Russia (Polyarnyy area) |
| 72, 112 | 788 | Yakutia, Russia |
| 76, −70 | 689 | Qaanaaq, north Greenland |
| 78, 12–18 | 1,065 | Svalbard (Longyearbyen) |

*Caveat:* the 2° grid tests one sample point per cell, so a cell counted "covered" may
be partly uncovered and vice versa. The IP-level number below is exact.

## Coverage on HERMES's own data — the number that matters

`mlab-collaboration.hermes.unified_ip_to_geoloc`, 99,279,146 rows:

| | Rows | Share |
|---|---|---|
| `metro IS NULL` | 7,862,879 | 7.92 % — **exactly** the rows with no lat/lon at all; not a polygon failure |
| Has coordinates | 91,416,267 | |
| … falls outside every polygon (`Unknown-NA-Unknown`) | **146** | 0.00016 % |
| Distinct metros actually used | 5,896 of 7,301 | 1,405 polygons never hit |

All 146 misses are high Arctic:

```
Nuiqsut AK (70.22,-150.98)  55    Longyearbyen SJ (78.22, 15.65)  5
Utqiagvik AK (71.29,-156.79) 55   Qaanaaq GL      (77.47,-69.23)  1
Prudhoe Bay AK (70.26,-148.34) 16 Kjøllefjord NO  (70.95, 27.35)  1
Polyarnyy RU (69.20, 33.44)  13
```

Spot-checks confirming the northern edge is about *seed density*, not a latitude cut —
these Arctic towns **are** covered: Tromsø, Hammerfest, Alta (NO), Murmansk, Norilsk,
Dudinka, Salekhard, Tiksi (71.6 °N!) (RU), Fairbanks, Kotzebue (AK), Inuvik, Iqaluit
(CA), Nuuk, Ilulissat (GL), Reykjavík. Uncovered: Utqiagvik, Longyearbyen, McMurdo.
Antarctica has 22 research-station polygons (Mirny, Casey, Davis, Palmer …) but **not**
McMurdo.

## Real defects found

### 1. Antimeridian double-coverage → wrong-country metro labels

990 of 16,200 grid cells match **two** polygons. They form a band from ~162 °E to
~−163 °W at nearly every latitude (54 lat-cells deep per longitude), caused by a handful
of huge Pacific cells overlapping across the dateline: Atka/US, Beringovskiy/RU,
Atafu/NZ, Funafuti/TV, Gisborne/NZ, Raoul Island/NZ, Tarawa/KI, Labasa/FJ, Kaitaia/NZ.

Both `enrich_ip_geoloc_add_metro.sql` and `04_mapping_union.sql` break the tie with
`ARRAY_AGG(... ORDER BY metro ASC LIMIT 1)` — **alphabetically**, which has no
geographic meaning. Verified mislabels at real capital coordinates:

| Place | Matches | Chosen (alphabetical) |
|---|---|---|
| Majuro, Marshall Is. | Atafu/NZ, Majuro/MH | **Atafu / NZ** ✗ |
| Tarawa, Kiribati | Atafu/NZ, Tarawa/KI | **Atafu / NZ** ✗ |
| Suva, Fiji | Nukualofa/TO, Suva/FJ | **Nukualofa / TO** ✗ |
| Pago Pago, Am. Samoa | Labasa/FJ, Pago Pago/AS | **Labasa / FJ** ✗ |
| Auckland, NZ | Auckland/NZ, Raoul Island/NZ | Auckland ✓ (luck) |
| Apia, Samoa | Apia/WS, Labasa/FJ | Apia ✓ (luck) |

Fix: break ties by distance to the seed point (or `population_sum DESC`, or prefer the
polygon whose `country_code` matches the point's geolocated country) instead of `metro ASC`.

### 2. Cells are unbounded over ocean — "covered" ≠ "meaningful"

Largest cells by true area: Papeete/PF **22.2 M km²**, Atafu/NZ 11.1 M, Majuro/MH 9.7 M,
Puerto Villamil/EC 9.7 M, Lihue/US(HI) 8.3 M, Hilo/US 6.7 M, Atka/US 6.6 M,
Grytviken/GS 5.9 M, Curepipe/MU 5.7 M, Beringovskiy/RU 5.7 M. Median cell is
14,578 km². Any coordinate in the mid-Pacific is "covered" — by Papeete, from thousands
of km away. Treat metro labels for offshore/low-confidence coordinates as noise.

### 3. Tessellation is not clipped to national borders

**1.084 %** of geolocated IP rows (991,026 / 91,416,121) get a metro whose country
differs from their own geolocated country. These are legitimate nearest-seed
assignments, not overlap bugs, but they matter for any per-country aggregation:

| Src → metro | Rows | Example |
|---|---|---|
| MA → ES | 122,619 | Cap Negro II, Morocco → Ceuta, Spain |
| US → CA | 92,098 | Gouverneur NY → Brockville ON |
| DE → CH | 75,429 | Albbruck → Aarau |
| DE → AT | 57,394 | Ravensburg → Bregenz |
| ID → SG | 50,587 | Batam → Singapore |
| DE → NL, AT → DE, BE → NL, FR → DE, IT → SM, DE → FR, DE → LU, BE → FR, CA → US, US → MX | 18k–44k each | Strasbourg/Kehl, Buffalo/Fort Erie, El Paso/Juárez … |

### 4. `state_resolved` regression in the Phase-B enrichment SQL

`04_mapping_union.sql` builds the metro key as
`COALESCE(mp.state_resolved, mp.state_iso2, 'NA')` (lines 111, 752, 1535), and so does
`scripts/backfill_group_identity.sql`. But **`enrich_ip_geoloc_add_metro.sql:26` and
`enrich_geolocation_add_metro.sql:26` still use bare `COALESCE(mp.state_iso2, 'NA')`.**

Consequence, measured on the live table (partitions 2025-02-20 … 2026-08-08):
**37.85 % of `unified_ip_to_geoloc.metro` values are `City-NA-CC`** — exactly the
pre-fix rate the 2026-06-21 "single naming authority" change reduced to 0.4 %. The fix
was lost for these two files.

Impact is partially masked: `04_mapping_union.sql:766,791` re-canonicalises with
`place := COALESCE(cm.metro, n.place)` from its own polygon lookup, so `place` gets the
`state_resolved` form wherever coordinates exist. But `unified_ip_to_geoloc.metro`
itself — which the dashboard's `_AM` grouping reads — is 37.85 % `-NA-`.

## Reproducing

Match predicate (note the inversion) and true area:

```sql
-- point → metro
SELECT p.city, p.state_resolved, p.country_code
FROM `mlab-collaboration.hermes.metro_polygons_with_population` p
WHERE NOT ST_CONTAINS(p.polygon, ST_GEOGPOINT(<lon>, <lat>));

-- cell area
SELECT city, (ST_AREA(ST_GEOGFROMTEXT('FULLGLOBE')) - ST_AREA(polygon))/1e6 AS km2
FROM `mlab-collaboration.hermes.metro_polygons_with_population`;
```

Global grid sweep (a `CROSS JOIN`; `NOT ST_CONTAINS` defeats spatial indexing, so keep
the grid ≤ 2°):

```sql
WITH grid AS (
  SELECT lon + 0.5 AS lon, lat + 0.5 AS lat
  FROM UNNEST(GENERATE_ARRAY(-180, 178, 2)) AS lon
  CROSS JOIN UNNEST(GENERATE_ARRAY(-90, 88, 2)) AS lat
)
SELECT g.lon, g.lat,
       COUNTIF(NOT ST_CONTAINS(p.polygon, ST_GEOGPOINT(g.lon, g.lat))) AS n_match
FROM grid g
CROSS JOIN `mlab-collaboration.hermes.metro_polygons_with_population` p
GROUP BY 1, 2;
```

Population weighting uses `bigquery-public-data.worldpop.population_grid_1km` (4.6 B
rows, 858 GiB, 21 vintages 2000–2020, 247 countries; **filter `last_updated`** or you
sum every vintage). Aggregating `latitude_centroid`/`longitude_centroid`/`population`
to 2° cells dry-runs at ~103 GiB.

Access note: `bq` CLI runs as `ls3748@cloudbank.org`, which is **denied** on
`mlab-collaboration.hermes`. Python ADC is `ls3748@columbia.edu`, which is the grantee —
use the Python path for this table.

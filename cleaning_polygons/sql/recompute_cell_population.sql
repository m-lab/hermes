-- Populate metro_polygons_v2.cell_population (plan section 21).
--
-- RUN 2026-08-11 against build_version 2026-08-v2. Result: 8,437 of 8,437 rows
-- set, summing to 7.8931 B -- exactly the WorldPop 2020 world population, which
-- is the sanity check. 45 seconds, 0.03 GiB billed (~$0.01), 4.8 M slot-ms,
-- billed to mlab-collaboration.
--
-- ---------------------------------------------------------------------------
-- The three population columns mean DIFFERENT things. Do not interchange them.
-- ---------------------------------------------------------------------------
--   seed_pop_max            population of the seed settlement itself (Natural
--                           Earth POP_MAX). Sums to 2.75 B globally, because it
--                           counts only the named places.
--
--   cell_population         population living inside the metro's Voronoi cell,
--                           computed here. Sums to 7.89 B -- everyone, because the
--                           cells tile all inhabited land. Shanghai: seed 14.99 M
--                           vs cell 31.97 M.
--
--   legacy_cell_population  the OLD table's population_sum, carried for
--                           comparison only. It is a cell figure against the OLD
--                           geometry, so it does not describe the new cells and
--                           must not be presented as if it did. (Confirmed
--                           cell-based, not seed-based: it summed to 8.13 B, i.e.
--                           world population, across 7,301 rows.)
--
-- ---------------------------------------------------------------------------
-- Two traps, both load-bearing
-- ---------------------------------------------------------------------------
-- 1. WorldPop holds 21 annual vintages (2000-2020) of ~219 M rows each. WITHOUT
--    the last_updated filter you sum every vintage and get ~143 B people. The
--    filter is not optional, and it is also what makes this cheap: 6.5 GiB dry
--    run instead of >100 GiB.
--
-- 2. Do NOT write this as CREATE OR REPLACE TABLE metro_polygons_v2. That drops
--    the CLUSTER BY (country_code, state_code), and BigQuery then refuses further
--    replaces with "Cannot replace a table with a different partitioning spec".
--    Stage into a small table and UPDATE, which preserves clustering.
--
-- Re-run once per geometry rebuild, never per pipeline run.

-- ---- Step 1: per-cell aggregation into a staging table.
-- (metro_id, partition_tier) is the unique row key: 8,437 rows, 8,437 pairs. A
-- metro can hold one state-tier cell and one country-tier cell, and those are
-- different areas with different populations.
CREATE OR REPLACE TABLE `mlab-collaboration.hermes._metro_v2_cell_pop` AS
SELECT
  mp.metro_id,
  mp.partition_tier,
  CAST(ROUND(SUM(p.population)) AS INT64) AS cell_population
FROM `bigquery-public-data.worldpop.population_grid_1km` p
JOIN `mlab-collaboration.hermes.metro_polygons_v2` mp
  ON ST_COVERS(mp.polygon, ST_GEOGPOINT(p.longitude_centroid, p.latitude_centroid))
WHERE p.last_updated = DATE '2020-01-01'   -- newest vintage; MIN is 2000-01-01
  AND p.population > 0
GROUP BY 1, 2;

-- ---- Step 2: check BEFORE touching the real table.
-- Expect ~7.89 B for the 2020 vintage. A total near 143 B means the last_updated
-- filter was dropped. A total far below means cells are missing populated land.
-- SELECT COUNT(*) AS rows_, ROUND(SUM(cell_population)/1e9, 4) AS billions
-- FROM `mlab-collaboration.hermes._metro_v2_cell_pop`;

-- ---- Step 3: apply. UPDATE, not CREATE OR REPLACE, to keep the clustering.
UPDATE `mlab-collaboration.hermes.metro_polygons_v2` t
SET cell_population = COALESCE(p.cell_population, 0)
FROM `mlab-collaboration.hermes._metro_v2_cell_pop` p
WHERE t.metro_id = p.metro_id
  AND t.partition_tier = p.partition_tier;

-- Cells containing no WorldPop point at all are genuinely unpopulated, not
-- unknown, so record 0 rather than leaving NULL. 217 cells on the 2026-08-v2
-- build: 40 Antarctic (WorldPop does not cover AQ), 165 slivers under 50 km2, and
-- 12 real but uninhabited Arctic/subantarctic cells -- Nuussuaq GL (172,103 km2 of
-- ice), Ennadai CA, Port-aux-Francais TF, Grytviken GS.
UPDATE `mlab-collaboration.hermes.metro_polygons_v2`
SET cell_population = 0
WHERE cell_population IS NULL;

DROP TABLE `mlab-collaboration.hermes._metro_v2_cell_pop`;

-- ---- Step 4: verify.
-- SELECT COUNT(*) AS rows_,
--        COUNTIF(cell_population IS NULL) AS still_null,
--        ROUND(SUM(cell_population)/1e9, 4) AS billions,
--        COUNTIF(cell_population = 0) AS zero_cells,
--        ROUND(SUM(seed_pop_max)/1e9, 4) AS seed_pop_billions
-- FROM `mlab-collaboration.hermes.metro_polygons_v2`;
-- 2026-08-11: 8437 | 0 | 7.8931 | 217 | 2.7521

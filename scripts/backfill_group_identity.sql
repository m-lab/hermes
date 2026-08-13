-- Backfill the group-identity columns onto an EXISTING partition.
-- No pipeline re-run: every new column is derivable from data already present.
-- ~3-7 TiB across 345 partitions vs ~175 TiB to re-run them (518 GiB/date).
-- See docs/proposals/2026-08-group-granularity.md.
--
-- Substitute ${DS} (dataset) and ${DAY} (partition).
--
-- Derivations:
--   src_group_label       transient_events_union.src_city -- written by 03 and
--                         never overwritten, so it still holds the detection key
--   src_metro / src_state polygon lookup on the existing src_lat/src_lon
--   src_city              MaxMind city NAME + metro's full state + country
--
-- The metro STATE must come from the polygon table, not by splitting the metro
-- label: state names contain '-' (Ile-de-France), so the label is not parseable.

-- Guard: on a pre-rollup partition src_city is still the MaxMind label
-- (~29,782 distinct/day vs ~3,977), and treating it as a metro would be wrong.
-- 2026-02-01..02-21 and 2026-07-04..07-08 are the known cases.
ASSERT (
  SELECT COUNTIF(
    detection_granularity IS NOT NULL
    OR src_group_label IS NOT NULL
    OR src_metro IS NOT NULL
  ) = 0
  FROM `mlab-collaboration.${DS}.events_with_as_and_geoloc`
  WHERE partition_date = DATE '${DAY}'
) AS 'Partition already has group-identity data. Refusing a non-idempotent second backfill.';

ASSERT (
  SELECT COUNT(DISTINCT src_city) < 10000
  FROM `mlab-collaboration.${DS}.events_with_as_and_geoloc`
  WHERE partition_date = DATE '${DAY}'
) AS 'Partition does not look rolled up -- src_city may already be the MaxMind label. Refusing.';

-- Every distinct source point in the partition, with its metro. LEFT JOIN so
-- points with no polygon still get a row: UPDATE ... FROM has no outer-join
-- form, and a missing row would silently skip those measurements entirely.
-- Positive geometry via ST_COVERS against ${METRO_POLYGONS} (metro_polygons_v2),
-- identical to _src_metro in src/hermes/sql/queries/04_mapping_union.sql so a
-- backfilled partition is indistinguishable from a freshly built one. NOT the old
-- `NOT ST_CONTAINS` against inverted polygons, and no alphabetical tie-break.
--
-- The final SELECT still starts from src_coords and LEFT JOINs, preserving the
-- property the comment above depends on: every distinct source point gets a row
-- even when no cell covers it.
CREATE TEMP TABLE _bf_metro AS
WITH src_coords AS (
  SELECT DISTINCT src_lat AS lat, src_lon AS lon
  FROM `mlab-collaboration.${DS}.events_with_as_and_geoloc`
  WHERE partition_date = DATE '${DAY}'
    AND src_lat IS NOT NULL AND src_lon IS NOT NULL
),
covered AS (
  SELECT u.lat, u.lon,
    ARRAY_AGG(STRUCT(mp.metro, mp.state_resolved AS metro_state) ORDER BY
      ST_DISTANCE(ST_GEOGPOINT(u.lon, u.lat),
                  ST_GEOGPOINT(mp.seed_lon, mp.seed_lat)), mp.metro_id
      LIMIT 1)[OFFSET(0)] AS m
  FROM src_coords u
  JOIN `${METRO_POLYGONS}` mp
    ON ST_COVERS(mp.polygon, ST_GEOGPOINT(u.lon, u.lat))
  GROUP BY 1, 2
),
nearby AS (
  SELECT u.lat, u.lon,
    ARRAY_AGG(STRUCT(mp.metro, mp.state_resolved AS metro_state) ORDER BY
      ST_DISTANCE(ST_GEOGPOINT(u.lon, u.lat),
                  ST_GEOGPOINT(mp.seed_lon, mp.seed_lat)), mp.metro_id
      LIMIT 1)[OFFSET(0)] AS m
  FROM src_coords u
  LEFT JOIN covered c ON c.lat = u.lat AND c.lon = u.lon
  JOIN `${METRO_POLYGONS}` mp
    ON ST_DWITHIN(mp.polygon, ST_GEOGPOINT(u.lon, u.lat), 100000)
  WHERE c.lat IS NULL
  GROUP BY 1, 2
)
SELECT
  u.lat,
  u.lon,
  COALESCE(c.m.metro, n.m.metro)             AS metro,
  COALESCE(c.m.metro_state, n.m.metro_state) AS metro_state
FROM src_coords u
LEFT JOIN covered c ON c.lat = u.lat AND c.lon = u.lon
LEFT JOIN nearby  n ON n.lat = u.lat AND n.lon = u.lon;

UPDATE `mlab-collaboration.${DS}.events_with_as_and_geoloc` e
SET
  e.detection_granularity = 'maxmind_city',
  e.client_geo_source     = 'maxmind',
  e.src_group_label       = t.src_city,
  -- already the metro on a rolled-up partition (asserted above)
  e.src_metro             = e.src_city,
  e.src_state             = COALESCE(m.metro_state, e.src_state),
  -- readable label, parsed from the RIGHT: ISO subdivision codes never contain
  -- '-', but city and state names do (Saint-Agapit-QC-CA, Ile-de-France)
  e.src_city = IF(m.metro_state IS NULL, t.src_city,
    CONCAT(
      ARRAY_TO_STRING(ARRAY(
        SELECT p FROM UNNEST(SPLIT(t.src_city, '-')) p WITH OFFSET o
        WHERE o <= ARRAY_LENGTH(SPLIT(t.src_city, '-')) - 3 ORDER BY o), '-'),
      '-', m.metro_state,
      '-', ARRAY_REVERSE(SPLIT(t.src_city, '-'))[SAFE_OFFSET(0)]
    ))
FROM `mlab-collaboration.${DS}.transient_events_union` t, _bf_metro m
WHERE e.id = t.id
  AND e.partition_date = DATE '${DAY}'
  AND t.partition_date = DATE '${DAY}'
  AND m.lat = e.src_lat
  AND m.lon = e.src_lon;

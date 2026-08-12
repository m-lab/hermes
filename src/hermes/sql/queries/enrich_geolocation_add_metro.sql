CREATE OR REPLACE TABLE `mlab-collaboration.hermes.geolocation` AS

WITH base AS (
  SELECT
    *,
    lat AS lat_key,
    lon AS lon_key,
    place AS place_key
  FROM `mlab-collaboration.hermes.geolocation`
),

unique_lat_lon AS (
  SELECT DISTINCT
    lat_key,
    lon_key
  FROM base
  WHERE lat_key IS NOT NULL AND lon_key IS NOT NULL
),

latlon_translation_to_metro AS (
  SELECT
    ul.lat_key, ul.lon_key,
    -- Positive geometry: ST_COVERS against ${METRO_POLYGONS} (metro_polygons_v2),
    -- NOT the old `NOT ST_CONTAINS` against inverted polygons. `metro` is taken
    -- straight from the table so there is exactly one naming authority -- this is
    -- also where the state_iso2-only regression lived, which put 37.85% of metros
    -- into the `City-NA-CC` form.
    ARRAY_AGG(mp.metro ORDER BY
      ST_DISTANCE(ST_GEOGPOINT(ul.lon_key, ul.lat_key),
                  ST_GEOGPOINT(mp.seed_lon, mp.seed_lat)), mp.metro_id
      LIMIT 1)[OFFSET(0)] AS metro
  FROM unique_lat_lon ul
  JOIN `${METRO_POLYGONS}` mp
    ON ST_COVERS(mp.polygon, ST_GEOGPOINT(ul.lon_key, ul.lat_key))
  GROUP BY 1, 2
),
-- Offshore of the boundary coastline: nearest cell within 100 km, so these keep a
-- metro instead of regressing to Unknown. Beyond 100 km they stay NULL rather
-- than being handed a metro thousands of km away.
latlon_nearby AS (
  SELECT
    ul.lat_key, ul.lon_key,
    ARRAY_AGG(mp.metro ORDER BY
      ST_DISTANCE(ST_GEOGPOINT(ul.lon_key, ul.lat_key),
                   ST_GEOGPOINT(mp.seed_lon, mp.seed_lat)), mp.metro_id
      LIMIT 1)[OFFSET(0)] AS metro
  FROM unique_lat_lon ul
  LEFT JOIN latlon_translation_to_metro t
    ON t.lat_key = ul.lat_key AND t.lon_key = ul.lon_key
  JOIN `${METRO_POLYGONS}` mp
    ON ST_DWITHIN(mp.polygon, ST_GEOGPOINT(ul.lon_key, ul.lat_key), 100000)
  WHERE t.lat_key IS NULL
  GROUP BY 1, 2
),

-- One metro per coordinate. No dedup step is needed any more: v2 cells tile each
-- country's land disjointly, so ST_COVERS returns exactly one cell per land
-- coordinate, and the ORDER BY above is only a determinism guard for a point on a
-- shared cell boundary. The old alphabetical `ORDER BY metro ASC` tie-break is
-- gone -- that is what labelled Suva as Nukualofa/TO.
latlon_to_metro AS (
  SELECT lat_key, lon_key, metro FROM latlon_translation_to_metro
  UNION ALL
  SELECT lat_key, lon_key, metro FROM latlon_nearby
),

final_output AS (
  SELECT
    b.* EXCEPT (metro, lat_key, lon_key, place_key),
    -- Keep the historical guard: a '%remainder%' metro is not a real place. v2 has
    -- no remainder cells, so this should never fire.
    CASE
      WHEN d.metro LIKE '%remainder%' THEN COALESCE(b.place_key, 'Unknown')
      ELSE d.metro
    END AS metro
  FROM base AS b
  LEFT JOIN latlon_to_metro AS d
    ON b.lat_key = d.lat_key AND b.lon_key = d.lon_key
  WHERE b.hostname IS NOT NULL
)
SELECT
*
FROM final_output
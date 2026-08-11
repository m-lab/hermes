-- Drop-in replacement for src/hermes/sql/queries/enrich_ip_geoloc_add_metro.sql
-- once metro_polygons_v2 is adopted (plan section 24, phases 1 and 4).
--
-- Four changes from the current version:
--   1. ST_COVERS on positive geometry, instead of NOT ST_CONTAINS on inverted
--      geometry.
--   2. The join is constrained to the row's own geolocated country, so a border
--      coordinate can no longer be handed a metro across the border. This is what
--      removed 993,508 cross-country assignments.
--   3. Overlap ties break on distance to the seed, then metro_id -- never on
--      alphabetical metro order, which is what labelled Suva as Nukualofa/TO.
--   4. `metro` is taken straight from the table rather than re-CONCAT'd here, so
--      there is exactly one naming authority. No consumer reconstructs the string.
--
-- Adds metro_assignment_method and metro_distance_km. Keep them: they are how a
-- geometrically sound assignment is told apart from a technically-assigned but
-- implausibly distant one (plan sections 16 and 17).

CREATE OR REPLACE TABLE `mlab-collaboration.hermes.unified_ip_to_geoloc` AS
WITH base AS (
  SELECT
    *,
    COALESCE(lat_ip_info, lat)          AS lat_key,
    COALESCE(lon_ip_info, lon)          AS lon_key,
    COALESCE(city_ip_info, city)        AS city_key,
    COALESCE(country_ip_info, country)  AS cc_key
  FROM `hermes.unified_ip_to_geoloc`
),

-- Resolve distinct (coordinate, country) triples, not distinct IPs: 99M rows
-- collapse to ~1M spatial lookups.
unique_coords AS (
  SELECT DISTINCT lat_key, lon_key, cc_key
  FROM base
  WHERE lat_key IS NOT NULL AND lon_key IS NOT NULL AND cc_key IS NOT NULL
),

covered AS (
  SELECT
    u.lat_key, u.lon_key, u.cc_key,
    ARRAY_AGG(
      STRUCT(
        mp.metro, mp.metro_id, mp.city, mp.state_resolved, mp.state_iso2,
        ST_DISTANCE(ST_GEOGPOINT(u.lon_key, u.lat_key),
                    ST_GEOGPOINT(mp.seed_lon, mp.seed_lat)) / 1000.0 AS km
      )
      ORDER BY
        ST_DISTANCE(ST_GEOGPOINT(u.lon_key, u.lat_key),
                    ST_GEOGPOINT(mp.seed_lon, mp.seed_lat)),
        mp.metro_id
      LIMIT 1
    )[OFFSET(0)] AS pick
  FROM unique_coords u
  JOIN `mlab-collaboration.hermes.metro_polygons_v2` mp
    ON mp.country_code = u.cc_key
   AND ST_COVERS(mp.polygon, ST_GEOGPOINT(u.lon_key, u.lat_key))
  GROUP BY u.lat_key, u.lon_key, u.cc_key
),

-- Coordinates no cell covers: slightly offshore of the boundary dataset's
-- coastline, or low-confidence geolocation. Nearest seed in the same country,
-- recorded as a fallback rather than silently presented as containment.
fallback AS (
  SELECT
    u.lat_key, u.lon_key, u.cc_key,
    ARRAY_AGG(
      STRUCT(
        mp.metro, mp.metro_id, mp.city, mp.state_resolved, mp.state_iso2,
        ST_DISTANCE(ST_GEOGPOINT(u.lon_key, u.lat_key),
                    ST_GEOGPOINT(mp.seed_lon, mp.seed_lat)) / 1000.0 AS km
      )
      ORDER BY
        ST_DISTANCE(ST_GEOGPOINT(u.lon_key, u.lat_key),
                    ST_GEOGPOINT(mp.seed_lon, mp.seed_lat)),
        mp.metro_id
      LIMIT 1
    )[OFFSET(0)] AS pick
  FROM unique_coords u
  LEFT JOIN covered c USING (lat_key, lon_key, cc_key)
  JOIN `mlab-collaboration.hermes.metro_polygons_v2` mp
    ON mp.country_code = u.cc_key
  WHERE c.lat_key IS NULL
  GROUP BY u.lat_key, u.lon_key, u.cc_key
),

resolved AS (
  SELECT lat_key, lon_key, cc_key, pick, 'polygon' AS method FROM covered
  UNION ALL
  SELECT lat_key, lon_key, cc_key, pick,
         IF(pick.km <= 100.0, 'country_nearest_fallback',
                              'country_nearest_fallback_far') AS method
  FROM fallback
)

SELECT
  b.* EXCEPT (metro, polygon, lat_key, lon_key, city_key, cc_key),
  r.pick.metro                       AS metro,
  r.pick.metro_id                    AS metro_id,
  ROUND(r.pick.km, 3)                AS metro_distance_km,
  COALESCE(r.method, 'unresolved')   AS metro_assignment_method,
  b.polygon
FROM base AS b
LEFT JOIN resolved AS r
  ON b.lat_key = r.lat_key
 AND b.lon_key = r.lon_key
 AND b.cc_key  = r.cc_key;

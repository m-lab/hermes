-- Canonical coordinate -> metro lookup against metro_polygons_v2
-- (plan sections 14-17). This is the reference implementation the enrichment
-- steps should adopt; it is written as a bulk set-based lookup because that is
-- how the pipeline uses it.
--
-- Ordinary positive geometry: ST_COVERS, never NOT ST_CONTAINS. ST_COVERS rather
-- than ST_CONTAINS because Voronoi cells share their boundaries, so a point lying
-- exactly on one must still match.
--
-- Rules, in order:
--   1. candidates are restricted to the point's own geolocated country, so a
--      border coordinate can never be handed a metro across the border;
--   2. containment;
--   3. on a shared boundary, break ties by great-circle distance to the seed,
--      then by metro_id. NEVER by alphabetical metro order -- that is precisely
--      the bug that labelled Suva as Nukualofa/TO in the old table.
--
-- When containment finds nothing -- the coordinate is slightly offshore of the
-- boundary dataset's coastline, which is common for atolls and for low-confidence
-- IP geolocation -- fall back to the nearest seed *in the same country* and record
-- that fact. metro_assignment_method and metro_distance_km are part of the
-- contract, not debug output: without them a plausible metro cannot be told from
-- an implausibly distant one.
--
-- Replace `coords` with the real source. It must yield one row per distinct
-- (lon, lat, country_code); resolving distinct coordinates rather than distinct
-- IPs is what keeps this cheap (99M IP rows collapse to ~1M coordinates).

WITH coords AS (
  SELECT DISTINCT
    COALESCE(lon_ip_info, lon)          AS lon,
    COALESCE(lat_ip_info, lat)          AS lat,
    COALESCE(country_ip_info, country)  AS country_code
  FROM `mlab-collaboration.hermes.unified_ip_to_geoloc`
  WHERE COALESCE(lat_ip_info, lat) IS NOT NULL
    AND COALESCE(lon_ip_info, lon) IS NOT NULL
    AND COALESCE(country_ip_info, country) IS NOT NULL
),

-- Rule 1 + 2 + 3: in-country containment, closest seed wins any boundary tie.
covered AS (
  SELECT
    c.lon, c.lat, c.country_code,
    ARRAY_AGG(
      STRUCT(
        mp.metro_id, mp.metro, mp.city, mp.state_resolved, mp.state_iso2,
        ST_DISTANCE(ST_GEOGPOINT(c.lon, c.lat),
                    ST_GEOGPOINT(mp.seed_lon, mp.seed_lat)) / 1000.0 AS metro_distance_km
      )
      ORDER BY
        ST_DISTANCE(ST_GEOGPOINT(c.lon, c.lat),
                    ST_GEOGPOINT(mp.seed_lon, mp.seed_lat)),
        mp.metro_id
      LIMIT 1
    )[OFFSET(0)] AS pick,
    COUNT(*) AS n_polygon_matches
  FROM coords c
  JOIN `mlab-collaboration.hermes.metro_polygons_v2` mp
    ON mp.country_code = c.country_code
   AND ST_COVERS(mp.polygon, ST_GEOGPOINT(c.lon, c.lat))
  GROUP BY c.lon, c.lat, c.country_code
),

-- Fallback for coordinates no cell covers: nearest seed in the same country.
uncovered AS (
  SELECT c.lon, c.lat, c.country_code
  FROM coords c
  LEFT JOIN covered v
    USING (lon, lat, country_code)
  WHERE v.lon IS NULL
),
fallback AS (
  SELECT
    u.lon, u.lat, u.country_code,
    ARRAY_AGG(
      STRUCT(
        mp.metro_id, mp.metro, mp.city, mp.state_resolved, mp.state_iso2,
        ST_DISTANCE(ST_GEOGPOINT(u.lon, u.lat),
                    ST_GEOGPOINT(mp.seed_lon, mp.seed_lat)) / 1000.0 AS metro_distance_km
      )
      ORDER BY
        ST_DISTANCE(ST_GEOGPOINT(u.lon, u.lat),
                    ST_GEOGPOINT(mp.seed_lon, mp.seed_lat)),
        mp.metro_id
      LIMIT 1
    )[OFFSET(0)] AS pick
  FROM uncovered u
  JOIN `mlab-collaboration.hermes.metro_polygons_v2` mp
    ON mp.country_code = u.country_code
  GROUP BY u.lon, u.lat, u.country_code
)

SELECT
  lon, lat, country_code,
  pick.metro_id, pick.metro, pick.city, pick.state_resolved, pick.state_iso2,
  pick.metro_distance_km,
  n_polygon_matches,
  'polygon' AS metro_assignment_method
FROM covered
UNION ALL
SELECT
  lon, lat, country_code,
  pick.metro_id, pick.metro, pick.city, pick.state_resolved, pick.state_iso2,
  pick.metro_distance_km,
  0 AS n_polygon_matches,
  IF(pick.metro_distance_km <= 100.0,
     'country_nearest_fallback',
     'country_nearest_fallback_far') AS metro_assignment_method
FROM fallback;

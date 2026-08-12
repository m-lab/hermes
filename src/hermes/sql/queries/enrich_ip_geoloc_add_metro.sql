-- Resolve `metro` for the rows enrichment just inserted, in ONE partition.
--
-- This used to be `CREATE OR REPLACE TABLE ... AS SELECT ... FROM itself`, which
-- rewrote all 99,561,656 rows (12.8 GiB) through a spatial join on every
-- enrichment run. That was tolerable only because the table grows slowly from
-- topology IPs. Client/source IPs would add ~2.2M rows/day (86% of client IPs are
-- new each day), so the nightly rewrite would pass 100 GiB within a year.
--
-- The table is now PARTITION BY partition_date CLUSTER BY ip_address, and
-- enrichment writes exactly one partition per run -- the rows
-- `_upload_geolocation_data` just inserted for ${DAY}. So the update is scoped to
-- that partition. Measured: reading one partition is 0.001 GiB against 0.742 GiB
-- for the whole table.
--
-- Positive geometry: ST_COVERS against ${METRO_POLYGONS} (metro_polygons_v2), NOT
-- the old `NOT ST_CONTAINS` against inverted polygons. `metro` is taken straight
-- from the table so there is exactly one naming authority -- this is also where
-- the state_iso2-only regression lived, which put 37.85% of metros into the
-- `City-NA-CC` form.
--
-- Single pass per coordinate: ST_DWITHIN(...,100km) is a superset of ST_COVERS (a
-- containing cell is at distance 0), so one join serves both containment and the
-- offshore coastline tolerance, and the ORDER BY expresses the preference.
-- Beyond 100 km a coordinate keeps NULL rather than being handed a metro
-- thousands of km away, which is what the old inverted table did.

MERGE `mlab-collaboration.hermes.unified_ip_to_geoloc` AS t
USING (
  WITH coords AS (
    SELECT DISTINCT
      COALESCE(lat_ip_info, lat) AS lat_key,
      COALESCE(lon_ip_info, lon) AS lon_key
    FROM `mlab-collaboration.hermes.unified_ip_to_geoloc`
    WHERE partition_date = DATE '${DAY}'
      AND COALESCE(lat_ip_info, lat) IS NOT NULL
      AND COALESCE(lon_ip_info, lon) IS NOT NULL
  )
  SELECT
    c.lat_key,
    c.lon_key,
    ARRAY_AGG(mp.metro ORDER BY
      ST_COVERS(mp.polygon, ST_GEOGPOINT(c.lon_key, c.lat_key)) DESC,
      ST_DISTANCE(ST_GEOGPOINT(c.lon_key, c.lat_key),
                  ST_GEOGPOINT(mp.seed_lon, mp.seed_lat)),
      mp.metro_id
      LIMIT 1)[SAFE_OFFSET(0)] AS metro
  FROM coords c
  JOIN `${METRO_POLYGONS}` mp
    ON ST_DWITHIN(mp.polygon, ST_GEOGPOINT(c.lon_key, c.lat_key), 100000)
  GROUP BY 1, 2
) AS m
ON  t.partition_date = DATE '${DAY}'
AND COALESCE(t.lat_ip_info, t.lat) = m.lat_key
AND COALESCE(t.lon_ip_info, t.lon) = m.lon_key
WHEN MATCHED THEN UPDATE SET
  -- Keep the historical guard: a '%remainder%' metro is not a real place, so fall
  -- back to the source city. v2 has no remainder cells, so this should never fire.
  metro = CASE
            WHEN m.metro LIKE '%remainder%'
              THEN COALESCE(t.city_ip_info, t.city, 'Unknown')
            ELSE m.metro
          END;

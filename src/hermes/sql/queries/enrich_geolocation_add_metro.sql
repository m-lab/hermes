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
    ul.lat_key,
    ul.lon_key,
    CONCAT(
      COALESCE(mp.city, 'Unknown'), '-',
      COALESCE(mp.state_iso2, 'NA'), '-',
      COALESCE(mp.country_code, 'Unknown')
    ) AS metro,
    mp.country_code,
    mp.polygon
  FROM unique_lat_lon ul
  LEFT JOIN `mlab-collaboration.hermes.metro_polygons_with_population` mp
    ON NOT(ST_CONTAINS(mp.polygon, ST_GEOGPOINT(ul.lon_key, ul.lat_key)))
),

deduped_latlon_to_metro AS (
  SELECT
    lat_key,
    lon_key,
    ARRAY_AGG(STRUCT(metro, polygon, country_code)
              ORDER BY metro ASC LIMIT 1)[OFFSET(0)] AS entry
  FROM latlon_translation_to_metro
  GROUP BY lat_key, lon_key
),

hostname_to_metro AS (
  SELECT
    hostname,
    b.lat_key,
    b.lon_key,
    ARRAY_AGG(STRUCT(entry.metro, entry.polygon, entry.country_code)
              ORDER BY entry.metro ASC LIMIT 1)[OFFSET(0)] AS entry
  FROM base AS b
  LEFT JOIN deduped_latlon_to_metro AS d
    ON b.lat_key = d.lat_key AND b.lon_key = d.lon_key
  WHERE b.lat_key IS NOT NULL
  GROUP BY hostname, lat_key, lon_key
),

final_output AS (
  SELECT
    b.* EXCEPT (metro, lat_key, lon_key, place_key),
    CASE
      WHEN h.entry.metro LIKE '%remainder%' THEN COALESCE(b.place_key, 'Unknown')
      ELSE h.entry.metro
    END AS metro
  FROM base AS b
  LEFT JOIN hostname_to_metro AS h
    ON b.hostname = h.hostname
   AND b.lat_key = h.lat_key
   AND b.lon_key = h.lon_key
  WHERE b.hostname IS NOT NULL
)
SELECT
*
FROM final_output
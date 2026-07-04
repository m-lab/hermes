CREATE OR REPLACE TABLE `mlab-collaboration.hermes.unified_ip_to_geoloc` AS
WITH base AS (
  SELECT
    *,
    COALESCE(lat_ip_info, lat)  AS lat_key,
    COALESCE(lon_ip_info, lon)  AS lon_key,
    COALESCE(city_ip_info, city) AS city_key
  FROM `hermes.unified_ip_to_geoloc`
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

-- Deduplicate by lat/lon: pick the first (alphabetically)
deduped_latlon_to_metro AS (
  SELECT
    lat_key,
    lon_key,
    ARRAY_AGG(STRUCT(metro, polygon, country_code) ORDER BY metro ASC LIMIT 1)[OFFSET(0)] AS entry
  FROM latlon_translation_to_metro
  GROUP BY lat_key, lon_key
),

-- Deduplicate by IP + partition_time: pick the first metro per IP per partition
ip_to_metro AS (
  SELECT
    b.ip_address,
    b.partition_date,
    ARRAY_AGG(
      STRUCT(entry.metro, entry.polygon, entry.country_code)
      ORDER BY entry.metro ASC
      LIMIT 1
    )[OFFSET(0)] AS entry
  FROM base AS b
  LEFT JOIN deduped_latlon_to_metro AS d
    ON b.lat_key = d.lat_key AND b.lon_key = d.lon_key
  GROUP BY b.ip_address, b.partition_date
)

SELECT
  -- DISTINCT
  b.* EXCEPT (metro, polygon, partition_date, lat_key, lon_key, city_key),
  CASE
    WHEN ip.entry.metro LIKE '%remainder%' THEN COALESCE(b.city_key, 'Unknown')
    ELSE ip.entry.metro
  END AS metro,
  b.polygon,
  b.partition_date
FROM base AS b
LEFT JOIN ip_to_metro AS ip
  ON b.ip_address = ip.ip_address
  AND b.partition_date = ip.partition_date
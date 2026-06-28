--------------------------------------------------------------------------------
-- HERMES (union) mapping: lookup tables + hop enrichment + final output
--
-- Inputs:
-- - `mlab-collaboration.hermes_union.transient_events_union` (partition_date = ${DAY})
-- - `measurement-lab.ndt_raw.hopannotation2`
-- - Various hermes lookup tables (as_metadata, rdns, geoloc, ip_to_as, ixp_members)
--
-- Output:
-- - `mlab-collaboration.hermes_union.events_with_as_and_geoloc`
--
-- This is a multi-statement BigQuery script. Temp tables created early on
-- remain visible to the final INSERT statement.
--------------------------------------------------------------------------------

-- ============================================================================
-- Reusable lookup temp tables
-- ============================================================================

-- 1) Closest AS metadata w.r.t. ${DAY} (IP-version agnostic)
CREATE TEMP TABLE _closest_as_metadata AS
SELECT *
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY asn
      ORDER BY ABS(DATE_DIFF(partition_date, DATE '${DAY}', DAY))
    ) AS rn
  FROM `mlab-collaboration.hermes.as_metadata`
)
WHERE rn = 1;

-- 2) Closest rDNS entry w.r.t. ${DAY} — UNION of IPv4 and IPv6 sources
CREATE TEMP TABLE _closest_rdns AS
SELECT * EXCEPT(rn) FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY ip_address
      ORDER BY ABS(DATE_DIFF(partition_date, DATE '${DAY}', DAY))
    ) AS rn
  FROM (
    SELECT *, 'v4' AS ip_version
    FROM `mlab-collaboration.hermes.unified_ip_to_rdns`
    UNION ALL
    SELECT *, 'v6' AS ip_version
    FROM `mlab-collaboration.hermes.unified_ip_to_rdns_ipv6`
  )
)
WHERE rn = 1;

-- 3) Closest geo entry w.r.t. ${DAY} — UNION of IPv4 and IPv6 sources
CREATE TEMP TABLE _closest_geo AS
SELECT * EXCEPT(rn) FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY ip_address
      ORDER BY ABS(DATE_DIFF(partition_date, DATE '${DAY}', DAY))
    ) AS rn
  FROM (
    SELECT *, 'v4' AS ip_version
    FROM `mlab-collaboration.hermes.unified_ip_to_geoloc`
    UNION ALL
    SELECT *, 'v6' AS ip_version
    FROM `mlab-collaboration.hermes.unified_ip_to_geoloc_ipv6`
  )
)
WHERE rn = 1;

-- 4) Extracted prefixes — IPv4 + IPv6 combined (stateless, no accumulation)
CREATE TEMP TABLE _extracted_prefixes AS
WITH
ixp_ranked AS (
  SELECT
    ipv4,
    asn,
    name,
    partition_date,
    ROW_NUMBER() OVER (
      PARTITION BY ipv4
      ORDER BY ABS(DATE_DIFF(partition_date, DATE('${DAY}'), DAY)) ASC
    ) AS rank
  FROM `mlab-collaboration.ix_data.ixp_members`
),
hop_prefixes AS (
  SELECT DISTINCT
    CASE
      WHEN ix.ipv4 IS NOT NULL
        THEN CONCAT(REGEXP_EXTRACT(hop.id, r'_([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)$'), '/32')
      ELSE raw.Annotations.Network.CIDR
    END AS ip_prefix,
    CASE
      WHEN ix.ipv4 IS NOT NULL THEN ix.asn
      ELSE raw.Annotations.Network.ASNumber
    END AS asn,
    NET.IP_FROM_STRING(
      CASE
        WHEN ix.ipv4 IS NOT NULL
          THEN REGEXP_EXTRACT(hop.id, r'_([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)$')
        ELSE REGEXP_EXTRACT(raw.Annotations.Network.CIDR, r'(.*)/')
      END
    ) AS network_bin,
    CASE
      WHEN ix.ipv4 IS NOT NULL THEN 32
      ELSE CAST(REGEXP_EXTRACT(raw.Annotations.Network.CIDR, r'/(.*)') AS INT64)
    END AS mask,
    IFNULL(ix.name, 'None') AS ixp,
    ix.partition_date AS ixp_partition_date
  FROM `measurement-lab.ndt_raw.hopannotation2` hop
  LEFT JOIN (
    SELECT ipv4, asn, name, partition_date
    FROM ixp_ranked
    WHERE rank = 1
  ) ix
    ON ix.ipv4 = REGEXP_EXTRACT(hop.id, r'_([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)$')
  WHERE date BETWEEN '2025-05-01' AND '${DAY}'
),
unified_data_v4 AS (
  SELECT
    ip_prefix,
    asn,
    NET.IP_FROM_STRING(REGEXP_EXTRACT(ip_prefix, r'(.*)/')) AS network_bin,
    CAST(REGEXP_EXTRACT(ip_prefix, r'/(.*)') AS INT64) AS mask,
    IFNULL(ixp, 'None') AS ixp
  FROM `mlab-collaboration.hermes.unified_ip_to_as`
),
extracted_prefixes_v4 AS (
  SELECT
    COALESCE(ed.ip_prefix, ud.ip_prefix) AS ip_prefix,
    COALESCE(ed.asn, ud.asn) AS asn,
    COALESCE(ed.network_bin, ud.network_bin) AS network_bin,
    COALESCE(ed.mask, ud.mask) AS mask,
    IFNULL(COALESCE(ed.ixp, ud.ixp), 'None') AS ixp,
    ed.ixp_partition_date,
    'v4' AS ip_version
  FROM hop_prefixes ed
  FULL OUTER JOIN unified_data_v4 ud
    ON ed.network_bin = ud.network_bin
   AND ed.mask = ud.mask
),
extracted_prefixes_v6 AS (
  SELECT
    ip_prefix,
    asn,
    NET.IP_FROM_STRING(REGEXP_EXTRACT(ip_prefix, r'(.*)/')) AS network_bin,
    CAST(REGEXP_EXTRACT(ip_prefix, r'/(.*)') AS INT64) AS mask,
    IFNULL(ixp, 'None') AS ixp,
    CAST(NULL AS DATE) AS ixp_partition_date,
    'v6' AS ip_version
  FROM `mlab-collaboration.hermes.unified_ip_to_as_ipv6`
)
SELECT * FROM extracted_prefixes_v4
UNION ALL
SELECT * FROM extracted_prefixes_v6;

-- ============================================================================
-- Main query: enrich hops + compute distances + produce final output
-- ============================================================================
-- Materialize into a temp table first so we can write both
-- events_with_as_and_geoloc AND giga_meter_measurements
-- from a single computation (avoids a ~3.4 TiB re-scan for the giga-meter filter).
CREATE TEMP TABLE _mapping_result AS

WITH
closest_metadata AS (
  SELECT * FROM _closest_as_metadata
),
closest_rdns_entry AS (
  SELECT * FROM _closest_rdns
),
closest_geo_entry AS (
  SELECT * FROM _closest_geo
),
extracted_prefixes AS (
  SELECT * FROM _extracted_prefixes
),

--------------------------------------------------------------------------------
-- Forward path: flatten hops, map to ASN/IXP, attach geo
--------------------------------------------------------------------------------

flattened_node_details AS (
  SELECT
    t.id,
    t.ip_version,
    t.dst_lat,
    t.dst_lon,
    t.src_lon,
    t.src_lat,
    t.dst_city,
    t.dst_country,
    s2s.dst_state,
    t.dst_asn,
    t.baseline_median_rtt AS baseline_rtt,
    node.ttl,
    node.addr,
    node.rdns_name,
    node.rtts
  FROM `mlab-collaboration.hermes_union.transient_events_union` t,
       UNNEST(t.node_details) AS node
  LEFT JOIN `hermes.site_to_state` s2s
    ON t.dst_site = s2s.dst_site
  WHERE t.partition_date = '${DAY}'
),

source_ip_address AS (
  SELECT DISTINCT node.addr AS ip, t.ip_version
  FROM `mlab-collaboration.hermes_union.transient_events_union` t,
       UNNEST(t.node_details) AS node
  WHERE t.partition_date = '${DAY}'
),

masked_ip_addresses_v4 AS (
  SELECT ip, asn, ixp, mask, ixp_partition_date
  FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY ip ORDER BY mask DESC) AS rn
    FROM (
      SELECT *, NET.SAFE_IP_FROM_STRING(ip) & NET.IP_NET_MASK(4, mask) AS network_bin
      FROM source_ip_address, UNNEST(GENERATE_ARRAY(8, 32)) mask
      WHERE BYTE_LENGTH(NET.SAFE_IP_FROM_STRING(ip)) = 4
    )
    JOIN extracted_prefixes USING (network_bin, mask)
    WHERE extracted_prefixes.ip_version = 'v4'
  )
  WHERE rn = 1
),

masked_ip_addresses_v6 AS (
  SELECT ip, asn, ixp, mask, ixp_partition_date
  FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY ip ORDER BY mask DESC) AS rn
    FROM (
      SELECT *, NET.SAFE_IP_FROM_STRING(ip) & NET.IP_NET_MASK(16, mask) AS network_bin
      FROM source_ip_address, UNNEST(GENERATE_ARRAY(8, 128)) mask
      WHERE BYTE_LENGTH(NET.SAFE_IP_FROM_STRING(ip)) = 16
    )
    JOIN extracted_prefixes USING (network_bin, mask)
    WHERE extracted_prefixes.ip_version = 'v6'
  )
  WHERE rn = 1
),

masked_ip_addresses AS (
  SELECT * FROM masked_ip_addresses_v4
  UNION ALL
  SELECT * FROM masked_ip_addresses_v6
),

node_with_prefix_matches AS (
  SELECT DISTINCT
    nd.id,
    nd.ip_version,
    nd.dst_lat,
    nd.dst_lon,
    nd.dst_asn,
    nd.src_lat,
    nd.src_lon,
    nd.dst_city,
    nd.dst_country,
    nd.dst_state,
    nd.baseline_rtt,
    nd.ttl,
    nd.addr,
    nd.rdns_name,
    nd.rtts,
    CASE
      WHEN BYTE_LENGTH(NET.SAFE_IP_FROM_STRING(nd.addr)) = 4 THEN (
        (NET.IP_NET_MASK(4, 8)  & NET.SAFE_IP_FROM_STRING(nd.addr)) = NET.SAFE_IP_FROM_STRING('10.0.0.0')
        OR (NET.IP_NET_MASK(4,12) & NET.SAFE_IP_FROM_STRING(nd.addr)) = NET.SAFE_IP_FROM_STRING('172.16.0.0')
        OR (NET.IP_NET_MASK(4,16) & NET.SAFE_IP_FROM_STRING(nd.addr)) = NET.SAFE_IP_FROM_STRING('192.168.0.0')
        OR (NET.IP_NET_MASK(4,10) & NET.SAFE_IP_FROM_STRING(nd.addr)) = NET.SAFE_IP_FROM_STRING('100.64.0.0')
      )
      WHEN BYTE_LENGTH(NET.SAFE_IP_FROM_STRING(nd.addr)) = 16 THEN (
        (NET.IP_NET_MASK(16,10) & NET.SAFE_IP_FROM_STRING(nd.addr)) = NET.SAFE_IP_FROM_STRING('fc00::')
        OR (NET.IP_NET_MASK(16,10) & NET.SAFE_IP_FROM_STRING(nd.addr)) = NET.SAFE_IP_FROM_STRING('fe80::')
        OR (NET.IP_NET_MASK(16,96) & NET.SAFE_IP_FROM_STRING(nd.addr)) = NET.SAFE_IP_FROM_STRING('::ffff:0:0')
        OR (NET.IP_NET_MASK(16,64) & NET.SAFE_IP_FROM_STRING(nd.addr)) = NET.SAFE_IP_FROM_STRING('2001:db8::')
      )
      ELSE FALSE
    END AS is_private,
    CASE
      WHEN BYTE_LENGTH(NET.SAFE_IP_FROM_STRING(nd.addr)) = 4 AND NET.IP_NET_MASK(4, 8)  & NET.SAFE_IP_FROM_STRING(nd.addr) = NET.SAFE_IP_FROM_STRING('10.0.0.0') THEN NULL
      WHEN BYTE_LENGTH(NET.SAFE_IP_FROM_STRING(nd.addr)) = 4 AND NET.IP_NET_MASK(4, 12) & NET.SAFE_IP_FROM_STRING(nd.addr) = NET.SAFE_IP_FROM_STRING('172.16.0.0') THEN NULL
      WHEN BYTE_LENGTH(NET.SAFE_IP_FROM_STRING(nd.addr)) = 4 AND NET.IP_NET_MASK(4, 16) & NET.SAFE_IP_FROM_STRING(nd.addr) = NET.SAFE_IP_FROM_STRING('192.168.0.0') THEN NULL
      WHEN BYTE_LENGTH(NET.SAFE_IP_FROM_STRING(nd.addr)) = 4 AND NET.IP_NET_MASK(4, 10) & NET.SAFE_IP_FROM_STRING(nd.addr) = NET.SAFE_IP_FROM_STRING('100.64.0.0') THEN NULL
      WHEN BYTE_LENGTH(NET.SAFE_IP_FROM_STRING(nd.addr)) = 16 AND NET.IP_NET_MASK(16,10) & NET.SAFE_IP_FROM_STRING(nd.addr) = NET.SAFE_IP_FROM_STRING('fc00::') THEN NULL
      WHEN BYTE_LENGTH(NET.SAFE_IP_FROM_STRING(nd.addr)) = 16 AND NET.IP_NET_MASK(16,10) & NET.SAFE_IP_FROM_STRING(nd.addr) = NET.SAFE_IP_FROM_STRING('fe80::') THEN NULL
      WHEN BYTE_LENGTH(NET.SAFE_IP_FROM_STRING(nd.addr)) = 16 AND NET.IP_NET_MASK(16,96) & NET.SAFE_IP_FROM_STRING(nd.addr) = NET.SAFE_IP_FROM_STRING('::ffff:0:0') THEN NULL
      WHEN BYTE_LENGTH(NET.SAFE_IP_FROM_STRING(nd.addr)) = 16 AND NET.IP_NET_MASK(16,64) & NET.SAFE_IP_FROM_STRING(nd.addr) = NET.SAFE_IP_FROM_STRING('2001:db8::') THEN NULL
      ELSE mp.asn
    END AS associated_asn,
    mp.ixp AS associated_ixp,
    mp.ixp_partition_date
  FROM flattened_node_details nd
  LEFT JOIN masked_ip_addresses mp
    ON nd.addr = mp.ip
),

node_with_geo_info AS (
  SELECT DISTINCT
    nwp.id,
    nwp.ip_version,
    nwp.dst_lat,
    nwp.dst_lon,
    nwp.src_lon,
    nwp.src_lat,
    nwp.baseline_rtt,
    nwp.ttl,
    nwp.addr,
    nwp.rdns_name,
    nwp.rtts,
    nwp.associated_asn,
    nwp.associated_ixp,
    nwp.ixp_partition_date,
    CASE
      WHEN nwp.is_private THEN NULL
      WHEN nwp.ttl = 1 THEN nwp.dst_lat
      ELSE COALESCE(geo.lat, ip_geo.lat_ip_info, ip_geo.lat)
    END AS latitude,
    CASE
      WHEN nwp.is_private THEN NULL
      WHEN nwp.ttl = 1 THEN nwp.dst_lon
      ELSE COALESCE(geo.lon, ip_geo.lon_ip_info, ip_geo.lon)
    END AS longitude,
    CASE
      WHEN nwp.is_private THEN NULL
      WHEN nwp.ttl = 1 THEN CONCAT(nwp.dst_city, '-', nwp.dst_state, '-', nwp.dst_country)
      ELSE COALESCE(geo.place, ip_geo.city_ip_info, ip_geo.city)
    END AS place,
    CASE WHEN nwp.is_private THEN NULL ELSE geo.clli END AS clli,
    CASE WHEN nwp.is_private THEN NULL ELSE ip_geo.metro END AS metro,
    CASE
      WHEN nwp.is_private THEN NULL
      WHEN nwp.ttl = 1 THEN nwp.dst_country
      ELSE COALESCE(geo.cc, ip_geo.country_ip_info, ip_geo.country)
    END AS cc,
    CASE WHEN nwp.is_private THEN -1 ELSE COALESCE(ip_geo.score, -1) END AS score,
    -- Geo mapping provenance
    CASE
      WHEN nwp.is_private THEN NULL
      WHEN nwp.ttl = 1 THEN 'server_metadata'
      WHEN geo.hostname IS NOT NULL THEN 'hoiho'
      WHEN ip_geo.lat_ip_info IS NOT NULL THEN 'ipinfo'
      WHEN ip_geo.lat IS NOT NULL THEN 'ripe_ipmap'
      ELSE NULL
    END AS geo_source,
    CASE
      WHEN nwp.is_private OR nwp.ttl = 1 THEN NULL
      WHEN geo.hostname IS NOT NULL THEN NULL  -- HOIHO table has no partition_date
      ELSE ip_geo.partition_date
    END AS geo_partition_date
  FROM node_with_prefix_matches nwp
  LEFT JOIN `mlab-collaboration.hermes.geolocation` geo
    ON REGEXP_REPLACE(nwp.rdns_name, r'\.$', '') = REGEXP_REPLACE(geo.hostname, r'\.$', '')
  LEFT JOIN closest_geo_entry ip_geo
    ON nwp.addr = ip_geo.ip_address
),

-- Reconstruct forward node_details array
reconstructed_node_details AS (
  SELECT
    id, ip_version, dst_lat, dst_lon, src_lat, src_lon,
    AVG(baseline_rtt) AS baseline_rtt,
    ARRAY_AGG(
      STRUCT(ttl, addr, rdns_name, rtts, associated_asn, associated_ixp,
             latitude, longitude, place, clli, metro, cc, score,
             geo_source, geo_partition_date, ixp_partition_date)
      ORDER BY ttl
    ) AS updated_node_details
  FROM node_with_geo_info
  GROUP BY id, ip_version, dst_lat, dst_lon, src_lat, src_lon
),

--------------------------------------------------------------------------------
-- Reverse path: flatten hops, map to ASN/IXP, attach geo
--------------------------------------------------------------------------------

reverse_flattened_node_details AS (
  SELECT
    t.id,
    t.ip_version,
    t.src_lat,
    t.src_lon,
    t.dst_lat,
    t.dst_lon,
    t.baseline_median_rtt AS baseline_rtt,
    reverse_node.hop_ip AS addr,
    reverse_node.hop_number AS ttl,
    rdns.hostname AS rdns_name,
    reverse_node.rtt AS rtts,
    reverse_node.hop_type AS hop_type
  FROM `mlab-collaboration.hermes_union.transient_events_union` t,
       UNNEST(t.reverse_node_details) AS reverse_node
  LEFT JOIN closest_rdns_entry rdns
    ON rdns.ip_address = reverse_node.hop_ip
  WHERE t.partition_date = '${DAY}'
),

reverse_source_ip_address AS (
  SELECT DISTINCT node.hop_ip AS ip, t.ip_version
  FROM `mlab-collaboration.hermes_union.transient_events_union` t,
       UNNEST(t.reverse_node_details) AS node
  WHERE t.partition_date = '${DAY}'
),

reverse_masked_ip_addresses_v4 AS (
  SELECT ip, asn, ixp, mask, ixp_partition_date
  FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY ip ORDER BY mask DESC) AS rn
    FROM (
      SELECT *, NET.SAFE_IP_FROM_STRING(ip) & NET.IP_NET_MASK(4, mask) AS network_bin
      FROM reverse_source_ip_address, UNNEST(GENERATE_ARRAY(8, 32)) mask
      WHERE BYTE_LENGTH(NET.SAFE_IP_FROM_STRING(ip)) = 4
    )
    JOIN extracted_prefixes USING (network_bin, mask)
    WHERE extracted_prefixes.ip_version = 'v4'
  )
  WHERE rn = 1
),

reverse_masked_ip_addresses_v6 AS (
  SELECT ip, asn, ixp, mask, ixp_partition_date
  FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY ip ORDER BY mask DESC) AS rn
    FROM (
      SELECT *, NET.SAFE_IP_FROM_STRING(ip) & NET.IP_NET_MASK(16, mask) AS network_bin
      FROM reverse_source_ip_address, UNNEST(GENERATE_ARRAY(8, 128)) mask
      WHERE BYTE_LENGTH(NET.SAFE_IP_FROM_STRING(ip)) = 16
    )
    JOIN extracted_prefixes USING (network_bin, mask)
    WHERE extracted_prefixes.ip_version = 'v6'
  )
  WHERE rn = 1
),

reverse_masked_ip_addresses AS (
  SELECT * FROM reverse_masked_ip_addresses_v4
  UNION ALL
  SELECT * FROM reverse_masked_ip_addresses_v6
),

reverse_node_with_prefix_matches AS (
  SELECT DISTINCT
    nd.id,
    nd.ip_version,
    nd.dst_lat,
    nd.dst_lon,
    nd.src_lat,
    nd.src_lon,
    nd.baseline_rtt,
    nd.ttl,
    nd.addr,
    nd.rdns_name,
    nd.rtts,
    CASE
      WHEN BYTE_LENGTH(NET.SAFE_IP_FROM_STRING(nd.addr)) = 4 THEN (
        (NET.IP_NET_MASK(4, 8)  & NET.SAFE_IP_FROM_STRING(nd.addr)) = NET.SAFE_IP_FROM_STRING('10.0.0.0')
        OR (NET.IP_NET_MASK(4,12) & NET.SAFE_IP_FROM_STRING(nd.addr)) = NET.SAFE_IP_FROM_STRING('172.16.0.0')
        OR (NET.IP_NET_MASK(4,16) & NET.SAFE_IP_FROM_STRING(nd.addr)) = NET.SAFE_IP_FROM_STRING('192.168.0.0')
        OR (NET.IP_NET_MASK(4,10) & NET.SAFE_IP_FROM_STRING(nd.addr)) = NET.SAFE_IP_FROM_STRING('100.64.0.0')
      )
      WHEN BYTE_LENGTH(NET.SAFE_IP_FROM_STRING(nd.addr)) = 16 THEN (
        (NET.IP_NET_MASK(16,10) & NET.SAFE_IP_FROM_STRING(nd.addr)) = NET.SAFE_IP_FROM_STRING('fc00::')
        OR (NET.IP_NET_MASK(16,10) & NET.SAFE_IP_FROM_STRING(nd.addr)) = NET.SAFE_IP_FROM_STRING('fe80::')
        OR (NET.IP_NET_MASK(16,96) & NET.SAFE_IP_FROM_STRING(nd.addr)) = NET.SAFE_IP_FROM_STRING('::ffff:0:0')
        OR (NET.IP_NET_MASK(16,64) & NET.SAFE_IP_FROM_STRING(nd.addr)) = NET.SAFE_IP_FROM_STRING('2001:db8::')
      )
      ELSE FALSE
    END AS is_private,
    CASE
      WHEN BYTE_LENGTH(NET.SAFE_IP_FROM_STRING(nd.addr)) = 4 AND NET.IP_NET_MASK(4, 8)  & NET.SAFE_IP_FROM_STRING(nd.addr) = NET.SAFE_IP_FROM_STRING('10.0.0.0') THEN NULL
      WHEN BYTE_LENGTH(NET.SAFE_IP_FROM_STRING(nd.addr)) = 4 AND NET.IP_NET_MASK(4, 12) & NET.SAFE_IP_FROM_STRING(nd.addr) = NET.SAFE_IP_FROM_STRING('172.16.0.0') THEN NULL
      WHEN BYTE_LENGTH(NET.SAFE_IP_FROM_STRING(nd.addr)) = 4 AND NET.IP_NET_MASK(4, 16) & NET.SAFE_IP_FROM_STRING(nd.addr) = NET.SAFE_IP_FROM_STRING('192.168.0.0') THEN NULL
      WHEN BYTE_LENGTH(NET.SAFE_IP_FROM_STRING(nd.addr)) = 4 AND NET.IP_NET_MASK(4, 10) & NET.SAFE_IP_FROM_STRING(nd.addr) = NET.SAFE_IP_FROM_STRING('100.64.0.0') THEN NULL
      WHEN BYTE_LENGTH(NET.SAFE_IP_FROM_STRING(nd.addr)) = 16 AND NET.IP_NET_MASK(16,10) & NET.SAFE_IP_FROM_STRING(nd.addr) = NET.SAFE_IP_FROM_STRING('fc00::') THEN NULL
      WHEN BYTE_LENGTH(NET.SAFE_IP_FROM_STRING(nd.addr)) = 16 AND NET.IP_NET_MASK(16,10) & NET.SAFE_IP_FROM_STRING(nd.addr) = NET.SAFE_IP_FROM_STRING('fe80::') THEN NULL
      WHEN BYTE_LENGTH(NET.SAFE_IP_FROM_STRING(nd.addr)) = 16 AND NET.IP_NET_MASK(16,96) & NET.SAFE_IP_FROM_STRING(nd.addr) = NET.SAFE_IP_FROM_STRING('::ffff:0:0') THEN NULL
      WHEN BYTE_LENGTH(NET.SAFE_IP_FROM_STRING(nd.addr)) = 16 AND NET.IP_NET_MASK(16,64) & NET.SAFE_IP_FROM_STRING(nd.addr) = NET.SAFE_IP_FROM_STRING('2001:db8::') THEN NULL
      ELSE mp.asn
    END AS associated_asn,
    mp.ixp AS associated_ixp,
    mp.ixp_partition_date,
    nd.hop_type
  FROM reverse_flattened_node_details nd
  LEFT JOIN reverse_masked_ip_addresses mp
    ON nd.addr = mp.ip
),

reverse_node_with_geo_info AS (
  SELECT DISTINCT
    nwp.id,
    nwp.ip_version,
    nwp.dst_lat,
    nwp.dst_lon,
    nwp.src_lat,
    nwp.src_lon,
    nwp.baseline_rtt,
    nwp.ttl,
    nwp.addr,
    nwp.rdns_name,
    nwp.rtts,
    nwp.associated_asn,
    nwp.associated_ixp,
    nwp.ixp_partition_date,
    CASE WHEN nwp.is_private THEN NULL ELSE COALESCE(geo.lat, ip_geo.lat_ip_info, ip_geo.lat) END AS latitude,
    CASE WHEN nwp.is_private THEN NULL ELSE COALESCE(geo.lon, ip_geo.lon_ip_info, ip_geo.lon) END AS longitude,
    CASE WHEN nwp.is_private THEN NULL ELSE COALESCE(geo.place, ip_geo.city_ip_info, ip_geo.city) END AS place,
    CASE WHEN nwp.is_private THEN NULL ELSE geo.clli END AS clli,
    CASE WHEN nwp.is_private THEN NULL ELSE COALESCE(geo.metro, ip_geo.metro) END AS metro,
    CASE WHEN nwp.is_private THEN NULL ELSE COALESCE(geo.cc, ip_geo.country_ip_info, ip_geo.country) END AS cc,
    CASE WHEN nwp.is_private THEN -1   ELSE COALESCE(ip_geo.score, -1) END AS score,
    -- Geo mapping provenance
    CASE
      WHEN nwp.is_private THEN NULL
      WHEN geo.hostname IS NOT NULL THEN 'hoiho'
      WHEN ip_geo.lat_ip_info IS NOT NULL THEN 'ipinfo'
      WHEN ip_geo.lat IS NOT NULL THEN 'ripe_ipmap'
      ELSE NULL
    END AS geo_source,
    CASE
      WHEN nwp.is_private THEN NULL
      WHEN geo.hostname IS NOT NULL THEN NULL  -- HOIHO table has no partition_date
      ELSE ip_geo.partition_date
    END AS geo_partition_date,
    nwp.hop_type
  FROM reverse_node_with_prefix_matches nwp
  LEFT JOIN `mlab-collaboration.hermes.geolocation` geo
    ON REGEXP_REPLACE(nwp.rdns_name, r'\.$', '') = REGEXP_REPLACE(geo.hostname, r'\.$', '')
  LEFT JOIN closest_geo_entry ip_geo
    ON nwp.addr = ip_geo.ip_address
),

-- Reconstruct reverse node_details array (keep hop_type, convert rtts to seconds)
reverse_reconstructed_node_details_without_flag AS (
  SELECT
    id, ip_version, dst_lat, dst_lon, src_lat, src_lon,
    AVG(baseline_rtt) AS baseline_rtt,
    ARRAY_AGG(
      STRUCT(ttl, addr, rdns_name, rtts / 1000 AS rtts,
             associated_asn, associated_ixp,
             latitude, longitude, place, clli, metro, cc, score,
             geo_source, geo_partition_date, ixp_partition_date,
             hop_type)
      ORDER BY ttl
    ) AS updated_node_details
  FROM reverse_node_with_geo_info
  GROUP BY id, ip_version, dst_lat, dst_lon, src_lat, src_lon
),

--------------------------------------------------------------------------------
-- Reverse path: distance / RTT checks
--------------------------------------------------------------------------------

reverse_valid_coords AS (
  SELECT
    id, ip_version, dst_lat, dst_lon, src_lat, src_lon, baseline_rtt,
    n.ttl, n.addr, n.rdns_name, n.rtts,
    n.associated_asn, n.associated_ixp,
    n.latitude, n.longitude, n.place, n.clli, n.metro, n.cc, n.score,
    n.geo_source, n.geo_partition_date, n.ixp_partition_date,
    n.hop_type
  FROM reverse_reconstructed_node_details_without_flag,
       UNNEST(updated_node_details) AS n
),

reverse_filled_coords AS (
  SELECT *,
    LAST_VALUE(latitude IGNORE NULLS)
      OVER (PARTITION BY id ORDER BY ttl DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS filled_latitude,
    LAST_VALUE(longitude IGNORE NULLS)
      OVER (PARTITION BY id ORDER BY ttl DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS filled_longitude
  FROM reverse_valid_coords
),

reverse_distances AS (
  SELECT *,
    LAG(filled_latitude)  OVER (PARTITION BY id ORDER BY ttl DESC) AS prev_latitude,
    LAG(filled_longitude) OVER (PARTITION BY id ORDER BY ttl DESC) AS prev_longitude,
    CASE
      WHEN LAG(filled_latitude) OVER (PARTITION BY id ORDER BY ttl DESC) IS NOT NULL
        AND LAG(filled_longitude) OVER (PARTITION BY id ORDER BY ttl DESC) IS NOT NULL
        AND filled_latitude IS NOT NULL AND filled_longitude IS NOT NULL
      THEN ST_DISTANCE(
        ST_GEOGPOINT(filled_longitude, filled_latitude),
        ST_GEOGPOINT(
          LAG(filled_longitude) OVER (PARTITION BY id ORDER BY ttl DESC),
          LAG(filled_latitude) OVER (PARTITION BY id ORDER BY ttl DESC)
        )
      ) / 1000
      ELSE 0
    END AS hop_distance_km
  FROM reverse_filled_coords
),

reverse_cumulative_calculations AS (
  SELECT *,
    SUM(hop_distance_km)
      OVER (PARTITION BY id ORDER BY ttl DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      AS cumulative_distance_km
  FROM reverse_distances
),

reverse_distance_rtt_checks AS (
  SELECT
    id, ip_version, ttl, addr, rdns_name, rtts,
    associated_asn, associated_ixp,
    latitude, longitude, place, clli, metro, cc, score,
    geo_source, geo_partition_date, ixp_partition_date,
    cumulative_distance_km, hop_type,
    CASE
      WHEN latitude IS NULL OR longitude IS NULL OR dst_lat IS NULL OR dst_lon IS NULL THEN NULL
      ELSE ST_DISTANCE(ST_GEOGPOINT(longitude, latitude), ST_GEOGPOINT(dst_lon, dst_lat)) / 1000
    END AS distance_to_destination_km,
    CASE
      WHEN cumulative_distance_km IS NULL OR latitude IS NULL OR longitude IS NULL OR dst_lat IS NULL OR dst_lon IS NULL THEN NULL
      ELSE (cumulative_distance_km / 200)
        + (ST_DISTANCE(ST_GEOGPOINT(longitude, latitude), ST_GEOGPOINT(dst_lon, dst_lat)) / (200 * 1000))
    END AS speed_of_internet_fiber,
    CASE
      WHEN cumulative_distance_km IS NULL OR latitude IS NULL OR longitude IS NULL OR dst_lat IS NULL OR dst_lon IS NULL THEN NULL
      WHEN (
        (cumulative_distance_km / 200)
        + ST_DISTANCE(ST_GEOGPOINT(longitude, latitude), ST_GEOGPOINT(dst_lon, dst_lat)) / (200 * 1000)
      ) > rtts THEN 'Above threshold'
      ELSE 'Below threshold'
    END AS distance_rtt_check,
    CASE
      WHEN rtts IS NULL THEN 'Not responsive'
      WHEN rtts > baseline_rtt THEN 'Above baseline'
      ELSE 'Within baseline'
    END AS above_baseline_flag,
    CASE
      WHEN rtts IS NULL THEN 'Not responsive'
      WHEN rtts + 3 > LAG(rtts) OVER (PARTITION BY id ORDER BY ttl) THEN 'Increasing'
      ELSE 'Stable/Decreasing'
    END AS increasing_latency_flag
  FROM reverse_cumulative_calculations
),

reverse_distance_rtt_checks_with_metas_info AS (
  SELECT
    id, ip_version, ttl, addr, rdns_name, rtts,
    rd.associated_asn, associated_ixp,
    rd.latitude, rd.longitude, place, clli, metro, cc, score,
    geo_source, geo_partition_date, ixp_partition_date,
    cumulative_distance_km, distance_to_destination_km,
    speed_of_internet_fiber, distance_rtt_check,
    above_baseline_flag, increasing_latency_flag,
    CASE
      WHEN COUNT(CASE WHEN above_baseline_flag = 'Within baseline' THEN 1 END)
        OVER (PARTITION BY id ORDER BY ttl ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) > 0
      THEN 'False'
      WHEN COUNT(CASE WHEN increasing_latency_flag = 'Stable/Decreasing' THEN 1 END)
        OVER (PARTITION BY id ORDER BY ttl ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) > 0
      THEN 'False'
      ELSE 'True'
    END AS baseline_consistency_flag,
    hop_type,
    as_metadata.organization.OrgName AS associated_org,
    as_metadata.PeeringDB_name AS associated_peeringdb_name,
    ARRAY_AGG(STRUCT(
      sub.facility_name AS facility_name,
      sub.facility_city AS facility_city,
      sub.distance_km
    )) AS facilities_info
  FROM reverse_distance_rtt_checks rd
  LEFT JOIN `mlab-collaboration.hermes.asn_facility_matched` sub
    ON rd.associated_asn = sub.asn AND rd.place = sub.city
  LEFT JOIN closest_metadata as_metadata
    ON CAST(as_metadata.asn AS INT64) = CAST(rd.associated_asn AS INT64)
  GROUP BY
    id, ip_version, ttl, addr, rdns_name, rtts, rd.associated_asn, associated_ixp,
    rd.latitude, rd.longitude, place, clli, metro, cc, score,
    geo_source, geo_partition_date, ixp_partition_date,
    cumulative_distance_km, distance_to_destination_km,
    speed_of_internet_fiber, distance_rtt_check,
    above_baseline_flag, increasing_latency_flag,
    hop_type, as_metadata.organization.OrgName, as_metadata.PeeringDB_name
),

with_speed_of_light_reverse_node_details AS (
  SELECT
    id, ip_version,
    ARRAY_AGG(
      STRUCT(
        ttl, addr, rdns_name, rtts,
        associated_asn, associated_ixp, associated_org, associated_peeringdb_name,
        latitude, longitude, place, clli, cc, score, metro,
        geo_source, geo_partition_date, ixp_partition_date,
        hop_type,
        facilities_info,
        cumulative_distance_km, distance_to_destination_km,
        speed_of_internet_fiber, distance_rtt_check,
        above_baseline_flag, increasing_latency_flag
      )
      ORDER BY ttl
    ) AS updated_node_details
  FROM reverse_distance_rtt_checks_with_metas_info
  GROUP BY id, ip_version
),

--------------------------------------------------------------------------------
-- Reverse path: interdomain symmetry + fishy type-4 flags
--------------------------------------------------------------------------------

-- Identify hops with hop_type IN (11, 12, 2) where ASN/org changes
interdomain_symmetry AS (
  SELECT
    id,
    ttl,
    TRUE AS is_interdomain_symmetry
  FROM (
    SELECT
      id,
      node.ttl,
      node.hop_type,
      node.associated_asn,
      node.associated_org,
      (
        SELECT hop.associated_asn
        FROM UNNEST(updated_node_details) AS hop
        WHERE hop.ttl < node.ttl AND hop.associated_asn IS NOT NULL
        ORDER BY hop.ttl DESC
        LIMIT 1
      ) AS prev_asn,
      (
        SELECT hop.associated_asn
        FROM UNNEST(updated_node_details) AS hop
        WHERE hop.ttl > node.ttl AND hop.associated_asn IS NOT NULL
        ORDER BY hop.ttl ASC
        LIMIT 1
      ) AS next_asn,
      (
        SELECT hop.associated_org
        FROM UNNEST(updated_node_details) AS hop
        WHERE hop.ttl < node.ttl AND hop.associated_org IS NOT NULL
        ORDER BY hop.ttl DESC
        LIMIT 1
      ) AS prev_org,
      (
        SELECT hop.associated_org
        FROM UNNEST(updated_node_details) AS hop
        WHERE hop.ttl > node.ttl AND hop.associated_org IS NOT NULL
        ORDER BY hop.ttl ASC
        LIMIT 1
      ) AS next_org
    FROM
      with_speed_of_light_reverse_node_details,
      UNNEST(updated_node_details) AS node
    WHERE node.hop_type IN (11, 12, 2)
  )
  WHERE (
    (prev_asn IS NOT NULL AND prev_asn != associated_asn) OR
    (prev_org IS NOT NULL AND prev_org != associated_org)
  )
),

-- RTTs of the last node with hop_type != 4
last_non_type_4 AS (
  SELECT
    id,
    MAX(IF(node.hop_type != 4, node.rtts, NULL)) AS last_rtts
  FROM
    with_speed_of_light_reverse_node_details,
    UNNEST(updated_node_details) AS node
  GROUP BY id
),

-- Identify type-4 hops with suspiciously high RTT jump (> 50 ms)
fishy_type_4 AS (
  SELECT
    n.id,
    node.ttl,
    TRUE AS is_fishy_type_4
  FROM
    with_speed_of_light_reverse_node_details n,
    UNNEST(updated_node_details) AS node
  LEFT JOIN last_non_type_4 l
    ON n.id = l.id
  WHERE
    node.hop_type = 4
    AND (node.rtts - l.last_rtts) > 50
),

-- Combine reverse node details with interdomain_symmetry and fishy_type_4 flags
reverse_reconstructed_node_details AS (
  SELECT
    wsld.id,
    wsld.ip_version,
    ARRAY_AGG(
      STRUCT(
        node.ttl, node.addr, node.rdns_name, node.rtts,
        node.associated_asn, node.associated_ixp,
        node.associated_org, node.associated_peeringdb_name,
        node.latitude, node.longitude, node.place, node.clli, node.cc, node.score, node.metro,
        node.geo_source, node.geo_partition_date, node.ixp_partition_date,
        node.hop_type, node.facilities_info,
        node.cumulative_distance_km, node.distance_to_destination_km,
        node.speed_of_internet_fiber, node.distance_rtt_check,
        node.above_baseline_flag, node.increasing_latency_flag,
        COALESCE(isd.is_interdomain_symmetry, FALSE) AS is_interdomain_symmetry,
        COALESCE(ft4.is_fishy_type_4, FALSE) AS is_fishy_type_4
      )
      ORDER BY node.ttl
    ) AS updated_node_details
  FROM with_speed_of_light_reverse_node_details wsld,
       UNNEST(wsld.updated_node_details) AS node
  LEFT JOIN interdomain_symmetry isd
    ON wsld.id = isd.id AND node.ttl = isd.ttl
  LEFT JOIN fishy_type_4 ft4
    ON wsld.id = ft4.id AND node.ttl = ft4.ttl
  GROUP BY wsld.id, wsld.ip_version
),

--------------------------------------------------------------------------------
-- Forward path: distance / RTT checks
--------------------------------------------------------------------------------

valid_coords AS (
  SELECT
    id, ip_version, dst_lat, dst_lon, src_lat, src_lon, baseline_rtt,
    n.ttl, n.addr, n.rdns_name, n.rtts,
    n.associated_asn, n.associated_ixp,
    n.latitude, n.longitude, n.place, n.clli, n.metro, n.cc, n.score,
    n.geo_source, n.geo_partition_date, n.ixp_partition_date
  FROM reconstructed_node_details,
       UNNEST(updated_node_details) AS n
),

filled_coords AS (
  SELECT *,
    LAST_VALUE(latitude IGNORE NULLS)
      OVER (PARTITION BY id ORDER BY ttl ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS filled_latitude,
    LAST_VALUE(longitude IGNORE NULLS)
      OVER (PARTITION BY id ORDER BY ttl ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS filled_longitude
  FROM valid_coords
),

distances AS (
  SELECT *,
    LAG(filled_latitude)  OVER (PARTITION BY id ORDER BY ttl) AS prev_latitude,
    LAG(filled_longitude) OVER (PARTITION BY id ORDER BY ttl) AS prev_longitude,
    CASE
      WHEN LAG(filled_latitude) OVER (PARTITION BY id ORDER BY ttl) IS NOT NULL
        AND LAG(filled_longitude) OVER (PARTITION BY id ORDER BY ttl) IS NOT NULL
        AND latitude IS NOT NULL AND longitude IS NOT NULL
      THEN ST_DISTANCE(
        ST_GEOGPOINT(filled_longitude, filled_latitude),
        ST_GEOGPOINT(
          LAG(filled_longitude) OVER (PARTITION BY id ORDER BY ttl),
          LAG(filled_latitude) OVER (PARTITION BY id ORDER BY ttl)
        )
      ) / 1000
      ELSE 0
    END AS hop_distance_km
  FROM filled_coords
),

cumulative_calculations AS (
  SELECT *,
    SUM(hop_distance_km)
      OVER (PARTITION BY id ORDER BY ttl ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      AS cumulative_distance_km
  FROM distances
),

distance_rtt_checks AS (
  SELECT
    id, ip_version, ttl, addr, rdns_name, rtts,
    associated_asn, associated_ixp,
    latitude, longitude, place, clli, metro, cc, score,
    geo_source, geo_partition_date, ixp_partition_date,
    dst_lat, dst_lon, src_lat, src_lon, baseline_rtt,
    cumulative_distance_km,
    CASE
      WHEN latitude IS NULL OR longitude IS NULL OR src_lat IS NULL OR src_lon IS NULL THEN NULL
      ELSE ST_DISTANCE(ST_GEOGPOINT(longitude, latitude), ST_GEOGPOINT(src_lon, src_lat)) / 1000
    END AS distance_to_destination_km,
    CASE
      WHEN cumulative_distance_km IS NULL OR latitude IS NULL OR longitude IS NULL OR src_lat IS NULL OR src_lon IS NULL THEN NULL
      ELSE (cumulative_distance_km / 200)
        + (ST_DISTANCE(ST_GEOGPOINT(longitude, latitude), ST_GEOGPOINT(dst_lon, dst_lat)) / (200 * 1000))
    END AS speed_of_internet_fiber,
    CASE
      WHEN cumulative_distance_km IS NULL OR latitude IS NULL OR longitude IS NULL OR src_lat IS NULL OR src_lon IS NULL THEN NULL
      WHEN (
        (cumulative_distance_km / 200)
        + ST_DISTANCE(ST_GEOGPOINT(longitude, latitude), ST_GEOGPOINT(dst_lon, dst_lat)) / (200 * 1000)
      ) > rtts THEN 'Above threshold'
      ELSE 'Below threshold'
    END AS distance_rtt_check,
    CASE
      WHEN rtts IS NULL THEN 'Not responsive'
      WHEN rtts > baseline_rtt THEN 'Above baseline'
      ELSE 'Within baseline'
    END AS above_baseline_flag,
    CASE
      WHEN rtts IS NULL THEN 'Not responsive'
      WHEN rtts + 3 > LAG(rtts) OVER (PARTITION BY id ORDER BY ttl) THEN 'Increasing'
      ELSE 'Stable/Decreasing'
    END AS increasing_latency_flag
  FROM cumulative_calculations
),

distance_rtt_checks_with_metas_info AS (
  SELECT
    id, ip_version, ttl, addr, rdns_name, rtts,
    rd.associated_asn, associated_ixp,
    rd.latitude, rd.longitude, place, clli, metro, cc, score,
    geo_source, geo_partition_date, ixp_partition_date,
    dst_lat, dst_lon, src_lat, src_lon,
    cumulative_distance_km, distance_to_destination_km,
    speed_of_internet_fiber, distance_rtt_check,
    above_baseline_flag, increasing_latency_flag,
    CASE
      WHEN EXISTS (
        SELECT 1
        FROM distance_rtt_checks
        WHERE above_baseline_flag = 'Within baseline' AND id = rd.id
      ) THEN 'False'
      WHEN EXISTS (
        SELECT 1
        FROM distance_rtt_checks
        WHERE increasing_latency_flag = 'Stable/Decreasing' AND id = rd.id
      ) THEN 'False'
      ELSE 'True'
    END AS baseline_consistency_flag,
    as_metadata.organization.OrgName AS associated_org,
    as_metadata.PeeringDB_name AS associated_peeringdb_name,
    ARRAY_AGG(STRUCT(
      sub.facility_name AS facility_name,
      sub.facility_city AS facility_city,
      sub.distance_km
    )) AS facilities_info
  FROM distance_rtt_checks rd
  LEFT JOIN `mlab-collaboration.hermes.asn_facility_matched` sub
    ON rd.associated_asn = sub.asn AND rd.place = sub.city
  LEFT JOIN closest_metadata as_metadata
    ON CAST(as_metadata.asn AS INT64) = CAST(rd.associated_asn AS INT64)
  GROUP BY
    id, ip_version, ttl, addr, rdns_name, rtts, rd.associated_asn, associated_ixp,
    rd.latitude, rd.longitude, place, clli, metro, cc, score,
    geo_source, geo_partition_date, ixp_partition_date,
    dst_lat, dst_lon, src_lat, src_lon,
    cumulative_distance_km, distance_to_destination_km,
    speed_of_internet_fiber, distance_rtt_check,
    above_baseline_flag, increasing_latency_flag,
    as_metadata.organization.OrgName, as_metadata.PeeringDB_name
),

forward_reconstructed_node_details_with_latency AS (
  SELECT
    id, ip_version,
    AVG(dst_lat) AS dst_lat,
    AVG(dst_lon) AS dst_lon,
    AVG(src_lat) AS src_lat,
    AVG(src_lon) AS src_lon,
    ARRAY_AGG(
      STRUCT(
        ttl, addr, rdns_name, rtts,
        associated_asn, associated_org, associated_peeringdb_name,
        associated_ixp,
        latitude, longitude, place, clli, cc, metro, score,
        geo_source, geo_partition_date, ixp_partition_date,
        facilities_info,
        cumulative_distance_km, distance_to_destination_km,
        speed_of_internet_fiber, distance_rtt_check,
        above_baseline_flag, increasing_latency_flag,
        baseline_consistency_flag
      )
      ORDER BY ttl
    ) AS updated_node_details
  FROM distance_rtt_checks_with_metas_info
  GROUP BY id, ip_version
),

--------------------------------------------------------------------------------
-- Path distances
--------------------------------------------------------------------------------

reverse_path_distances AS (
  SELECT
    rrd.id,
    rrd.ip_version,
    (
      SELECT node.cumulative_distance_km
      FROM UNNEST(rrd.updated_node_details) AS node
      ORDER BY node.ttl
      LIMIT 1
    ) AS total_gcd_distance
  FROM reverse_reconstructed_node_details rrd
),

forward_path_distances AS (
  SELECT
    rrd.id,
    rrd.ip_version,
    (
      SELECT
        node.cumulative_distance_km +
          ST_DISTANCE(
            ST_GEOGPOINT(
              LAST_VALUE(node.longitude IGNORE NULLS)
                OVER (ORDER BY node.ttl ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
              LAST_VALUE(node.latitude IGNORE NULLS)
                OVER (ORDER BY node.ttl ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)),
            ST_GEOGPOINT(rrd.src_lon, rrd.src_lat))
          / 1000
      FROM UNNEST(rrd.updated_node_details) AS node
      ORDER BY node.ttl DESC
      LIMIT 1
    ) AS total_gcd_distance
  FROM forward_reconstructed_node_details_with_latency rrd
),

--------------------------------------------------------------------------------
-- Final join: attach enriched forward + reverse arrays to events
--------------------------------------------------------------------------------

final_results AS (
  SELECT
    t.* EXCEPT (node_details, reverse_node_details, partition_date),
    f.updated_node_details AS forward_updated_node_details,
    COALESCE(
      fpd.total_gcd_distance,
      ST_DISTANCE(
        ST_GEOGPOINT(t.src_lon, t.src_lat),
        ST_GEOGPOINT(t.dst_lon, t.dst_lat))
        / 1000) AS forward_distance,
    COALESCE(
      rpd.total_gcd_distance,
      ST_DISTANCE(
        ST_GEOGPOINT(t.src_lon, t.src_lat),
        ST_GEOGPOINT(t.dst_lon, t.dst_lat))
        / 1000) AS reverse_distance,
    COALESCE(
      fpd.total_gcd_distance,
      ST_DISTANCE(
        ST_GEOGPOINT(t.src_lon, t.src_lat),
        ST_GEOGPOINT(t.dst_lon, t.dst_lat))
        / 1000)
      + COALESCE(
        rpd.total_gcd_distance,
        ST_DISTANCE(
          ST_GEOGPOINT(t.src_lon, t.src_lat),
          ST_GEOGPOINT(t.dst_lon, t.dst_lat))
          / 1000) AS both_way_distance,
    r.updated_node_details AS reverse_updated_node_details,
    CASE
      WHEN t.id LIKE '%virtual%' THEN TRUE
      ELSE FALSE
    END AS is_virtual,
    EXISTS(
      SELECT 1
      FROM UNNEST(f.updated_node_details) AS ud
      WHERE CAST(ud.associated_asn AS INT64) = CAST(t.src_asn AS INT64)
    ) AS is_reaching_dst_asn,
    CAST('${DAY}' AS DATE) AS partition_date
  FROM `mlab-collaboration.hermes_union.transient_events_union` t
  LEFT JOIN forward_reconstructed_node_details_with_latency f
    ON t.id = f.id AND t.ip_version = f.ip_version
  LEFT JOIN reverse_reconstructed_node_details r
    ON t.id = r.id AND t.ip_version = r.ip_version
  LEFT JOIN forward_path_distances fpd
    ON t.id = fpd.id AND t.ip_version = fpd.ip_version
  LEFT JOIN reverse_path_distances rpd
    ON t.id = rpd.id AND t.ip_version = rpd.ip_version
  WHERE t.partition_date = '${DAY}'
),

--------------------------------------------------------------------------------
-- AS loop detection (forward + reverse)
--------------------------------------------------------------------------------

forward_as_loop_detection AS (
  SELECT
    id,
    forward_asn_path,
    ARRAY_LENGTH(forward_asn_path) AS forward_asn_count,
    ARRAY_LENGTH(
      ARRAY(
        SELECT DISTINCT asn
        FROM UNNEST(forward_asn_path) AS asn
        WHERE asn IS NOT NULL AND asn != -1
      )
    ) AS distinct_valid_forward_asn_count,
    ARRAY_LENGTH(
      ARRAY(
        SELECT asn
        FROM UNNEST(forward_asn_path) AS asn
        WHERE asn IS NULL OR asn = -1
      )
    ) AS null_or_negative_one_count,
    CASE
      WHEN ARRAY_LENGTH(
        ARRAY(
          SELECT asn
          FROM UNNEST(forward_asn_path) AS asn
          WHERE asn IS NULL OR asn = -1
        )
      ) > 0 THEN TRUE
      ELSE FALSE
    END AS forward_unresponse_within_AS,
    CASE
      WHEN ARRAY_LENGTH(forward_asn_path) > ARRAY_LENGTH(
        ARRAY(
          SELECT DISTINCT asn
          FROM UNNEST(forward_asn_path) AS asn
          WHERE asn IS NOT NULL AND asn != -1
        )
      ) AND ARRAY_LENGTH(
        ARRAY(
          SELECT asn
          FROM UNNEST(forward_asn_path) AS asn
          WHERE asn IS NULL OR asn = -1
        )
      ) = 0 THEN TRUE
      ELSE FALSE
    END AS forward_loop
  FROM (
    SELECT
      id,
      ARRAY_AGG(
        IF(associated_asn IS NULL, -1, associated_asn) ORDER BY ttl
      ) AS forward_asn_path
    FROM node_with_geo_info
    GROUP BY id
  )
),

reverse_as_loop_detection AS (
  SELECT
    id,
    reverse_asn_path,
    ARRAY_LENGTH(reverse_asn_path) AS reverse_asn_count,
    ARRAY_LENGTH(
      ARRAY(
        SELECT DISTINCT asn
        FROM UNNEST(reverse_asn_path) AS asn
        WHERE asn IS NOT NULL AND asn != -1
      )
    ) AS distinct_valid_reverse_asn_count,
    ARRAY_LENGTH(
      ARRAY(
        SELECT asn
        FROM UNNEST(reverse_asn_path) AS asn
        WHERE asn IS NULL OR asn = -1
      )
    ) AS null_or_negative_one_count,
    CASE
      WHEN ARRAY_LENGTH(
        ARRAY(
          SELECT asn
          FROM UNNEST(reverse_asn_path) AS asn
          WHERE asn IS NULL OR asn = -1
        )
      ) > 0 THEN TRUE
      ELSE FALSE
    END AS reverse_unresponsive_within_AS,
    CASE
      WHEN ARRAY_LENGTH(reverse_asn_path) > ARRAY_LENGTH(
        ARRAY(
          SELECT DISTINCT asn
          FROM UNNEST(reverse_asn_path) AS asn
          WHERE asn IS NOT NULL AND asn != -1
        )
      ) AND ARRAY_LENGTH(
        ARRAY(
          SELECT asn
          FROM UNNEST(reverse_asn_path) AS asn
          WHERE asn IS NULL OR asn = -1
        )
      ) = 0 THEN TRUE
      ELSE FALSE
    END AS reverse_loop
  FROM (
    SELECT
      id,
      ARRAY_AGG(
        IF(associated_asn IS NULL, -1, associated_asn) ORDER BY ttl
      ) AS reverse_asn_path
    FROM reverse_node_with_geo_info
    GROUP BY id
  )
)

-- Final results with loop detection flags
SELECT
  fr.*,
  ral.reverse_unresponsive_within_AS,
  fal.forward_unresponse_within_AS,
  fal.forward_loop,
  ral.reverse_loop
FROM final_results fr
LEFT JOIN forward_as_loop_detection fal
  ON fr.id = fal.id
LEFT JOIN reverse_as_loop_detection ral
  ON fr.id = ral.id;

-- ============================================================================
-- Write to both output tables from the temp table (no re-scan).
-- Explicit column names so INSERT is immune to column ordering differences.
-- ============================================================================
INSERT INTO `mlab-collaboration.hermes_union.events_with_as_and_geoloc`
  (id, dst, src, ndt_rtt, ndt_throughput, ndt_loss_rate, traceroute_rtt,
   total_windows, is_consistent, src_city, src_lat, src_state, src_lon,
   dst_lat, dst_lon, src_asn, dst_site, dst_city, dst_asn, dst_country,
   src_country, src_asn_name, client_name, start, window_start, reach_dest,
   number_of_measurements_baseline, number_of_unique_src_ips_baseline,
   unique_ip_count_per_site, measurement_count_per_site,
   baseline_median_rtt, baseline_median_throughput,
   baseline_median_upload_throughput, baseline_median_loss, ip_version,
   revtr_system_label, revtr_stop_reason, revtr_fail_reason,
   is_try_from_destination_AS, revtr_id,
   city_median_rtt, city_ninetyth_percentile_rtt,
   city_oneth_percentile_rtt, city_tenth_percentile_rtt,
   city_median_throughput, city_ninetyth_percentile_throughput,
   forward_updated_node_details, forward_distance, reverse_distance,
   both_way_distance, reverse_updated_node_details,
   is_virtual, is_reaching_dst_asn, partition_date,
   reverse_unresponsive_within_AS, forward_unresponse_within_AS,
   forward_loop, reverse_loop,
   anomaly_ratio_rtt, anomaly_rtt_count,
   anomaly_ratio_throughput, anomaly_throughput_count,
   anomaly_ratio_upload_throughput, anomaly_upload_throughput_count,
   anomaly_loss_ratio,
   difference_latency, difference_throughput, difference_upload_throughput,
   median_upload_throughput,
   wasserstein_throughput_result, wasserstein_upload_throughput_result,
   mann_whitney_latency, mann_whitney_throughput, mann_whitney_upload_throughput,
   t_test_latency)
SELECT
  id, dst, src, ndt_rtt, ndt_throughput, ndt_loss_rate, traceroute_rtt,
  total_windows, is_consistent, src_city, src_lat, src_state, src_lon,
  dst_lat, dst_lon, src_asn, dst_site, dst_city, dst_asn, dst_country,
  src_country, src_asn_name, client_name, start, window_start, reach_dest,
  number_of_measurements_baseline, number_of_unique_src_ips_baseline,
  unique_ip_count_per_site, measurement_count_per_site,
  baseline_median_rtt, baseline_median_throughput,
  baseline_median_upload_throughput, baseline_median_loss, ip_version,
  revtr_system_label, revtr_stop_reason, revtr_fail_reason,
  is_try_from_destination_AS, revtr_id,
  city_median_rtt, city_ninetyth_percentile_rtt,
  city_oneth_percentile_rtt, city_tenth_percentile_rtt,
  city_median_throughput, city_ninetyth_percentile_throughput,
  forward_updated_node_details, forward_distance, reverse_distance,
  both_way_distance, reverse_updated_node_details,
  is_virtual, is_reaching_dst_asn, partition_date,
  reverse_unresponsive_within_AS, forward_unresponse_within_AS,
  forward_loop, reverse_loop,
  anomaly_ratio_rtt, anomaly_rtt_count,
  anomaly_ratio_throughput, anomaly_throughput_count,
  anomaly_ratio_upload_throughput, anomaly_upload_throughput_count,
  anomaly_loss_ratio,
  difference_latency, difference_throughput, difference_upload_throughput,
  median_upload_throughput,
  wasserstein_throughput_result, wasserstein_upload_throughput_result,
  mann_whitney_latency, mann_whitney_throughput, mann_whitney_upload_throughput,
  t_test_latency
FROM _mapping_result;

INSERT INTO `mlab-collaboration.hermes_union.giga_meter_measurements`
  (id, dst, src, ndt_rtt, ndt_throughput, ndt_loss_rate, traceroute_rtt,
   total_windows, is_consistent, src_city, src_lat, src_state, src_lon,
   dst_lat, dst_lon, src_asn, dst_site, dst_city, dst_asn, dst_country,
   src_country, src_asn_name, client_name, start, window_start, reach_dest,
   number_of_measurements_baseline, number_of_unique_src_ips_baseline,
   unique_ip_count_per_site, measurement_count_per_site,
   baseline_median_rtt, baseline_median_throughput,
   baseline_median_upload_throughput, baseline_median_loss, ip_version,
   revtr_system_label, revtr_stop_reason, revtr_fail_reason,
   is_try_from_destination_AS, revtr_id,
   city_median_rtt, city_ninetyth_percentile_rtt,
   city_oneth_percentile_rtt, city_tenth_percentile_rtt,
   city_median_throughput, city_ninetyth_percentile_throughput,
   forward_updated_node_details, forward_distance, reverse_distance,
   both_way_distance, reverse_updated_node_details,
   is_virtual, is_reaching_dst_asn, partition_date,
   reverse_unresponsive_within_AS, forward_unresponse_within_AS,
   forward_loop, reverse_loop,
   anomaly_ratio_rtt, anomaly_rtt_count,
   anomaly_ratio_throughput, anomaly_throughput_count,
   anomaly_ratio_upload_throughput, anomaly_upload_throughput_count,
   anomaly_loss_ratio,
   difference_latency, difference_throughput, difference_upload_throughput,
   median_upload_throughput,
   wasserstein_throughput_result, wasserstein_upload_throughput_result,
   mann_whitney_latency, mann_whitney_throughput, mann_whitney_upload_throughput,
   t_test_latency)
SELECT
  id, dst, src, ndt_rtt, ndt_throughput, ndt_loss_rate, traceroute_rtt,
  total_windows, is_consistent, src_city, src_lat, src_state, src_lon,
  dst_lat, dst_lon, src_asn, dst_site, dst_city, dst_asn, dst_country,
  src_country, src_asn_name, client_name, start, window_start, reach_dest,
  number_of_measurements_baseline, number_of_unique_src_ips_baseline,
  unique_ip_count_per_site, measurement_count_per_site,
  baseline_median_rtt, baseline_median_throughput,
  baseline_median_upload_throughput, baseline_median_loss, ip_version,
  revtr_system_label, revtr_stop_reason, revtr_fail_reason,
  is_try_from_destination_AS, revtr_id,
  city_median_rtt, city_ninetyth_percentile_rtt,
  city_oneth_percentile_rtt, city_tenth_percentile_rtt,
  city_median_throughput, city_ninetyth_percentile_throughput,
  forward_updated_node_details, forward_distance, reverse_distance,
  both_way_distance, reverse_updated_node_details,
  is_virtual, is_reaching_dst_asn, partition_date,
  reverse_unresponsive_within_AS, forward_unresponse_within_AS,
  forward_loop, reverse_loop,
  anomaly_ratio_rtt, anomaly_rtt_count,
  anomaly_ratio_throughput, anomaly_throughput_count,
  anomaly_ratio_upload_throughput, anomaly_upload_throughput_count,
  anomaly_loss_ratio,
  difference_latency, difference_throughput, difference_upload_throughput,
  median_upload_throughput,
  wasserstein_throughput_result, wasserstein_upload_throughput_result,
  mann_whitney_latency, mann_whitney_throughput, mann_whitney_upload_throughput,
  t_test_latency
FROM _mapping_result
WHERE client_name = 'giga-meter'
   OR src IN (
        SELECT ip_address
        FROM `mlab-collaboration.hermes_union.giga_school_ips`
        WHERE month_start = DATE_TRUNC(CAST('${DAY}' AS DATE), MONTH)
      );

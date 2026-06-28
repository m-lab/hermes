-- Comment out the following line to create the table
-- CREATE OR REPLACE TABLE `mlab-collaboration.hermes.transient_events_upd_with_as_and_geoloc_union`
-- PARTITION BY partition_date  -- This defines partitioning by the 'partition_date' column
-- AS

INSERT INTO `mlab-collaboration.hermes.transient_events_upd_with_as_and_geoloc_union`

WITH
  -- Closest metadata AS with respect to time
  closest_metadata AS (
    SELECT *
      FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY asn ORDER BY ABS(DATE_DIFF(partition_date, DATE '${DAY}', DAY))) AS rn
        FROM `mlab-collaboration.hermes.as_metadata`
      )
      WHERE rn = 1
  ),
  closest_rdns_entry AS (
      SELECT *
      FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY ip_address ORDER BY ABS(DATE_DIFF(partition_date, DATE '${DAY}', DAY))) AS rn
        FROM `mlab-collaboration.hermes.unified_ip_to_rdns`
      )
      WHERE rn = 1
  ),
  closest_geo_entry AS (
      SELECT *
      FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY ip_address ORDER BY ABS(DATE_DIFF(partition_date, DATE '${DAY}', DAY))) AS rn
        FROM `mlab-collaboration.hermes.unified_ip_to_geoloc`
      )
      WHERE rn = 1
  ),
  -- First CTE: Extract data from IP to AS mapping table
  unified_data AS (
    SELECT
      ip_prefix,
      asn,
      NET.IP_FROM_STRING(REGEXP_EXTRACT(ip_prefix, r'(.*)/')) AS network_bin,
      CAST(REGEXP_EXTRACT(ip_prefix, r'/(.*)') AS INT64) AS mask,
      IFNULL(ixp, 'None') AS ixp
    FROM
      `mlab-collaboration.hermes.unified_ip_to_as`
  ),
  -- Second CTE: Extract data from hopannotation2 with logic for prefixes
  ixp_ranked AS (
      SELECT
        ipv4,
        asn,
        name,
        partition_date,
        -- Compute absolute difference between partition_date and '${DAY}'
        ABS(DATE_DIFF(partition_date, DATE('${DAY}'), DAY)) AS date_diff,
        -- Rank partition_date per ipv4 based on closest date
        ROW_NUMBER() OVER (
          PARTITION BY ipv4
          ORDER BY ABS(DATE_DIFF(partition_date, DATE('${DAY}'), DAY)) ASC
        ) AS rank
      FROM `mlab-collaboration.ix_data.ixp_members`
    ),
  extracted_data AS (
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

        IFNULL(ix.name, 'None') AS ixp

      FROM `measurement-lab.ndt_raw.hopannotation2` hop
      LEFT JOIN (
        -- Select only the closest partition_date per ipv4
        SELECT ipv4, asn, name, partition_date
        FROM ixp_ranked
        WHERE rank = 1
      ) ix
      ON ix.ipv4 = REGEXP_EXTRACT(hop.id, r'_([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)$')

      WHERE date >= '2025-03-01'
  ),
  -- Third CTE: Join the two tables and identify the source of the mapping
  extracted_prefixes AS (
    SELECT
      COALESCE(ed.ip_prefix, ud.ip_prefix) AS ip_prefix,
      COALESCE(ed.asn, ud.asn) AS asn,
      COALESCE(ed.network_bin, ud.network_bin) AS network_bin,
      COALESCE(ed.mask, ud.mask) AS mask,
      IFNULL(COALESCE(ed.ixp, ud.ixp), 'None') AS ixp,
      -- Identify the source of the mapping
      CASE
        WHEN ud.ip_prefix IS NOT NULL THEN 'hop_annotations'
        ELSE 'unified_ip_to_as'
        END
        AS mapping_source
    FROM
      extracted_data ed
    FULL OUTER JOIN
      unified_data ud
      ON
        ed.network_bin = ud.network_bin
        AND ed.mask = ud.mask
  ),
  -- Fourth CTE: Extract data from reverse_paths
  reverse_flattened_node_details AS (
    SELECT
      t.id,
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
    FROM
      `mlab-collaboration.hermes.transient_events_union` t,
      UNNEST(t.reverse_node_details) AS reverse_node
    LEFT JOIN closest_rdns_entry rdns
      ON rdns.ip_address = reverse_node.hop_ip
    WHERE t.partition_date = '${DAY}'
  ),
  -- Fifth CTE: Extract data from forward_paths
  flattened_node_details AS (
    SELECT
      t.id,
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
    FROM
      `mlab-collaboration.hermes.transient_events_union` t,
      UNNEST(t.node_details) AS node
    LEFT JOIN `hermes.site_to_state` s2s
      ON t.dst_site = s2s.dst_site
    WHERE t.partition_date = '${DAY}'
  ),
  -- Sixth CTE: Extract source IP addresses
  source_ip_address AS (
    SELECT DISTINCT node.addr AS ip,
    FROM
      `mlab-collaboration.hermes.transient_events_union` t,
      UNNEST(t.node_details) AS node
    WHERE t.partition_date = '${DAY}'
  ),
  -- Seventh CTE: Perform masking on source IP addresses to find the most specific match
  masked_ip_addresses AS (
    SELECT
      ip,
      asn,
      ixp,
      mask
    FROM
      (
        SELECT
          *,
          ROW_NUMBER() OVER (PARTITION BY ip ORDER BY mask DESC) AS rn  -- prioritize longest match
        FROM
          (
            SELECT *, NET.SAFE_IP_FROM_STRING(ip) & NET.IP_NET_MASK(4, mask) AS network_bin
            FROM source_ip_address, UNNEST(GENERATE_ARRAY(8, 32)) mask
            WHERE BYTE_LENGTH(NET.SAFE_IP_FROM_STRING(ip)) = 4
          )
        JOIN extracted_prefixes
          USING (network_bin, mask)
      )
    WHERE rn = 1  -- keeps the most specific match
  ),
  -- Eighth CTE: Join the forward paths with the masked IP addresses
  node_with_prefix_matches AS (
    SELECT DISTINCT
      nd.id,
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
      (
        (NET.IP_NET_MASK(4, 8)  & NET.SAFE_IP_FROM_STRING(nd.addr)) = NET.SAFE_IP_FROM_STRING('10.0.0.0')
        OR (NET.IP_NET_MASK(4,12) & NET.SAFE_IP_FROM_STRING(nd.addr)) = NET.SAFE_IP_FROM_STRING('172.16.0.0')
        OR (NET.IP_NET_MASK(4,16) & NET.SAFE_IP_FROM_STRING(nd.addr)) = NET.SAFE_IP_FROM_STRING('192.168.0.0')
        OR (NET.IP_NET_MASK(4,10) & NET.SAFE_IP_FROM_STRING(nd.addr)) = NET.SAFE_IP_FROM_STRING('100.64.0.0')
      ) AS is_private,
    CASE
      WHEN
        NET.IP_NET_MASK(4, 8)  & NET.SAFE_IP_FROM_STRING(nd.addr) = NET.SAFE_IP_FROM_STRING('10.0.0.0') THEN NULL
      WHEN
        NET.IP_NET_MASK(4, 12) & NET.SAFE_IP_FROM_STRING(nd.addr) = NET.SAFE_IP_FROM_STRING('172.16.0.0') THEN NULL
      WHEN
        NET.IP_NET_MASK(4, 16) & NET.SAFE_IP_FROM_STRING(nd.addr) = NET.SAFE_IP_FROM_STRING('192.168.0.0') THEN NULL
      WHEN
        NET.IP_NET_MASK(4, 10) & NET.SAFE_IP_FROM_STRING(nd.addr) = NET.SAFE_IP_FROM_STRING('100.64.0.0') THEN NULL
      ELSE mp.asn
    END AS associated_asn,
    mp.ixp AS associated_ixp
    FROM
      flattened_node_details nd
    LEFT JOIN
      masked_ip_addresses mp
      ON
        nd.addr = mp.ip
  ),
  -- Ninth CTE: Extract reverse IP addresses
  reverse_source_ip_address AS (
    SELECT DISTINCT node.hop_ip AS ip,
    FROM
      `mlab-collaboration.hermes.transient_events_union` t,
      UNNEST(t.reverse_node_details) AS node
    WHERE t.partition_date = '${DAY}'
  ),
  -- Tenth CTE: Perform masking on reverse IP addresses to find the most specific match
  reverse_masked_ip_addresses AS (
    SELECT
      ip,
      asn,
      ixp,
      mask
    FROM
      (
        SELECT
          *,
          ROW_NUMBER() OVER (PARTITION BY ip ORDER BY mask DESC) AS rn  -- prioritize longest match
        FROM
          (
            SELECT *, NET.SAFE_IP_FROM_STRING(ip) & NET.IP_NET_MASK(4, mask) AS network_bin
            FROM reverse_source_ip_address, UNNEST(GENERATE_ARRAY(8, 32)) mask
            WHERE BYTE_LENGTH(NET.SAFE_IP_FROM_STRING(ip)) = 4
          )
        JOIN extracted_prefixes
          USING (network_bin, mask)
      )
    WHERE rn = 1  -- keeps the most specific match
  ),
  -- Eleventh CTE: Join the reverse paths with the masked IP addresses
  reverse_node_with_prefix_matches AS (
    SELECT DISTINCT
      nd.id,
      nd.dst_lat,
      nd.dst_lon,
      nd.src_lat,
      nd.src_lon,
      nd.baseline_rtt,
      nd.ttl,
      nd.addr,
      nd.rdns_name,
      nd.rtts,
     (
        (NET.IP_NET_MASK(4, 8)  & NET.SAFE_IP_FROM_STRING(nd.addr)) = NET.SAFE_IP_FROM_STRING('10.0.0.0')
        OR (NET.IP_NET_MASK(4,12) & NET.SAFE_IP_FROM_STRING(nd.addr)) = NET.SAFE_IP_FROM_STRING('172.16.0.0')
        OR (NET.IP_NET_MASK(4,16) & NET.SAFE_IP_FROM_STRING(nd.addr)) = NET.SAFE_IP_FROM_STRING('192.168.0.0')
        OR (NET.IP_NET_MASK(4,10) & NET.SAFE_IP_FROM_STRING(nd.addr)) = NET.SAFE_IP_FROM_STRING('100.64.0.0')
      ) AS is_private,
      CASE
        WHEN
          NET.IP_NET_MASK(4, 8)  & NET.SAFE_IP_FROM_STRING(nd.addr) = NET.SAFE_IP_FROM_STRING('10.0.0.0') THEN NULL
        WHEN
          NET.IP_NET_MASK(4, 12) & NET.SAFE_IP_FROM_STRING(nd.addr) = NET.SAFE_IP_FROM_STRING('172.16.0.0') THEN NULL
        WHEN
          NET.IP_NET_MASK(4, 16) & NET.SAFE_IP_FROM_STRING(nd.addr) = NET.SAFE_IP_FROM_STRING('192.168.0.0') THEN NULL
        WHEN
          NET.IP_NET_MASK(4, 10) & NET.SAFE_IP_FROM_STRING(nd.addr) = NET.SAFE_IP_FROM_STRING('100.64.0.0') THEN NULL
        ELSE mp.asn
      END AS associated_asn,
      mp.ixp AS associated_ixp,
      nd.hop_type
    FROM
      reverse_flattened_node_details nd
    LEFT JOIN
      reverse_masked_ip_addresses AS mp
      ON
        nd.addr = mp.ip
  ),
  -- Twelfth CTE: Join the forward and reverse paths with geolocation data
  node_with_geo_info AS (
    SELECT DISTINCT
      nwp.id,
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
      CASE WHEN nwp.is_private THEN NULL
           WHEN nwp.ttl = 1 THEN nwp.dst_lat
           ELSE COALESCE(geo.lat, ip_geo.lat_ip_info, ip_geo.lat)
      END AS latitude,
      CASE WHEN nwp.is_private THEN NULL
           WHEN nwp.ttl = 1 THEN nwp.dst_lon
           ELSE COALESCE(geo.lon, ip_geo.lon_ip_info, ip_geo.lon)
      END AS longitude,
      CASE WHEN nwp.is_private THEN NULL
           WHEN nwp.ttl = 1 THEN CONCAT(nwp.dst_city, '-', nwp.dst_state, '-', nwp.dst_country)
           ELSE COALESCE(geo.place, ip_geo.city_ip_info, ip_geo.city)
      END AS place,
      CASE WHEN nwp.is_private THEN NULL ELSE geo.clli END AS clli,
      CASE WHEN nwp.is_private THEN NULL ELSE ip_geo.metro END AS metro,
      CASE WHEN nwp.is_private THEN NULL
           WHEN nwp.ttl = 1 THEN nwp.dst_country
           ELSE COALESCE(geo.cc, ip_geo.country_ip_info, ip_geo.country)
      END AS cc,
      CASE WHEN nwp.is_private THEN -1 ELSE COALESCE(ip_geo.score, -1) END AS score,
    FROM
      node_with_prefix_matches nwp
    LEFT JOIN `mlab-collaboration.hermes.geolocation` geo
      ON REGEXP_REPLACE(nwp.rdns_name, r'\.$', '') = REGEXP_REPLACE(geo.hostname, r'\.$', '')
    LEFT JOIN closest_geo_entry ip_geo
      ON nwp.addr = ip_geo.ip_address
  ),
  -- Thirteenth CTE: Join the reverse paths with geolocation data
  reverse_node_with_geo_info AS (
    SELECT DISTINCT
      nwp.id,
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
      CASE WHEN nwp.is_private THEN NULL ELSE COALESCE(geo.lat, ip_geo.lat_ip_info, ip_geo.lat) END AS latitude,
      CASE WHEN nwp.is_private THEN NULL ELSE COALESCE(geo.lon, ip_geo.lon_ip_info,  ip_geo.lon) END AS longitude,
      CASE WHEN nwp.is_private THEN NULL ELSE COALESCE(geo.place, ip_geo.city_ip_info, ip_geo.city) END AS place,
      CASE WHEN nwp.is_private THEN NULL ELSE geo.clli END AS clli,
      CASE WHEN nwp.is_private THEN NULL ELSE COALESCE(geo.metro, ip_geo.metro) END AS metro,
      CASE WHEN nwp.is_private THEN NULL ELSE COALESCE(geo.cc, ip_geo.country_ip_info, ip_geo.country) END AS cc,
      CASE WHEN nwp.is_private THEN -1   ELSE COALESCE(ip_geo.score, -1) END AS score,
      nwp.hop_type
    FROM
      reverse_node_with_prefix_matches nwp
    LEFT JOIN `mlab-collaboration.hermes.geolocation` geo
      ON REGEXP_REPLACE(nwp.rdns_name, r'\.$', '') = REGEXP_REPLACE(geo.hostname, r'\.$', '')
    LEFT JOIN closest_geo_entry ip_geo
      ON nwp.addr = ip_geo.ip_address
  ),
  -- Thirteenth CTE: Reconstruct the node details
  reconstructed_node_details AS (
    SELECT
      id,
      dst_lat,
      dst_lon,
      src_lat,
      src_lon,
      AVG(baseline_rtt) AS baseline_rtt,
      ARRAY_AGG(
        STRUCT(
          ttl,
          addr,
          rdns_name,
          rtts,
          associated_asn,
          associated_ixp,
          latitude,
          longitude,
          place,
          clli,
          metro,
          cc,
          score)
        ORDER BY ttl) AS updated_node_details
    FROM
      node_with_geo_info
    GROUP BY
      id, dst_lat, dst_lon, src_lat, src_lon
  ),
  -- Thirteenth CTE: Reconstruct the reverse node details but things are a bit more complicated here
  reverse_reconstructed_node_details_without_flag AS (
    SELECT
      id,
      dst_lat,
      dst_lon,
      src_lat,
      src_lon,
      AVG(baseline_rtt) AS baseline_rtt,
      ARRAY_AGG(
        STRUCT(
          ttl,
          addr,
          rdns_name,
          rtts / 1000 AS rtts,  -- RTTs in seconds
          associated_asn,
          associated_ixp,
          latitude,
          longitude,
          place,
          clli,
          metro,
          cc,
          score,
          hop_type)
        ORDER BY ttl) AS updated_node_details
    FROM
      reverse_node_with_geo_info
    GROUP BY
      id, dst_lat, dst_lon, src_lat, src_lon
  ),
  reverse_valid_coords AS (
    SELECT
      id,
      dst_lat,
      dst_lon,
      src_lat,
      src_lon,
      baseline_rtt,
      current_node.ttl,
      current_node.addr,
      current_node.rdns_name,
      current_node.rtts,
      current_node.associated_asn,
      current_node.associated_ixp,
      current_node.latitude,
      current_node.longitude,
      current_node.place,
      current_node.clli,
      current_node.metro,
      current_node.cc,
      current_node.score,
      current_node.hop_type,
      current_node.ttl AS original_ttl
    FROM
      reverse_reconstructed_node_details_without_flag,
      UNNEST(updated_node_details) AS current_node WITH OFFSET AS idx
  ),
  reverse_filled_coords AS (
    SELECT
      id,
      src_lat,
      src_lon,
      dst_lat,
      dst_lon,
      baseline_rtt,
      ttl,
      addr,
      rdns_name,
      rtts,
      associated_asn,
      associated_ixp,
      latitude,
      longitude,
      place,
      clli,
      metro,
      cc,
      score,
      hop_type,
      -- Forward-fill the last valid latitude in reverse order
      LAST_VALUE(latitude IGNORE NULLS)
        OVER (
          PARTITION BY id
          ORDER BY ttl DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS filled_latitude,
      -- Forward-fill the last valid longitude in reverse order
      LAST_VALUE(longitude IGNORE NULLS)
        OVER (
          PARTITION BY id
          ORDER BY ttl DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS filled_longitude
    FROM
      reverse_valid_coords
  ),
  reverse_distances AS (
    SELECT
      id,
      dst_lat,
      dst_lon,
      src_lat,
      src_lon,
      baseline_rtt,
      ttl,
      addr,
      rdns_name,
      rtts,
      associated_asn,
      associated_ixp,
      latitude,
      longitude,
      place,
      clli,
      metro,
      cc,
      filled_latitude,
      filled_longitude,
      score,
      hop_type,
      -- Use LAG to get previous coordinates in descending TTL order
      LAG(filled_latitude) OVER (PARTITION BY id ORDER BY ttl DESC) AS prev_latitude,
      LAG(filled_longitude) OVER (PARTITION BY id ORDER BY ttl DESC) AS prev_longitude,
      CASE
        WHEN
          LAG(filled_latitude) OVER (PARTITION BY id ORDER BY ttl DESC) IS NOT NULL
          AND LAG(filled_longitude) OVER (PARTITION BY id ORDER BY ttl DESC) IS NOT NULL
          AND filled_latitude IS NOT NULL
          AND filled_longitude IS NOT NULL
          THEN
            ST_DISTANCE(
              ST_GEOGPOINT(filled_longitude, filled_latitude),
              ST_GEOGPOINT(
                LAG(filled_longitude) OVER (PARTITION BY id ORDER BY ttl DESC),
                LAG(filled_latitude) OVER (PARTITION BY id ORDER BY ttl DESC)))
            / 1000  -- Convert meters to kilometers
        ELSE 0
        END AS hop_distance_km
    FROM
      reverse_filled_coords
  ),
  reverse_cumulative_calculations AS (
    SELECT
      id,
      dst_lat,
      dst_lon,
      baseline_rtt,
      ttl,
      addr,
      rdns_name,
      rtts,
      associated_asn,
      associated_ixp,
      latitude,
      longitude,
      place,
      clli,
      metro,
      cc,
      score,
      hop_type,
      prev_latitude,
      prev_longitude,
      hop_distance_km,
      -- Calculate cumulative distance from the last TTL to the current one
      SUM(hop_distance_km)
        OVER (
          PARTITION BY id
          ORDER BY ttl DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_distance_km
    FROM
      reverse_distances
  ),
  -- Calculate the distance checks
  reverse_distance_rtt_checks AS (
    SELECT
      id,
      ttl,
      addr,
      rdns_name,
      rtts,
      associated_asn,
      associated_ixp,
      latitude,
      longitude,
      place,
      clli,
      metro,
      cc,
      score,
      hop_type,
      cumulative_distance_km,
      CASE
        WHEN latitude IS NULL OR longitude IS NULL OR dst_lat IS NULL OR dst_lon IS NULL THEN NULL
        ELSE
          ST_DISTANCE(
            ST_GEOGPOINT(longitude, latitude),
            ST_GEOGPOINT(dst_lon, dst_lat))
          / 1000
        END AS distance_to_destination_km,
      CASE
        WHEN
          cumulative_distance_km IS NULL
          OR latitude IS NULL
          OR longitude IS NULL
          OR dst_lat IS NULL
          OR dst_lon IS NULL
          THEN NULL
        ELSE
          (cumulative_distance_km / 200)
          + (
            ST_DISTANCE(
              ST_GEOGPOINT(longitude, latitude),
              ST_GEOGPOINT(dst_lon, dst_lat))
            / (200 * 1000))
        END AS speed_of_internet_fiber,
      CASE
        WHEN
          cumulative_distance_km IS NULL
          OR latitude IS NULL
          OR longitude IS NULL
          OR dst_lat IS NULL
          OR dst_lon IS NULL
          THEN NULL
        WHEN
          (
            cumulative_distance_km / 200
            + ST_DISTANCE(
              ST_GEOGPOINT(longitude, latitude),
              ST_GEOGPOINT(dst_lon, dst_lat))
              / (200 * 1000))
          > rtts
          THEN 'Above threshold'
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
    FROM
      reverse_cumulative_calculations
  ),

  -- Add the AS Meta Info
  reverse_distance_rtt_checks_with_metas_info AS (
    SELECT DISTINCT
      id,
      ttl,
      addr,
      rdns_name,
      rtts,
      associated_asn,
      associated_ixp,
      rd.latitude,
      rd.longitude,
      place,
      clli,
      metro,
      cc,
      score,
      cumulative_distance_km,
      distance_to_destination_km,
      speed_of_internet_fiber,
      distance_rtt_check,
      above_baseline_flag,
      increasing_latency_flag,
      CASE
        WHEN
          COUNT(CASE WHEN above_baseline_flag = 'Within baseline' THEN 1 END)
            OVER (PARTITION BY id ORDER BY ttl ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
          > 0
          THEN 'False'
        WHEN
          COUNT(CASE WHEN increasing_latency_flag = 'Stable/Decreasing' THEN 1 END)
            OVER (PARTITION BY id ORDER BY ttl ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
          > 0
          THEN 'False'
        ELSE 'True'
        END AS baseline_consistency_flag,
      hop_type,
      as_metadata.organization.OrgName AS associated_org,
      as_metadata.PeeringDB_name AS associated_peeringdb_name,
        -- Closest facility info
            ARRAY_AGG(STRUCT(
      sub.facility_name AS facility_name,
      sub.facility_city AS facility_city,
      sub.distance_km
    )) AS facilities_info
  FROM reverse_distance_rtt_checks rd
  LEFT JOIN mlab-collaboration.hermes.asn_facility_matched sub
    ON rd.associated_asn = sub.asn AND rd.place = sub.city
  LEFT JOIN closest_metadata as_metadata
    ON CAST(as_metadata.asn AS INT64) = CAST(rd.associated_asn AS INT64)
  GROUP BY
    id, ttl, addr, rdns_name, rtts, rd.associated_asn, rd.associated_ixp,
    rd.latitude, rd.longitude, rd.place, clli, metro, cc, score, cumulative_distance_km,
    distance_to_destination_km, speed_of_internet_fiber, distance_rtt_check,
    above_baseline_flag, increasing_latency_flag, as_metadata.organization.OrgName,
    as_metadata.PeeringDB_name, hop_type
  ),
  with_speed_of_light_reverse_node_details AS (
    SELECT
      id,
      ARRAY_AGG(
        STRUCT(
          ttl,
          addr,
          rdns_name,
          rtts,
          associated_asn,
          associated_ixp,
          associated_org,
          associated_peeringdb_name,
          latitude,
          longitude,
          place,
          clli,
          cc,
          score,
          metro,
          hop_type,
          facilities_info,
          cumulative_distance_km,
          distance_to_destination_km,
          speed_of_internet_fiber,
          distance_rtt_check,
          above_baseline_flag,
          increasing_latency_flag
          )
        ORDER BY ttl) AS updated_node_details
    FROM
      reverse_distance_rtt_checks_with_metas_info
    GROUP BY
      id
  ),
  -- Identify hops with hop_type 2 with different associated_asn
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
    -- First non-null ASN before
          (
            SELECT hop.associated_asn
            FROM UNNEST(updated_node_details) AS hop
            WHERE hop.ttl < node.ttl AND hop.associated_asn IS NOT NULL
            ORDER BY hop.ttl DESC
            LIMIT 1
          ) AS prev_asn,

          -- First non-null ASN after
          (
            SELECT hop.associated_asn
            FROM UNNEST(updated_node_details) AS hop
            WHERE hop.ttl > node.ttl AND hop.associated_asn IS NOT NULL
            ORDER BY hop.ttl ASC
            LIMIT 1
          ) AS next_asn,

          -- First non-null Org before
          (
            SELECT hop.associated_org
            FROM UNNEST(updated_node_details) AS hop
            WHERE hop.ttl < node.ttl AND hop.associated_org IS NOT NULL
            ORDER BY hop.ttl DESC
            LIMIT 1
          ) AS prev_org,

          -- First non-null Org after
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

  -- Determine the RTTs of the last node with hop_type != 4
  last_non_type_4 AS (
    SELECT
      id,
      MAX(IF(node.hop_type != 4, node.rtts, NULL)) AS last_rtts
    FROM
      with_speed_of_light_reverse_node_details,
      UNNEST(updated_node_details) AS node
    GROUP BY
      id
  ),

  -- Identify hops with hop_type 4 where the speed is much higher than the latency between some hops
  fishy_type_4 AS (
    SELECT
      n.id,
      node.ttl,
      TRUE AS is_fishy_type_4
    FROM
      with_speed_of_light_reverse_node_details n,
      UNNEST(updated_node_details) AS node
    LEFT JOIN
      last_non_type_4 l
      ON
        n.id = l.id
    WHERE
      node.hop_type = 4
      AND (node.rtts - l.last_rtts) > 50
  ),

  -- Combine results into the final aggregated structure
  reverse_reconstructed_node_details AS (
    SELECT
      wsld.id,
      ARRAY_AGG(
        STRUCT(
          node.ttl,
          node.addr,
          node.rdns_name,
          node.rtts,
          node.associated_asn,
          node.associated_ixp,
          node.associated_org,
          node.associated_peeringdb_name,
          node.latitude,
          node.longitude,
          node.place,
          node.clli,
          node.cc,
--           node.metro,
          node.score,
          node.hop_type,
          node.facilities_info,
          node.cumulative_distance_km,
          node.distance_to_destination_km,
          node.speed_of_internet_fiber,
          node.distance_rtt_check,
          above_baseline_flag,
          increasing_latency_flag,
          COALESCE(isd.is_interdomain_symmetry, FALSE) AS is_interdomain_symmetry,
          COALESCE(ft4.is_fishy_type_4, FALSE) AS is_fishy_type_4)
        ORDER BY node.ttl) AS updated_node_details
    FROM
      with_speed_of_light_reverse_node_details wsld,
      UNNEST(wsld.updated_node_details) AS node
    LEFT JOIN
      interdomain_symmetry isd
      ON
        wsld.id = isd.id
        AND node.ttl = isd.ttl
    LEFT JOIN
      fishy_type_4 ft4
      ON
        wsld.id = ft4.id
        AND node.ttl = ft4.ttl
    GROUP BY
      wsld.id
  ),
  valid_coords AS (
    SELECT
      id,
      dst_lat,
      dst_lon,
      src_lat,
      src_lon,
      baseline_rtt,
      current_node.ttl,
      current_node.addr,
      current_node.rdns_name,
      current_node.rtts,
      current_node.associated_asn,
      current_node.associated_ixp,
      current_node.latitude,
      current_node.longitude,
      current_node.place,
      current_node.clli,
      current_node.metro,
      current_node.cc,
      current_node.score,
      current_node.ttl AS original_ttl
    FROM
      reconstructed_node_details,
      UNNEST(updated_node_details) AS current_node WITH OFFSET AS idx
  ),
  -- Forward-fill the last valid coordinates for each id
  filled_coords AS (
    SELECT
      id,
      dst_lat,
      dst_lon,
      src_lat,
      src_lon,
      baseline_rtt,
      ttl,
      addr,
      rdns_name,
      rtts,
      associated_asn,
      associated_ixp,
      latitude,
      longitude,
      place,
      clli,
      metro,
      score,
      cc,
      -- Forward-fill the last valid latitude
      LAST_VALUE(latitude IGNORE NULLS)
        OVER (
          PARTITION BY id
          ORDER BY ttl
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS filled_latitude,
      -- Forward-fill the last valid longitude
      LAST_VALUE(longitude IGNORE NULLS)
        OVER (
          PARTITION BY id
          ORDER BY ttl
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS filled_longitude
    FROM
      valid_coords
  ),
  distances AS (
    SELECT
      id,
      dst_lat,
      dst_lon,
      src_lat,
      src_lon,
      baseline_rtt,
      ttl,
      addr,
      rdns_name,
      rtts,
      associated_asn,
      associated_ixp,
      latitude,
      longitude,
      place,
      clli,
      metro,
      cc,
      score,
      filled_latitude,
      filled_longitude,
      LAG(filled_latitude) OVER (PARTITION BY id ORDER BY ttl) AS prev_latitude,
      LAG(filled_longitude) OVER (PARTITION BY id ORDER BY ttl) AS prev_longitude,
      CASE
        WHEN
          LAG(filled_latitude) OVER (PARTITION BY id ORDER BY ttl) IS NOT NULL
          AND LAG(filled_longitude) OVER (PARTITION BY id ORDER BY ttl) IS NOT NULL
          AND latitude IS NOT NULL
          AND longitude IS NOT NULL
          THEN
            ST_DISTANCE(
              ST_GEOGPOINT(filled_longitude, filled_latitude),
              ST_GEOGPOINT(
                LAG(filled_longitude) OVER (PARTITION BY id ORDER BY ttl),
                LAG(filled_latitude) OVER (PARTITION BY id ORDER BY ttl)))
            / 1000  -- Convert meters to kilometers
        ELSE 0
        END AS hop_distance_km
    FROM
      filled_coords
  ),
  cumulative_calculations AS (
    SELECT
      id,
      dst_lat,
      dst_lon,
      src_lat,
      src_lon,
      baseline_rtt,
      ttl,
      addr,
      rdns_name,
      rtts,
      associated_asn,
      associated_ixp,
      latitude,
      longitude,
      place,
      clli,
      metro,
      cc,
      score,
      prev_latitude,
      prev_longitude,
      hop_distance_km,
      -- Calculate cumulative distance including valid distances
      SUM(hop_distance_km)
        OVER (
          PARTITION BY id
          ORDER BY ttl
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_distance_km
    FROM
      distances
  ),
  -- Calculate the distance checks
  distance_rtt_checks AS (
    SELECT
      id,
      ttl,
      addr,
      rdns_name,
      rtts,
      associated_asn,
      associated_ixp,
      latitude,
      longitude,
      place,
      clli,
      metro,
      dst_lat,
      dst_lon,
      src_lat,
      src_lon,
      cc,
      score,
      cumulative_distance_km,
      CASE
        WHEN latitude IS NULL OR longitude IS NULL OR src_lat IS NULL OR src_lon IS NULL THEN NULL
        ELSE
          ST_DISTANCE(
            ST_GEOGPOINT(longitude, latitude),
            ST_GEOGPOINT(src_lon, src_lat))
          / 1000
        END AS distance_to_destination_km,
      CASE
        WHEN
          cumulative_distance_km IS NULL
          OR latitude IS NULL
          OR longitude IS NULL
          OR src_lat IS NULL
          OR src_lon IS NULL
          THEN NULL
        ELSE
          (cumulative_distance_km / 200)
          + (
            ST_DISTANCE(
              ST_GEOGPOINT(longitude, latitude),
              ST_GEOGPOINT(dst_lon, dst_lat))
            / (200 * 1000))
        END AS speed_of_internet_fiber,
      CASE
        WHEN
          cumulative_distance_km IS NULL
          OR latitude IS NULL
          OR longitude IS NULL
          OR src_lat IS NULL
          OR src_lon IS NULL
          THEN NULL
        WHEN
          (
            cumulative_distance_km / 200
            + ST_DISTANCE(
              ST_GEOGPOINT(longitude, latitude),
              ST_GEOGPOINT(dst_lon, dst_lat))
              / (200 * 1000))
          > rtts
          THEN 'Above threshold'
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
        END AS increasing_latency_flag,
    FROM
      cumulative_calculations
  ),
distance_rtt_checks_with_metas_info AS (
  SELECT DISTINCT
    id,
    ttl,
    addr,
    rdns_name,
    rtts,
    rd.associated_asn,
    rd.associated_ixp,
    rd.latitude,
    rd.longitude,
    rd.place,
    clli,
    metro,
    cc,
    score,
    dst_lat,
    dst_lon,
    src_lat,
    src_lon,
    cumulative_distance_km,
    distance_to_destination_km,
    speed_of_internet_fiber,
    distance_rtt_check,
    above_baseline_flag,
    increasing_latency_flag,
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
    )) AS facilities_info,
  FROM distance_rtt_checks rd
  LEFT JOIN mlab-collaboration.hermes.asn_facility_matched sub
  ON rd.associated_asn = sub.asn AND rd.place = sub.city
  LEFT JOIN closest_metadata as_metadata
    ON CAST(as_metadata.asn AS INT64) = CAST(rd.associated_asn AS INT64)
  GROUP BY
    id, ttl, addr, rdns_name, rtts, rd.associated_asn, rd.associated_ixp,
    rd.latitude, rd.longitude, rd.place, clli, metro, cc, score, dst_lat,
    dst_lon, src_lat, src_lon, cumulative_distance_km,
    distance_to_destination_km, speed_of_internet_fiber, distance_rtt_check,
    above_baseline_flag, increasing_latency_flag, as_metadata.organization.OrgName,
    as_metadata.PeeringDB_name
),
  -- Final step: Generate the output table with the specified format
  forward_reconstructed_node_details_with_latency AS (
    SELECT
      id,
      AVG(dst_lat) AS dst_lat,
      AVG(dst_lon) AS dst_lon,
      AVG(src_lat) AS src_lat,
      AVG(src_lon) AS src_lon,
      ARRAY_AGG(
        STRUCT(
          ttl,
          addr,
          rdns_name,
          rtts,
          associated_asn,
          associated_org,
          associated_peeringdb_name,
          associated_ixp,
          latitude,
          longitude,
          place,
          clli,
          cc,
          metro,
          score,
          facilities_info,
          cumulative_distance_km,
          distance_to_destination_km,
          speed_of_internet_fiber,
          distance_rtt_check,
          above_baseline_flag,
          increasing_latency_flag,
          baseline_consistency_flag)
        ORDER BY ttl) AS updated_node_details
    FROM
      distance_rtt_checks_with_metas_info
    GROUP BY
      id
  ),
  reverse_path_distances AS (
    SELECT
      rrd.id,
      rrd.updated_node_details,
      (
        SELECT node.cumulative_distance_km
        FROM UNNEST(rrd.updated_node_details) AS node
        ORDER BY node.ttl
        LIMIT 1
      ) AS total_gcd_distance
    FROM
      reverse_reconstructed_node_details rrd
  ),
  forward_path_distances AS (
    SELECT
      rrd.id,
      rrd.updated_node_details,
      (
        SELECT
          LAST_VALUE(node.longitude IGNORE NULLS)
            OVER (ORDER BY node.ttl ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
        FROM UNNEST(rrd.updated_node_details) AS node
        LIMIT 1
      ) AS last_non_empty_lon,
      (
        SELECT
          LAST_VALUE(node.latitude IGNORE NULLS)
            OVER (ORDER BY node.ttl ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
        FROM UNNEST(rrd.updated_node_details) AS node
        LIMIT 1
      ) AS last_non_empty_lat,
      (
        SELECT
          node.cumulative_distance_km +
            -- Calculate the great circle distance from the last non-empty LAT/LON to dst_lat/dst_lon
            ST_DISTANCE(
              ST_GEOGPOINT(
                LAST_VALUE(node.longitude IGNORE NULLS)
                  OVER (ORDER BY node.ttl ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
                LAST_VALUE(node.latitude IGNORE NULLS)
                  OVER (
                    ORDER BY node.ttl ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                  )),
              ST_GEOGPOINT(rrd.src_lon, rrd.src_lat))
            / 1000  -- Convert meters to kilometers
        FROM
          UNNEST(rrd.updated_node_details) AS node
        ORDER BY
          node.ttl DESC
        LIMIT 1
      ) AS total_gcd_distance
    FROM
      forward_reconstructed_node_details_with_latency rrd
  ),
  final_results AS (
    SELECT
      t.* EXCEPT (node_details, reverse_node_details),
      rnd.updated_node_details AS forward_updated_node_details,
      COALESCE(
        rnd.total_gcd_distance,
        ST_DISTANCE(
          ST_GEOGPOINT(src_lon, src_lat),
          ST_GEOGPOINT(dst_lon, dst_lat))
          / 1000) AS forward_distance,
      COALESCE(
        rrnd.total_gcd_distance,
        ST_DISTANCE(
          ST_GEOGPOINT(src_lon, src_lat),
          ST_GEOGPOINT(dst_lon, dst_lat))
          / 1000) AS reverse_distance,
      COALESCE(
        rrnd.total_gcd_distance,
        ST_DISTANCE(
          ST_GEOGPOINT(src_lon, src_lat),
          ST_GEOGPOINT(dst_lon, dst_lat))
          / 1000)
        + COALESCE(
          rnd.total_gcd_distance,
          ST_DISTANCE(
            ST_GEOGPOINT(src_lon, src_lat),
            ST_GEOGPOINT(dst_lon, dst_lat))
            / 1000) AS both_way_distance,
      rrnd.updated_node_details AS reverse_updated_node_details,
      CASE
          WHEN t.id LIKE '%virtual%' THEN TRUE
          ELSE FALSE
      END AS is_virtual,
      EXISTS(
        SELECT 1
        FROM UNNEST(rnd.updated_node_details) AS ud
        WHERE CAST(ud.associated_asn AS INT64) = CAST(t.src_asn AS INT64)
      ) AS is_reaching_dst_asn
    FROM
      `mlab-collaboration.hermes.transient_events` t
    LEFT JOIN
      forward_path_distances rnd
      ON
        t.id = rnd.id
    LEFT JOIN
      reverse_path_distances rrnd
      ON
        t.id = rrnd.id
    WHERE t.partition_date = '${DAY}'
  ),
-- Detect Forward Path AS Loops
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
    -- Count NULL or -1 elements in the path
    ARRAY_LENGTH(
      ARRAY(
        SELECT asn
        FROM UNNEST(forward_asn_path) AS asn
        WHERE asn IS NULL OR asn = -1
      )
    ) AS null_or_negative_one_count,
    -- Loop induced by NULL or -1
    CASE
      WHEN ARRAY_LENGTH(
        ARRAY(
          SELECT asn
          FROM UNNEST(forward_asn_path) AS asn
          WHERE asn IS NULL OR asn = -1
        )
      ) > 0 THEN TRUE
      ELSE FALSE
    END AS loop_induced_by_null_or_negative_one,
    -- Loop induced by another ASN
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
    END AS loop_induced_by_asn
  FROM (
    SELECT
      id,
      ARRAY_AGG(
        IF(associated_asn IS NULL, -1, associated_asn) ORDER BY ttl
      ) AS forward_asn_path
    FROM
      node_with_geo_info
    GROUP BY
      id
  )
),
-- Detect Reverse Path AS Loops
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
    -- Count NULL or -1 elements in the path
    ARRAY_LENGTH(
      ARRAY(
        SELECT asn
        FROM UNNEST(reverse_asn_path) AS asn
        WHERE asn IS NULL OR asn = -1
      )
    ) AS null_or_negative_one_count,
    -- Loop induced by NULL or -1
    CASE
      WHEN ARRAY_LENGTH(
        ARRAY(
          SELECT asn
          FROM UNNEST(reverse_asn_path) AS asn
          WHERE asn IS NULL OR asn = -1
        )
      ) > 0 THEN TRUE
      ELSE FALSE
    END AS loop_induced_by_null_or_negative_one,
    -- Loop induced by another ASN
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
    END AS loop_induced_by_asn
  FROM (
    SELECT
      id,
      ARRAY_AGG(
        IF(associated_asn IS NULL, -1, associated_asn) ORDER BY ttl
      ) AS reverse_asn_path
    FROM
      reverse_node_with_geo_info
    GROUP BY
      id
  )
)
-- Final Results with Loops
SELECT
  fr.*,
  ral.loop_induced_by_null_or_negative_one AS reverse_unresponsive_within_AS,
  fal.loop_induced_by_null_or_negative_one AS forward_unresponse_within_AS,
  fal.loop_induced_by_asn AS forward_loop,
  ral.loop_induced_by_asn AS reverse_loop,
FROM
  final_results fr
LEFT JOIN
  forward_as_loop_detection fal
ON
  fr.id = fal.id
LEFT JOIN
  reverse_as_loop_detection ral
ON
  fr.id = ral.id

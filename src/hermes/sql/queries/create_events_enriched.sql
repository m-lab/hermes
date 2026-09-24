--------------------------------------------------------------------------------
-- Canonical, compatibility-first HERMES interface.
--
-- This view deliberately leaves events_with_as_and_geoloc untouched.  It maps
-- both historical and current rows into one nested schema while the physical
-- pipeline evolves incrementally.
--
-- The view is published in the SAME dataset it reads, so it moves with ${DS}:
-- a staging bootstrap publishes hermes_staging.events_enriched over staging data
-- and can never point at production. It used to take a separate ${PUBLISHED_DS},
-- which defaulted to `hermes` while the source was `hermes_union` -- two datasets
-- to keep in step, and a staging render left ${PUBLISHED_DS} unsubstituted.
--
-- `mlab-collaboration.hermes.events_enriched` is deliberately left in place and
-- is no longer managed here. It still points at the same physical table, so it
-- keeps working while consumers cut over to the hermes_union name; drop it once
-- they have.
--
-- Parameters:
--   ${DS} operational dataset that is both read and published to
--         (hermes_union / hermes_staging)
--------------------------------------------------------------------------------

CREATE OR REPLACE VIEW `mlab-collaboration.${DS}.events_enriched`
OPTIONS (
  description = 'Canonical HERMES measurement, endpoint, performance, and symmetric path interface'
)
AS
WITH legacy_rows AS (
  -- Contain the legacy endpoint vocabulary at this boundary.  Everything after
  -- this CTE uses client/server terminology.
  SELECT
    e.* EXCEPT(src, dst),
    e.src AS client_ip,
    e.dst AS server_ip
  FROM `mlab-collaboration.${DS}.events_with_as_and_geoloc` AS e
),

canonical_hops AS (
  SELECT
    e.*,

    -- Scamper is measured from the M-Lab server to the NDT client.  Keep that
    -- measured direction (and say so explicitly in the path record below) rather
    -- than reversing the hops and implying the RTTs originated at the client.
    ARRAY(
      SELECT AS STRUCT
        hop.ttl,
        hop.addr AS ip,
        hop.rtts AS rtt_ms,
        hop.associated_asn AS asn,
        hop.associated_org AS as_name,
        hop.associated_peeringdb_name AS peeringdb_name,
        hop.associated_ixp AS ixp,
        hop.rdns_name,
        hop.latitude,
        hop.longitude,
        hop.place AS city,
        CAST(NULL AS STRING) AS region,
        hop.metro,
        hop.cc AS country_code,
        hop.clli,
        hop.geo_source,
        hop.score AS geo_score,
        hop.geo_partition_date,
        hop.ixp_partition_date,
        hop.segment_distance_km,
        SUM(hop.segment_distance_km) OVER (
          ORDER BY hop.ttl ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_distance_km,
        hop.distance_to_destination_km AS remaining_distance_km,
        -- The legacy speed_of_internet_fiber value is a lower-bound RTT in ms,
        -- not a speed.  Preserve it under a truthful name and expose the 200,000
        -- km/s propagation assumption separately.
        IF(hop.speed_of_internet_fiber IS NULL, NULL, 200000.0)
          AS propagation_speed_km_s,
        hop.speed_of_internet_fiber AS fiber_lower_bound_rtt_ms,
        hop.distance_rtt_check,
        hop.above_baseline_flag,
        hop.increasing_latency_flag,
        hop.baseline_consistency_flag,
        hop.facilities_info AS facilities
      FROM (
        SELECT
          raw_hop.*,
          COALESCE(ABS(
            raw_hop.cumulative_distance_km
            - LAG(raw_hop.cumulative_distance_km) OVER (ORDER BY raw_hop.ttl)
          ), 0.0) AS segment_distance_km
        FROM UNNEST(e.forward_updated_node_details) AS raw_hop
      ) AS hop
      ORDER BY hop.ttl
    ) AS forward_hops,

    -- RevTr is measured from the client back to the M-Lab server.  The physical
    -- table accumulated this distance in descending TTL order; recompute it in
    -- measured (ascending TTL) order so canonical cumulative distance always
    -- increases along the exposed path.
    ARRAY(
      SELECT AS STRUCT
        hop.ttl,
        hop.addr AS ip,
        hop.rtts AS rtt_ms,
        hop.associated_asn AS asn,
        hop.associated_org AS as_name,
        hop.associated_peeringdb_name AS peeringdb_name,
        hop.associated_ixp AS ixp,
        hop.rdns_name,
        hop.latitude,
        hop.longitude,
        hop.place AS city,
        CAST(NULL AS STRING) AS region,
        hop.metro,
        hop.cc AS country_code,
        hop.clli,
        hop.geo_source,
        hop.score AS geo_score,
        hop.geo_partition_date,
        hop.ixp_partition_date,
        hop.segment_distance_km,
        SUM(hop.segment_distance_km) OVER (
          ORDER BY hop.ttl ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_distance_km,
        hop.distance_to_destination_km AS remaining_distance_km,
        IF(hop.speed_of_internet_fiber IS NULL, NULL, 200000.0)
          AS propagation_speed_km_s,
        hop.speed_of_internet_fiber AS fiber_lower_bound_rtt_ms,
        hop.distance_rtt_check,
        hop.above_baseline_flag,
        hop.increasing_latency_flag,
        hop.hop_type AS revtr_hop_type,
        hop.is_interdomain_symmetry AS uses_interdomain_symmetry,
        hop.is_fishy_type_4,
        hop.facilities_info AS facilities
      FROM (
        SELECT
          raw_hop.*,
          COALESCE(ABS(
            raw_hop.cumulative_distance_km
            - LAG(raw_hop.cumulative_distance_km) OVER (ORDER BY raw_hop.ttl)
          ), 0.0) AS segment_distance_km
        FROM UNNEST(e.reverse_updated_node_details) AS raw_hop
      ) AS hop
      ORDER BY hop.ttl
    ) AS reverse_hops,

    CASE
      WHEN e.src_lat IS NULL OR e.src_lon IS NULL
        OR e.dst_lat IS NULL OR e.dst_lon IS NULL
      THEN NULL
      ELSE ST_DISTANCE(
        ST_GEOGPOINT(e.src_lon, e.src_lat),
        ST_GEOGPOINT(e.dst_lon, e.dst_lat)
      ) / 1000
    END AS endpoint_geodesic_distance_km
  FROM legacy_rows AS e
),

path_summaries AS (
  SELECT
    c.*,
    ARRAY_LENGTH(c.forward_hops) AS forward_total_hop_count,
    (SELECT COUNTIF(h.ip IS NOT NULL AND h.ip != '*') FROM UNNEST(c.forward_hops) h)
      AS forward_responsive_hop_count,
    (SELECT COUNTIF(h.latitude IS NOT NULL AND h.longitude IS NOT NULL)
     FROM UNNEST(c.forward_hops) h) AS forward_geolocated_hop_count,
    ARRAY_LENGTH(c.reverse_hops) AS reverse_total_hop_count,
    (SELECT COUNTIF(h.ip IS NOT NULL AND h.ip != '*') FROM UNNEST(c.reverse_hops) h)
      AS reverse_responsive_hop_count,
    (SELECT COUNTIF(h.latitude IS NOT NULL AND h.longitude IS NOT NULL)
     FROM UNNEST(c.reverse_hops) h) AS reverse_geolocated_hop_count
  FROM canonical_hops AS c
)

SELECT
  id AS measurement_id,
  TIMESTAMP_SECONDS(start) AS measurement_time,
  partition_date,
  ip_version,

  STRUCT(
    client_ip AS ip,
    src_asn AS asn,
    src_asn_name AS as_name,
    src_city AS city,
    src_state AS region,
    src_metro AS metro,
    src_country AS country_code,
    src_lat AS latitude,
    src_lon AS longitude,
    -- Rows produced before provenance columns were introduced used the legacy
    -- MaxMind-city grouping.  Normalize that known historical fact in the view
    -- rather than rewriting the 25-TB physical table.
    CASE
      WHEN detection_granularity IS NULL OR detection_granularity = 'maxmind_city'
      THEN 'city'
      ELSE detection_granularity
    END AS grouping_granularity,
    COALESCE(src_group_label, src_city) AS group_label,
    COALESCE(
      client_geo_source,
      IF(
        detection_granularity IS NULL OR detection_granularity = 'maxmind_city',
        'maxmind',
        'ipinfo'
      )
    ) AS geo_source,
    client_name
  ) AS client,

  STRUCT(
    server_ip AS ip,
    dst_asn AS asn,
    dst_site AS site,
    dst_city AS city,
    CAST(NULL AS STRING) AS region,
    (
      SELECT h.metro
      FROM UNNEST(forward_hops) h
      WHERE h.ip = server_ip AND h.metro IS NOT NULL
      LIMIT 1
    ) AS metro,
    dst_country AS country_code,
    dst_lat AS latitude,
    dst_lon AS longitude,
    'server_metadata' AS geo_source
  ) AS server,

  STRUCT(
    ndt_rtt AS ndt_rtt_ms,
    ndt_throughput AS download_mbps,
    median_upload_throughput AS upload_mbps,
    ndt_loss_rate AS loss_rate,
    traceroute_rtt AS traceroute_rtt_ms,
    STRUCT(
      baseline_median_rtt AS ndt_rtt_ms,
      baseline_median_throughput AS download_mbps,
      baseline_median_upload_throughput AS upload_mbps,
      baseline_median_loss AS loss_rate,
      number_of_measurements_baseline AS measurement_count,
      number_of_unique_src_ips_baseline AS unique_client_ip_count
    ) AS baseline,
    -- Two DIFFERENT kinds of evidence; do not conflate them.
    --
    -- *_anomalous_sample_fraction: of the individual measurements in this
    --   window, what fraction was worse than baseline. Continuous 0..1.
    --   (RTT: samples above baseline_median_rtt + 5ms. Download/upload: samples
    --   below the baseline median.) Computed in 02 as a SAFE_DIVIDE of array
    --   lengths.
    --
    -- *_significant: did the GROUP pass its statistical gate. RTT requires
    --   (t-test OR Mann-Whitney) p < 0.05 AND median >= baseline + 5ms;
    --   download/upload require t-test AND Mann-Whitney AND Wasserstein all
    --   p < 0.05 AND a >= 20% median regression.
    --
    -- The underlying anomaly_*_count columns are NOT measurement counts. Each is
    -- SUM() of a per-group 0/1 verdict flag over a group that normally holds one
    -- row, which is why every predicate in this repo reads `>= 0.5` -- that is
    -- "the flag is set", not a fraction threshold. Exposing them as BOOL here
    -- states the contract those predicates were already assuming.
    --
    -- `>= 0.5` rather than `= 1` on purpose: the value is not strictly 0/1.
    -- StatisticalTestsResults is grouped on 13 keys but joined on 4, so a group
    -- whose extra keys vary gets duplicate rows and the SUM can exceed 1 (2 was
    -- observed on 2026-09-22, in 2 of ~147,602 groups). See
    -- docs/reference/anomaly-fields.md.
    STRUCT(
      anomaly_ratio_rtt AS rtt_anomalous_sample_fraction,
      anomaly_rtt_count >= 0.5 AS rtt_significant,
      anomaly_ratio_throughput AS download_anomalous_sample_fraction,
      anomaly_throughput_count >= 0.5 AS download_significant,
      anomaly_ratio_upload_throughput AS upload_anomalous_sample_fraction,
      anomaly_upload_throughput_count >= 0.5 AS upload_significant,
      anomaly_loss_ratio AS loss_ratio,
      difference_latency AS rtt_difference_ms,
      difference_throughput AS download_difference_mbps,
      difference_upload_throughput AS upload_difference_mbps
    ) AS anomaly
  ) AS performance,

  STRUCT(
    'server_to_client' AS direction,
    'scamper' AS measurement_method,
    forward_distance AS distance_km,
    endpoint_geodesic_distance_km AS geodesic_distance_km,
    SAFE_DIVIDE(forward_distance, endpoint_geodesic_distance_km) AS detour_ratio,
    forward_total_hop_count AS total_hop_count,
    forward_responsive_hop_count AS responsive_hop_count,
    forward_geolocated_hop_count AS geolocated_hop_count,
    SAFE_DIVIDE(forward_geolocated_hop_count, forward_total_hop_count)
      AS geolocation_coverage,
    forward_loop AS loop_detected,
    forward_unresponse_within_AS AS unresponsive_within_as,
    ARRAY(SELECT h.asn FROM UNNEST(forward_hops) h WHERE h.asn IS NOT NULL)
      AS as_path,
    ARRAY(SELECT h.country_code FROM UNNEST(forward_hops) h
          WHERE h.country_code IS NOT NULL) AS country_path,
    ARRAY(SELECT h.metro FROM UNNEST(forward_hops) h WHERE h.metro IS NOT NULL)
      AS metro_path,
    ARRAY(SELECT h.ixp FROM UNNEST(forward_hops) h
          WHERE h.ixp IS NOT NULL AND h.ixp != 'None') AS ixp_path,
    forward_hops AS hops
  ) AS server_to_client_path,

  STRUCT(
    'client_to_server' AS direction,
    'reverse_traceroute' AS measurement_method,
    reverse_distance AS distance_km,
    endpoint_geodesic_distance_km AS geodesic_distance_km,
    SAFE_DIVIDE(reverse_distance, endpoint_geodesic_distance_km) AS detour_ratio,
    reverse_total_hop_count AS total_hop_count,
    reverse_responsive_hop_count AS responsive_hop_count,
    reverse_geolocated_hop_count AS geolocated_hop_count,
    SAFE_DIVIDE(reverse_geolocated_hop_count, reverse_total_hop_count)
      AS geolocation_coverage,
    reverse_loop AS loop_detected,
    reverse_unresponsive_within_AS AS unresponsive_within_as,
    ARRAY(SELECT h.asn FROM UNNEST(reverse_hops) h WHERE h.asn IS NOT NULL)
      AS as_path,
    ARRAY(SELECT h.country_code FROM UNNEST(reverse_hops) h
          WHERE h.country_code IS NOT NULL) AS country_path,
    ARRAY(SELECT h.metro FROM UNNEST(reverse_hops) h WHERE h.metro IS NOT NULL)
      AS metro_path,
    ARRAY(SELECT h.ixp FROM UNNEST(reverse_hops) h
          WHERE h.ixp IS NOT NULL AND h.ixp != 'None') AS ixp_path,
    STRUCT(
      revtr_id AS measurement_id,
      revtr_system_label AS system_label,
      revtr_stop_reason AS stop_reason,
      revtr_fail_reason AS fail_reason,
      is_try_from_destination_AS AS tried_from_client_as
    ) AS revtr,
    reverse_hops AS hops
  ) AS client_to_server_path,

  both_way_distance AS round_trip_distance_km,

  STRUCT(
    is_consistent,
    reach_dest AS reaches_client,
    is_virtual,
    is_reaching_dst_asn AS reaches_client_asn,
    total_windows,
    unique_ip_count_per_site,
    measurement_count_per_site
  ) AS quality
FROM path_summaries;

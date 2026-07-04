--------------------------------------------------------------------------------
-- HERMES (union): Temporal correlation — compare edge frequency before vs
-- during anomalies
--
-- Input:  `mlab-collaboration.hermes_union.events_with_as_and_geoloc`
-- Output: `mlab-collaboration.hermes_union.temporal_correlations`
-- Partition: partition_date
--
-- Adapted from hermes_core/temporal_tomography_upd.sql for the union pipeline.
--------------------------------------------------------------------------------

INSERT INTO `mlab-collaboration.hermes_union.temporal_correlations`
WITH
  final_results AS (
    SELECT *
    FROM `mlab-collaboration.hermes_union.events_with_as_and_geoloc`
    WHERE partition_date = '${DAY}'
      AND NOT EXISTS (
        SELECT 1
        FROM UNNEST(reverse_updated_node_details) AS node
        WHERE node.is_interdomain_symmetry = TRUE
          OR node.is_fishy_type_4 = TRUE
      )
      AND NOT EXISTS (
        SELECT 1
        FROM UNNEST(forward_updated_node_details) AS node
        WHERE node.distance_rtt_check = 'Above threshold' AND node.rtts > 1
      )
  ),
  augmented_as_path AS (
    SELECT
      id,
      ARRAY_CONCAT(
        ARRAY(
          SELECT
            IFNULL(
              CAST(
                CASE
                  WHEN STRPOS(ud.place, '-') > 0 THEN CONCAT(ud.associated_asn, '-', ud.place)
                  ELSE CONCAT(ud.associated_asn, '-', ud.place, '-', ud.cc)
                END AS STRING),
              '*')
          FROM UNNEST(forward_updated_node_details) AS ud
        ),
        IF(
          EXISTS(
            SELECT 1
            FROM UNNEST(forward_updated_node_details) AS ud
            WHERE CAST(ud.associated_asn AS INT64) = CAST(src_asn AS INT64)
          ),
          [],
          ['*' || CAST(CONCAT(src_asn, '-', src_city) AS STRING)]
        )
      ) AS cleaned_forward_as_path,
      ARRAY_CONCAT(
        ARRAY(
          SELECT
            IFNULL(
              CAST(
                CASE
                  WHEN STRPOS(ud.place, '-') > 0 THEN CONCAT(ud.associated_asn, '-', ud.place)
                  ELSE CONCAT(ud.associated_asn, '-', ud.place, '-', ud.cc)
                END AS STRING),
              '*')
          FROM UNNEST(reverse_updated_node_details) AS ud
        ),
        IF(
          EXISTS(
            SELECT 1
            FROM UNNEST(reverse_updated_node_details) AS ud
            WHERE CAST(ud.associated_asn AS INT64) = CAST(dst_asn AS INT64)
          ),
          [],
          ['*' || CAST(CONCAT(dst_asn, '-', dst_city) AS STRING)]
        )
      ) AS cleaned_reverse_as_path
    FROM final_results
  ),
  baseline_stats AS (
    SELECT
      src_asn,
      src_city,
      dst_site,
      AVG(baseline_median_rtt) AS baseline_rtt,
      STDDEV(ndt_rtt) AS stddev_rtt,
      AVG(baseline_median_throughput) AS baseline_throughput,
      AVG(anomaly_ratio_throughput) AS anomaly_ratio_throughput,
      AVG(anomaly_ratio_rtt) AS anomaly_ratio_rtt
    FROM final_results
    GROUP BY src_asn, src_city, dst_site
  ),
  path_changes AS (
    SELECT
      fr.id,
      fr.src_asn,
      fr.src_city,
      fr.src_country,
      fr.dst_site,
      fr.dst_asn,
      fr.window_start AS date,
      TIMESTAMP_SECONDS(fr.start) AS start_timestamp,
      fr.ndt_rtt,
      fr.ndt_throughput,
      fr.forward_distance,
      fr.reverse_distance,
      bs.baseline_rtt,
      bs.stddev_rtt,
      bs.anomaly_ratio_rtt,
      (
        SELECT ARRAY_AGG(x.path ORDER BY x.min_ttl)
        FROM (
          SELECT
            IFNULL(CONCAT(fwd.associated_asn, '-', fwd.place), '*') AS path,
            MIN(fwd.ttl) AS min_ttl
          FROM UNNEST(fr.forward_updated_node_details) fwd
          GROUP BY path
        ) x
      ) AS forward_as_path,
      (
        SELECT ARRAY_AGG(x.place ORDER BY x.min_ttl)
        FROM (
          SELECT fwd.place AS place, MIN(fwd.ttl) AS min_ttl
          FROM UNNEST(fr.forward_updated_node_details) fwd
          WHERE fwd.place IS NOT NULL
          GROUP BY place
        ) x
      ) AS forward_geo_path,
      (
        SELECT ARRAY_AGG(x.path ORDER BY x.min_ttl)
        FROM (
          SELECT
            IFNULL(CONCAT(rwd.associated_asn, '-', rwd.place), '*') AS path,
            MIN(rwd.ttl) AS min_ttl
          FROM UNNEST(fr.reverse_updated_node_details) AS rwd
          GROUP BY path
        ) x
      ) AS reverse_as_path,
      (
        SELECT ARRAY_AGG(x.place ORDER BY x.min_ttl)
        FROM (
          SELECT rwd.place AS place, MIN(rwd.ttl) AS min_ttl
          FROM UNNEST(fr.reverse_updated_node_details) AS rwd
          WHERE rwd.place IS NOT NULL
          GROUP BY place
        ) x
      ) AS reverse_geo_path,
      ARRAY(
        SELECT rtts
        FROM UNNEST(fr.forward_updated_node_details)
      ) AS forward_rtts,
      ARRAY(
        SELECT rtts
        FROM UNNEST(fr.reverse_updated_node_details)
      ) AS reverse_rtts,
      ARRAY(
        SELECT COALESCE(CAST(CONCAT(associated_asn, '-', place) AS STRING), '*')
        FROM UNNEST(forward_updated_node_details)
      ) AS forward_as_path_original,
      ARRAY(
        SELECT COALESCE(CAST(CONCAT(associated_asn, '-', place) AS STRING), '*')
        FROM UNNEST(reverse_updated_node_details)
      ) AS reverse_as_path_original,
      ((bs.anomaly_ratio_rtt >= 0.8
        AND fr.ndt_rtt > bs.baseline_rtt + 5
        AND fr.anomaly_rtt_count > 0.5)
       OR
       (bs.anomaly_ratio_throughput >= 0.8
        AND fr.ndt_throughput < bs.baseline_throughput
        AND fr.anomaly_throughput_count > 0.5)) AS is_anomaly
    FROM final_results fr
    JOIN baseline_stats bs
      ON fr.src_asn = bs.src_asn
      AND fr.src_city = bs.src_city
      AND fr.dst_site = bs.dst_site
    JOIN augmented_as_path ap
      ON fr.id = ap.id
  ),
  grouped_path_changes AS (
    SELECT
      CONCAT(src_asn, ' - ', src_city, ' - ', dst_site) AS src_dst_pair,
      date,
      ARRAY_AGG(STRUCT(
        start_timestamp,
        forward_as_path,
        reverse_as_path,
        forward_geo_path,
        reverse_geo_path,
        forward_rtts,
        reverse_rtts,
        forward_distance,
        reverse_distance,
        ndt_rtt,
        ndt_throughput,
        is_anomaly
      )) AS paths
    FROM path_changes
    GROUP BY src_asn, src_city, dst_site, date
  ),
  anomalies AS (
    SELECT
      src_dst_pair,
      ARRAY(
        SELECT AS STRUCT
          start_timestamp, forward_as_path, reverse_as_path,
          forward_geo_path, reverse_geo_path, forward_rtts, reverse_rtts,
          forward_distance, reverse_distance, ndt_rtt, ndt_throughput
        FROM UNNEST(paths)
        WHERE is_anomaly = TRUE AND date >= PARSE_TIMESTAMP('%Y-%m-%d', '${DAY}')
      ) AS during_anomaly_paths,
      ARRAY(
        SELECT AS STRUCT
          start_timestamp, forward_as_path, reverse_as_path,
          forward_geo_path, reverse_geo_path, forward_rtts, reverse_rtts,
          forward_distance, reverse_distance, ndt_rtt, ndt_throughput
        FROM UNNEST(paths)
        WHERE is_anomaly = FALSE AND date >= PARSE_TIMESTAMP('%Y-%m-%d', '${DAY}')
      ) AS during_regular_path,
      ARRAY(
        SELECT AS STRUCT
          start_timestamp, forward_as_path, reverse_as_path,
          forward_geo_path, reverse_geo_path, forward_rtts, reverse_rtts,
          forward_distance, reverse_distance, ndt_rtt, ndt_throughput
        FROM UNNEST(paths)
        WHERE is_anomaly = TRUE AND date < PARSE_TIMESTAMP('%Y-%m-%d', '${DAY}')
      ) AS before_anomaly_paths,
      ARRAY(
        SELECT AS STRUCT
          start_timestamp, forward_as_path, reverse_as_path,
          forward_geo_path, reverse_geo_path, forward_rtts, reverse_rtts,
          forward_distance, reverse_distance, ndt_rtt, ndt_throughput
        FROM UNNEST(paths)
        WHERE is_anomaly = FALSE AND date < PARSE_TIMESTAMP('%Y-%m-%d', '${DAY}')
      ) AS before_regular_paths
    FROM grouped_path_changes
  ),
  before_incident_counts AS (
    SELECT
      ARRAY_AGG(DISTINCT src_dst_pair) AS agg_src_dst_pair,
      CONCAT(path.forward_as_path[i], '-', path.forward_as_path[i+1]) AS edge,
      COUNT(*) AS total_count_before_incident
    FROM anomalies,
    UNNEST(before_anomaly_paths) AS path,
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(path.forward_as_path) - 2)) AS i
    GROUP BY edge
  ),
  during_incident_counts AS (
    SELECT
      ARRAY_AGG(DISTINCT src_dst_pair) AS agg_src_dst_pair,
      CONCAT(path.forward_as_path[i], '-', path.forward_as_path[i+1]) AS edge,
      COUNT(*) AS total_count_during_incident
    FROM anomalies,
    UNNEST(during_anomaly_paths) AS path,
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(path.forward_as_path) - 2)) AS i
    GROUP BY edge
  ),
  total_paths_before_incident AS (
    SELECT COUNT(*) AS total_paths_before
    FROM anomalies, UNNEST(before_anomaly_paths) AS path
  ),
  total_paths_during_incident AS (
    SELECT COUNT(*) AS total_paths_during
    FROM anomalies, UNNEST(during_anomaly_paths) AS path
  ),
  final_counts AS (
    SELECT
      bi.edge,
      bi.agg_src_dst_pair AS before_agg_src_dst_pair,
      di.agg_src_dst_pair AS during_agg_src_dst_pair,
      bi.total_count_before_incident,
      di.total_count_during_incident,
      SAFE_DIVIDE(di.total_count_during_incident, tpdi.total_paths_during) AS fraction_during_incident,
      SAFE_DIVIDE(bi.total_count_before_incident, tpbi.total_paths_before) AS fraction_before_incident
    FROM before_incident_counts bi
    LEFT JOIN during_incident_counts di ON bi.edge = di.edge
    CROSS JOIN total_paths_before_incident tpbi
    CROSS JOIN total_paths_during_incident tpdi
  )

SELECT
  DATE('${DAY}') AS partition_date,
  edge,
  before_agg_src_dst_pair,
  during_agg_src_dst_pair,
  total_count_before_incident,
  total_count_during_incident,
  fraction_before_incident,
  fraction_during_incident,
  SAFE_DIVIDE(fraction_during_incident, fraction_before_incident) AS ratio_before_after
FROM final_counts;

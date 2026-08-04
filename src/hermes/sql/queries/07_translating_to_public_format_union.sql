-- CREATE OR REPLACE TABLE `mlab-collaboration.hermes_union.events_explained_daily`
-- PARTITION BY partition_date  -- This defines partitioning by the 'partition_date' column
-- AS
DELETE FROM `mlab-collaboration.hermes_union.events_explained_daily`
WHERE partition_date = '${DAY}';

INSERT INTO `mlab-collaboration.hermes_union.events_explained_daily`
-- Rebuild the total set of anomalous src-dst pairs
WITH
  -- Data-sufficiency gate: only consider user groups with >= 10 measurements on
  -- the incident day (DATE(window_start) >= DAY), counted per ip_version.
  dayof_counts AS (
    SELECT src_asn, src_city, dst_site, ip_version, COUNT(*) AS n_dayof
    FROM `mlab-collaboration.hermes_union.events_with_as_and_geoloc`
    WHERE partition_date = '${DAY}' AND DATE(window_start) >= '${DAY}'
    GROUP BY src_asn, src_city, dst_site, ip_version
  ),
  -- Per-group geographic distance extents: max forward/reverse distance among the
  -- day-of vs the baseline measurements. The dashboard tags "distance" when a day-of
  -- measurement went farther than ANY baseline measurement (either direction).
  distance_extents AS (
    SELECT src_asn, src_city, dst_site, ip_version,
      MAX(IF(DATE(window_start) >= '${DAY}', forward_distance, NULL)) AS max_daily_forward_distance,
      MAX(IF(DATE(window_start) <  '${DAY}', forward_distance, NULL)) AS max_baseline_forward_distance,
      MAX(IF(DATE(window_start) >= '${DAY}', reverse_distance, NULL)) AS max_daily_reverse_distance,
      MAX(IF(DATE(window_start) <  '${DAY}', reverse_distance, NULL)) AS max_baseline_reverse_distance
    FROM `mlab-collaboration.hermes_union.events_with_as_and_geoloc`
    WHERE partition_date = '${DAY}'
    GROUP BY src_asn, src_city, dst_site, ip_version
  ),
  -- Step 1: Recompute all anomalous src-dst pairs based on original logic
  total_anomalous_src_dst_pairs AS (
    SELECT DISTINCT
      CONCAT(fr.src_asn, ' - ', fr.src_city, ' - ', fr.dst_site) AS src_dst_pair,
      fr.src_asn,
      fr.src_city,
      fr.src_state,
      fr.src_country,
      fr.dst_site,
      dst_asn,
      dst_city,
      dst_country,
      fr.ip_version,
      ARRAY_AGG(DISTINCT fr.src) AS observed_ips,
      ANY_VALUE(baseline_median_rtt) AS baseline_median_rtt,
      ANY_VALUE(baseline_median_throughput) AS baseline_median_throughput,
      APPROX_QUANTILES(fr.ndt_rtt, 100)[OFFSET(50)] AS median_daily_rtt,
      APPROX_QUANTILES(fr.ndt_throughput, 100)[OFFSET(50)] AS median_daily_throughput,
      AVG(ndt_rtt) AS mean_daily_rtt,
      AVG(ndt_throughput) AS mean_daily_throughput,
      ANY_VALUE(fr.anomaly_ratio_rtt) AS anomaly_ratio_rtt,
      ANY_VALUE(fr.anomaly_ratio_throughput) AS anomaly_ratio_throughput
    FROM
      `mlab-collaboration.hermes_union.events_with_as_and_geoloc` AS fr
    JOIN dayof_counts dc
      ON dc.src_asn = fr.src_asn AND dc.src_city = fr.src_city
         AND dc.dst_site = fr.dst_site AND dc.ip_version = fr.ip_version
    WHERE
      partition_date = '${DAY}'
      AND DATE(fr.window_start) >= '${DAY}'
      AND dc.n_dayof >= 10
      AND ((fr.ndt_rtt > fr.baseline_median_rtt + 5 AND fr.anomaly_ratio_rtt >= 0.8 AND fr.anomaly_rtt_count >= 0.5) OR
      (fr.ndt_throughput < fr.baseline_median_throughput AND fr.anomaly_throughput_count >= 0.5 AND fr.anomaly_ratio_throughput >= 0.8))
    GROUP BY fr.src_asn, fr.src_city, fr.dst_site, fr.dst_asn, fr.src_country, fr.src_state, dst_city, dst_country, fr.ip_version
  ),
  closest_metadata AS (
    SELECT * EXCEPT (partition_date)
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                 PARTITION BY asn
                 ORDER BY ABS(DATE_DIFF(partition_date, DATE '${DAY}', DAY))
               ) AS rn
        FROM `hermes.as_metadata`
      )
      WHERE rn = 1
  ),
  -- Step 2: Resolved anomalies from expanded query, enriched with observed_ips
  -- resolved AS (
  --   SELECT
  --     SPLIT(src_dst_str, ' - ')[SAFE_OFFSET(0)] AS src_asn,
  --     SPLIT(src_dst_str, ' - ')[SAFE_OFFSET(1)] AS src_city,
  --     SPLIT(src_dst_str, ' - ')[SAFE_OFFSET(2)] AS dst_site,
  --     dst_asn,
  --     src_state,
  --     src_country,
  --     dst_city,
  --     dst_country,
  --     partition_date,
  --     iteration_number,
  --     edge_asn_metro AS source_events,
  --     information_source,
  --     is_interdomain,
  --     ratio_anomaly,
  --     fraction_anomalies_explained_by_edge,
  --     cumulative_anomalies_explained,
  --     baseline_median_rtt,
  --     baseline_median_throughput,
  --     ta.anomaly_ratio_throughput,
  --     ta.anomaly_ratio_rtt,
  --     ta.observed_ips
  --   FROM
  --     `mlab-collaboration.hermes.correlation_hyperedges_tomography` ocd,
  --     UNNEST(anomalous_src_dst_pairs_impacted) AS src_dst_str
  --   FULL OUTER JOIN total_anomalous_src_dst_pairs ta
  --     ON SPLIT(src_dst_str, ' - ')[SAFE_OFFSET(0)] = CAST(ta.src_asn AS STRING)
  --     AND SPLIT(src_dst_str, ' - ')[SAFE_OFFSET(1)] = ta.src_city
  --     AND SPLIT(src_dst_str, ' - ')[SAFE_OFFSET(2)] = ta.dst_site
  --   WHERE partition_date = '${DAY}'
  -- ),

  resolved AS (
    SELECT
    DISTINCT
      SPLIT(src_dst_str, ' - ')[SAFE_OFFSET(0)] AS src_asn,
      SPLIT(src_dst_str, ' - ')[SAFE_OFFSET(1)] AS src_city,
      SPLIT(src_dst_str, ' - ')[SAFE_OFFSET(2)] AS dst_site,
      dst_asn,
      src_state,
      src_country,
      dst_city,
      dst_country,
      ta.ip_version,
      ocd.partition_date,
      ocd.iteration_number,
      ocd.edge_asn_metro AS source_events,
      CONCAT(metadata_left.asnName, ' --- ' , metadata_right.asnName) AS source_events_org,
      ocd.information_source,
      ocd.is_interdomain,
      ocd.ratio_anomaly,
      ocd.fraction_anomalies_explained_by_edge,
      ocd.cumulative_anomalies_explained,
      ta.baseline_median_rtt,
      ta.baseline_median_throughput,
      median_daily_rtt,
      median_daily_throughput,
      mean_daily_rtt,
      mean_daily_throughput,
      ta.anomaly_ratio_throughput,
      ta.anomaly_ratio_rtt,
      ta.observed_ips,
      ocd.attribution_method,
      ocd.confidence_tier
    FROM
      `mlab-collaboration.hermes_union.correlation_hyperedges_tomography_v2` AS ocd
    CROSS JOIN UNNEST(ocd.anomalous_src_dst_pairs_impacted) AS src_dst_str
    FULL OUTER JOIN total_anomalous_src_dst_pairs AS ta
      ON SPLIT(src_dst_str, ' - ')[SAFE_OFFSET(0)] = CAST(ta.src_asn AS STRING)
      AND SPLIT(src_dst_str, ' - ')[SAFE_OFFSET(1)] = ta.src_city
      AND SPLIT(src_dst_str, ' - ')[SAFE_OFFSET(2)] = ta.dst_site
    JOIN closest_metadata metadata_left
      ON
      CAST(ocd.from_asn AS STRING) = CAST(metadata_left.asn AS STRING)
    JOIN closest_metadata metadata_right
      ON
      CAST(ocd.to_asn AS STRING) = CAST(metadata_right.asn AS STRING)
    WHERE ocd.partition_date = '${DAY}'
  ),

  -- Step 3: Identify unresolved anomalies
  unresolved AS (
    SELECT
    DISTINCT
      src_asn,
      src_city,
      src_state,
      src_country,
      dst_site,
      dst_city,
      dst_country,
      dst_asn,
      ip_version,
      CAST('${DAY}' AS DATE) AS partition_date,
      CAST(NULL AS INT64) AS iteration_number,
      CAST(NULL AS STRING) AS source_events,
      CAST(NULL AS STRING) AS source_events_org,
      CAST(NULL AS STRING) AS information_source,
      CAST(NULL AS STRING) AS is_interdomain,
      CAST(NULL AS FLOAT64) AS ratio_anomaly,
      CAST(NULL AS FLOAT64) AS fraction_anomalies_explained_by_edge,
      CAST(NULL AS FLOAT64) AS cumulative_fraction_anomalies_explained_so_far,
      baseline_median_rtt,
      baseline_median_throughput,
      median_daily_rtt,
      median_daily_throughput,
      mean_daily_rtt,
      mean_daily_throughput,
      anomaly_ratio_rtt,
      anomaly_ratio_throughput,
      observed_ips,
      CAST(NULL AS STRING) AS attribution_method,
      CAST(NULL AS STRING) AS confidence_tier
    FROM total_anomalous_src_dst_pairs
    WHERE CONCAT(src_asn, ' - ', src_city, ' - ', dst_site) NOT IN (
      SELECT DISTINCT src_dst_str
      FROM `mlab-collaboration.hermes_union.correlation_hyperedges_tomography_v2`,
      UNNEST(anomalous_src_dst_pairs_impacted) AS src_dst_str
      WHERE partition_date = '${DAY}'
    )
  ),
anomalies AS (
      SELECT DISTINCT CONCAT(fr.src_asn, ' - ', fr.src_city, ' - ', fr.dst_site) AS src_dst_pair
      FROM `mlab-collaboration.hermes_union.events_with_as_and_geoloc` AS fr
        WHERE
      -- revtr_stop_reason = 'REACHES'
      DATE(window_start) >= partition_date AND partition_date = '${DAY}'
      AND (
          (fr.anomaly_ratio_rtt >= 0.8
            AND fr.ndt_rtt > fr.baseline_median_rtt + 5
            AND fr.anomaly_rtt_count >= 0.5)
          OR
          (fr.anomaly_ratio_throughput >= 0.8
            AND fr.ndt_throughput < fr.baseline_median_throughput
            AND fr.anomaly_throughput_count >= 0.5)
        )
      AND NOT EXISTS (
        SELECT 1
        FROM UNNEST(reverse_updated_node_details) AS node
        WHERE node.is_interdomain_symmetry = TRUE OR node.is_fishy_type_4 = TRUE
      )
      AND NOT EXISTS (
        SELECT 1
        FROM UNNEST(forward_updated_node_details) AS node
        WHERE node.distance_rtt_check = 'Above threshold'
      )
),
combined AS (
  -- Step 4: Combine resolved and unresolved anomalies
  SELECT
    CAST(src_asn AS int64) AS src_asn,
    CONCAT(src_city, ' - ', src_asn) AS user_group,
    src_city,
    src_state,
    src_country,
    dst_site,
    dst_city,
    dst_country,
    dst_asn,
    ip_version,
    partition_date,
    baseline_median_rtt,
    baseline_median_throughput,
    median_daily_rtt,
    median_daily_throughput,
    mean_daily_rtt,
    mean_daily_throughput,
    anomaly_ratio_rtt,
    anomaly_ratio_throughput,
    observed_ips,
    source_events,
    source_events_org,
    information_source,
    is_interdomain,
    fraction_anomalies_explained_by_edge,
    attribution_method,
    confidence_tier
  FROM resolved

  UNION ALL

  SELECT DISTINCT
    CAST(src_asn AS int64) AS src_asn,
    CONCAT(src_city, ' - ', src_asn) AS user_group,
    src_city,
    src_state,
    src_country,
    dst_site,
    dst_city,
    dst_country,
    dst_asn,
    ip_version,
    partition_date,
    baseline_median_rtt,
    baseline_median_throughput,
    median_daily_rtt,
    median_daily_throughput,
    mean_daily_rtt,
    mean_daily_throughput,
    anomaly_ratio_rtt,
    anomaly_ratio_throughput,
    observed_ips,
    source_events,
    source_events_org,
    information_source,
    is_interdomain,
    fraction_anomalies_explained_by_edge,
    attribution_method,
    confidence_tier
  FROM unresolved
  WHERE partition_date = '${DAY}'
),
combined_with_AS_meta AS (
  SELECT
    src_asn,
    src_city,
    CONCAT(src_country, '-', src_state) AS src_state,
    src_country,
    metadata_src_asn.asnName AS src_as_name,
    metadata_src_asn.organization.orgName AS src_organization,
    dst_site,
    dst_asn,
    dst_city,
    dst_country,
    metadata_dst_asn.asnName AS dst_as_name,
    metadata_dst_asn.organization.orgName AS dst_organization,
    ip_version,
    partition_date,
    baseline_median_rtt,
    baseline_median_throughput,
    median_daily_rtt,
    median_daily_throughput,
    mean_daily_rtt,
    mean_daily_throughput,
    anomaly_ratio_rtt,
    anomaly_ratio_throughput,
    observed_ips,
    -- Transform source_events to use " --- " instead of " - " and map ASNs to their AS Names
    ARRAY_TO_STRING(
      ARRAY(
        SELECT
          COALESCE(part)  -- Replace ASN with AS name if found
        FROM UNNEST(SPLIT(source_events, ' - ')) AS part
      ),
      ' --- '  -- New separator
    ) AS source_events,
    source_events_org,
    information_source,
    is_interdomain,
    fraction_anomalies_explained_by_edge,
    attribution_method,
    confidence_tier,
  FROM combined
  JOIN closest_metadata metadata_src_asn
    ON
  CAST(src_asn AS STRING) = CAST(metadata_src_asn.asn AS STRING)
  JOIN closest_metadata metadata_dst_asn
    ON
  CAST(dst_asn AS STRING) = CAST(metadata_dst_asn.asn AS STRING)
),
anomaly_data AS (
  -- Extract anomalies for each src_asn, src_city, dst_site
  SELECT
    src_asn,
    src_city,
    ip_version,
    partition_date,
    dst_site,
    -- Latency anomaly flag
    CASE WHEN MAX(anomaly_ratio_rtt) >= 0.8 AND MAX(anomaly_rtt_count) > 0.5 THEN 1 ELSE 0 END AS is_latency_anomaly,
    -- Throughput anomaly flag
    CASE WHEN MAX(anomaly_ratio_throughput) >= 0.8 AND MAX(anomaly_throughput_count) > 0.5 THEN 1 ELSE 0 END AS is_throughput_anomaly
  FROM
    `mlab-collaboration.hermes_union.events_with_as_and_geoloc`
  WHERE partition_date = '${DAY}'
  GROUP BY
    src_asn, src_city, ip_version, partition_date, dst_site
),
anomaly_summary AS (
  -- Aggregate anomalies for each src_asn, src_city, partition_date
  SELECT
    src_asn,
    src_city,
    ip_version,
    partition_date,
    COUNTIF(is_latency_anomaly = 1) AS latency_anomaly_sites,
    COUNTIF(is_throughput_anomaly = 1) AS throughput_anomaly_sites,
    COUNTIF(is_latency_anomaly = 1 OR is_throughput_anomaly = 1) AS total_anomalous_sites,
    COUNT(*) AS total_sites,
  FROM
    anomaly_data
  GROUP BY
    src_asn, src_city, ip_version, partition_date
),
combined_with_anomaly_summary AS (
  -- Combine the original query results with anomaly summary
  SELECT
    combined.*,
    summary.total_anomalous_sites,
    total_sites,
    -- Per-group path-distance extents (day-of vs baseline max forward/reverse km).
    -- Joined here, where src_city is still the full canonical metro, so it matches
    -- distance_extents (which keys on the same canonical src_city); final_result
    -- shortens src_city afterwards, so a join further down would mismatch.
    dist.max_daily_forward_distance,
    dist.max_baseline_forward_distance,
    dist.max_daily_reverse_distance,
    dist.max_baseline_reverse_distance,
  FROM
    combined_with_AS_meta AS combined
  INNER JOIN anomaly_summary AS summary
  ON
    CAST(combined.src_asn AS INT64) = summary.src_asn
    -- src_city is the full canonical metro (City-state_resolved-CC) on both sides;
    -- join on it directly rather than reconstructing it (the old reconstruction
    -- assumed the legacy City-RegionISO2-CC shape and breaks on normalized src nodes).
    AND combined.src_city = summary.src_city
    AND combined.ip_version = summary.ip_version
    -- AND combined.partition_date = summary.partition_date
  LEFT JOIN distance_extents AS dist
    ON CAST(dist.src_asn AS STRING) = CAST(combined.src_asn AS STRING)
    AND dist.src_city = combined.src_city
    AND dist.dst_site = combined.dst_site
    AND dist.ip_version = combined.ip_version
),
final_result AS (
  -- Add or update the information_source and source_events fields
  SELECT
    src_asn,
    -- Shorten the canonical metro to a display city only at the very end.
    SPLIT(src_city, '-')[SAFE_OFFSET(0)] AS src_city,
    src_state,
    src_country,
    src_as_name,
    src_organization,
    dst_site,
    dst_asn,
    dst_city,
    dst_country,
    dst_as_name,
    dst_organization,
    ip_version,
    partition_date,
    baseline_median_rtt,
    baseline_median_throughput,
    median_daily_rtt,
    median_daily_throughput,
    mean_daily_rtt,
    mean_daily_throughput,
    anomaly_ratio_rtt,
    anomaly_ratio_throughput,
    observed_ips,
    source_events_org,
    CASE
      WHEN source_events = '' AND total_anomalous_sites >= 2 AND SAFE_DIVIDE(total_anomalous_sites, total_sites) >= 0.75 THEN
        'source'
      ELSE information_source
    END AS information_source,
    CASE
      WHEN source_events = '' AND total_anomalous_sites >= 2 AND SAFE_DIVIDE(total_anomalous_sites, total_sites) >= 0.75 THEN
        'source'
      ELSE is_interdomain
    END AS is_interdomain,
    fraction_anomalies_explained_by_edge,
    total_anomalous_sites,
    CASE
      WHEN source_events = '' AND total_anomalous_sites >= 2 AND SAFE_DIVIDE(total_anomalous_sites, total_sites) >= 0.75 THEN
        CONCAT(src_asn, ' - ', src_city)
      ELSE source_events
    END AS source_events,
    max_daily_forward_distance,
    max_baseline_forward_distance,
    max_daily_reverse_distance,
    max_baseline_reverse_distance,
    attribution_method,
    confidence_tier
  FROM
    combined_with_anomaly_summary
)
SELECT * FROM final_result


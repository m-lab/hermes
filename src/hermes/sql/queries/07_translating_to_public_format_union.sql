-- CREATE OR REPLACE TABLE `mlab-collaboration.${DS}.events_explained_daily`
-- PARTITION BY partition_date  -- This defines partitioning by the 'partition_date' column
-- AS
DELETE FROM `mlab-collaboration.${DS}.events_explained_daily`
WHERE partition_date = '${DAY}';

INSERT INTO `mlab-collaboration.${DS}.events_explained_daily`
  (src_asn, src_city, src_state, src_country, src_as_name, src_organization,
   dst_site, dst_asn, dst_city, dst_country, dst_as_name, dst_organization,
   ip_version, partition_date, baseline_median_rtt,
   baseline_median_throughput, median_daily_rtt, median_daily_throughput,
   mean_daily_rtt, mean_daily_throughput, anomaly_ratio_rtt,
   anomaly_ratio_throughput, observed_ips, source_events_org,
   information_source, is_interdomain, fraction_anomalies_explained_by_edge,
   total_anomalous_sites, source_events, max_daily_forward_distance,
   max_baseline_forward_distance, max_daily_reverse_distance,
   max_baseline_reverse_distance, attribution_method, confidence_tier,
   detection_granularity, src_metro, src_group_label, n_dayof,
   src_match_granularity, client_geo_source, n_baseline,
   baseline_median_upload_throughput, median_daily_upload_throughput,
   mean_daily_upload_throughput, anomaly_ratio_upload_throughput,
   upload_anomaly_sites, total_anomalous_sites_all_signals, anomaly_signals)
-- Rebuild the total set of anomalous src-dst pairs
WITH
  -- Data-sufficiency gate: only consider user groups with >= 10 measurements on
  -- the incident day (DATE(window_start) >= DAY), counted per ip_version.
  --
  -- Both counts come from ONE pass over the partition. The window split moved from
  -- the WHERE clause into COUNTIF so the baseline half can be counted alongside the
  -- day-of half without a second scan; the partition is the pipeline's single
  -- largest table, so a second CTE over it would be the expensive way to do this.
  -- Groups with no day-of measurements now reach this CTE with n_dayof = 0 rather
  -- than being dropped by the WHERE -- harmless, since the >= 10 gate below still
  -- excludes them.
  --
  -- n_baseline exists so consumers stop recomputing it. The dashboard was
  -- re-deriving both counts per page load by aggregating the whole partition, and
  -- doing it on a COARSER key (short city) than the detector groups on -- which
  -- pooled neighbouring populations and let a 6-measurement group pass a
  -- 20-measurement threshold on its neighbours' totals. Counting here, on
  -- src_group_label, is both cheaper and the only correct place to do it.
  group_counts AS (
    SELECT src_asn, src_group_label, dst_site, ip_version,
      COUNTIF(DATE(window_start) >= '${DAY}') AS n_dayof,
      COUNTIF(DATE(window_start) <  '${DAY}') AS n_baseline
    FROM `mlab-collaboration.${DS}.events_with_as_and_geoloc`
    WHERE partition_date = '${DAY}'
    GROUP BY src_asn, src_group_label, dst_site, ip_version
  ),
  -- The granularity anomaly detection ran at. src_metro is deliberately NOT
  -- taken here: it is already carried on the anomalous-pair rows, and sourcing
  -- it twice from two different groupings would let the metro used to match a
  -- hyperedge disagree with the metro reported on the row.
  -- See docs/proposals/2026-08-group-granularity.md.
  group_identity AS (
    SELECT src_asn, src_group_label, dst_site, ip_version,
      MIN(detection_granularity) AS detection_granularity,
      MIN(client_geo_source) AS client_geo_source
    FROM `mlab-collaboration.${DS}.events_with_as_and_geoloc`
    WHERE partition_date = '${DAY}'
    GROUP BY src_asn, src_group_label, dst_site, ip_version
  ),
  -- Per-group geographic distance extents: max forward/reverse distance among the
  -- day-of vs the baseline measurements. The dashboard tags "distance" when a day-of
  -- measurement went farther than ANY baseline measurement (either direction).
  distance_extents AS (
    SELECT src_asn, src_group_label, dst_site, ip_version,
      MAX(IF(DATE(window_start) >= '${DAY}', forward_distance, NULL)) AS max_daily_forward_distance,
      MAX(IF(DATE(window_start) <  '${DAY}', forward_distance, NULL)) AS max_baseline_forward_distance,
      MAX(IF(DATE(window_start) >= '${DAY}', reverse_distance, NULL)) AS max_daily_reverse_distance,
      MAX(IF(DATE(window_start) <  '${DAY}', reverse_distance, NULL)) AS max_baseline_reverse_distance
    FROM `mlab-collaboration.${DS}.events_with_as_and_geoloc`
    WHERE partition_date = '${DAY}'
    GROUP BY src_asn, src_group_label, dst_site, ip_version
  ),
  -- Step 1: Recompute all anomalous src-dst pairs based on original logic
  total_anomalous_src_dst_pairs AS (
    SELECT DISTINCT
      CONCAT(fr.src_asn, ' - ', fr.src_group_label, ' - ', fr.dst_site, ' - ', fr.ip_version)
        AS src_dst_pair,
      fr.src_asn,
      fr.src_group_label,
      MIN(fr.detection_granularity) AS detection_granularity,
      MIN(fr.src_city) AS src_city,
      MIN(fr.src_metro) AS src_metro,
      MIN(dc.n_dayof) AS n_dayof,
      MIN(dc.n_baseline) AS n_baseline,
      fr.src_state,
      fr.src_country,
      fr.dst_site,
      dst_asn,
      dst_city,
      dst_country,
      fr.ip_version,
      ARRAY_AGG(DISTINCT fr.src ORDER BY fr.src) AS observed_ips,
      MIN(baseline_median_rtt) AS baseline_median_rtt,
      MIN(baseline_median_throughput) AS baseline_median_throughput,
      MIN(baseline_median_upload_throughput) AS baseline_median_upload_throughput,
      APPROX_QUANTILES(fr.ndt_rtt, 100)[OFFSET(50)] AS median_daily_rtt,
      APPROX_QUANTILES(fr.ndt_throughput, 100)[OFFSET(50)] AS median_daily_throughput,
      APPROX_QUANTILES(mdu.upload_throughput_mbps, 100)[OFFSET(50)] AS median_daily_upload_throughput,
      AVG(ndt_rtt) AS mean_daily_rtt,
      AVG(ndt_throughput) AS mean_daily_throughput,
      AVG(mdu.upload_throughput_mbps) AS mean_daily_upload_throughput,
      MIN(fr.anomaly_ratio_rtt) AS anomaly_ratio_rtt,
      MIN(fr.anomaly_ratio_throughput) AS anomaly_ratio_throughput,
      MIN(fr.anomaly_ratio_upload_throughput) AS anomaly_ratio_upload_throughput,
      ARRAY_CONCAT(
        IF(LOGICAL_OR(fr.anomaly_ratio_rtt >= 0.8
          AND fr.ndt_rtt > fr.baseline_median_rtt + 5
          AND fr.anomaly_rtt_count >= 0.5), ['latency'], []),
        IF(LOGICAL_OR(fr.anomaly_ratio_throughput >= 0.8
          AND fr.ndt_throughput < fr.baseline_median_throughput
          AND fr.anomaly_throughput_count >= 0.5), ['download'], []),
        IF(LOGICAL_OR(fr.anomaly_ratio_upload_throughput >= 0.8
          AND fr.median_upload_throughput < fr.baseline_median_upload_throughput
          AND fr.anomaly_upload_throughput_count >= 0.5), ['upload'], [])
      ) AS anomaly_signals
    FROM
      `mlab-collaboration.${DS}.events_with_as_and_geoloc` AS fr
    JOIN group_counts dc
      ON dc.src_asn = fr.src_asn AND dc.src_group_label = fr.src_group_label
         AND dc.dst_site = fr.dst_site AND dc.ip_version = fr.ip_version
    LEFT JOIN `mlab-collaboration.${DS}.merged_download_upload` AS mdu
      ON mdu.id = fr.id AND mdu.partition_date = DATE '${DAY}'
    WHERE
      fr.partition_date = '${DAY}'
      AND DATE(fr.window_start) >= '${DAY}'
      AND dc.n_dayof >= 10
      AND ((fr.ndt_rtt > fr.baseline_median_rtt + 5 AND fr.anomaly_ratio_rtt >= 0.8 AND fr.anomaly_rtt_count >= 0.5) OR
      (fr.ndt_throughput < fr.baseline_median_throughput AND fr.anomaly_throughput_count >= 0.5 AND fr.anomaly_ratio_throughput >= 0.8) OR
      (fr.median_upload_throughput < fr.baseline_median_upload_throughput AND fr.anomaly_upload_throughput_count >= 0.5 AND fr.anomaly_ratio_upload_throughput >= 0.8))
    GROUP BY fr.src_asn, fr.src_group_label, fr.dst_site, fr.dst_asn, fr.src_country, fr.src_state, dst_city, dst_country, fr.ip_version
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
  -- Phase D stores pair identities as strings. Read the immutable components
  -- from both ends because a source label itself may contain the historical
  -- ` - ` delimiter (for example, "Er Rachidia-Meknès - Tafilalet-MA").
  -- New rows end in ip_version; historical three-component rows do not.
  correlation_pair_parts AS (
    SELECT
      ocd.*,
      src_dst_str,
      SPLIT(src_dst_str, ' - ') AS pair_parts
    FROM `mlab-collaboration.${DS}.correlation_hyperedges_tomography_v2` AS ocd
    CROSS JOIN UNNEST(ocd.anomalous_src_dst_pairs_impacted) AS src_dst_str
    WHERE ocd.partition_date = '${DAY}'
  ),
  correlation_pairs AS (
    SELECT
      * EXCEPT (pair_parts),
      pair_parts[SAFE_OFFSET(0)] AS pair_src_asn,
      ARRAY_TO_STRING(
        ARRAY_SLICE(
          pair_parts,
          1,
          ARRAY_LENGTH(pair_parts) - IF(
            pair_parts[SAFE_OFFSET(ARRAY_LENGTH(pair_parts) - 1)] IN ('v4', 'v6'),
            3,
            2
          )
        ),
        ' - '
      ) AS pair_src_group_label,
      pair_parts[SAFE_OFFSET(
        ARRAY_LENGTH(pair_parts) - IF(
          pair_parts[SAFE_OFFSET(ARRAY_LENGTH(pair_parts) - 1)] IN ('v4', 'v6'),
          2,
          1
        )
      )] AS pair_dst_site,
      IF(
        pair_parts[SAFE_OFFSET(ARRAY_LENGTH(pair_parts) - 1)] IN ('v4', 'v6'),
        pair_parts[SAFE_OFFSET(ARRAY_LENGTH(pair_parts) - 1)],
        NULL
      ) AS pair_ip_version
    FROM correlation_pair_parts
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
      ocd.pair_src_asn AS src_asn,
      -- Take the label from the matched event row, NOT from the pair string:
      -- a metro-keyed hyperedge would otherwise put a METRO in this column, and
      -- the INNER JOIN to anomaly_summary (keyed on real labels) would silently
      -- drop every resolved row.
      COALESCE(ta.src_group_label,
               ocd.pair_src_group_label) AS src_group_label,
      ta.src_city,
      ta.src_metro,
      ta.n_dayof,
      ta.n_baseline,
      -- Which source vocabulary matched. When the exact tested label matched,
      -- report the detection regime itself; the metro fallback remains for
      -- historical city-detected partitions with metro-keyed Phase-D output.
      IF(ocd.pair_src_group_label = ta.src_group_label,
         ta.detection_granularity, 'metro') AS src_match_granularity,
      ocd.pair_dst_site AS dst_site,
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
      ta.baseline_median_upload_throughput,
      median_daily_rtt,
      median_daily_throughput,
      median_daily_upload_throughput,
      mean_daily_rtt,
      mean_daily_throughput,
      mean_daily_upload_throughput,
      ta.anomaly_ratio_throughput,
      ta.anomaly_ratio_rtt,
      ta.anomaly_ratio_upload_throughput,
      ta.anomaly_signals,
      ta.observed_ips,
      ocd.attribution_method,
      ocd.confidence_tier
    FROM
      correlation_pairs AS ocd
    FULL OUTER JOIN total_anomalous_src_dst_pairs AS ta
      ON ocd.pair_src_asn = CAST(ta.src_asn AS STRING)
      -- Hyperedges built BEFORE this change key their pair strings on the metro
      -- src_city; those built after key on src_group_label. A hyperedge's own
      -- content (from_asn/to_asn/edge_asn_metro) is an intermediary hop pair and
      -- is granularity-independent, so the historical corpus stays valid -- only
      -- its pair LABELS are in the old vocabulary. Accept both so no Phase D
      -- re-run is needed. For metro-keyed hyperedges one edge then matches every
      -- city group in that metro, i.e. attribution stays metro-level for those
      -- partitions, which is exactly what they have always meant.
      AND (ocd.pair_src_group_label = ta.src_group_label
        OR ocd.pair_src_group_label = ta.src_metro)
      AND ocd.pair_dst_site = ta.dst_site
      -- New hyperedges carry ip_version as the fourth identity component. Keep
      -- three-component historical rows readable when reformatting old dates.
      AND (ocd.pair_ip_version IS NULL
        OR ocd.pair_ip_version = ta.ip_version)
      -- Defense in depth: an attributed path direction must be supported by at
      -- least one signal eligible for that direction. This also contains any
      -- legacy three-component pair collision.
      AND (
        (ocd.information_source = 'forward' AND (
          'latency' IN UNNEST(ta.anomaly_signals)
          OR 'download' IN UNNEST(ta.anomaly_signals)
        ))
        OR (ocd.information_source = 'reverse' AND (
          'latency' IN UNNEST(ta.anomaly_signals)
          OR 'upload' IN UNNEST(ta.anomaly_signals)
        ))
      )
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
      src_group_label,
      src_city,
      src_metro,
      n_dayof,
      n_baseline,
      CAST(NULL AS STRING) AS src_match_granularity,
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
      baseline_median_upload_throughput,
      median_daily_rtt,
      median_daily_throughput,
      median_daily_upload_throughput,
      mean_daily_rtt,
      mean_daily_throughput,
      mean_daily_upload_throughput,
      anomaly_ratio_rtt,
      anomaly_ratio_throughput,
      anomaly_ratio_upload_throughput,
      anomaly_signals,
      observed_ips,
      CAST(NULL AS STRING) AS attribution_method,
      CAST(NULL AS STRING) AS confidence_tier
    FROM total_anomalous_src_dst_pairs
    -- Mirror every identity and direction condition in `resolved`, or a pair
    -- rejected there would disappear instead of becoming unresolved.
    WHERE NOT EXISTS (
      SELECT 1
      FROM correlation_pairs AS ocd
      WHERE ocd.pair_src_asn = CAST(src_asn AS STRING)
        AND (ocd.pair_src_group_label = src_group_label
          OR ocd.pair_src_group_label = src_metro)
        AND ocd.pair_dst_site = dst_site
        AND (ocd.pair_ip_version IS NULL
          OR ocd.pair_ip_version = ip_version)
        AND (
          (ocd.information_source = 'forward' AND (
            'latency' IN UNNEST(anomaly_signals)
            OR 'download' IN UNNEST(anomaly_signals)
          ))
          OR (ocd.information_source = 'reverse' AND (
            'latency' IN UNNEST(anomaly_signals)
            OR 'upload' IN UNNEST(anomaly_signals)
          ))
        )
    )
  ),
anomalies AS (
      SELECT DISTINCT CONCAT(
        fr.src_asn, ' - ', fr.src_group_label, ' - ', fr.dst_site, ' - ', fr.ip_version
      ) AS src_dst_pair
      FROM `mlab-collaboration.${DS}.events_with_as_and_geoloc` AS fr
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
          OR
          (fr.anomaly_ratio_upload_throughput >= 0.8
            AND fr.median_upload_throughput < fr.baseline_median_upload_throughput
            AND fr.anomaly_upload_throughput_count >= 0.5)
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
    src_group_label,
    src_city,
    src_metro,
    n_dayof,
    n_baseline,
    src_match_granularity,
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
    baseline_median_upload_throughput,
    median_daily_rtt,
    median_daily_throughput,
    median_daily_upload_throughput,
    mean_daily_rtt,
    mean_daily_throughput,
    mean_daily_upload_throughput,
    anomaly_ratio_rtt,
    anomaly_ratio_throughput,
    anomaly_ratio_upload_throughput,
    anomaly_signals,
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
    src_group_label,
    src_city,
    src_metro,
    n_dayof,
    n_baseline,
    src_match_granularity,
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
    baseline_median_upload_throughput,
    median_daily_rtt,
    median_daily_throughput,
    median_daily_upload_throughput,
    mean_daily_rtt,
    mean_daily_throughput,
    mean_daily_upload_throughput,
    anomaly_ratio_rtt,
    anomaly_ratio_throughput,
    anomaly_ratio_upload_throughput,
    anomaly_signals,
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
    src_group_label,
    src_city,
    src_metro,
    n_dayof,
    n_baseline,
    src_match_granularity,
    -- `src_country` is emitted separately. Keep the dashboard-facing state
    -- value as its resolved name (for example, `Alberta`), not `CA-Alberta`.
    src_state,
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
    baseline_median_upload_throughput,
    median_daily_rtt,
    median_daily_throughput,
    median_daily_upload_throughput,
    mean_daily_rtt,
    mean_daily_throughput,
    mean_daily_upload_throughput,
    anomaly_ratio_rtt,
    anomaly_ratio_throughput,
    anomaly_ratio_upload_throughput,
    anomaly_signals,
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
    src_group_label,
    ip_version,
    partition_date,
    dst_site,
    -- Latency anomaly flag
    CASE WHEN MAX(anomaly_ratio_rtt) >= 0.8 AND MAX(anomaly_rtt_count) > 0.5 THEN 1 ELSE 0 END AS is_latency_anomaly,
    -- Throughput anomaly flag
    CASE WHEN MAX(anomaly_ratio_throughput) >= 0.8 AND MAX(anomaly_throughput_count) > 0.5 THEN 1 ELSE 0 END AS is_throughput_anomaly,
    -- The count is zero unless step 02's 10-current/25-baseline upload gate
    -- passed, so this cannot promote a low-power upload comparison.
    CASE WHEN MAX(anomaly_ratio_upload_throughput) >= 0.8
      AND MAX(anomaly_upload_throughput_count) > 0.5
      AND MAX(median_upload_throughput) < MAX(baseline_median_upload_throughput)
      THEN 1 ELSE 0 END AS is_upload_anomaly
  FROM
    `mlab-collaboration.${DS}.events_with_as_and_geoloc`
  WHERE partition_date = '${DAY}'
  GROUP BY
    src_asn, src_group_label, ip_version, partition_date, dst_site
),
anomaly_summary AS (
  -- Aggregate anomalies for each src_asn, src_city, partition_date
  SELECT
    src_asn,
    src_group_label,
    ip_version,
    partition_date,
    COUNTIF(is_latency_anomaly = 1) AS latency_anomaly_sites,
    COUNTIF(is_throughput_anomaly = 1) AS throughput_anomaly_sites,
    COUNTIF(is_upload_anomaly = 1) AS upload_anomaly_sites,
    -- Compatibility denominator: intentionally excludes upload so historical
    -- fraction_anomalies_explained_by_edge remains comparable across rollout.
    COUNTIF(is_latency_anomaly = 1 OR is_throughput_anomaly = 1) AS total_anomalous_sites,
    COUNTIF(is_latency_anomaly = 1 OR is_throughput_anomaly = 1 OR is_upload_anomaly = 1)
      AS total_anomalous_sites_all_signals,
    COUNT(*) AS total_sites,
  FROM
    anomaly_data
  GROUP BY
    src_asn, src_group_label, ip_version, partition_date
),
combined_with_anomaly_summary AS (
  -- Combine the original query results with anomaly summary
  SELECT
    combined.*,
    summary.total_anomalous_sites,
    summary.upload_anomaly_sites,
    summary.total_anomalous_sites_all_signals,
    total_sites,
    -- Per-group path-distance extents (day-of vs baseline max forward/reverse km).
    -- Identity joins use the immutable label. src_city remains display metadata
    -- and is shortened only in final_result.
    dist.max_daily_forward_distance,
    dist.max_baseline_forward_distance,
    dist.max_daily_reverse_distance,
    dist.max_baseline_reverse_distance,
    gi.detection_granularity,
    gi.client_geo_source,
  FROM
    combined_with_AS_meta AS combined
  INNER JOIN anomaly_summary AS summary
  ON
    CAST(combined.src_asn AS INT64) = summary.src_asn
    -- Never reconstruct or join on src_city here: display-state substitution is
    -- not injective, while src_group_label is exactly what detection grouped on.
    AND combined.src_group_label = summary.src_group_label
    AND combined.ip_version = summary.ip_version
    -- AND combined.partition_date = summary.partition_date
  LEFT JOIN distance_extents AS dist
    ON CAST(dist.src_asn AS STRING) = CAST(combined.src_asn AS STRING)
    AND dist.src_group_label = combined.src_group_label
    AND dist.dst_site = combined.dst_site
    AND dist.ip_version = combined.ip_version
  LEFT JOIN group_identity AS gi
    ON CAST(gi.src_asn AS STRING) = CAST(combined.src_asn AS STRING)
    AND gi.src_group_label = combined.src_group_label
    AND gi.dst_site = combined.dst_site
    AND gi.ip_version = combined.ip_version
),
final_result AS (
  -- Add or update the information_source and source_events fields
  SELECT
    src_asn,
    -- Display city name. A city label is City-Region-CC and can be parsed
    -- from the right. A metro label is City-FullState-CC; remove the exact
    -- src_state/src_country suffix instead, because both city and state names
    -- may contain '-'.
    --
    -- SPLIT(src_city,'-')[OFFSET(0)] truncated 4.17% of names (1,261 of 30,221),
    -- collapsing Saint-Agapit and Saint-Georges both to "Saint". Right-parsing
    -- src_city instead is equally wrong the other way: Bergkamen-Nordrhein-
    -- Westfalen-DE -> "Bergkamen-Nordrhein".
    IF(
      detection_granularity = 'metro',
      IF(
        ENDS_WITH(src_group_label, CONCAT('-', src_state, '-', src_country)),
        SUBSTR(
          src_group_label,
          1,
          LENGTH(src_group_label) - LENGTH(CONCAT('-', src_state, '-', src_country))
        ),
        src_group_label
      ),
      ARRAY_TO_STRING(ARRAY(
        SELECT p FROM UNNEST(SPLIT(src_group_label, '-')) p WITH OFFSET o
        WHERE o <= ARRAY_LENGTH(SPLIT(src_group_label, '-')) - 3 ORDER BY o), '-')
    ) AS src_city,
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
    confidence_tier,
    -- Detection provenance, last (appended columns)
    detection_granularity,
    src_metro,
    -- The exact group this row describes, and its day-of measurement count.
    -- Now unambiguous: 07 keys on src_group_label, so one row == one tested
    -- population. Together these make the table joinable back to
    -- events_with_as_and_geoloc and filterable by sample size.
    src_group_label,
    n_dayof,
    src_match_granularity,
    client_geo_source,
    -- Appended after client_geo_source, matching the position ALTER TABLE ADD
    -- COLUMN gives it on the live table. Do not reorder to sit beside n_dayof.
    n_baseline,
    -- Upload parity block, appended in migration order.
    baseline_median_upload_throughput,
    median_daily_upload_throughput,
    mean_daily_upload_throughput,
    anomaly_ratio_upload_throughput,
    upload_anomaly_sites,
    total_anomalous_sites_all_signals,
    anomaly_signals
  FROM
    combined_with_anomaly_summary
)
SELECT * FROM final_result

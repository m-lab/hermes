--------------------------------------------------------------------------------
-- HERMES (union): anomaly detection only (no topology joins)
--
-- Input:  `mlab-collaboration.hermes_union.merged_download_upload`
-- Output: `mlab-collaboration.hermes_union.anomaly_counts_union`
-- Partition: partition_date = 2026-04-08
--
-- Processes IPv4 and IPv6 independently via ip_version partitioning.
--------------------------------------------------------------------------------
-- CREATE OR REPLACE TABLE `mlab-collaboration.hermes_union.anomaly_counts_union`
-- PARTITION BY partition_date
-- AS
INSERT INTO `mlab-collaboration.hermes_union.anomaly_counts_union`

WITH
--------------------------------------------------------------------------------
-- A) Find consistent IP addresses (distance + "metro_rank"), then keep them.
--------------------------------------------------------------------------------
All_Client_Locations AS (
  SELECT DISTINCT
    CONCAT(client.Geo.City, '-', client.Geo.Subdivision1ISOCode, '-', client.Geo.CountryCode) AS client_city,
    client.Geo.Latitude  AS client_lat,
    client.Geo.Longitude AS client_lon,
    ip_version
  FROM `mlab-collaboration.hermes_union.merged_download_upload`
  WHERE
    partition_date BETWEEN '${ONE_WEEK_EARLIER}' AND '${DAY}'
    AND client.Geo.Latitude IS NOT NULL
    AND client.Geo.Longitude IS NOT NULL
),

All_Server_Locations AS (
  SELECT DISTINCT
    server.Geo.City        AS server_city,
    server.Geo.Latitude    AS server_lat,
    server.Geo.Longitude   AS server_lon,
    ip_version
  FROM `mlab-collaboration.hermes_union.merged_download_upload`
  WHERE
    partition_date BETWEEN '${ONE_WEEK_EARLIER}' AND '${DAY}'
    AND server.Geo.Latitude IS NOT NULL
    AND server.Geo.Longitude IS NOT NULL
    AND server.Geo.City IS NOT NULL
),

MinDistancesPerCity AS (
  SELECT
    c.client_city,
    c.ip_version,
    MIN(
      ST_DISTANCE(
        ST_GEOGPOINT(c.client_lon, c.client_lat),
        ST_GEOGPOINT(s.server_lon, s.server_lat)
      ) / 1000
    ) AS min_gcd_km
  FROM All_Client_Locations c
  CROSS JOIN All_Server_Locations s
  WHERE c.ip_version = s.ip_version
  GROUP BY c.client_city, c.ip_version
),

DistanceCalc AS (
  SELECT
    CONCAT(client.Geo.City, '-', client.Geo.Subdivision1ISOCode, '-', client.Geo.CountryCode) AS client_city,
    client_ip,
    ip_version,
    ST_DISTANCE(
      ST_GEOGPOINT(client.Geo.Longitude, client.Geo.Latitude),
      ST_GEOGPOINT(server.Geo.Longitude, server.Geo.Latitude)
    ) / 1000 AS gcd_km,
    metro_rank,
    client_name AS client_type
  FROM `mlab-collaboration.hermes_union.merged_download_upload`
  WHERE
    partition_date BETWEEN '${ONE_WEEK_EARLIER}' AND '${DAY}'
),

FilteredIPs AS (
  SELECT
    d.client_ip,
    d.ip_version,
    md.min_gcd_km
  FROM DistanceCalc d
  JOIN MinDistancesPerCity md
    ON d.client_city = md.client_city
    AND d.ip_version = md.ip_version
  WHERE
    (
      ABS(d.gcd_km - md.min_gcd_km) < 25
      AND d.metro_rank IN ('0', '1', '2', '3')
    )
    OR (d.client_type != 'ist')
),

ConsistentIPs AS (
  SELECT DISTINCT client_ip, ip_version
  FROM FilteredIPs
),

--------------------------------------------------------------------------------
-- B) For the consistent IPs, limit each group to 40% of measurements.
--------------------------------------------------------------------------------
ConsistentSRCCounts AS (
  SELECT
    ndt.client_ip,
    ndt.ip_version,
    COUNT(*) AS cnt
  FROM `mlab-collaboration.hermes_union.merged_download_upload` ndt
  JOIN ConsistentIPs cip
    ON ndt.client_ip = cip.client_ip
   AND ndt.ip_version = cip.ip_version
  WHERE
    ndt.partition_date BETWEEN '${ONE_WEEK_EARLIER}' AND '${DAY}'
  GROUP BY ndt.client_ip, ndt.ip_version
),

AllMeasurementsForTrimming AS (
  SELECT
    client.Network.ASNumber AS src_asn,
    CONCAT(client.Geo.City, '-', client.Geo.Subdivision1ISOCode, '-', client.Geo.CountryCode) AS src_city,
    client.Geo.CountryCode AS src_country,
    server.Site AS dst_site,
    ndt.client_ip,
    ndt.ip_version,
    id AS measurement_id
  FROM `mlab-collaboration.hermes_union.merged_download_upload` ndt
  JOIN ConsistentSRCCounts csc
    ON ndt.client_ip = csc.client_ip
    AND ndt.ip_version = csc.ip_version
  WHERE
    ndt.partition_date >= '${ONE_WEEK_EARLIER}' AND ndt.partition_date < '${DAY}'
),

Enumerated AS (
  SELECT
    am.*,
    COUNT(*) OVER (PARTITION BY src_asn, src_city, dst_site, ip_version) AS group_total_measurements,
    ROW_NUMBER() OVER (
      PARTITION BY src_asn, src_city, dst_site, client_ip, ip_version
      ORDER BY (SELECT NULL)
    ) AS ip_measurement_index
  FROM AllMeasurementsForTrimming am
),

TrimmedMeasurements AS (
  SELECT
    *
  FROM Enumerated
  WHERE ip_measurement_index <= CAST(FLOOR(0.4 * group_total_measurements) AS INT64)
),

ReAgg AS (
  SELECT
    src_asn,
    src_city,
    src_country,
    dst_site,
    ip_version,
    COUNT(DISTINCT client_ip) AS unique_ip_count_per_site,
    COUNT(*) AS measurement_count_per_site,
    ARRAY_AGG(measurement_id) AS keep_ids
  FROM TrimmedMeasurements
  GROUP BY src_asn, src_city, src_country, dst_site, ip_version
),

FilteredASCityPairs AS (
  SELECT
    src_asn,
    src_city,
    dst_site,
    ip_version,
    unique_ip_count_per_site,
    measurement_count_per_site,
    measurement_id
  FROM ReAgg,
       UNNEST(keep_ids) AS measurement_id
),

--------------------------------------------------------------------------------
-- C) Baseline arrays + current arrays + statistical tests
--------------------------------------------------------------------------------
BaselineMetrics AS (
  SELECT
    ndt.date AS ndt_date,
    ndt.client.Network.ASNumber AS src_asn,
    CONCAT(ndt.client.Geo.City, '-', ndt.client.Geo.Subdivision1ISOCode, '-', ndt.client.Geo.CountryCode) AS src_city,
    ndt.client.Geo.CountryCode AS src_country,
    ndt.server.Site AS dst_site,
    ndt.server.Geo.city AS dst_city,
    ndt.server.Geo.CountryCode AS dst_country,
    ndt.server.Network.ASNumber AS dst_asn,
    ndt.client.Network.ASName AS src_asn_name,
    ndt.ip_version,
    ndt.download_min_rtt AS rtt,
    ndt.download_throughput_mbps AS median_throughput,
    ndt.upload_throughput_mbps AS upload_throughput_mbps,
    ndt.download_loss_rate AS loss_rate,
    COUNT(*) OVER (
      PARTITION BY
        ndt.client.Network.ASNumber,
        CONCAT(ndt.client.Geo.city, '-', ndt.client.Geo.Subdivision1ISOCode, '-', ndt.client.Geo.CountryCode),
        ndt.server.Site,
        ndt.ip_version
    ) AS number_of_measurements_baseline,
    COUNT(DISTINCT ndt.client_ip) OVER (
      PARTITION BY
        ndt.client.Network.ASNumber,
        CONCAT(ndt.client.Geo.city, '-', ndt.client.Geo.Subdivision1ISOCode, '-', ndt.client.Geo.CountryCode),
        ndt.server.Site,
        ndt.ip_version
    ) AS number_of_unique_src_ips_baseline
  FROM `mlab-collaboration.hermes_union.merged_download_upload` ndt
  JOIN FilteredASCityPairs f
    ON ndt.id = f.measurement_id
  WHERE
    ndt.partition_date >= '${ONE_WEEK_EARLIER}'
    AND ndt.partition_date < '${DAY}'
),


BaselineRTTArrays AS (
  SELECT
    src_asn,
    src_city,
    dst_site,
    ip_version,
    ARRAY_AGG(rtt IGNORE NULLS) AS baseline_rtt_array,
    ARRAY_AGG(median_throughput IGNORE NULLS) AS baseline_throughput_array,
    ARRAY_AGG(upload_throughput_mbps IGNORE NULLS) AS baseline_upload_throughput_array
  FROM BaselineMetrics
  GROUP BY src_asn, src_city, dst_site, ip_version
),


BaselineLossArrays AS (
  SELECT
    src_asn,
    src_city,
    dst_site,
    ip_version,
    ARRAY_AGG(loss_rate IGNORE NULLS) AS baseline_loss_array,
    ARRAY_AGG(IF(loss_rate > 0.01, loss_rate, NULL) IGNORE NULLS) AS baseline_positive_loss_array
  FROM BaselineMetrics
  GROUP BY src_asn, src_city, dst_site, ip_version
),


BaselineMetricsAggregated AS (
  SELECT DISTINCT
    src_asn,
    src_city,
    src_country,
    dst_site,
    dst_city,
    dst_country,
    dst_asn,
    src_asn_name,
    ip_version,
    TRUE AS is_consistent,
    number_of_measurements_baseline,
    number_of_unique_src_ips_baseline,

    PERCENTILE_CONT(rtt, 0.5) OVER (
      PARTITION BY src_asn, src_city, dst_site, ip_version
    ) AS baseline_median_rtt,

    AVG(rtt) OVER (
      PARTITION BY src_asn, src_city, dst_site, ip_version
    ) AS baseline_mean_rtt,

    STDDEV(rtt) OVER (
      PARTITION BY src_asn, src_city, dst_site, ip_version
    ) AS baseline_std_rtt,

    PERCENTILE_CONT(median_throughput, 0.5) OVER (
      PARTITION BY src_asn, src_city, dst_site, ip_version
    ) AS baseline_median_throughput,

    AVG(median_throughput) OVER (
      PARTITION BY src_asn, src_city, dst_site, ip_version
    ) AS baseline_mean_throughput,

    PERCENTILE_CONT(upload_throughput_mbps, 0.5) OVER (
      PARTITION BY src_asn, src_city, dst_site, ip_version
    ) AS baseline_median_upload_throughput,

    PERCENTILE_CONT(loss_rate, 0.50) OVER (
      PARTITION BY src_asn, src_city, dst_site, ip_version
    ) AS baseline_median_loss,

    PERCENTILE_CONT(loss_rate, 0.95) OVER (
      PARTITION BY src_asn, src_city, dst_site, ip_version
    ) AS baseline_p95_loss,

    AVG(CASE WHEN loss_rate > 0.01 THEN 1.0 ELSE 0.0 END) OVER (
      PARTITION BY src_asn, src_city, dst_site, ip_version
    ) AS baseline_lossy_fraction,

    SUM(CASE WHEN loss_rate > 0.01 THEN 1 ELSE 0 END) OVER (
      PARTITION BY src_asn, src_city, dst_site, ip_version
    ) AS baseline_lossy_count

  FROM BaselineMetrics
),


CurrentRTTsPerGroup AS (
  SELECT
    ndt.client.Network.ASNumber AS src_asn,
    CONCAT(ndt.client.Geo.city, '-', ndt.client.Geo.Subdivision1ISOCode, '-', ndt.client.Geo.CountryCode) AS src_city,
    ndt.server.Site AS dst_site,
    ndt.ip_version,
    ARRAY_AGG(ndt.download_min_rtt IGNORE NULLS) AS current_rtt_array,
    ARRAY_AGG(ndt.download_throughput_mbps IGNORE NULLS) AS current_throughput_array,
    ARRAY_AGG(ndt.upload_throughput_mbps IGNORE NULLS) AS current_upload_throughput_array,
    STDDEV(ndt.download_min_rtt) AS current_rtt_stddev
  FROM `mlab-collaboration.hermes_union.merged_download_upload` ndt
  JOIN ConsistentSRCCounts c
    ON ndt.client_ip = c.client_ip
   AND ndt.ip_version = c.ip_version
  JOIN ConsistentIPs cip
    ON ndt.client_ip = cip.client_ip
   AND ndt.ip_version = cip.ip_version
  WHERE ndt.partition_date = '${DAY}'
  GROUP BY src_asn, src_city, dst_site, ndt.ip_version
),

CurrentLossPerGroup AS (
  SELECT
    ndt.client.Network.ASNumber AS src_asn,
    CONCAT(
      ndt.client.Geo.city, '-', ndt.client.Geo.Subdivision1ISOCode, '-', ndt.client.Geo.CountryCode
    ) AS src_city,
    ndt.server.Site AS dst_site,
    ndt.ip_version,

    ARRAY_AGG(ndt.download_loss_rate IGNORE NULLS) AS current_loss_array,
    ARRAY_AGG(IF(ndt.download_loss_rate > 0.01, ndt.download_loss_rate, NULL) IGNORE NULLS)
      AS current_positive_loss_array,

    COUNTIF(ndt.download_loss_rate > 0.01) AS current_lossy_count,
    COUNT(ndt.download_loss_rate) AS current_loss_total
  FROM `mlab-collaboration.hermes_union.merged_download_upload` ndt
  JOIN ConsistentSRCCounts c
    ON ndt.client_ip = c.client_ip
   AND ndt.ip_version = c.ip_version
  JOIN ConsistentIPs cip
    ON ndt.client_ip = cip.client_ip
   AND ndt.ip_version = cip.ip_version
  WHERE ndt.partition_date = '${DAY}'
  GROUP BY src_asn, src_city, dst_site, ndt.ip_version
),

CombinedRTTs AS (
  SELECT
    c.src_asn,
    c.src_city,
    c.dst_site,
    c.ip_version,

    c.current_rtt_array,
    c.current_throughput_array,
    c.current_upload_throughput_array,

    cl.current_loss_array,
    cl.current_positive_loss_array,
    cl.current_lossy_count,
    cl.current_loss_total,

    b.baseline_rtt_array,
    b.baseline_throughput_array,
    b.baseline_upload_throughput_array,

    bl.baseline_loss_array,
    bl.baseline_positive_loss_array,

    c.current_rtt_stddev,

    (
      SELECT APPROX_QUANTILES(val, 100)[OFFSET(50)]
      FROM UNNEST(baseline_rtt_array) AS val
    ) AS baseline_median_rtt,

    (
      SELECT APPROX_QUANTILES(val, 100)[OFFSET(50)]
      FROM UNNEST(baseline_throughput_array) AS val
    ) AS baseline_median_throughput,

    (
      SELECT APPROX_QUANTILES(val, 100)[OFFSET(50)]
      FROM UNNEST(baseline_upload_throughput_array) AS val
    ) AS baseline_median_upload_throughput

  FROM CurrentRTTsPerGroup c
  JOIN CurrentLossPerGroup cl
    ON c.src_asn = cl.src_asn
   AND c.src_city = cl.src_city
   AND c.dst_site = cl.dst_site
   AND c.ip_version = cl.ip_version
  JOIN BaselineRTTArrays b
    ON c.src_asn = b.src_asn
   AND c.src_city = b.src_city
   AND c.dst_site = b.dst_site
   AND c.ip_version = b.ip_version
  JOIN BaselineLossArrays bl
    ON c.src_asn = bl.src_asn
   AND c.src_city = bl.src_city
   AND c.dst_site = bl.dst_site
   AND c.ip_version = bl.ip_version
),

-- SubsampledGroups AS (
--   SELECT
--     src_asn,
--     src_city,
--     dst_site,
--     ip_version,

--     IF(
--       ARRAY_LENGTH(current_rtt_array) > 25,
--       ARRAY(
--         SELECT x
--         FROM UNNEST(current_rtt_array) AS x
--         ORDER BY RAND()
--         LIMIT 25
--       ),
--       current_rtt_array
--     ) AS current_rtt_array,

--     IF(
--       ARRAY_LENGTH(current_throughput_array) > 25,
--       ARRAY(
--         SELECT x
--         FROM UNNEST(current_throughput_array) AS x
--         ORDER BY RAND()
--         LIMIT 25
--       ),
--       current_throughput_array
--     ) AS current_throughput_array,

--     IF(
--       ARRAY_LENGTH(current_upload_throughput_array) > 25,
--       ARRAY(
--         SELECT x
--         FROM UNNEST(current_upload_throughput_array) AS x
--         ORDER BY RAND()
--         LIMIT 25
--       ),
--       current_upload_throughput_array
--     ) AS current_upload_throughput_array,

--     IF(
--       ARRAY_LENGTH(baseline_rtt_array) > ,
--       ARRAY(
--         SELECT x
--         FROM UNNEST(baseline_rtt_array) AS x
--         ORDER BY RAND()
--         LIMIT 25
--       ),
--       baseline_rtt_array
--     ) AS baseline_rtt_array,

--     IF(
--       ARRAY_LENGTH(baseline_throughput_array) > 25,
--       ARRAY(
--         SELECT x
--         FROM UNNEST(baseline_throughput_array) AS x
--         ORDER BY RAND()
--         LIMIT 25
--       ),
--       baseline_throughput_array
--     ) AS baseline_throughput_array,

--     IF(
--       ARRAY_LENGTH(baseline_upload_throughput_array) > 25,
--       ARRAY(
--         SELECT x
--         FROM UNNEST(baseline_upload_throughput_array) AS x
--         ORDER BY RAND()
--         LIMIT 25
--       ),
--       baseline_upload_throughput_array
--     ) AS baseline_upload_throughput_array,

--     current_rtt_stddev,
--     baseline_median_rtt,
--     baseline_median_throughput,
--     baseline_median_upload_throughput
--   FROM CombinedRTTs
-- ),

CandidateGroups AS (
  SELECT
    *,
    (
      SELECT APPROX_QUANTILES(x, 100)[OFFSET(50)]
      FROM UNNEST(current_rtt_array) AS x
    ) AS current_median_rtt,

    (
      SELECT APPROX_QUANTILES(x, 100)[OFFSET(50)]
      FROM UNNEST(current_throughput_array) AS x
    ) AS current_median_throughput,

    (
      SELECT APPROX_QUANTILES(x, 100)[OFFSET(50)]
      FROM UNNEST(current_upload_throughput_array) AS x
    ) AS current_median_upload_throughput,

    (
      SELECT APPROX_QUANTILES(x, 100)[OFFSET(95)]
      FROM UNNEST(current_loss_array) AS x
    ) AS current_p95_loss,

    SAFE_DIVIDE(current_lossy_count, NULLIF(current_loss_total, 0)) AS current_lossy_fraction,

    (
      SELECT COUNTIF(x > 0.01)
      FROM UNNEST(baseline_loss_array) AS x
    ) AS baseline_lossy_count,

    ARRAY_LENGTH(baseline_loss_array) AS baseline_loss_total,

    SAFE_DIVIDE(
      (
        SELECT COUNTIF(x > 0.01)
        FROM UNNEST(baseline_loss_array) AS x
      ),
      NULLIF(ARRAY_LENGTH(baseline_loss_array), 0)
    ) AS baseline_lossy_fraction

  FROM CombinedRTTs
  WHERE
    ARRAY_LENGTH(current_rtt_array) > 2
    AND ARRAY_LENGTH(baseline_rtt_array) > 2
),
StatisticalTestsResults AS (
  SELECT
    src_asn,
    src_city,
    dst_site,
    ip_version,

    `hermes.mann_whitney_u_test`(current_rtt_array, baseline_rtt_array) AS mann_whitney,
    `hermes.welchs_t_test`(current_rtt_array, baseline_rtt_array) AS t_test_result,
    ARRAY_LENGTH(current_rtt_array) AS current_number_of_measurements,
    ARRAY_LENGTH(baseline_rtt_array) AS baseline_number_of_measurements,
    current_rtt_stddev,

    `hermes.mann_whitney_u_test`(
      current_throughput_array, baseline_throughput_array
    ) AS mann_whitney_throughput,

    `hermes.welchs_t_test`(
      current_throughput_array, baseline_throughput_array
    ) AS t_test_result_throughput,

    `hermes.compute_wasserstein_p_value`(
      current_throughput_array, baseline_throughput_array, 50
    ) AS wasserstein_throughput_result,

    `hermes.mann_whitney_u_test`(
      current_upload_throughput_array, baseline_upload_throughput_array
    ) AS mann_whitney_upload_throughput,

    `hermes.welchs_t_test`(
      current_upload_throughput_array, baseline_upload_throughput_array
    ) AS t_test_result_upload_throughput,

    `hermes.compute_wasserstein_p_value`(
      current_upload_throughput_array, baseline_upload_throughput_array, 50
    ) AS wasserstein_upload_throughput_result,

    -- Loss occurrence: one-sided two-proportion z-test
    baseline_lossy_count,
    baseline_loss_total,
    current_lossy_count,
    current_loss_total,
    baseline_lossy_fraction,
    current_lossy_fraction,

    SAFE_DIVIDE(
      baseline_lossy_count + current_lossy_count,
      NULLIF(baseline_loss_total + current_loss_total, 0)
    ) AS pooled_lossy_fraction,

    SAFE_DIVIDE(
      current_lossy_fraction - baseline_lossy_fraction,
      NULLIF(
        SQRT(
          SAFE_DIVIDE(
            (SAFE_DIVIDE(baseline_lossy_count + current_lossy_count,
                         NULLIF(baseline_loss_total + current_loss_total, 0)))
            *
            (1 - SAFE_DIVIDE(baseline_lossy_count + current_lossy_count,
                             NULLIF(baseline_loss_total + current_loss_total, 0))),
            1
          ) *
          (
            SAFE_DIVIDE(1.0, NULLIF(current_loss_total, 0))
            +
            SAFE_DIVIDE(1.0, NULLIF(baseline_loss_total, 0))
          )
        ),
        0
      )
    ) AS z_loss_occurrence,

    -- Optional p-value approximation thresholding can be done from z directly.
    -- For one-sided alpha=0.05, compare z > 1.645.

    -- Loss severity test: among lossy tests only
    CASE
      WHEN ARRAY_LENGTH(current_positive_loss_array) >= 5
       AND ARRAY_LENGTH(baseline_positive_loss_array) >= 5
      THEN `hermes.mann_whitney_u_test`(
        current_positive_loss_array,
        baseline_positive_loss_array
      )
      ELSE NULL
    END AS mann_whitney_loss_severity,

    SAFE_DIVIDE(
      ARRAY_LENGTH((
        SELECT ARRAY_AGG(rtt)
        FROM UNNEST(current_rtt_array) AS rtt
        WHERE rtt > baseline_median_rtt + 5
      )),
      ARRAY_LENGTH(current_rtt_array)
    ) AS anomaly_ratio_rtt,

    SAFE_DIVIDE(
      ARRAY_LENGTH((
        SELECT ARRAY_AGG(x)
        FROM UNNEST(current_throughput_array) AS x
        WHERE x < baseline_median_throughput
      )),
      ARRAY_LENGTH(current_throughput_array)
    ) AS anomaly_ratio_throughput,

    SAFE_DIVIDE(
      ARRAY_LENGTH((
        SELECT ARRAY_AGG(x)
        FROM UNNEST(current_upload_throughput_array) AS x
        WHERE x IS NOT NULL AND x < baseline_median_upload_throughput
      )),
      ARRAY_LENGTH(current_upload_throughput_array)
    ) AS anomaly_ratio_upload_throughput

  FROM CandidateGroups
),

CurrentDayAggregated AS (
  SELECT
    TRUE AS is_consistent,
    ndt.client.Network.ASNumber AS src_asn,
    ndt.client.Geo.CountryCode AS src_country,
    ndt.client.Network.ASName AS src_asn_name,
    AVG(ndt.client.Geo.Latitude) AS src_lat,
    AVG(ndt.client.Geo.Longitude) AS src_lon,
    CONCAT(
      ndt.client.Geo.city, '-', ndt.client.Geo.Subdivision1ISOCode, '-', ndt.client.Geo.CountryCode
    ) AS src_city,
    ndt.client.Geo.Subdivision1ISOCode AS src_state,
    ndt.server.Network.ASNumber AS dst_asn,
    ndt.server.Geo.CountryCode AS dst_country,
    ndt.server.Site AS dst_site,
    ndt.server.Geo.city AS dst_city,
    ndt.server.Geo.Latitude AS dst_lat,
    ndt.server.Geo.Longitude AS dst_lon,
    ndt.ip_version,
    CAST(AVG(b.number_of_unique_src_ips_baseline) AS INT64) AS unique_ip_count_per_site,
    CAST(AVG(b.number_of_measurements_baseline) AS INT64) AS measurement_count_per_site,

    CAST('${DAY}' AS TIMESTAMP) AS current_day_ts,

    APPROX_QUANTILES(ndt.download_min_rtt, 100)[OFFSET(50)] AS median_rtt,
    APPROX_QUANTILES(ndt.download_throughput_mbps, 100)[OFFSET(50)] AS median_throughput,
    APPROX_QUANTILES(ndt.upload_throughput_mbps, 100)[OFFSET(50)] AS median_upload_throughput,

    APPROX_QUANTILES(ndt.download_loss_rate, 100)[OFFSET(50)] AS median_loss_rate,
    APPROX_QUANTILES(ndt.download_loss_rate, 100)[OFFSET(95)] AS current_p95_loss,
    AVG(CASE WHEN ndt.download_loss_rate > 0.01 THEN 1.0 ELSE 0.0 END) AS current_lossy_fraction,
    COUNTIF(ndt.download_loss_rate > 0.01) AS current_lossy_count,
    COUNT(ndt.download_loss_rate) AS current_loss_total,

    AVG(ndt.download_min_rtt) AS mean_rtt,
    APPROX_QUANTILES(ndt.download_min_rtt, 100)[OFFSET(20)] AS current_20th_rtt,

    ANY_VALUE(ndt.client_name) AS client_name
  FROM `mlab-collaboration.hermes_union.merged_download_upload` ndt
  JOIN ConsistentSRCCounts c
    ON ndt.client_ip = c.client_ip
   AND ndt.ip_version = c.ip_version
  JOIN ConsistentIPs cip
    ON ndt.client_ip = cip.client_ip
   AND ndt.ip_version = cip.ip_version
  JOIN BaselineMetricsAggregated b
    ON ndt.client.Network.ASNumber = b.src_asn
   AND CONCAT(
      ndt.client.Geo.city, '-', ndt.client.Geo.Subdivision1ISOCode, '-', ndt.client.Geo.CountryCode
    ) = b.src_city
   AND ndt.server.Site = b.dst_site
   AND ndt.ip_version = b.ip_version
  WHERE ndt.partition_date = '${DAY}'
  GROUP BY
    src_asn, src_city, dst_site, dst_city, dst_asn, dst_country,
    src_country, src_asn_name, is_consistent, src_state, dst_lat, dst_lon,
    ndt.ip_version
),

DayLevelAnomaly AS (
  SELECT
    c.is_consistent,
    c.src_asn,
    c.src_country,
    c.src_asn_name,
    c.src_lat,
    c.src_lon,
    COALESCE(c.src_state, 'Unknown') AS src_state,
    c.dst_lat,
    c.dst_lon,
    c.src_city,
    c.dst_site,
    c.dst_city,
    c.dst_asn,
    c.dst_country,
    c.ip_version,

    -- Baseline stats
    b.baseline_median_rtt,
    b.baseline_mean_rtt,
    b.baseline_std_rtt,
    b.baseline_median_throughput,
    b.baseline_mean_throughput,
    b.baseline_median_upload_throughput,
    b.baseline_median_loss,
    b.baseline_p95_loss,
    b.baseline_lossy_fraction,
    b.baseline_lossy_count,
    b.number_of_measurements_baseline,
    b.number_of_unique_src_ips_baseline,

    -- Current stats
    c.median_rtt,
    c.mean_rtt,
    c.median_throughput,
    c.median_upload_throughput,
    c.median_loss_rate,
    c.current_20th_rtt,
    c.unique_ip_count_per_site,
    c.measurement_count_per_site,
    c.current_day_ts,
    c.current_p95_loss,
    c.current_lossy_fraction,
    c.current_lossy_count,
    c.current_loss_total,

    -- Tests (nullable for non-candidate groups)
    st.t_test_result AS t_test_latency,
    st.mann_whitney  AS mann_whitney_latency,
    st.mann_whitney_throughput,
    st.wasserstein_throughput_result,
    st.mann_whitney_upload_throughput,
    st.wasserstein_upload_throughput_result,
    st.mann_whitney_loss_severity,
    st.z_loss_occurrence,

    -- Ratios (nullable for non-candidate groups)
    st.anomaly_ratio_rtt,
    st.anomaly_ratio_throughput,
    st.anomaly_ratio_upload_throughput,


    -- Anomaly flags + ratios
    IF(
      st.t_test_result IS NOT NULL
      AND st.mann_whitney IS NOT NULL
      AND ((st.t_test_result.p_value < 0.05 OR st.mann_whitney.p_value < 0.05)
        AND (c.median_rtt >= b.baseline_median_rtt + 5)),
      1, 0
    ) AS anomaly_rtt,

    IF(
      st.t_test_result_throughput IS NOT NULL
      AND st.mann_whitney_throughput IS NOT NULL
      AND st.wasserstein_throughput_result IS NOT NULL
      AND st.t_test_result_throughput.p_value < 0.05
      AND st.mann_whitney_throughput.p_value < 0.05
      AND st.wasserstein_throughput_result.p_value < 0.05
      AND SAFE_DIVIDE(
            (c.median_throughput - b.baseline_median_throughput),
            NULLIF(b.baseline_median_throughput, 0)
          ) <= -0.20,
      1, 0
    ) AS anomaly_throughput,

    IF(
      st.t_test_result_upload_throughput IS NOT NULL
      AND st.mann_whitney_upload_throughput IS NOT NULL
      AND st.wasserstein_upload_throughput_result IS NOT NULL
      AND st.t_test_result_upload_throughput.p_value < 0.05
      AND st.mann_whitney_upload_throughput.p_value < 0.05
      AND st.wasserstein_upload_throughput_result.p_value < 0.05
      AND SAFE_DIVIDE(
            (c.median_upload_throughput - b.baseline_median_upload_throughput),
            NULLIF(b.baseline_median_upload_throughput, 0)
          ) <= -0.20,
      1, 0
    ) AS anomaly_upload_throughput,

    IF(
      st.z_loss_occurrence IS NOT NULL
      AND st.z_loss_occurrence > 1.645
      AND c.current_lossy_fraction >= 0.10
      AND c.current_lossy_fraction >= b.baseline_lossy_fraction + 0.10
      AND (
        st.mann_whitney_loss_severity IS NULL
        OR st.mann_whitney_loss_severity.p_value < 0.05
        OR c.current_p95_loss >= GREATEST(0.03, b.baseline_p95_loss + 0.02)
      ),
      1, 0
    ) AS anomaly_loss_ratio,

    (c.median_throughput - b.baseline_median_throughput) AS difference_throughput,
    (c.median_upload_throughput - b.baseline_median_upload_throughput) AS difference_upload_throughput,
    (c.median_rtt - b.baseline_median_rtt) AS difference_latency,

    c.client_name
  FROM CurrentDayAggregated c
  JOIN BaselineMetricsAggregated b
    ON c.src_asn  = b.src_asn
   AND c.src_city = b.src_city
   AND c.dst_site = b.dst_site
   AND c.ip_version = b.ip_version
  LEFT JOIN StatisticalTestsResults st
    ON c.src_asn  = st.src_asn
   AND c.src_city = st.src_city
   AND c.dst_site = st.dst_site
   AND c.ip_version = st.ip_version
),

--------------------------------------------------------------------------------
-- D) Group-level anomaly counts (this is what topology step joins on)
--------------------------------------------------------------------------------
AnomalyCounts AS (
  SELECT
    src_asn,
    src_country,
    src_asn_name,
    src_city,
    AVG(src_lat) AS src_lat,
    AVG(src_lon) AS src_lon,
    src_state,
    AVG(dst_lat) AS dst_lat,
    AVG(dst_lon) AS dst_lon,
    dst_site,
    dst_city,
    dst_asn,
    dst_country,
    ip_version,

    baseline_median_rtt,
    baseline_median_throughput,
    baseline_median_upload_throughput,
    baseline_median_loss,
    baseline_p95_loss,
    baseline_lossy_fraction,
    baseline_lossy_count,

    is_consistent,

    MAX(unique_ip_count_per_site) AS unique_ip_count_per_site,
    MAX(measurement_count_per_site) AS measurement_count_per_site,
    MAX(number_of_measurements_baseline) AS number_of_measurements_baseline,
    MAX(number_of_unique_src_ips_baseline) AS number_of_unique_src_ips_baseline,
    COUNT(*) AS total_group_rows,

    ANY_VALUE(anomaly_ratio_throughput) AS anomaly_ratio_throughput,
    ANY_VALUE(anomaly_ratio_upload_throughput) AS anomaly_ratio_upload_throughput,
    ANY_VALUE(anomaly_ratio_rtt) AS anomaly_ratio_rtt,
    ANY_VALUE(anomaly_loss_ratio) AS anomaly_loss_ratio,

    ANY_VALUE(difference_latency) AS difference_latency,
    ANY_VALUE(difference_throughput) AS difference_throughput,
    ANY_VALUE(difference_upload_throughput) AS difference_upload_throughput,

    ANY_VALUE(wasserstein_throughput_result) AS wasserstein_throughput_result,
    ANY_VALUE(wasserstein_upload_throughput_result) AS wasserstein_upload_throughput_result,

    ANY_VALUE(mann_whitney_latency) AS mann_whitney_latency,
    ANY_VALUE(mann_whitney_throughput) AS mann_whitney_throughput,
    ANY_VALUE(mann_whitney_upload_throughput) AS mann_whitney_upload_throughput,

    ANY_VALUE(z_loss_occurrence) AS     z_loss_occurrence,
    ANY_VALUE(mann_whitney_loss_severity) AS mann_whitney_loss_severity,

    ANY_VALUE(t_test_latency) AS t_test_latency,

    ANY_VALUE(client_name) AS client_name,

    SUM(anomaly_rtt) AS anomaly_rtt_count,
    SUM(anomaly_throughput) AS anomaly_throughput_count,
    SUM(anomaly_upload_throughput) AS anomaly_upload_throughput_count,
    SUM(anomaly_loss_ratio) AS anomaly_loss_rate_count,

    CAST('${DAY}' AS DATE) AS partition_date
  FROM DayLevelAnomaly
  GROUP BY
    src_asn,
    src_city,
    dst_site,
    dst_city,
    dst_asn,
    dst_country,
    src_country,
    ip_version,
    baseline_median_rtt,
    baseline_median_throughput,
    baseline_median_upload_throughput,
    baseline_median_loss,
    baseline_p95_loss,
    baseline_lossy_fraction,
    baseline_lossy_count,
    src_asn_name,
    src_state,
    is_consistent
)

SELECT * FROM AnomalyCounts;

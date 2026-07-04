--------------------------------------------------------------------------------
-- Example: You can uncomment and use if you want to create/replace a table
-- CREATE OR REPLACE TABLE `mlab-collaboration.hermes.transient_events_union`
-- PARTITION BY partition_date
-- AS
INSERT INTO `mlab-collaboration.hermes.transient_events_union`
--------------------------------------------------------------------------------

WITH
--------------------------------------------------------------------------------
-- A) Find consistent IP addresses (distance + "metro_rank"), then keep them.
--------------------------------------------------------------------------------
-- Upload metrics keyed by access_token (from raw.Upload.ClientMetadata).
-- We join these onto NDT download rows via raw.Download access_token.
UploadsByAccessToken AS (
  SELECT
    u.date,
    md.Value AS access_token,
    u.a.MeanThroughputMbps AS upload_throughput_mbps,
    u.a.MinRTT AS upload_min_rtt,
    u.a.LossRate AS upload_loss_rate
  FROM `measurement-lab.ndt_raw.ndt7` u
  CROSS JOIN UNNEST(u.raw.Upload.ClientMetadata) AS md
  WHERE
    u.date BETWEEN '${ONE_WEEK_EARLIER}' AND '${DAY}'
    AND u.raw.Upload IS NOT NULL
    AND md.Name = 'access_token'
),

All_Client_Locations AS (
  SELECT DISTINCT
    CONCAT(client.Geo.City, '-', client.Geo.Subdivision1ISOCode, '-', client.Geo.CountryCode) AS client_city,
    client.Geo.Latitude  AS client_lat,
    client.Geo.Longitude AS client_lon
  FROM `measurement-lab.ndt.ndt7_union`
  WHERE
    date BETWEEN '${ONE_WEEK_EARLIER}' AND '${DAY}'
    AND client.Geo.Latitude IS NOT NULL
    AND client.Geo.Longitude IS NOT NULL
),

All_Server_Locations AS (
  SELECT DISTINCT
    server.Geo.City        AS server_city,
    server.Geo.Latitude    AS server_lat,
    server.Geo.Longitude   AS server_lon
  FROM `measurement-lab.ndt.ndt7_union`
  WHERE
    date BETWEEN '${ONE_WEEK_EARLIER}' AND '${DAY}'
    AND server.Geo.Latitude IS NOT NULL
    AND server.Geo.Longitude IS NOT NULL
    AND server.Geo.City IS NOT NULL
),

MinDistancesPerCity AS (
  SELECT
    c.client_city,
    MIN(ST_DISTANCE(
          ST_GEOGPOINT(c.client_lon, c.client_lat),
          ST_GEOGPOINT(s.server_lon, s.server_lat)
        ) / 1000) AS min_gcd_km
  FROM All_Client_Locations c
  CROSS JOIN All_Server_Locations s
  GROUP BY c.client_city
),

DistanceCalc AS (
  SELECT
    CONCAT(client.Geo.City, '-', client.Geo.Subdivision1ISOCode, '-', client.Geo.CountryCode) AS client_city,
    raw.ClientIP AS client_ip,
    ST_DISTANCE(
      ST_GEOGPOINT(client.Geo.Longitude, client.Geo.Latitude),
      ST_GEOGPOINT(server.Geo.Longitude, server.Geo.Latitude)
    ) / 1000 AS gcd_km,
    cm.Value AS metro_rank,
    (
      SELECT cm2.Value
      FROM UNNEST(ndt.raw.Download.ClientMetadata) AS cm2
      WHERE cm2.Name = 'client_name'
    ) AS client_type
  FROM `measurement-lab.ndt.ndt7_union` ndt
  CROSS JOIN UNNEST(ndt.raw.Download.ClientMetadata) cm
  WHERE
    date BETWEEN '${ONE_WEEK_EARLIER}' AND '${DAY}'
    AND cm.Name = 'metro_rank'
),

FilteredIPs AS (
  SELECT
    d.client_ip,
    md.min_gcd_km
  FROM DistanceCalc d
  JOIN MinDistancesPerCity md
    ON d.client_city = md.client_city
  WHERE
    (
      ABS(d.gcd_km - md.min_gcd_km) < 250
      AND d.metro_rank IN ('0', '1', '2', '3')
    )
    OR (d.client_type != 'ist')
),

ConsistentIPs AS (
  SELECT DISTINCT client_ip
  FROM FilteredIPs
  WHERE NOT REGEXP_CONTAINS(client_ip, ':') -- exclude IPv6
),

--------------------------------------------------------------------------------
-- B) For the consistent IPs, limit each group to 40% of measurements.
--------------------------------------------------------------------------------
ConsistentSRCCounts AS (
  SELECT
    raw.ClientIP,
    COUNT(*) AS cnt
  FROM `measurement-lab.ndt.ndt7_union`
  WHERE
    date BETWEEN '${ONE_WEEK_EARLIER}' AND '${DAY}'
    AND raw.ClientIP IN (SELECT client_ip FROM ConsistentIPs)
  GROUP BY raw.ClientIP
),

AllMeasurementsForTrimming AS (
  SELECT
    ndt.client.Network.ASNumber AS src_asn,
    CONCAT(ndt.client.Geo.City, '-', ndt.client.Geo.Subdivision1ISOCode, '-', ndt.client.Geo.CountryCode) AS src_city,
    ndt.client.Geo.CountryCode AS src_country,
    ndt.server.Site AS dst_site,
    ndt.raw.ClientIP AS client_ip,
    ndt.id AS measurement_id
  FROM `measurement-lab.ndt.ndt7_union` ndt
  JOIN ConsistentSRCCounts csc
    ON ndt.raw.ClientIP = csc.ClientIP
  WHERE
    ndt.date >= '${ONE_WEEK_EARLIER}' AND ndt.date < '${DAY}'
    AND NOT REGEXP_CONTAINS(ndt.raw.ClientIP, ':') -- exclude IPv6
),

Enumerated AS (
  SELECT
    am.*,
    COUNT(*) OVER (PARTITION BY src_asn, src_city, dst_site) AS group_total_measurements,
    ROW_NUMBER() OVER (
      PARTITION BY src_asn, src_city, dst_site, client_ip
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
    COUNT(DISTINCT client_ip) AS unique_ip_count_per_site,
    COUNT(*) AS measurement_count_per_site,
    ARRAY_AGG(measurement_id) AS keep_ids
  FROM TrimmedMeasurements
  GROUP BY src_asn, src_city, src_country, dst_site
),

FilteredASCityPairs AS (
  SELECT
    src_asn,
    src_city,
    dst_site,
    unique_ip_count_per_site,
    measurement_count_per_site,
    measurement_id
  FROM ReAgg,
       UNNEST(keep_ids) AS measurement_id
  WHERE
    unique_ip_count_per_site >= 5
    AND measurement_count_per_site >= 25
),

--------------------------------------------------------------------------------
-- C) Build baseline arrays and do anomaly logic for day-level comparisons.
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
    -- Latency: we keep download MinRTT (as before) + also attach upload MinRTT.
    ndt.a.MinRTT AS download_min_rtt,
    u.upload_min_rtt AS upload_min_rtt,
    ndt.a.MinRTT AS rtt,

    -- Throughput: keep download throughput (as before) + attach upload throughput.
    ndt.a.MeanThroughputMbps AS median_throughput,
    u.upload_throughput_mbps AS upload_throughput_mbps,
    ndt.a.LossRate AS loss_rate,
    u.upload_loss_rate AS upload_loss_rate,
    COUNT(*) OVER (
      PARTITION BY
        ndt.client.Network.ASNumber,
        CONCAT(ndt.client.Geo.city, '-', ndt.client.Geo.Subdivision1ISOCode, '-', ndt.client.Geo.CountryCode),
        ndt.server.Site
    ) AS number_of_measurements_baseline,
    COUNT(DISTINCT ndt.raw.ClientIP) OVER (
      PARTITION BY
        ndt.client.Network.ASNumber,
        CONCAT(ndt.client.Geo.city, '-', ndt.client.Geo.Subdivision1ISOCode, '-', ndt.client.Geo.CountryCode),
        ndt.server.Site
    ) AS number_of_unique_src_ips_baseline
  FROM `measurement-lab.ndt.ndt7_union` ndt
  LEFT JOIN (
    SELECT
      d.date,
      md.Value AS access_token,
      ANY_VALUE(u.upload_throughput_mbps) AS upload_throughput_mbps,
      ANY_VALUE(u.upload_min_rtt) AS upload_min_rtt,
      ANY_VALUE(u.upload_loss_rate) AS upload_loss_rate
    FROM `measurement-lab.ndt.ndt7_union` d
    CROSS JOIN UNNEST(d.raw.Download.ClientMetadata) AS md
    LEFT JOIN UploadsByAccessToken u
      ON u.access_token = md.Value AND u.date = d.date
    WHERE
      d.date BETWEEN '${ONE_WEEK_EARLIER}' AND '${DAY}'
      AND d.raw.Download IS NOT NULL
      AND md.Name = 'access_token'
    GROUP BY d.date, access_token
  ) u
    ON u.date = ndt.date
   AND u.access_token = (
     SELECT cm2.Value
     FROM UNNEST(ndt.raw.Download.ClientMetadata) AS cm2
     WHERE cm2.Name = 'access_token'
     LIMIT 1
   )
  JOIN FilteredASCityPairs f
    ON ndt.id = f.measurement_id
  WHERE
    ndt.date >= '${ONE_WEEK_EARLIER}'
    AND ndt.date < '${DAY}'
    AND NOT REGEXP_CONTAINS(ndt.raw.ClientIP, ':') -- exclude IPv6
),

BaselineRTTArrays AS (
  SELECT
    src_asn,
    src_city,
    dst_site,
    ARRAY_AGG(rtt) AS baseline_rtt_array,
    ARRAY_AGG(median_throughput) AS baseline_throughput_array,
    ARRAY_AGG(upload_throughput_mbps) AS baseline_upload_throughput_array
  FROM BaselineMetrics
  GROUP BY src_asn, src_city, dst_site
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
    TRUE AS is_consistent,
    number_of_measurements_baseline,
    number_of_unique_src_ips_baseline,
    PERCENTILE_CONT(rtt, 0.5) OVER (PARTITION BY src_asn, src_city, dst_site) AS baseline_median_rtt,
    AVG(rtt)     OVER (PARTITION BY src_asn, src_city, dst_site) AS baseline_mean_rtt,
    STDDEV(rtt)  OVER (PARTITION BY src_asn, src_city, dst_site) AS baseline_std_rtt,
    PERCENTILE_CONT(median_throughput, 0.5) OVER (PARTITION BY src_asn, src_city, dst_site)
      AS baseline_median_throughput,
    AVG(median_throughput) OVER (PARTITION BY src_asn, src_city, dst_site)
      AS baseline_mean_throughput,
    PERCENTILE_CONT(upload_throughput_mbps, 0.5) OVER (PARTITION BY src_asn, src_city, dst_site)
      AS baseline_median_upload_throughput,
    PERCENTILE_CONT(loss_rate, 0.95) OVER (PARTITION BY src_asn, src_city, dst_site)
      AS baseline_loss_rate
  FROM BaselineMetrics
),

-- Current day’s MinRTT/Throughput arrays for ${DAY}
CurrentRTTsPerGroup AS (
  SELECT
    ndt.client.Network.ASNumber AS src_asn,
    CONCAT(ndt.client.Geo.city, '-', ndt.client.Geo.Subdivision1ISOCode, '-', ndt.client.Geo.CountryCode) AS src_city,
    ndt.server.Site AS dst_site,
    ARRAY_AGG(ndt.a.MinRTT) AS current_rtt_array,
    ARRAY_AGG(ndt.a.MeanThroughputMbps) AS current_throughput_array,
    ARRAY_AGG(u.upload_throughput_mbps) AS current_upload_throughput_array,
    STDDEV(ndt.a.MinRTT) AS current_rtt_stddev
  FROM `measurement-lab.ndt.ndt7_union` ndt
  LEFT JOIN (
    SELECT
      d.id,
      ANY_VALUE(u.upload_throughput_mbps) AS upload_throughput_mbps
    FROM `measurement-lab.ndt.ndt7_union` d
    CROSS JOIN UNNEST(d.raw.Download.ClientMetadata) AS md
    LEFT JOIN UploadsByAccessToken u
      ON u.access_token = md.Value AND u.date = d.date
    WHERE
      d.date = '${DAY}'
      AND d.raw.Download IS NOT NULL
      AND md.Name = 'access_token'
    GROUP BY d.id
  ) u
    ON u.id = ndt.id
  JOIN ConsistentSRCCounts c
    ON ndt.raw.ClientIP = c.ClientIP
  WHERE ndt.date = '${DAY}' AND NOT REGEXP_CONTAINS(ndt.raw.ClientIP, ':') AND ndt.raw.ClientIP IN (
    SELECT client_ip
    FROM ConsistentIPs
  )
  GROUP BY src_asn, src_city, dst_site
),

CombinedRTTs AS (
  SELECT
    c.src_asn,
    c.src_city,
    c.dst_site,
    c.current_rtt_array,
    c.current_throughput_array,
    c.current_upload_throughput_array,
    b.baseline_rtt_array,
    b.baseline_throughput_array,
    b.baseline_upload_throughput_array,
    c.current_rtt_stddev,
    (
      SELECT APPROX_QUANTILES(val, 100)[OFFSET(50)]
      FROM UNNEST(baseline_rtt_array) AS val
    ) AS baseline_median_rtt,
    (
      SELECT APPROX_QUANTILES(val, 100)[OFFSET(50)]
      FROM UNNEST(baseline_throughput_array) AS val
    ) AS baseline_median_throughput
  FROM CurrentRTTsPerGroup c
  JOIN BaselineRTTArrays b
    ON  c.src_asn  = b.src_asn
    AND c.src_city = b.src_city
    AND c.dst_site = b.dst_site
),

StatisticalTestsResults AS (
  SELECT
    src_asn,
    src_city,
    dst_site,
    `hermes.mann_whitney_u_test`(current_rtt_array, baseline_rtt_array) AS mann_whitney,
    `hermes.welchs_t_test`(current_rtt_array, baseline_rtt_array)     AS t_test_result,
    ARRAY_LENGTH(current_rtt_array)   AS current_number_of_measurements,
    ARRAY_LENGTH(baseline_rtt_array)  AS baseline_number_of_measurements,
    current_rtt_stddev,

    `hermes.mann_whitney_u_test`(current_throughput_array, baseline_throughput_array)
      AS mann_whitney_throughput,
    `hermes.welchs_t_test`(current_throughput_array, baseline_throughput_array)
      AS t_test_result_throughput,
    `hermes.compute_wasserstein_p_value`(current_throughput_array, baseline_throughput_array, 35)
      AS wasserstein_throughput_result,

    `hermes.mann_whitney_u_test`(current_upload_throughput_array, baseline_upload_throughput_array)
      AS mann_whitney_upload_throughput,
    `hermes.welchs_t_test`(current_upload_throughput_array, baseline_upload_throughput_array)
      AS t_test_result_upload_throughput,
    `hermes.compute_wasserstein_p_value`(current_upload_throughput_array, baseline_upload_throughput_array, 35)
      AS wasserstein_upload_throughput_result,

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
         WHERE x IS NOT NULL
           AND x < (
             SELECT APPROX_QUANTILES(val, 100)[OFFSET(50)]
             FROM UNNEST(baseline_upload_throughput_array) AS val
           )
      )),
      ARRAY_LENGTH(current_upload_throughput_array)
    ) AS anomaly_ratio_upload_throughput
  FROM CombinedRTTs
  WHERE
    ARRAY_LENGTH(current_rtt_array) > 2
    AND ARRAY_LENGTH(baseline_rtt_array) > 2
),

CurrentDayAggregated AS (
  SELECT
    TRUE AS is_consistent,
    ndt.client.Network.ASNumber AS src_asn,
    ndt.client.Geo.CountryCode  AS src_country,
    ndt.client.Network.ASName   AS src_asn_name,
    AVG(ndt.client.Geo.Latitude)  AS src_lat,
    AVG(ndt.client.Geo.Longitude) AS src_lon,
    CONCAT(
      ndt.client.Geo.city, '-', ndt.client.Geo.Subdivision1ISOCode, '-', ndt.client.Geo.CountryCode
    ) AS src_city,
    ndt.client.Geo.Subdivision1ISOCode AS src_state,
    ndt.server.Network.ASNumber  AS dst_asn,
    ndt.server.Geo.CountryCode   AS dst_country,
    ndt.server.Site              AS dst_site,
    ndt.server.Geo.city          AS dst_city,
    ndt.server.Geo.Latitude      AS dst_lat,
    ndt.server.Geo.Longitude     AS dst_lon,
    CAST(AVG(b.number_of_unique_src_ips_baseline) AS INT64) AS unique_ip_count_per_site,
    CAST(AVG(b.number_of_measurements_baseline)   AS INT64) AS measurement_count_per_site,

    CAST('${DAY}' AS TIMESTAMP) AS current_day_ts,

    APPROX_QUANTILES(ndt.a.MinRTT, 100)[OFFSET(50)] AS median_rtt,
    APPROX_QUANTILES(ndt.a.MeanThroughputMbps, 100)[OFFSET(50)] AS median_throughput,
    APPROX_QUANTILES(u.upload_throughput_mbps, 100)[OFFSET(50)] AS median_upload_throughput,
    APPROX_QUANTILES(ndt.a.LossRate, 100)[OFFSET(50)] AS median_loss_rate,
    AVG(ndt.a.MinRTT)  AS mean_rtt,
    APPROX_QUANTILES(ndt.a.MinRTT, 100)[OFFSET(20)] AS current_20th_rtt
  FROM `measurement-lab.ndt.ndt7_union` ndt
  LEFT JOIN (
    SELECT
      d.id,
      ANY_VALUE(u.upload_throughput_mbps) AS upload_throughput_mbps
    FROM `measurement-lab.ndt.ndt7_union` d
    CROSS JOIN UNNEST(d.raw.Download.ClientMetadata) AS md
    LEFT JOIN UploadsByAccessToken u
      ON u.access_token = md.Value AND u.date = d.date
    WHERE
      d.date = '${DAY}'
      AND d.raw.Download IS NOT NULL
      AND md.Name = 'access_token'
    GROUP BY d.id
  ) u
    ON u.id = ndt.id
  JOIN ConsistentSRCCounts c
    ON ndt.raw.ClientIP = c.ClientIP
  JOIN BaselineMetricsAggregated b
    ON ndt.client.Network.ASNumber = b.src_asn
    AND CONCAT(
      ndt.client.Geo.city, '-', ndt.client.Geo.Subdivision1ISOCode, '-', ndt.client.Geo.CountryCode
    ) = b.src_city
    AND ndt.server.Site = b.dst_site
  WHERE ndt.date = '${DAY}' AND NOT REGEXP_CONTAINS(ndt.raw.ClientIP, ':') AND ndt.raw.ClientIP IN (
    SELECT client_ip
    FROM ConsistentIPs
  )
  GROUP BY
    src_asn, src_city, dst_site, dst_city, dst_asn, dst_country,
    src_country, src_asn_name, is_consistent, src_state, dst_lat, dst_lon
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

    -- Baseline stats
    b.baseline_median_rtt,
    b.baseline_mean_rtt,
    b.baseline_std_rtt,
    b.baseline_median_throughput,
    b.baseline_mean_throughput,
    b.baseline_median_upload_throughput,
    b.baseline_loss_rate,
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

    -- Stats tests
    st.t_test_result.t_stat             AS t_statistic,
    st.t_test_result.degrees_of_freedom AS degrees_of_freedom,
    st.t_test_result.p_value            AS p_value_t_test,
    st.mann_whitney.p_value             AS whitney_p_value,
    st.current_rtt_stddev               AS current_rtt_stddev,
    st.current_number_of_measurements   AS current_number_of_measurements,
    st.baseline_number_of_measurements  AS baseline_number_of_measurements,

    st.t_test_result_throughput.p_value AS p_value_t_test_throughput,
    st.mann_whitney_throughput.p_value  AS whitney_p_value_throughput,
    st.wasserstein_throughput_result.p_value AS wasserstein_p_value_throughput,
    st.t_test_result_upload_throughput.p_value AS p_value_t_test_upload_throughput,
    st.mann_whitney_upload_throughput.p_value  AS whitney_p_value_upload_throughput,
    st.wasserstein_upload_throughput_result.p_value AS wasserstein_p_value_upload_throughput,

    -- Anomalies:
    IF(
      (
        (st.t_test_result.p_value < 0.05 OR st.mann_whitney.p_value < 0.05)
        AND (c.median_rtt >= b.baseline_median_rtt + 5)
      ),
      1,
      0
    ) AS anomaly_rtt,
    st.anomaly_ratio_rtt,

    IF(
      (
        st.t_test_result_throughput.p_value < 0.05
        AND st.mann_whitney_throughput.p_value < 0.05
        AND st.wasserstein_throughput_result.p_value < 0.05
        AND (
          (c.median_throughput - b.baseline_median_throughput)
          / NULLIF(b.baseline_median_throughput, 0)
        ) <= -0.20
      ),
      1,
      0
    ) AS anomaly_throughput,
    st.anomaly_ratio_throughput,

    IF(
      (
        st.t_test_result_upload_throughput.p_value < 0.05
        AND st.mann_whitney_upload_throughput.p_value < 0.05
        AND st.wasserstein_upload_throughput_result.p_value < 0.05
        AND (
          (c.median_upload_throughput - b.baseline_median_upload_throughput)
          / NULLIF(b.baseline_median_upload_throughput, 0)
        ) <= -0.20
      ),
      1,
      0
    ) AS anomaly_upload_throughput,
    st.anomaly_ratio_upload_throughput,

    IF(c.median_loss_rate > 0.05, 1, 0) AS anomaly_loss_ratio,

    st.t_test_result AS t_test_latency,
    st.mann_whitney  AS mann_whitney_latency,
    st.mann_whitney_throughput,
    st.wasserstein_throughput_result,
    st.mann_whitney_upload_throughput,
    st.wasserstein_upload_throughput_result,

    (c.median_throughput - b.baseline_median_throughput) AS difference_throughput,
    (c.median_upload_throughput - b.baseline_median_upload_throughput) AS difference_upload_throughput,
    (c.median_rtt - b.baseline_median_rtt)               AS difference_latency
  FROM CurrentDayAggregated c
  JOIN BaselineMetricsAggregated b
    ON c.src_asn  = b.src_asn
   AND c.src_city = b.src_city
   AND c.dst_site = b.dst_site
  JOIN StatisticalTestsResults st
    ON c.src_asn  = st.src_asn
   AND c.src_city = st.src_city
   AND c.dst_site = st.dst_site
),

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
    baseline_median_rtt,
    baseline_median_throughput,
    baseline_median_upload_throughput,
    baseline_loss_rate,
    is_consistent,
    MAX(unique_ip_count_per_site) AS unique_ip_count_per_site,
    MAX(measurement_count_per_site) AS measurement_count_per_site,
    MAX(number_of_measurements_baseline) AS number_of_measurements_baseline,
    MAX(number_of_unique_src_ips_baseline) AS number_of_unique_src_ips_baseline,
    MAX(current_number_of_measurements) AS current_number_of_measurements,
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
    ANY_VALUE(t_test_latency) AS t_test_latency,
    SUM(anomaly_rtt)         AS anomaly_rtt_count,
    SUM(anomaly_throughput)  AS anomaly_throughput_count,
    SUM(anomaly_upload_throughput) AS anomaly_upload_throughput_count,
    SUM(anomaly_loss_ratio)  AS anomaly_loss_rate_count
  FROM DayLevelAnomaly
  GROUP BY
    src_asn,
    src_city,
    dst_site,
    dst_city,
    dst_asn,
    dst_country,
    src_country,
    baseline_median_rtt,
    baseline_median_throughput,
    baseline_median_upload_throughput,
    baseline_loss_rate,
    src_asn_name,
    src_state,
    is_consistent
),

--------------------------------------------------------------------------------
-- D) City-level stats, scamper expansions, final merges
--------------------------------------------------------------------------------
ScamperDataIntermediary AS (
  SELECT
    scamper.id,
    ANY_VALUE(a.total_group_rows) AS total_windows,
    ANY_VALUE(a.anomaly_ratio_rtt)        AS anomaly_ratio_rtt,
    ANY_VALUE(a.anomaly_ratio_throughput) AS anomaly_ratio_throughput,
    ANY_VALUE(a.anomaly_ratio_upload_throughput) AS anomaly_ratio_upload_throughput,
    ANY_VALUE(a.anomaly_loss_ratio)       AS anomaly_loss_ratio,
    ANY_VALUE(a.anomaly_throughput_count) AS anomaly_throughput_count,
    ANY_VALUE(a.anomaly_upload_throughput_count) AS anomaly_upload_throughput_count,
    ANY_VALUE(a.anomaly_rtt_count)        AS anomaly_rtt_count,
    ANY_VALUE(a.difference_latency)       AS difference_latency,
    ANY_VALUE(a.difference_throughput)    AS difference_throughput,
    ANY_VALUE(a.difference_upload_throughput) AS difference_upload_throughput,
    ANY_VALUE(a.wasserstein_throughput_result) AS wasserstein_throughput_result,
    ANY_VALUE(a.wasserstein_upload_throughput_result) AS wasserstein_upload_throughput_result,
    ANY_VALUE(a.mann_whitney_latency)     AS mann_whitney_latency,
    ANY_VALUE(a.mann_whitney_throughput)  AS mann_whitney_throughput,
    ANY_VALUE(a.mann_whitney_upload_throughput)  AS mann_whitney_upload_throughput,
    ANY_VALUE(a.t_test_latency)           AS t_test_latency,
    AVG(a.baseline_median_rtt)           AS baseline_median_rtt,
    AVG(a.baseline_median_throughput)     AS baseline_median_throughput,
    AVG(a.baseline_median_upload_throughput)     AS baseline_median_upload_throughput,
    AVG(a.baseline_loss_rate)            AS baseline_loss_rate,

    a.src_city AS src_city,
    a.src_asn AS src_asn,
    AVG(a.src_lat) AS src_lat,
    a.src_state AS src_state,
    AVG(a.src_lon) AS src_lon,
    AVG(a.dst_lat) AS dst_lat,
    AVG(a.dst_lon) AS dst_lon,
    a.dst_site AS dst_site,
    a.dst_city AS dst_city,
    a.dst_country AS dst_country,
    a.dst_asn AS dst_asn,
    a.src_country AS src_country,
    a.src_asn_name AS src_asn_name,

    TIMESTAMP_TRUNC(TIMESTAMP_SECONDS(scamper.raw.Tracelb.start.sec), HOUR) AS window_start,
    MAX(a.is_consistent) AS is_consistent,
    AVG(a.number_of_measurements_baseline) AS number_of_measurements_baseline,
    AVG(a.number_of_unique_src_ips_baseline) AS number_of_unique_src_ips_baseline,
    AVG(a.unique_ip_count_per_site) AS unique_ip_count_per_site,
    AVG(a.measurement_count_per_site) AS measurement_count_per_site
  FROM `measurement-lab.ndt.scamper1` scamper
  JOIN AnomalyCounts a
    ON CONCAT(
         scamper.client.Geo.city, '-',
         scamper.client.Geo.Subdivision1ISOCode, '-',
         scamper.client.Geo.CountryCode
       ) = a.src_city
    AND scamper.client.Network.ASNumber = a.src_asn
    AND scamper.server.Site = a.dst_site
  WHERE scamper.date BETWEEN '${ONE_WEEK_EARLIER}' AND '${DAY}'
  GROUP BY
    scamper.id, a.src_city, a.src_asn, a.src_country, window_start,
    a.dst_site, a.src_state, a.dst_city, a.dst_asn, a.dst_country, a.src_asn_name
),

ScamperData AS (
  SELECT
    s.raw,
    s.raw.Tracelb.src           AS src,
    s.raw.Tracelb.dst           AS dst,
    s.raw.Tracelb.start.Sec     AS start_sec,
    s.raw.Tracelb.nodes         AS nodes,
    sc.*
  FROM `measurement-lab.ndt.scamper1` s
  JOIN ScamperDataIntermediary sc
    ON sc.id = s.id
  WHERE s.date BETWEEN '${ONE_WEEK_EARLIER}' AND '${DAY}'
),

FilteredData AS (
  SELECT
    sd.*,
    ndt.a.MinRTT            AS ndt_rtt,
    ndt.a.MeanThroughputMbps AS ndt_throughput
  FROM ScamperData sd
  JOIN `measurement-lab.ndt.ndt7_union` ndt
    ON ndt.id = sd.id
  WHERE
    NOT REGEXP_CONTAINS(sd.raw.Tracelb.src, ':') -- exclude IPv6
    AND ndt.date BETWEEN '${ONE_WEEK_EARLIER}' AND '${DAY}'
    AND sd.src_city IS NOT NULL
    AND sd.src_asn  IS NOT NULL
),

--------------------------------------------------------------------------------
-- E) Expand traceroute nodes
--------------------------------------------------------------------------------
WithoutDstNodeExtraction AS (
  SELECT *
  FROM (
    SELECT
      fd.* EXCEPT(nodes),
      node.addr                 AS addr,
      node.name                 AS rdns_name,
      replies.RTT               AS rtts,
      probe.TTL                 AS probe_ttl,
      replies.TTL               AS probe_rttl,
      probe.Flowid              AS flow_id,
      link_item.addr            AS link_addr,
      ROW_NUMBER() OVER (
        PARTITION BY fd.id, probe.TTL, node.addr  -- Key fix: use node.addr (responding IP)
        ORDER BY replies.RTT ASC
      ) AS rtt_order
    FROM FilteredData fd
    CROSS JOIN UNNEST(fd.nodes)              AS node
    CROSS JOIN UNNEST(node.links)            AS link
    CROSS JOIN UNNEST(link.Links)            AS link_item
    CROSS JOIN UNNEST(link_item.Probes)      AS probe
    CROSS JOIN UNNEST(probe.Replies)         AS replies
    WHERE probe.Flowid = 1
  )
  WHERE rtt_order = 1  -- Filter here, after window function is computed
),

LastNonStarRTT AS (
  SELECT
    w.*,
    FIRST_VALUE(rtts) OVER (PARTITION BY w.id ORDER BY rtt_order) AS traceroute_rtt
  FROM WithoutDstNodeExtraction w
  WHERE addr != '*'
),

DstCheck AS (
  SELECT id, ARRAY_AGG(DISTINCT link_addr) AS all_link_addrs
  FROM LastNonStarRTT
  GROUP BY id
),

MaxTTLPerID AS (
  SELECT id, MAX(probe_ttl) AS max_probe_ttl
  FROM LastNonStarRTT
  GROUP BY id
),

FinalNodeExtraction AS (
  SELECT
    ne.*,
    CASE
      WHEN EXISTS(
        SELECT 1
        FROM UNNEST(dc.all_link_addrs) AS addr
        WHERE addr = ne.dst
      ) THEN TRUE
      ELSE FALSE
    END AS add_dst
  FROM LastNonStarRTT ne
  LEFT JOIN DstCheck dc USING(id)
),

NodeExtraction AS (
  SELECT DISTINCT
    id,
    src,
    dst,
    src_city,
    src_asn,
    dst_site,
    dst_city,
    dst_asn,
    dst_country,
    src_country,
    src_asn_name,
    src_lat,
    src_state,
    src_lon,
    dst_lat,
    dst_lon,
    is_consistent,
    window_start,
    total_windows,
    ndt_rtt,
    ndt_throughput,
    traceroute_rtt,
    start_sec,
    addr,
    rdns_name,
    rtts,
    probe_ttl,
    probe_rttl,
    flow_id,
    link_addr,
    number_of_measurements_baseline,
    number_of_unique_src_ips_baseline,
    unique_ip_count_per_site,
    measurement_count_per_site
  FROM FinalNodeExtraction

  UNION DISTINCT

  SELECT DISTINCT
    FinalNodeExtraction.id,
    src,
    dst,
    src_city,
    src_asn,
    dst_site,
    dst_city,
    dst_asn,
    dst_country,
    src_country,
    src_asn_name,
    src_lat,
    src_state,
    src_lon,
    dst_lat,
    dst_lon,
    is_consistent,
    window_start,
    total_windows,
    ndt_rtt,
    ndt_throughput,
    traceroute_rtt,
    start_sec,
    CAST(dst AS STRING)          AS addr,
    CAST(NULL AS STRING)         AS rdns_name,
    traceroute_rtt               AS rtts,
    mt.max_probe_ttl + 1         AS probe_ttl,
    CAST(NULL AS INT64)          AS probe_rttl,
    CAST(NULL AS INT64)          AS flow_id,
    dst AS link_addr,
    number_of_measurements_baseline,
    number_of_unique_src_ips_baseline,
    unique_ip_count_per_site,
    measurement_count_per_site
  FROM FinalNodeExtraction
  LEFT JOIN MaxTTLPerID mt USING(id)
  WHERE add_dst = TRUE
),

--------------------------------------------------------------------------------
-- F) Generate TTL sequence, flatten, re-group traceroutes
--------------------------------------------------------------------------------
SequenceGenerator AS (
  SELECT
    ne.id,
    ne.src,
    ne.dst,
    ne.window_start,
    ne.ndt_rtt,
    ne.ndt_throughput,
    ne.traceroute_rtt,
    ne.total_windows,
    ne.is_consistent,
    ne.src_city,
    ne.src_asn,
    ne.dst_site,
    ne.dst_city,
    ne.dst_asn,
    ne.dst_country,
    ne.src_lat,
    ne.src_state,
    ne.src_lon,
    ne.dst_lat,
    ne.dst_lon,
    ne.src_country,
    ne.src_asn_name,
    ne.start_sec,
    GENERATE_ARRAY(1, MAX(ne.probe_ttl)) AS ttl_sequence,
    ANY_VALUE(ne.number_of_measurements_baseline)   AS number_of_measurements_baseline,
    ANY_VALUE(ne.number_of_unique_src_ips_baseline) AS number_of_unique_src_ips_baseline,
    ANY_VALUE(ne.unique_ip_count_per_site)          AS unique_ip_count_per_site,
    ANY_VALUE(ne.measurement_count_per_site)        AS measurement_count_per_site
  FROM NodeExtraction ne
  GROUP BY
    ne.id, ne.src, ne.dst, ne.window_start,
    ne.ndt_rtt, ne.ndt_throughput, ne.traceroute_rtt,
    ne.total_windows, ne.is_consistent,
    ne.src_city, ne.src_asn, ne.dst_site, ne.dst_city, ne.dst_asn,
    ne.dst_country, ne.src_lat, ne.src_state, ne.src_lon,
    ne.dst_lat, ne.dst_lon, ne.src_country, ne.src_asn_name,
    ne.start_sec
),

FlattenedSequence AS (
  SELECT
    sg.* EXCEPT(ttl_sequence),
    ttl
  FROM SequenceGenerator sg
  CROSS JOIN UNNEST(sg.ttl_sequence) AS ttl
  ORDER BY ttl
),

ExpandedNodeDetails AS (
  SELECT
    fs.id,
    fs.dst,
    fs.src,
    ANY_VALUE(fs.ndt_rtt)         AS ndt_rtt,
    ANY_VALUE(fs.window_start)    AS window_start,
    ANY_VALUE(fs.ndt_throughput)  AS ndt_throughput,
    ANY_VALUE(fs.traceroute_rtt)  AS traceroute_rtt,
    ANY_VALUE(fs.total_windows)   AS total_windows,
    ANY_VALUE(fs.is_consistent)   AS is_consistent,
    ANY_VALUE(fs.src_city)        AS src_city,
    ANY_VALUE(fs.src_lat)         AS src_lat,
    ANY_VALUE(fs.src_state)       AS src_state,
    ANY_VALUE(fs.src_lon)         AS src_lon,
    ANY_VALUE(fs.src_asn)         AS src_asn,
    ANY_VALUE(fs.dst_lat)         AS dst_lat,
    ANY_VALUE(fs.dst_lon)         AS dst_lon,
    ANY_VALUE(fs.dst_site)        AS dst_site,
    ANY_VALUE(fs.dst_city)        AS dst_city,
    ANY_VALUE(fs.dst_asn)         AS dst_asn,
    ANY_VALUE(fs.dst_country)     AS dst_country,
    ANY_VALUE(fs.src_country)     AS src_country,
    ANY_VALUE(fs.src_asn_name)    AS src_asn_name,
    ANY_VALUE(fs.start_sec)       AS start_sec,

    ARRAY_AGG(
      STRUCT(
        fs.ttl AS ttl,
        CASE WHEN fs.ttl = 1 THEN fs.src ELSE COALESCE(n.addr, '*') END AS addr,
        CASE WHEN fs.ttl = 1 THEN fs.dst_site ELSE COALESCE(n.rdns_name, '*') END AS rdns_name,
        CASE WHEN fs.ttl = 1 THEN 0.0 ELSE COALESCE(n.rtts, -1) END AS rtts
      )
      ORDER BY fs.ttl
    ) AS expanded_details,

    CASE
      WHEN ARRAY_AGG(COALESCE(n.addr, '*') ORDER BY fs.ttl DESC LIMIT 1)[SAFE_OFFSET(0)] = fs.dst
      THEN TRUE
      ELSE FALSE
    END AS reach_dest,

    AVG(fs.number_of_measurements_baseline)   AS number_of_measurements_baseline,
    AVG(fs.number_of_unique_src_ips_baseline) AS number_of_unique_src_ips_baseline,
    AVG(fs.unique_ip_count_per_site)          AS unique_ip_count_per_site,
    AVG(fs.measurement_count_per_site)        AS measurement_count_per_site
  FROM FlattenedSequence fs
  LEFT JOIN NodeExtraction n
    ON  fs.id  = n.id
    AND fs.src = n.src
    AND fs.dst = n.dst
    AND fs.ttl = n.probe_ttl
  GROUP BY fs.id, fs.dst, fs.src
),

AggregatedData AS (
  SELECT
    id,
    src  AS dst,
    dst  AS src,
    ndt_rtt,
    ndt_throughput,
    traceroute_rtt,
    total_windows,
    is_consistent,
    src_city,
    src_lat,
    src_state,
    src_lon,
    dst_lat,
    dst_lon,
    src_asn,
    dst_site,
    dst_city,
    dst_asn,
    dst_country,
    src_country,
    src_asn_name,
    start_sec AS start,
    window_start,
    reach_dest,
    expanded_details AS node_details,
    number_of_measurements_baseline,
    number_of_unique_src_ips_baseline,
    unique_ip_count_per_site,
    measurement_count_per_site
  FROM ExpandedNodeDetails
),
-- 1) Attach the test/anomaly fields from DayLevelAnomaly onto AggregatedData
AggregatedDataWithAnomalies AS (
  SELECT
    -- All columns from the traceroute expansions
    ag.*,
    -- Pull in the test results & anomaly fields from DayLevelAnomaly
    da.anomaly_ratio_rtt,
    da.anomaly_ratio_throughput,
    da.anomaly_loss_ratio,
    da.anomaly_throughput_count,
    da.anomaly_rtt_count,
    da.baseline_median_rtt,
    da.baseline_median_throughput,
    da.baseline_loss_rate,
    da.current_number_of_measurements,
    da.difference_throughput,
    da.difference_latency,
    da.t_test_latency        AS t_test_latency_struct,
    da.mann_whitney_latency  AS mann_whitney_latency_struct,
    da.mann_whitney_throughput AS mann_whitney_throughput_struct,
    da.wasserstein_throughput_result AS wasserstein_throughput_struct,
  FROM AggregatedData ag

  -- We do a LEFT JOIN so all traceroute expansions appear,
  -- even if no matching row in DayLevelAnomaly.
  LEFT JOIN AnomalyCounts da
    ON  ag.src_asn  = da.src_asn
    AND ag.src_city = da.src_city
    AND ag.dst_site = da.dst_site
),

--------------------------------------------------------------------------------
-- 2) Match the reverse‐path data if needed
--------------------------------------------------------------------------------
matched_reverse_path_on_date AS (
  SELECT
    t.*,
    ROW_NUMBER() OVER (PARTITION BY t.raw.uuid ORDER BY t.raw.date DESC) AS rn
  FROM `measurement-lab.revtr_raw.revtr1` t
  WHERE t.date BETWEEN '${ONE_WEEK_EARLIER}' AND '${DAY}'
),

filtered_reverse_path_on_date AS (
  SELECT *
  FROM matched_reverse_path_on_date
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY raw.uuid
    ORDER BY
      CASE
        WHEN raw.stop_reason = 'REACHES' THEN 1
        ELSE 2
      END,
      rn ASC
  ) = 1
),

WithReversePathData AS (
  SELECT
    -- Bring in everything from AggregatedDataWithAnomalies
    aga.* EXCEPT (node_details),
    -- Attach reverse‐path columns from revtr
    node_details,
    t2.raw.revtr_hops AS reverse_node_details,
    t2.raw.label      AS revtr_system_label,
    t2.raw.stop_reason AS revtr_stop_reason,
    t2.raw.fail_reason AS revtr_fail_reason,
    t2.raw.is_try_from_destination_AS,
    t2.raw.id AS revtr_id

  FROM filtered_reverse_path_on_date t2
  -- FULL OUTER JOIN if you want to preserve rows that appear in only one side
  FULL OUTER JOIN AggregatedDataWithAnomalies aga
    ON t2.raw.uuid = aga.id
),

--------------------------------------------------------------------------------
-- 3) City‐level percentile summary, then final join
--------------------------------------------------------------------------------
CityServerLatencyThroughputSummary AS (
  SELECT
    CONCAT(
      ndt.client.Geo.city, '-', ndt.client.Geo.Subdivision1ISOCode, '-', ndt.client.Geo.CountryCode
    ) AS src_city,
    ndt.client.Network.ASNumber AS src_asn,
    ndt.server.Site AS dst_site,
    APPROX_QUANTILES(ndt.a.MinRTT, 100)[OFFSET(1)]   AS oneth_percentile_rtt,
    APPROX_QUANTILES(ndt.a.MinRTT, 100)[OFFSET(10)]  AS tenth_percentile_rtt,
    APPROX_QUANTILES(ndt.a.MinRTT, 100)[OFFSET(50)]  AS median_rtt,
    APPROX_QUANTILES(ndt.a.MinRTT, 100)[OFFSET(90)]  AS ninetyth_percentile_rtt,

    APPROX_QUANTILES(ndt.a.MeanThroughputMbps, 100)[OFFSET(50)]
      AS median_throughput,
    APPROX_QUANTILES(ndt.a.MeanThroughputMbps, 100)[OFFSET(90)]
      AS ninetyth_percentile_throughput

  FROM `measurement-lab.ndt.ndt7_union` ndt
  WHERE ndt.date = '${DAY}' AND NOT REGEXP_CONTAINS(ndt.raw.ClientIP, ':')
  GROUP BY
    ndt.client.Geo.City, ndt.client.Network.ASNumber,
    ndt.server.Site, ndt.client.Geo.Subdivision1ISOCode, ndt.client.Geo.CountryCode
),

with_median_lat_throughput AS (
  SELECT
    aga.*,

    -- Add city-level stats
    cslts.median_rtt                       AS city_median_rtt,
    cslts.ninetyth_percentile_rtt          AS city_ninetyth_percentile_rtt,
    cslts.oneth_percentile_rtt             AS city_oneth_percentile_rtt,
    cslts.tenth_percentile_rtt             AS city_tenth_percentile_rtt,
    cslts.median_throughput                AS city_median_throughput,
    cslts.ninetyth_percentile_throughput   AS city_ninetyth_percentile_throughput,

    -- Partition date or “day” label
    CAST('${DAY}' AS DATE) AS partition_date

  FROM WithReversePathData aga
  INNER JOIN CityServerLatencyThroughputSummary cslts
    ON cslts.src_city = aga.src_city
    AND cslts.src_asn = aga.src_asn
    AND cslts.dst_site = aga.dst_site
)

SELECT
*
FROM with_median_lat_throughput
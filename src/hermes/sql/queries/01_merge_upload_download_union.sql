--------------------------------------------------------------------------------
-- HERMES (union): merge download + upload into one row per test
--
-- Output: `mlab-collaboration.hermes_union.merged_download_upload`
-- Partition: partition_date (same as ndt.date)
--
-- Notes:
-- - Reads downloads from `measurement-lab.ndt.ndt7_union`
-- - Reads uploads from `measurement-lab.ndt_raw.ndt7` (raw.Upload.*)
-- - Joins upload to download via access_token in ClientMetadata
--------------------------------------------------------------------------------
-- CREATE OR REPLACE TABLE `mlab-collaboration.hermes_union.merged_download_upload`
-- PARTITION BY partition_date
-- AS
INSERT INTO `mlab-collaboration.hermes_union.merged_download_upload`

WITH
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
    u.date = '${DAY}'
    AND u.raw.Upload IS NOT NULL
    AND md.Name = 'access_token'
),

Downloads AS (
  SELECT
    ndt.id,
    ndt.date,
    ndt.client,
    ndt.server,
    ndt.a AS download_a,
    ndt.raw.ClientIP AS client_ip,
    (
      SELECT cm.Value
      FROM UNNEST(ndt.raw.Download.ClientMetadata) AS cm
      WHERE cm.Name = 'access_token'
      LIMIT 1
    ) AS access_token,
    (
      SELECT cm.Value
      FROM UNNEST(ndt.raw.Download.ClientMetadata) AS cm
      WHERE cm.Name = 'metro_rank'
      LIMIT 1
    ) AS metro_rank,
    (
      SELECT cm.Value
      FROM UNNEST(ndt.raw.Download.ClientMetadata) AS cm
      WHERE cm.Name = 'client_name'
      LIMIT 1
    ) AS client_name
  FROM `measurement-lab.ndt.ndt7_union` ndt
  WHERE
    ndt.date = '${DAY}'
    AND ndt.raw.Download IS NOT NULL
),

UploadsCollapsed AS (
  -- Multiple upload rows can share the same access_token; collapse per day+token.
  SELECT
    date,
    access_token,
    ANY_VALUE(upload_throughput_mbps) AS upload_throughput_mbps,
    ANY_VALUE(upload_min_rtt) AS upload_min_rtt,
    ANY_VALUE(upload_loss_rate) AS upload_loss_rate
  FROM UploadsByAccessToken
  GROUP BY date, access_token
)

SELECT
  d.id,
  d.date,
  d.client,
  d.server,
  d.client_ip,
  d.access_token,
  d.metro_rank,
  d.client_name,

  -- Download metrics (same fields used elsewhere)
  d.download_a.MinRTT AS download_min_rtt,
  d.download_a.MeanThroughputMbps AS download_throughput_mbps,
  d.download_a.LossRate AS download_loss_rate,

  -- Upload metrics
  u.upload_throughput_mbps,
  u.upload_min_rtt,
  u.upload_loss_rate,

  IF(REGEXP_CONTAINS(d.client_ip, ':'), 'v6', 'v4') AS ip_version,

  CAST(d.date AS DATE) AS partition_date
FROM Downloads d
LEFT JOIN UploadsCollapsed u
  ON u.date = d.date AND u.access_token = d.access_token;


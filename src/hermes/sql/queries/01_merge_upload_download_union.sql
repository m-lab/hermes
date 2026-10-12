--------------------------------------------------------------------------------
-- HERMES (union): merge download + upload into one row per test
--
-- Output: `mlab-collaboration.${DS}.merged_download_upload`
-- Partition: partition_date (same as ndt.date)
--
-- Notes:
-- - Reads downloads AND uploads from `measurement-lab.ndt.ndt7_union`
-- - Joins upload to download via access_token in ClientMetadata
-- - Download and upload are separate ndt7 connections with DIFFERENT ids; the
--   access_token is the only link between them.
--------------------------------------------------------------------------------
-- CREATE OR REPLACE TABLE `mlab-collaboration.${DS}.merged_download_upload`
-- PARTITION BY partition_date
-- AS
INSERT INTO `mlab-collaboration.${DS}.merged_download_upload`

WITH
UploadsByAccessToken AS (
  -- Uploads come from ndt7_union, the same source as downloads. They used to be
  -- read from ndt_raw.ndt7, which lacks every upload at the `<metro><ASN>` sites
  -- (fra174, ind23372, ...): on 2026-10-06 ndt7_union held 3,586,871 uploads and
  -- ndt_raw.ndt7 2,675,616; 847,828 of the 911,255 missing ones were at those
  -- sites, which therefore showed 0% upload coverage.
  SELECT
    u.date,
    u.id AS upload_id,
    (
      SELECT cm.Value
      FROM UNNEST(u.raw.Upload.ClientMetadata) AS cm
      WHERE cm.Name = 'access_token'
      ORDER BY cm.Value
      LIMIT 1
    ) AS access_token,
    u.a.MeanThroughputMbps AS upload_throughput_mbps,
    u.a.MinRTT AS upload_min_rtt,
    u.a.LossRate AS upload_loss_rate
  FROM `measurement-lab.ndt.ndt7_union` u
  WHERE
    u.date = '${DAY}'
    AND u.raw.Upload IS NOT NULL
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
      ORDER BY cm.Value
      LIMIT 1
    ) AS access_token,
    (
      SELECT cm.Value
      FROM UNNEST(ndt.raw.Download.ClientMetadata) AS cm
      WHERE cm.Name = 'metro_rank'
      ORDER BY cm.Value
      LIMIT 1
    ) AS metro_rank,
    (
      SELECT cm.Value
      FROM UNNEST(ndt.raw.Download.ClientMetadata) AS cm
      WHERE cm.Name = 'client_name'
      ORDER BY cm.Value
      LIMIT 1
    ) AS client_name
  FROM `measurement-lab.ndt.ndt7_union` ndt
  WHERE
    ndt.date = '${DAY}'
    AND ndt.raw.Download IS NOT NULL
),

UploadsCollapsed AS (
  -- One access_token can carry several uploads (~0.2% of tokens on 2026-10-06,
  -- 20,727 uploads): a client re-running the whole test under one token, a
  -- client opening parallel connections (N uploads in the same second), or
  -- failed retries at ~0 Mbps. Keep the FASTEST upload, which is the best
  -- estimate of the client's upload capacity and skips the failed retries. The
  -- slowest was kept before, which biased upload throughput low. The trailing
  -- keys only make the choice deterministic on ties.
  SELECT
    date,
    access_token,
    ARRAY_AGG(
      STRUCT(upload_throughput_mbps, upload_min_rtt, upload_loss_rate, upload_id)
      ORDER BY upload_throughput_mbps DESC, upload_min_rtt, upload_loss_rate, upload_id
      LIMIT 1
    )[OFFSET(0)] AS selected_upload
  FROM UploadsByAccessToken
  WHERE access_token IS NOT NULL
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
  u.selected_upload.upload_throughput_mbps,
  u.selected_upload.upload_min_rtt,
  u.selected_upload.upload_loss_rate,

  IF(REGEXP_CONTAINS(d.client_ip, ':'), 'v6', 'v4') AS ip_version,

  CAST(d.date AS DATE) AS partition_date,

  -- The selected upload's own connection id. Appended last: this INSERT is
  -- positional and add_upload_id_column.sql appends the column. Step 03 joins
  -- revTr on it -- a test's upload connection carries its own revTr, which
  -- gave 194,746 tests (2026-10-06) a reverse path the download id lacked.
  u.selected_upload.upload_id
FROM Downloads d
LEFT JOIN UploadsCollapsed u
  ON u.date = d.date AND u.access_token = d.access_token;

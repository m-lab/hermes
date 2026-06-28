--------------------------------------------------------------------------------
-- HERMES (union): attach topology (scamper + revtr) to anomalies
--
-- Input:
-- - `mlab-collaboration.hermes_union.anomaly_counts_union` (partition_date = 2026-04-08)
-- - `measurement-lab.ndt.scamper1`            (MDA / multipath traceroutes)
-- - `measurement-lab.autojoin_autoload_v2_ndt.scamper2_union` (BYOS standard traceroutes)
-- - `measurement-lab.revtr_raw.revtr1`
-- - `mlab-collaboration.hermes_union.merged_download_upload` (for ndt_rtt/throughput + city percentiles)
--
-- Output: `mlab-collaboration.hermes_union.transient_events_union`
--
-- Processes IPv4 and IPv6 via ip_version column.
--------------------------------------------------------------------------------
-- CREATE OR REPLACE TABLE `mlab-collaboration.hermes_union.transient_events_union`
-- PARTITION BY partition_date
-- AS
INSERT INTO `mlab-collaboration.hermes_union.transient_events_union`

WITH
AnomalyCounts AS (
  SELECT *
  FROM `mlab-collaboration.hermes_union.anomaly_counts_union`
  WHERE partition_date = '${DAY}'
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
    AVG(a.baseline_median_loss)          AS baseline_median_loss,

    a.src_city AS src_city,
    a.src_asn AS src_asn,
    AVG(a.src_lat) AS src_lat,
    ANY_VALUE(a.src_state) AS src_state,
    AVG(a.src_lon) AS src_lon,
    AVG(a.dst_lat) AS dst_lat,
    AVG(a.dst_lon) AS dst_lon,
    a.dst_site AS dst_site,
    ANY_VALUE(a.dst_city) AS dst_city,
    ANY_VALUE(a.dst_country) AS dst_country,
    ANY_VALUE(a.dst_asn) AS dst_asn,
    ANY_VALUE(a.src_country) AS src_country,
    ANY_VALUE(a.src_asn_name) AS src_asn_name,
    ANY_VALUE(a.client_name) AS client_name,
    a.ip_version AS ip_version,

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
    AND a.ip_version = IF(REGEXP_CONTAINS(scamper.raw.Tracelb.src, ':'), 'v6', 'v4')
  WHERE scamper.date BETWEEN '${ONE_WEEK_EARLIER}' AND '${DAY}'
  GROUP BY
    scamper.id, a.src_city, a.src_asn, a.dst_site, window_start,
    a.ip_version
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
    ndt.download_min_rtt             AS ndt_rtt,
    ndt.download_throughput_mbps     AS ndt_throughput,
    ndt.download_loss_rate           AS ndt_loss_rate
  FROM ScamperData sd
  JOIN `mlab-collaboration.hermes_union.merged_download_upload` ndt
    ON ndt.id = sd.id
  WHERE
    ndt.partition_date BETWEEN '${ONE_WEEK_EARLIER}' AND '${DAY}'
    AND sd.src_city IS NOT NULL
    AND sd.src_asn  IS NOT NULL
),

--------------------------------------------------------------------------------
-- D.2) scamper2_union: BYOS standard traceroutes (parallel to D for scamper1)
--------------------------------------------------------------------------------
Scamper2DataIntermediary AS (
  SELECT
    scamper.raw.Metadata.UUID AS id,
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
    AVG(a.baseline_median_loss)          AS baseline_median_loss,

    a.src_city AS src_city,
    a.src_asn AS src_asn,
    AVG(a.src_lat) AS src_lat,
    ANY_VALUE(a.src_state) AS src_state,
    AVG(a.src_lon) AS src_lon,
    AVG(a.dst_lat) AS dst_lat,
    AVG(a.dst_lon) AS dst_lon,
    a.dst_site AS dst_site,
    ANY_VALUE(a.dst_city) AS dst_city,
    ANY_VALUE(a.dst_country) AS dst_country,
    ANY_VALUE(a.dst_asn) AS dst_asn,
    ANY_VALUE(a.src_country) AS src_country,
    ANY_VALUE(a.src_asn_name) AS src_asn_name,
    ANY_VALUE(a.client_name) AS client_name,
    a.ip_version AS ip_version,

    TIMESTAMP_TRUNC(TIMESTAMP_SECONDS(SAFE_CAST(scamper.raw.Trace.start.sec AS INT64)), HOUR) AS window_start,
    MAX(a.is_consistent) AS is_consistent,
    AVG(a.number_of_measurements_baseline) AS number_of_measurements_baseline,
    AVG(a.number_of_unique_src_ips_baseline) AS number_of_unique_src_ips_baseline,
    AVG(a.unique_ip_count_per_site) AS unique_ip_count_per_site,
    AVG(a.measurement_count_per_site) AS measurement_count_per_site
  FROM `measurement-lab.autojoin_autoload_v2_ndt.scamper2_union` scamper
  JOIN `mlab-collaboration.hermes_union.merged_download_upload` ndt
    ON ndt.id = scamper.raw.Metadata.UUID
    AND ndt.client_ip = scamper.raw.Trace.dst
    AND ndt.partition_date = CAST(scamper.date AS DATE)
    AND ndt.partition_date BETWEEN '${ONE_WEEK_EARLIER}' AND '${DAY}'
  JOIN AnomalyCounts a
    ON ndt.client.Network.ASNumber = a.src_asn
    AND ndt.server.Site = a.dst_site
    AND CONCAT(
         ndt.client.Geo.city, '-',
         ndt.client.Geo.Subdivision1ISOCode, '-',
         ndt.client.Geo.CountryCode
       ) = a.src_city
    AND a.ip_version = IF(REGEXP_CONTAINS(scamper.raw.Trace.src, ':'), 'v6', 'v4')
  WHERE scamper.date BETWEEN '${ONE_WEEK_EARLIER}' AND '${DAY}'
  GROUP BY
    scamper.raw.Metadata.UUID, a.src_city, a.src_asn, a.dst_site, window_start,
    a.ip_version
),

Scamper2Data AS (
  SELECT
    s.raw.Trace.src           AS src,
    s.raw.Trace.dst           AS dst,
    SAFE_CAST(s.raw.Trace.start.sec AS INT64) AS start_sec,
    s.raw.Trace.hops          AS hops,
    sc.*
  FROM `measurement-lab.autojoin_autoload_v2_ndt.scamper2_union` s
  JOIN Scamper2DataIntermediary sc
    ON sc.id = s.raw.Metadata.UUID
  WHERE s.date BETWEEN '${ONE_WEEK_EARLIER}' AND '${DAY}'
),

Scamper2FilteredData AS (
  SELECT
    sd.*,
    ndt.download_min_rtt             AS ndt_rtt,
    ndt.download_throughput_mbps     AS ndt_throughput,
    ndt.download_loss_rate           AS ndt_loss_rate
  FROM Scamper2Data sd
  JOIN `mlab-collaboration.hermes_union.merged_download_upload` ndt
    ON ndt.id = sd.id
    AND ndt.client_ip = sd.dst
  WHERE
    ndt.partition_date BETWEEN '${ONE_WEEK_EARLIER}' AND '${DAY}'
    AND sd.src_city IS NOT NULL
    AND sd.src_asn  IS NOT NULL
),

--------------------------------------------------------------------------------
-- E) Expand traceroute nodes — scamper1 (MDA) + scamper2 (BYOS), merged
--------------------------------------------------------------------------------
WithoutDstNodeExtraction AS (
  SELECT *
  FROM (
    SELECT
      fd.* EXCEPT(nodes, raw),
      node.addr                 AS addr,
      node.name                 AS rdns_name,
      replies.RTT               AS rtts,
      probe.TTL                 AS probe_ttl,
      replies.TTL               AS probe_rttl,
      probe.Flowid              AS flow_id,
      link_item.addr            AS link_addr,
      ROW_NUMBER() OVER (
        PARTITION BY fd.id, probe.TTL, node.addr
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
  WHERE rtt_order = 1
),

-- scamper2: flat hops (no multipath — no Flowid / link_item nesting)
Scamper2WithoutDstNodeExtraction AS (
  SELECT *
  FROM (
    SELECT
      fd.* EXCEPT(hops),
      hop.addr                              AS addr,
      NULLIF(hop.name, '')                  AS rdns_name,
      SAFE_CAST(hop.rtt AS FLOAT64)         AS rtts,
      SAFE_CAST(hop.probe_ttl AS INT64)     AS probe_ttl,
      CAST(NULL AS INT64)                   AS probe_rttl,
      1                                     AS flow_id,
      hop.addr                              AS link_addr,
      ROW_NUMBER() OVER (
        PARTITION BY fd.id, SAFE_CAST(hop.probe_ttl AS INT64), hop.addr
        ORDER BY SAFE_CAST(hop.rtt AS FLOAT64) ASC
      ) AS rtt_order
    FROM Scamper2FilteredData fd
    CROSS JOIN UNNEST(fd.hops) AS hop
    WHERE SAFE_CAST(hop.probe_ttl AS INT64) IS NOT NULL
  )
  WHERE rtt_order = 1
),

-- Merge: prefer scamper1; scamper2 only fills gaps where scamper1 has no data
Scamper1IDs AS (
  SELECT DISTINCT id FROM WithoutDstNodeExtraction
),

MergedNodeExpansion AS (
  SELECT * FROM WithoutDstNodeExtraction

  UNION ALL

  SELECT s2.*
  FROM Scamper2WithoutDstNodeExtraction s2
  LEFT JOIN Scamper1IDs s1 ON s1.id = s2.id
  WHERE s1.id IS NULL
),

LastNonStarRTT AS (
  SELECT
    w.*,
    FIRST_VALUE(rtts) OVER (PARTITION BY w.id ORDER BY probe_ttl DESC, rtts ASC) AS traceroute_rtt
  FROM MergedNodeExpansion w
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

-- Deduplicate to one row per (id, TTL).  link_addr / probe_rttl / flow_id
-- were only needed for DstCheck (destination-reach test) and are dropped here
-- to avoid multiplying hops when MDA discovers multiple next-hops or multiple
-- routers at the same TTL.
NodeExtraction AS (
  SELECT
    id, src, dst, src_city, src_asn, dst_site, dst_city, dst_asn, dst_country,
    src_country, src_asn_name, client_name, src_lat, src_state, src_lon, dst_lat, dst_lon,
    is_consistent, window_start, total_windows, ndt_rtt, ndt_throughput, ndt_loss_rate, traceroute_rtt,
    start_sec, addr, rdns_name, rtts, probe_ttl,
    number_of_measurements_baseline, number_of_unique_src_ips_baseline,
    unique_ip_count_per_site, measurement_count_per_site,
    baseline_median_rtt, baseline_median_throughput, baseline_median_upload_throughput,
    baseline_median_loss,
    anomaly_ratio_rtt, anomaly_rtt_count,
    anomaly_ratio_throughput, anomaly_throughput_count,
    anomaly_ratio_upload_throughput, anomaly_upload_throughput_count,
    anomaly_loss_ratio,
    difference_latency, difference_throughput, difference_upload_throughput,
    wasserstein_throughput_result, wasserstein_upload_throughput_result,
    mann_whitney_latency, mann_whitney_throughput, mann_whitney_upload_throughput,
    t_test_latency,
    ip_version
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY id, probe_ttl
        ORDER BY rtts ASC
      ) AS ttl_rn
    FROM FinalNodeExtraction
  )
  WHERE ttl_rn = 1

  UNION ALL

  SELECT
    FinalNodeExtraction.id,
    src, dst, src_city, src_asn, dst_site, dst_city, dst_asn, dst_country,
    src_country, src_asn_name, client_name, src_lat, src_state, src_lon, dst_lat, dst_lon,
    is_consistent, window_start, total_windows, ndt_rtt, ndt_throughput, ndt_loss_rate, traceroute_rtt,
    start_sec,
    CAST(dst AS STRING)          AS addr,
    CAST(NULL AS STRING)         AS rdns_name,
    traceroute_rtt               AS rtts,
    mt.max_probe_ttl + 1         AS probe_ttl,
    number_of_measurements_baseline, number_of_unique_src_ips_baseline,
    unique_ip_count_per_site, measurement_count_per_site,
    baseline_median_rtt, baseline_median_throughput, baseline_median_upload_throughput,
    baseline_median_loss,
    anomaly_ratio_rtt, anomaly_rtt_count,
    anomaly_ratio_throughput, anomaly_throughput_count,
    anomaly_ratio_upload_throughput, anomaly_upload_throughput_count,
    anomaly_loss_ratio,
    difference_latency, difference_throughput, difference_upload_throughput,
    wasserstein_throughput_result, wasserstein_upload_throughput_result,
    mann_whitney_latency, mann_whitney_throughput, mann_whitney_upload_throughput,
    t_test_latency,
    ip_version
  FROM FinalNodeExtraction
  LEFT JOIN MaxTTLPerID mt USING(id)
  WHERE add_dst = TRUE
),

SequenceGenerator AS (
  SELECT
    ne.id, ne.src, ne.dst,
    ANY_VALUE(ne.window_start) AS window_start,
    ANY_VALUE(ne.ndt_rtt) AS ndt_rtt,
    ANY_VALUE(ne.ndt_throughput) AS ndt_throughput,
    ANY_VALUE(ne.ndt_loss_rate) AS ndt_loss_rate,
    ANY_VALUE(ne.traceroute_rtt) AS traceroute_rtt,
    ANY_VALUE(ne.total_windows) AS total_windows,
    MAX(ne.is_consistent) AS is_consistent,
    ANY_VALUE(ne.src_city) AS src_city,
    ANY_VALUE(ne.src_asn) AS src_asn,
    ANY_VALUE(ne.dst_site) AS dst_site,
    ANY_VALUE(ne.dst_city) AS dst_city,
    ANY_VALUE(ne.dst_asn) AS dst_asn,
    ANY_VALUE(ne.dst_country) AS dst_country,
    ANY_VALUE(ne.src_lat) AS src_lat,
    ANY_VALUE(ne.src_state) AS src_state,
    ANY_VALUE(ne.src_lon) AS src_lon,
    ANY_VALUE(ne.dst_lat) AS dst_lat,
    ANY_VALUE(ne.dst_lon) AS dst_lon,
    ANY_VALUE(ne.src_country) AS src_country,
    ANY_VALUE(ne.src_asn_name) AS src_asn_name,
    ANY_VALUE(ne.client_name) AS client_name,
    ANY_VALUE(ne.start_sec) AS start_sec,
    ne.ip_version AS ip_version,
    GENERATE_ARRAY(1, MAX(ne.probe_ttl)) AS ttl_sequence,
    ANY_VALUE(ne.number_of_measurements_baseline)   AS number_of_measurements_baseline,
    ANY_VALUE(ne.number_of_unique_src_ips_baseline) AS number_of_unique_src_ips_baseline,
    ANY_VALUE(ne.unique_ip_count_per_site)          AS unique_ip_count_per_site,
    ANY_VALUE(ne.measurement_count_per_site)        AS measurement_count_per_site,
    ANY_VALUE(ne.baseline_median_rtt)               AS baseline_median_rtt,
    ANY_VALUE(ne.baseline_median_throughput)         AS baseline_median_throughput,
    ANY_VALUE(ne.baseline_median_upload_throughput)  AS baseline_median_upload_throughput,
    ANY_VALUE(ne.baseline_median_loss)              AS baseline_median_loss,
    ANY_VALUE(ne.anomaly_ratio_rtt)                 AS anomaly_ratio_rtt,
    ANY_VALUE(ne.anomaly_rtt_count)                 AS anomaly_rtt_count,
    ANY_VALUE(ne.anomaly_ratio_throughput)           AS anomaly_ratio_throughput,
    ANY_VALUE(ne.anomaly_throughput_count)           AS anomaly_throughput_count,
    ANY_VALUE(ne.anomaly_ratio_upload_throughput)    AS anomaly_ratio_upload_throughput,
    ANY_VALUE(ne.anomaly_upload_throughput_count)    AS anomaly_upload_throughput_count,
    ANY_VALUE(ne.anomaly_loss_ratio)                AS anomaly_loss_ratio,
    ANY_VALUE(ne.difference_latency)                AS difference_latency,
    ANY_VALUE(ne.difference_throughput)              AS difference_throughput,
    ANY_VALUE(ne.difference_upload_throughput)       AS difference_upload_throughput,
    ANY_VALUE(ne.wasserstein_throughput_result)      AS wasserstein_throughput_result,
    ANY_VALUE(ne.wasserstein_upload_throughput_result) AS wasserstein_upload_throughput_result,
    ANY_VALUE(ne.mann_whitney_latency)              AS mann_whitney_latency,
    ANY_VALUE(ne.mann_whitney_throughput)            AS mann_whitney_throughput,
    ANY_VALUE(ne.mann_whitney_upload_throughput)     AS mann_whitney_upload_throughput,
    ANY_VALUE(ne.t_test_latency)                    AS t_test_latency
  FROM NodeExtraction ne
  GROUP BY ne.id, ne.src, ne.dst, ne.ip_version
),

FlattenedSequence AS (
  SELECT
    sg.* EXCEPT(ttl_sequence),
    ttl
  FROM SequenceGenerator sg
  CROSS JOIN UNNEST(sg.ttl_sequence) AS ttl
  ORDER BY ttl
),

NodeExtractionOnePerTTL AS (
  SELECT * EXCEPT(_dedup)
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY id, src, dst, probe_ttl
        ORDER BY
          CASE WHEN addr = '*' THEN 1 ELSE 0 END,
          rtts ASC,
          addr ASC
      ) AS _dedup
    FROM NodeExtraction
  )
  WHERE _dedup = 1
),

ExpandedRows AS (
  SELECT
    fs.*,

    CASE
      WHEN fs.ttl = 1 THEN fs.src
      ELSE COALESCE(n.addr, '*')
    END AS hop_addr,

    CASE
      WHEN fs.ttl = 1 THEN fs.dst_site
      ELSE COALESCE(n.rdns_name, '*')
    END AS hop_rdns_name,

    CASE
      WHEN fs.ttl = 1 THEN 0.0
      ELSE COALESCE(n.rtts, -1)
    END AS hop_rtts

  FROM FlattenedSequence fs
  LEFT JOIN NodeExtractionOnePerTTL n
    ON  fs.id  = n.id
    AND fs.src = n.src
    AND fs.dst = n.dst
    AND fs.ttl = n.probe_ttl
),

ExpandedRowsDeduped AS (
  SELECT * EXCEPT(_rn)
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY id, src, dst, ttl
        ORDER BY
          CASE WHEN hop_addr = '*' THEN 1 ELSE 0 END,
          hop_rtts ASC,
          hop_addr ASC
      ) AS _rn
    FROM ExpandedRows
  )
  WHERE _rn = 1
),

ExpandedNodeDetails AS (
  SELECT
    er.id,
    er.dst,
    er.src,

    ANY_VALUE(er.ndt_rtt)         AS ndt_rtt,
    ANY_VALUE(er.window_start)    AS window_start,
    ANY_VALUE(er.ndt_throughput)  AS ndt_throughput,
    ANY_VALUE(er.ndt_loss_rate)  AS ndt_loss_rate,
    ANY_VALUE(er.traceroute_rtt)  AS traceroute_rtt,
    ANY_VALUE(er.total_windows)   AS total_windows,
    ANY_VALUE(er.is_consistent)   AS is_consistent,

    ANY_VALUE(er.src_city)        AS src_city,
    ANY_VALUE(er.src_lat)         AS src_lat,
    ANY_VALUE(er.src_state)       AS src_state,
    ANY_VALUE(er.src_lon)         AS src_lon,
    ANY_VALUE(er.src_asn)         AS src_asn,

    ANY_VALUE(er.dst_lat)         AS dst_lat,
    ANY_VALUE(er.dst_lon)         AS dst_lon,
    ANY_VALUE(er.dst_site)        AS dst_site,
    ANY_VALUE(er.dst_city)        AS dst_city,
    ANY_VALUE(er.dst_asn)         AS dst_asn,
    ANY_VALUE(er.dst_country)     AS dst_country,

    ANY_VALUE(er.src_country)     AS src_country,
    ANY_VALUE(er.src_asn_name)    AS src_asn_name,
    ANY_VALUE(er.client_name)     AS client_name,
    ANY_VALUE(er.start_sec)       AS start_sec,
    er.ip_version,

    ANY_VALUE(er.baseline_median_rtt)              AS baseline_median_rtt,
    ANY_VALUE(er.baseline_median_throughput)        AS baseline_median_throughput,
    ANY_VALUE(er.baseline_median_upload_throughput) AS baseline_median_upload_throughput,
    ANY_VALUE(er.baseline_median_loss)             AS baseline_median_loss,

    ANY_VALUE(er.anomaly_ratio_rtt)                AS anomaly_ratio_rtt,
    ANY_VALUE(er.anomaly_rtt_count)                AS anomaly_rtt_count,
    ANY_VALUE(er.anomaly_ratio_throughput)          AS anomaly_ratio_throughput,
    ANY_VALUE(er.anomaly_throughput_count)          AS anomaly_throughput_count,
    ANY_VALUE(er.anomaly_ratio_upload_throughput)   AS anomaly_ratio_upload_throughput,
    ANY_VALUE(er.anomaly_upload_throughput_count)   AS anomaly_upload_throughput_count,
    ANY_VALUE(er.anomaly_loss_ratio)               AS anomaly_loss_ratio,
    ANY_VALUE(er.difference_latency)               AS difference_latency,
    ANY_VALUE(er.difference_throughput)             AS difference_throughput,
    ANY_VALUE(er.difference_upload_throughput)      AS difference_upload_throughput,
    ANY_VALUE(er.wasserstein_throughput_result)     AS wasserstein_throughput_result,
    ANY_VALUE(er.wasserstein_upload_throughput_result) AS wasserstein_upload_throughput_result,
    ANY_VALUE(er.mann_whitney_latency)             AS mann_whitney_latency,
    ANY_VALUE(er.mann_whitney_throughput)           AS mann_whitney_throughput,
    ANY_VALUE(er.mann_whitney_upload_throughput)    AS mann_whitney_upload_throughput,
    ANY_VALUE(er.t_test_latency)                   AS t_test_latency,

    ARRAY_AGG(
      STRUCT(
        er.ttl AS ttl,
        er.hop_addr AS addr,
        er.hop_rdns_name AS rdns_name,
        er.hop_rtts AS rtts
      )
      ORDER BY er.ttl
    ) AS expanded_details,

    ARRAY_AGG(er.hop_addr ORDER BY er.ttl DESC LIMIT 1)[SAFE_OFFSET(0)] = er.dst AS reach_dest,

    AVG(er.number_of_measurements_baseline)   AS number_of_measurements_baseline,
    AVG(er.number_of_unique_src_ips_baseline) AS number_of_unique_src_ips_baseline,
    AVG(er.unique_ip_count_per_site)          AS unique_ip_count_per_site,
    AVG(er.measurement_count_per_site)        AS measurement_count_per_site

  FROM ExpandedRowsDeduped er
  GROUP BY er.id, er.dst, er.src, er.ip_version
),

AggregatedData AS (
  SELECT
    id,
    src  AS dst,
    dst  AS src,
    ndt_rtt,
    ndt_throughput,
    ndt_loss_rate,
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
    client_name,
    start_sec AS start,
    window_start,
    reach_dest,
    expanded_details AS node_details,
    number_of_measurements_baseline,
    number_of_unique_src_ips_baseline,
    unique_ip_count_per_site,
    measurement_count_per_site,
    baseline_median_rtt,
    baseline_median_throughput,
    baseline_median_upload_throughput,
    baseline_median_loss,
    ip_version,
    anomaly_ratio_rtt,
    anomaly_rtt_count,
    anomaly_ratio_throughput,
    anomaly_throughput_count,
    anomaly_ratio_upload_throughput,
    anomaly_upload_throughput_count,
    anomaly_loss_ratio,
    difference_latency,
    difference_throughput,
    difference_upload_throughput,
    wasserstein_throughput_result,
    wasserstein_upload_throughput_result,
    mann_whitney_latency,
    mann_whitney_throughput,
    mann_whitney_upload_throughput,
    t_test_latency,
    (baseline_median_upload_throughput + difference_upload_throughput) AS median_upload_throughput
  FROM ExpandedNodeDetails
),

--------------------------------------------------------------------------------
-- 2) Match the reverse-path data (revtr)
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
      CASE WHEN raw.stop_reason = 'REACHES' THEN 1 ELSE 2 END,
      rn ASC
  ) = 1
),

WithReversePathData AS (
  SELECT
    ag.*,
    t2.raw.revtr_hops AS reverse_node_details,
    t2.raw.label      AS revtr_system_label,
    t2.raw.stop_reason AS revtr_stop_reason,
    t2.raw.fail_reason AS revtr_fail_reason,
    t2.raw.is_try_from_destination_AS,
    t2.raw.id AS revtr_id
  FROM filtered_reverse_path_on_date t2
  FULL OUTER JOIN AggregatedData ag
    ON t2.raw.uuid = ag.id
),

--------------------------------------------------------------------------------
-- 3) City-level percentile summary, then final join
--------------------------------------------------------------------------------
CityServerLatencyThroughputSummary AS (
  SELECT
    CONCAT(
      ndt.client.Geo.city, '-', ndt.client.Geo.Subdivision1ISOCode, '-', ndt.client.Geo.CountryCode
    ) AS src_city,
    ndt.client.Network.ASNumber AS src_asn,
    ndt.server.Site AS dst_site,
    ndt.ip_version,
    APPROX_QUANTILES(ndt.download_min_rtt, 100)[OFFSET(1)]   AS oneth_percentile_rtt,
    APPROX_QUANTILES(ndt.download_min_rtt, 100)[OFFSET(10)]  AS tenth_percentile_rtt,
    APPROX_QUANTILES(ndt.download_min_rtt, 100)[OFFSET(50)]  AS median_rtt,
    APPROX_QUANTILES(ndt.download_min_rtt, 100)[OFFSET(90)]  AS ninetyth_percentile_rtt,

    APPROX_QUANTILES(ndt.download_throughput_mbps, 100)[OFFSET(50)] AS median_throughput,
    APPROX_QUANTILES(ndt.download_throughput_mbps, 100)[OFFSET(90)] AS ninetyth_percentile_throughput
  FROM `mlab-collaboration.hermes_union.merged_download_upload` ndt
  WHERE ndt.partition_date = '${DAY}'
  GROUP BY
    ndt.client.Geo.City, ndt.client.Network.ASNumber,
    ndt.server.Site, ndt.client.Geo.Subdivision1ISOCode, ndt.client.Geo.CountryCode,
    ndt.ip_version
),

with_median_lat_throughput AS (
  SELECT
    aga.* EXCEPT(
      anomaly_ratio_rtt, anomaly_rtt_count,
      anomaly_ratio_throughput, anomaly_throughput_count,
      anomaly_ratio_upload_throughput, anomaly_upload_throughput_count,
      anomaly_loss_ratio,
      difference_latency, difference_throughput, difference_upload_throughput,
      wasserstein_throughput_result, wasserstein_upload_throughput_result,
      mann_whitney_latency, mann_whitney_throughput, mann_whitney_upload_throughput,
      t_test_latency,
      median_upload_throughput
    ),

    cslts.median_rtt                       AS city_median_rtt,
    cslts.ninetyth_percentile_rtt          AS city_ninetyth_percentile_rtt,
    cslts.oneth_percentile_rtt             AS city_oneth_percentile_rtt,
    cslts.tenth_percentile_rtt             AS city_tenth_percentile_rtt,
    cslts.median_throughput                AS city_median_throughput,
    cslts.ninetyth_percentile_throughput   AS city_ninetyth_percentile_throughput,

    CAST('${DAY}' AS DATE) AS partition_date,

    -- Anomaly fields last (matches ALTER TABLE append order in transient_events_union)
    aga.anomaly_ratio_rtt,
    aga.anomaly_rtt_count,
    aga.anomaly_ratio_throughput,
    aga.anomaly_throughput_count,
    aga.anomaly_ratio_upload_throughput,
    aga.anomaly_upload_throughput_count,
    aga.anomaly_loss_ratio,
    aga.difference_latency,
    aga.difference_throughput,
    aga.difference_upload_throughput,
    aga.wasserstein_throughput_result,
    aga.wasserstein_upload_throughput_result,
    aga.mann_whitney_latency,
    aga.mann_whitney_throughput,
    aga.mann_whitney_upload_throughput,
    aga.t_test_latency,
    aga.median_upload_throughput
  FROM WithReversePathData aga
  INNER JOIN CityServerLatencyThroughputSummary cslts
    ON cslts.src_city = aga.src_city
    AND cslts.src_asn = aga.src_asn
    AND cslts.dst_site = aga.dst_site
    AND cslts.ip_version = aga.ip_version
)

SELECT * FROM with_median_lat_throughput;

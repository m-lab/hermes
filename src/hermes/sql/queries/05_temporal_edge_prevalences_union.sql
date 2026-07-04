-- 05_temporal_edge_prevalences_union.sql
-- Per (group, direction, edge): prevalence in the usual (healthy) route U vs the
-- day-of anomalous route D, with per-hop RTT for congestion localization.
-- Parameters: ${DAY}
WITH meas AS (
  SELECT
    id,
    CONCAT(src_asn, ' - ', src_city, ' - ', dst_site) AS src_dst_pair,
    ip_version,
    DATE(window_start) >= '${DAY}' AS is_day,
    (
      (anomaly_ratio_rtt >= 0.8 AND ndt_rtt > baseline_median_rtt + 5 AND anomaly_rtt_count >= 0.5)
      OR (anomaly_ratio_throughput >= 0.8 AND ndt_throughput < baseline_median_throughput AND anomaly_throughput_count >= 0.5)
    ) AS is_anomaly,
    forward_updated_node_details AS fwd,
    reverse_updated_node_details AS rev
  FROM `mlab-collaboration.hermes_union.events_with_as_and_geoloc`
  WHERE partition_date = '${DAY}' AND DATE(window_start) >= partition_date - 7
),
-- one ordered node list + per-node rtt per (measurement, direction)
hops AS (
  -- Canonicalize each hop's place to the polygon metro (lat/lon-derived) so a place
  -- isn't split by ISO2-vs-full region naming across geo sources.
  SELECT id, src_dst_pair, ip_version, is_day, is_anomaly, 'forward' AS direction,
    n.ttl, IFNULL(CONCAT(n.associated_asn, '-', COALESCE(al.canon_metro, n.place)), '*') AS node, n.rtts
  FROM meas, UNNEST(fwd) AS n
  LEFT JOIN `mlab-collaboration.hermes_union.place_canonical_metro` al ON al.place = n.place
  UNION ALL
  SELECT id, src_dst_pair, ip_version, is_day, is_anomaly, 'reverse' AS direction,
    n.ttl, IFNULL(CONCAT(n.associated_asn, '-', COALESCE(al.canon_metro, n.place)), '*') AS node, n.rtts
  FROM meas, UNNEST(rev) AS n
  LEFT JOIN `mlab-collaboration.hermes_union.place_canonical_metro` al ON al.place = n.place
),
ordered AS (
  SELECT id, src_dst_pair, ip_version, is_day, is_anomaly, direction, node, rtts,
    ROW_NUMBER() OVER (PARTITION BY id, direction ORDER BY ttl) AS rn
  FROM hops
),
edges AS (   -- consecutive resolved hops -> an edge, tagged U (healthy) or D (day-of anomalous)
  SELECT a.src_dst_pair, a.ip_version, a.direction, a.id,
    CONCAT(a.node, '-', b.node) AS edge, b.rtts AS dst_rtt,
    (NOT a.is_anomaly) AS in_u,
    (a.is_anomaly AND a.is_day) AS in_d
  FROM ordered a JOIN ordered b
    ON a.id = b.id AND a.direction = b.direction AND b.rn = a.rn + 1
  WHERE a.node != '*' AND b.node != '*'
),
totals AS (   -- per group/direction: # distinct healthy and day-of measurements
  SELECT src_dst_pair, ip_version, direction,
    COUNT(DISTINCT IF(NOT is_anomaly, id, NULL)) AS healthy_n,
    COUNT(DISTINCT IF(is_anomaly AND is_day, id, NULL)) AS dayof_n
  FROM ordered GROUP BY src_dst_pair, ip_version, direction
),
agg AS (
  SELECT src_dst_pair, ip_version, direction, edge,
    COUNT(DISTINCT IF(in_u, id, NULL)) AS u_n,
    COUNT(DISTINCT IF(in_d, id, NULL)) AS d_n,
    AVG(IF(in_u, dst_rtt, NULL)) AS base_hop_rtt,
    AVG(IF(in_d, dst_rtt, NULL)) AS day_hop_rtt
  FROM edges GROUP BY src_dst_pair, ip_version, direction, edge
)
SELECT a.src_dst_pair, a.ip_version, a.direction, a.edge,
  SAFE_DIVIDE(a.u_n, t.healthy_n) AS prev_u,
  SAFE_DIVIDE(a.d_n, t.dayof_n)  AS prev_d,
  a.base_hop_rtt, a.day_hop_rtt, t.healthy_n, t.dayof_n
FROM agg a JOIN totals t USING (src_dst_pair, ip_version, direction)
WHERE t.dayof_n > 0;

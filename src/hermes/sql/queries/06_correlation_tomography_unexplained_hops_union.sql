-- correlation_tomography_unexplained_hops.sql
-- Per-hop latency flags for single-path (path-local) attribution. Parameters: ${DAY}
WITH base AS (
  SELECT id,
    CONCAT(src_asn, ' - ', src_city, ' - ', dst_site) AS src_dst_pair,
    forward_updated_node_details AS fwd,
    reverse_updated_node_details AS rev
  FROM `mlab-collaboration.hermes_union.events_with_as_and_geoloc`
  WHERE partition_date = '${DAY}' AND DATE(window_start) >= partition_date
    -- Only anomalous measurements: path-local attribution targets the day's
    -- anomalous groups, and this keeps the hop download bounded (otherwise it
    -- unnests every measurement's path — tens of millions of rows).
    AND (
      (anomaly_ratio_rtt >= 0.8 AND ndt_rtt > baseline_median_rtt + 5 AND anomaly_rtt_count >= 0.5)
      OR (anomaly_ratio_throughput >= 0.8 AND ndt_throughput < baseline_median_throughput AND anomaly_throughput_count >= 0.5)
      OR (anomaly_ratio_upload_throughput >= 0.8 AND median_upload_throughput < baseline_median_upload_throughput AND anomaly_upload_throughput_count >= 0.5)
    )
)
-- Canonicalize each hop's place to the polygon metro (same as the prepare SQL) so
-- path-local edges aren't split by ISO2-vs-full region naming (e.g. 'Dallas-TX-US'
-- vs 'Dallas-Texas-US'). The UNNEST + place_canonical_metro join is already flat
-- here, so no de-correlation gymnastics are needed.
SELECT id, src_dst_pair, 'forward' AS information_source,
  n.ttl,
  IFNULL(CONCAT(n.associated_asn, '-', COALESCE(n.metro, al.canon_metro, n.place)), '*') AS asn_metro,
  n.above_baseline_flag, n.increasing_latency_flag, n.distance_rtt_check, n.rtts
FROM base, UNNEST(fwd) AS n
LEFT JOIN `mlab-collaboration.hermes_union.place_canonical_metro` al ON al.place = n.place
UNION ALL
SELECT id, src_dst_pair, 'reverse' AS information_source,
  n.ttl,
  IFNULL(CONCAT(n.associated_asn, '-', COALESCE(n.metro, al.canon_metro, n.place)), '*') AS asn_metro,
  n.above_baseline_flag, n.increasing_latency_flag, n.distance_rtt_check, n.rtts
FROM base, UNNEST(rev) AS n
LEFT JOIN `mlab-collaboration.hermes_union.place_canonical_metro` al ON al.place = n.place;

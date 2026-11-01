--------------------------------------------------------------------------------
-- HERMES (union): Correlation tomography — Phase 1: Extract edges
--
-- Script that scans events_with_as_and_geoloc ONCE into temp tables, then
-- returns all precomputed edges as the final SELECT (downloaded by Python).
-- Group identity follows docs/proposals/2026-08-group-granularity.md.
--
-- Parameters: ${DAY}
--------------------------------------------------------------------------------

-- 1. Base events (broad filter)
CREATE TEMP TABLE _base_events AS
SELECT *
FROM `mlab-collaboration.${DS}.events_with_as_and_geoloc`
WHERE partition_date = '${DAY}'
  AND DATE(window_start) >= partition_date
  AND NOT EXISTS (
    SELECT 1
    FROM UNNEST(reverse_updated_node_details) AS node
    WHERE node.is_interdomain_symmetry = TRUE OR node.is_fishy_type_4 = TRUE
  )
  AND NOT EXISTS (
    SELECT 1
    FROM UNNEST(forward_updated_node_details) AS node
    WHERE node.distance_rtt_check = 'Above threshold'
  );

-- 2. Strict filter for anomaly classification: drop a measurement whose path is
-- longer than its RTT allows (distance/100 km per ms > ndt_rtt). Only a comparison
-- that is TRUE drops it. A bare `NOT x > ndt_rtt` is NULL when ndt_rtt (or the
-- distance) is NULL, and WHERE NULL dropped every measurement without a download
-- RTT -- 58,643 rows/day, 648 of them throughput-anomalous -- though nothing here
-- contradicts them.
CREATE TEMP TABLE final_results AS
SELECT *
FROM _base_events
WHERE NOT IFNULL(forward_distance / 100 > ndt_rtt, FALSE)
  AND NOT IFNULL(reverse_distance / 100 > ndt_rtt, FALSE);

-- 3. Extract ordered AS paths
-- Canonicalize each hop's place to the polygon metro (lat/lon-derived) so the same
-- physical place isn't split by ISO2-vs-full region naming across geo sources
-- (e.g. 'Miami-FL-US' from server_metadata vs 'Miami-Florida-US' from ipinfo).
-- The UNNEST + place_canonical_metro join is flattened into CTEs (rather than a
-- correlated scalar subquery) because BigQuery cannot de-correlate a subquery that
-- joins another table inside a correlated UNNEST.
CREATE TEMP TABLE processed_paths AS
WITH forward_hops AS (
  SELECT
    fr.id,
    -- Prefer the lat/lon-derived metro (source-independent, full region name,
    -- complete) over the place_canonical_metro string lookup, which is keyed on
    -- place and misses code-form variants like 'Johannesburg-GT-ZA'; raw place last.
    --
    -- A hop with a known AS but no location (step 04 refuted or could not place
    -- it, and keeps associated_asn "for AS-level analysis") is '<asn>-unknown',
    -- not '*'. It used to become '*', so the AS-level cover never saw that AS on
    -- ~7% of anomalous measurements. '<asn>-unknown' may merge several routers of
    -- the AS, so the Python covers use it ONLY as AS-level evidence: never as a
    -- node / metro / IXP candidate, and no edge with such an endpoint can be an
    -- edge-level culprit (hermes/pipeline/edge_format.py). Step 05 still names
    -- such a hop '*', so neither step can match an edge through it.
    CASE WHEN fwd.associated_asn IS NULL THEN '*'
      ELSE CONCAT(fwd.associated_asn, '-', COALESCE(fwd.metro, al.canon_metro, fwd.place, 'unknown'))
    END AS path,
    fwd.ttl
  FROM final_results AS fr,
       UNNEST(fr.forward_updated_node_details) AS fwd
  LEFT JOIN `mlab-collaboration.${DS}.place_canonical_metro` al
    ON al.place = fwd.place
),
forward_agg AS (
  SELECT id, ARRAY_AGG(path ORDER BY min_ttl) AS forward_as_path
  FROM (
    SELECT id, path, MIN(ttl) AS min_ttl FROM forward_hops GROUP BY id, path
  )
  GROUP BY id
),
reverse_hops AS (
  SELECT
    fr.id,
    -- '<asn>-unknown' for a located-AS / unlocated hop: see forward_hops.
    CASE WHEN rwd.associated_asn IS NULL THEN '*'
      ELSE CONCAT(rwd.associated_asn, '-', COALESCE(rwd.metro, al.canon_metro, rwd.place, 'unknown'))
    END AS path,
    rwd.ttl
  FROM final_results AS fr,
       UNNEST(fr.reverse_updated_node_details) AS rwd
  LEFT JOIN `mlab-collaboration.${DS}.place_canonical_metro` al
    ON al.place = rwd.place
),
reverse_agg AS (
  SELECT id, ARRAY_AGG(path ORDER BY min_ttl) AS reverse_as_path
  FROM (
    SELECT id, path, MIN(ttl) AS min_ttl FROM reverse_hops GROUP BY id, path
  )
  GROUP BY id
)
SELECT
  fr.id,
  fa.forward_as_path,
  ra.reverse_as_path
FROM final_results AS fr
LEFT JOIN forward_agg AS fa ON fr.id = fa.id
LEFT JOIN reverse_agg AS ra ON fr.id = ra.id;

-- 4. Return all edges (last statement = what Python downloads)
WITH path_with_anomaly AS (
  SELECT
    fr.id,
    -- ip_version is part of the tested population. Omitting it merged IPv4 and
    -- IPv6 evidence under one opaque Python key and could assign a forward
    -- hyperedge from one family to an upload-only event in the other.
    CONCAT(fr.src_asn, ' - ', fr.src_group_label, ' - ', fr.dst_site, ' - ', fr.ip_version)
      AS src_dst_pair,
    pp.forward_as_path,
    pp.reverse_as_path,
    -- Both directions are eligible for every anomaly type; see
    -- 05_temporal_edge_prevalences_union.sql for why. Flags kept per direction.
    (
      (fr.anomaly_ratio_rtt >= 0.8 AND fr.ndt_rtt > fr.baseline_median_rtt + 5 AND fr.anomaly_rtt_count >= 0.5)
      OR (fr.anomaly_ratio_throughput >= 0.8 AND fr.ndt_throughput < fr.baseline_median_throughput AND fr.anomaly_throughput_count >= 0.5)
      OR (fr.anomaly_ratio_upload_throughput >= 0.8 AND fr.median_upload_throughput < fr.baseline_median_upload_throughput AND fr.anomaly_upload_throughput_count >= 0.5)
    ) AS is_forward_anomaly,
    (
      (fr.anomaly_ratio_rtt >= 0.8 AND fr.ndt_rtt > fr.baseline_median_rtt + 5 AND fr.anomaly_rtt_count >= 0.5)
      OR (fr.anomaly_ratio_throughput >= 0.8 AND fr.ndt_throughput < fr.baseline_median_throughput AND fr.anomaly_throughput_count >= 0.5)
      OR (fr.anomaly_ratio_upload_throughput >= 0.8 AND fr.median_upload_throughput < fr.baseline_median_upload_throughput AND fr.anomaly_upload_throughput_count >= 0.5)
    ) AS is_reverse_anomaly,
    (
      (fr.anomaly_ratio_rtt >= 0.8 AND fr.ndt_rtt > fr.baseline_median_rtt + 5 AND fr.anomaly_rtt_count >= 0.5)
      OR (fr.anomaly_ratio_throughput >= 0.8 AND fr.ndt_throughput < fr.baseline_median_throughput AND fr.anomaly_throughput_count >= 0.5)
      OR (fr.anomaly_ratio_upload_throughput >= 0.8 AND fr.median_upload_throughput < fr.baseline_median_upload_throughput AND fr.anomaly_upload_throughput_count >= 0.5)
    ) AS is_anomaly
  FROM final_results fr
  JOIN processed_paths pp ON fr.id = pp.id
)
-- Forward anomalous
SELECT p.id, p.src_dst_pair,
  CONCAT(p.forward_as_path[OFFSET(i)], ' - ', p.forward_as_path[OFFSET(i+1)]) AS edge,
  CASE WHEN TRIM(SPLIT(p.forward_as_path[OFFSET(i)], '-')[SAFE_OFFSET(0)]) <= TRIM(SPLIT(p.forward_as_path[OFFSET(i+1)], '-')[SAFE_OFFSET(0)])
    THEN CONCAT(TRIM(p.forward_as_path[OFFSET(i)]), ' - ', TRIM(p.forward_as_path[OFFSET(i+1)]))
    ELSE CONCAT(TRIM(p.forward_as_path[OFFSET(i+1)]), ' - ', TRIM(p.forward_as_path[OFFSET(i)]))
  END AS canonical_edge,
  'forward' AS information_source,
  CASE WHEN LEFT(p.forward_as_path[OFFSET(i)], 1) = '*' OR LEFT(p.forward_as_path[OFFSET(i+1)], 1) = '*' THEN 'undetermined'
    WHEN TRIM(SPLIT(p.forward_as_path[OFFSET(i)], '-')[SAFE_OFFSET(0)]) = TRIM(SPLIT(p.forward_as_path[OFFSET(i+1)], '-')[SAFE_OFFSET(0)]) THEN 'intradomain'
    ELSE 'interdomain'
  END AS is_interdomain,
  'anomalous' AS path_type
FROM path_with_anomaly p, UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(p.forward_as_path) - 2)) AS i
WHERE p.is_forward_anomaly = TRUE

UNION ALL

-- Forward non-anomalous
SELECT p.id, p.src_dst_pair,
  CONCAT(p.forward_as_path[OFFSET(i)], ' - ', p.forward_as_path[OFFSET(i+1)]) AS edge,
  CASE WHEN TRIM(SPLIT(p.forward_as_path[OFFSET(i)], '-')[SAFE_OFFSET(0)]) <= TRIM(SPLIT(p.forward_as_path[OFFSET(i+1)], '-')[SAFE_OFFSET(0)])
    THEN CONCAT(TRIM(p.forward_as_path[OFFSET(i)]), ' - ', TRIM(p.forward_as_path[OFFSET(i+1)]))
    ELSE CONCAT(TRIM(p.forward_as_path[OFFSET(i+1)]), ' - ', TRIM(p.forward_as_path[OFFSET(i)]))
  END AS canonical_edge,
  'forward' AS information_source,
  CASE WHEN LEFT(p.forward_as_path[OFFSET(i)], 1) = '*' OR LEFT(p.forward_as_path[OFFSET(i+1)], 1) = '*' THEN 'undetermined'
    WHEN TRIM(SPLIT(p.forward_as_path[OFFSET(i)], '-')[SAFE_OFFSET(0)]) = TRIM(SPLIT(p.forward_as_path[OFFSET(i+1)], '-')[SAFE_OFFSET(0)]) THEN 'intradomain'
    ELSE 'interdomain'
  END AS is_interdomain,
  'non_anomalous' AS path_type
FROM path_with_anomaly p, UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(p.forward_as_path) - 2)) AS i
WHERE p.is_forward_anomaly = FALSE AND p.is_anomaly = FALSE

UNION ALL

-- Reverse anomalous
SELECT p.id, p.src_dst_pair,
  CONCAT(p.reverse_as_path[OFFSET(i)], ' - ', p.reverse_as_path[OFFSET(i+1)]) AS edge,
  CASE WHEN TRIM(SPLIT(p.reverse_as_path[OFFSET(i)], '-')[SAFE_OFFSET(0)]) <= TRIM(SPLIT(p.reverse_as_path[OFFSET(i+1)], '-')[SAFE_OFFSET(0)])
    THEN CONCAT(TRIM(p.reverse_as_path[OFFSET(i)]), ' - ', TRIM(p.reverse_as_path[OFFSET(i+1)]))
    ELSE CONCAT(TRIM(p.reverse_as_path[OFFSET(i+1)]), ' - ', TRIM(p.reverse_as_path[OFFSET(i)]))
  END AS canonical_edge,
  'reverse' AS information_source,
  CASE WHEN LEFT(p.reverse_as_path[OFFSET(i)], 1) = '*' OR LEFT(p.reverse_as_path[OFFSET(i+1)], 1) = '*' THEN 'undetermined'
    WHEN TRIM(SPLIT(p.reverse_as_path[OFFSET(i)], '-')[SAFE_OFFSET(0)]) = TRIM(SPLIT(p.reverse_as_path[OFFSET(i+1)], '-')[SAFE_OFFSET(0)]) THEN 'intradomain'
    ELSE 'interdomain'
  END AS is_interdomain,
  'anomalous' AS path_type
FROM path_with_anomaly p, UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(p.reverse_as_path) - 2)) AS i
WHERE p.is_reverse_anomaly = TRUE

UNION ALL

-- Reverse non-anomalous
SELECT p.id, p.src_dst_pair,
  CONCAT(p.reverse_as_path[OFFSET(i)], ' - ', p.reverse_as_path[OFFSET(i+1)]) AS edge,
  CASE WHEN TRIM(SPLIT(p.reverse_as_path[OFFSET(i)], '-')[SAFE_OFFSET(0)]) <= TRIM(SPLIT(p.reverse_as_path[OFFSET(i+1)], '-')[SAFE_OFFSET(0)])
    THEN CONCAT(TRIM(p.reverse_as_path[OFFSET(i)]), ' - ', TRIM(p.reverse_as_path[OFFSET(i+1)]))
    ELSE CONCAT(TRIM(p.reverse_as_path[OFFSET(i+1)]), ' - ', TRIM(p.reverse_as_path[OFFSET(i)]))
  END AS canonical_edge,
  'reverse' AS information_source,
  CASE WHEN LEFT(p.reverse_as_path[OFFSET(i)], 1) = '*' OR LEFT(p.reverse_as_path[OFFSET(i+1)], 1) = '*' THEN 'undetermined'
    WHEN TRIM(SPLIT(p.reverse_as_path[OFFSET(i)], '-')[SAFE_OFFSET(0)]) = TRIM(SPLIT(p.reverse_as_path[OFFSET(i+1)], '-')[SAFE_OFFSET(0)]) THEN 'intradomain'
    ELSE 'interdomain'
  END AS is_interdomain,
  'non_anomalous' AS path_type
FROM path_with_anomaly p, UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(p.reverse_as_path) - 2)) AS i
WHERE p.is_reverse_anomaly = FALSE AND p.is_anomaly = FALSE;

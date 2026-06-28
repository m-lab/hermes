--------------------------------------------------------------------------------
-- HERMES (union): Iterative correlation tomography — identify culprit edges
--
-- Input:  `mlab-collaboration.hermes_union.events_with_as_and_geoloc`
-- Output: `mlab-collaboration.hermes_union.correlation_hyperedges_tomography`
-- Partition: partition_date
--
-- Optimizations vs original:
--   1. Source table scanned ONCE into _base_events (broad filter) and
--      final_results (strict filter). Post-loop query uses _base_events.
--   2. Edge extraction pre-computed into _precomputed_edges before the loop.
--      Loop body is lightweight aggregation only — no repeated UNNEST.
--   3. Early termination at 95% anomalies explained.
--------------------------------------------------------------------------------
BEGIN
  DECLARE partition_date_var DATE DEFAULT '${DAY}';
  DECLARE num_rows INT64;
  DECLARE iteration_count INT64 DEFAULT 1;
  DECLARE MAX_ITERATIONS INT64 DEFAULT 200;
  DECLARE total_anomalies INT64;
  DECLARE cumulative_anomalies_explained INT64 DEFAULT 0;
  DECLARE cumulative_fraction_anomalies_explained_so_far FLOAT64 DEFAULT 0.0;
  DECLARE anomalies_explained_this_iteration INT64;
  DECLARE done BOOL DEFAULT FALSE;

  -- ==========================================================================
  -- 1. Scan source table ONCE (broad filter — no distance/RTT checks)
  -- ==========================================================================
  CREATE OR REPLACE TEMP TABLE _base_events AS
  SELECT *
  FROM `mlab-collaboration.hermes_union.events_with_as_and_geoloc`
  WHERE partition_date = partition_date_var
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

  -- ==========================================================================
  -- 2. Strict filter for the iterative loop (adds distance/RTT checks)
  -- ==========================================================================
  CREATE OR REPLACE TEMP TABLE final_results AS
  SELECT *
  FROM _base_events
  WHERE NOT forward_distance / 100 > ndt_rtt
    AND NOT reverse_distance / 100 > ndt_rtt;

  -- ==========================================================================
  -- 3. Pre-compute ordered AS paths per measurement
  -- ==========================================================================
  CREATE OR REPLACE TEMP TABLE processed_paths AS
  SELECT
    fr.id,
    fr.src_asn,
    fr.src_city,
    fr.dst_site,
    (
      SELECT ARRAY_AGG(x.path ORDER BY x.min_ttl)
      FROM (
        SELECT
          IFNULL(CONCAT(fwd.associated_asn, '-', fwd.place), '*') AS path,
          MIN(fwd.ttl) AS min_ttl
        FROM UNNEST(fr.forward_updated_node_details) fwd
        GROUP BY path
      ) x
    ) AS forward_as_path,
    (
      SELECT ARRAY_AGG(x.place ORDER BY x.min_ttl)
      FROM (
        SELECT fwd.place AS place, MIN(fwd.ttl) AS min_ttl
        FROM UNNEST(fr.forward_updated_node_details) fwd
        WHERE fwd.place IS NOT NULL
        GROUP BY place
      ) x
    ) AS forward_geo_path,
    (
      SELECT ARRAY_AGG(x.path ORDER BY x.min_ttl)
      FROM (
        SELECT
          IFNULL(CONCAT(rwd.associated_asn, '-', rwd.place), '*') AS path,
          MIN(rwd.ttl) AS min_ttl
        FROM UNNEST(fr.reverse_updated_node_details) AS rwd
        GROUP BY path
      ) x
    ) AS reverse_as_path,
    (
      SELECT ARRAY_AGG(x.place ORDER BY x.min_ttl)
      FROM (
        SELECT rwd.place AS place, MIN(rwd.ttl) AS min_ttl
        FROM UNNEST(fr.reverse_updated_node_details) AS rwd
        WHERE rwd.place IS NOT NULL
        GROUP BY place
      ) x
    ) AS reverse_geo_path
  FROM final_results AS fr;

  -- ==========================================================================
  -- 4. Pre-compute ALL edges from paths (done ONCE, not per iteration)
  --    Each row = one edge from one measurement, with path_type assigned.
  -- ==========================================================================
  CREATE OR REPLACE TEMP TABLE _precomputed_edges AS
  WITH path_with_anomaly AS (
    SELECT
      fr.id,
      CONCAT(fr.src_asn, ' - ', fr.src_city, ' - ', fr.dst_site) AS src_dst_pair,
      FORMAT_TIMESTAMP('%Y-%m-%d', TIMESTAMP_TRUNC(fr.window_start, DAY)) AS day,
      pp.forward_as_path,
      pp.reverse_as_path,
      (
        (fr.anomaly_ratio_rtt >= 0.8
          AND fr.ndt_rtt > fr.baseline_median_rtt + 5
          AND fr.anomaly_rtt_count >= 0.5)
        OR
        (fr.anomaly_ratio_throughput >= 0.8
          AND fr.ndt_throughput < fr.baseline_median_throughput
          AND fr.anomaly_throughput_count >= 0.5)
      ) AS is_forward_anomaly,
      (
        (fr.anomaly_ratio_rtt >= 0.8
          AND fr.ndt_rtt > fr.baseline_median_rtt + 5
          AND fr.anomaly_rtt_count >= 0.5)
        OR
        (fr.anomaly_ratio_upload_throughput >= 0.8
          AND fr.median_upload_throughput < fr.baseline_median_upload_throughput
          AND fr.anomaly_upload_throughput_count >= 0.5)
      ) AS is_reverse_anomaly,
      (
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
      ) AS is_anomaly
    FROM final_results fr
    JOIN processed_paths pp ON fr.id = pp.id
  )
  -- Forward edges from forward-anomalous measurements
  SELECT
    p.id, p.src_dst_pair, p.day,
    CONCAT(p.forward_as_path[OFFSET(i)], ' - ', p.forward_as_path[OFFSET(i+1)]) AS edge,
    CASE
      WHEN TRIM(SPLIT(p.forward_as_path[OFFSET(i)], '-')[SAFE_OFFSET(0)])
           <= TRIM(SPLIT(p.forward_as_path[OFFSET(i+1)], '-')[SAFE_OFFSET(0)])
      THEN CONCAT(TRIM(p.forward_as_path[OFFSET(i)]), ' - ', TRIM(p.forward_as_path[OFFSET(i+1)]))
      ELSE CONCAT(TRIM(p.forward_as_path[OFFSET(i+1)]), ' - ', TRIM(p.forward_as_path[OFFSET(i)]))
    END AS canonical_edge,
    'forward' AS information_source,
    CASE
      WHEN LEFT(p.forward_as_path[OFFSET(i)], 1) = '*'
        OR LEFT(p.forward_as_path[OFFSET(i+1)], 1) = '*' THEN 'undetermined'
      WHEN TRIM(SPLIT(p.forward_as_path[OFFSET(i)], '-')[SAFE_OFFSET(0)])
         = TRIM(SPLIT(p.forward_as_path[OFFSET(i+1)], '-')[SAFE_OFFSET(0)]) THEN 'intradomain'
      ELSE 'interdomain'
    END AS is_interdomain,
    'anomalous' AS path_type
  FROM path_with_anomaly p,
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(p.forward_as_path) - 2)) AS i
  WHERE p.is_forward_anomaly = TRUE
    AND CAST(p.day AS DATE) = partition_date_var

  UNION ALL

  -- Forward edges from non-anomalous measurements
  SELECT
    p.id, p.src_dst_pair, p.day,
    CONCAT(p.forward_as_path[OFFSET(i)], ' - ', p.forward_as_path[OFFSET(i+1)]) AS edge,
    CASE
      WHEN TRIM(SPLIT(p.forward_as_path[OFFSET(i)], '-')[SAFE_OFFSET(0)])
           <= TRIM(SPLIT(p.forward_as_path[OFFSET(i+1)], '-')[SAFE_OFFSET(0)])
      THEN CONCAT(TRIM(p.forward_as_path[OFFSET(i)]), ' - ', TRIM(p.forward_as_path[OFFSET(i+1)]))
      ELSE CONCAT(TRIM(p.forward_as_path[OFFSET(i+1)]), ' - ', TRIM(p.forward_as_path[OFFSET(i)]))
    END AS canonical_edge,
    'forward' AS information_source,
    CASE
      WHEN LEFT(p.forward_as_path[OFFSET(i)], 1) = '*'
        OR LEFT(p.forward_as_path[OFFSET(i+1)], 1) = '*' THEN 'undetermined'
      WHEN TRIM(SPLIT(p.forward_as_path[OFFSET(i)], '-')[SAFE_OFFSET(0)])
         = TRIM(SPLIT(p.forward_as_path[OFFSET(i+1)], '-')[SAFE_OFFSET(0)]) THEN 'intradomain'
      ELSE 'interdomain'
    END AS is_interdomain,
    'non_anomalous' AS path_type
  FROM path_with_anomaly p,
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(p.forward_as_path) - 2)) AS i
  WHERE p.is_forward_anomaly = FALSE
    AND p.is_anomaly = FALSE
    AND CAST(p.day AS DATE) = partition_date_var

  UNION ALL

  -- Reverse edges from reverse-anomalous measurements
  SELECT
    p.id, p.src_dst_pair, p.day,
    CONCAT(p.reverse_as_path[OFFSET(i)], ' - ', p.reverse_as_path[OFFSET(i+1)]) AS edge,
    CASE
      WHEN TRIM(SPLIT(p.reverse_as_path[OFFSET(i)], '-')[SAFE_OFFSET(0)])
           <= TRIM(SPLIT(p.reverse_as_path[OFFSET(i+1)], '-')[SAFE_OFFSET(0)])
      THEN CONCAT(TRIM(p.reverse_as_path[OFFSET(i)]), ' - ', TRIM(p.reverse_as_path[OFFSET(i+1)]))
      ELSE CONCAT(TRIM(p.reverse_as_path[OFFSET(i+1)]), ' - ', TRIM(p.reverse_as_path[OFFSET(i)]))
    END AS canonical_edge,
    'reverse' AS information_source,
    CASE
      WHEN LEFT(p.reverse_as_path[OFFSET(i)], 1) = '*'
        OR LEFT(p.reverse_as_path[OFFSET(i+1)], 1) = '*' THEN 'undetermined'
      WHEN TRIM(SPLIT(p.reverse_as_path[OFFSET(i)], '-')[SAFE_OFFSET(0)])
         = TRIM(SPLIT(p.reverse_as_path[OFFSET(i+1)], '-')[SAFE_OFFSET(0)]) THEN 'intradomain'
      ELSE 'interdomain'
    END AS is_interdomain,
    'anomalous' AS path_type
  FROM path_with_anomaly p,
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(p.reverse_as_path) - 2)) AS i
  WHERE p.is_reverse_anomaly = TRUE
    AND CAST(p.day AS DATE) = partition_date_var

  UNION ALL

  -- Reverse edges from non-anomalous measurements
  SELECT
    p.id, p.src_dst_pair, p.day,
    CONCAT(p.reverse_as_path[OFFSET(i)], ' - ', p.reverse_as_path[OFFSET(i+1)]) AS edge,
    CASE
      WHEN TRIM(SPLIT(p.reverse_as_path[OFFSET(i)], '-')[SAFE_OFFSET(0)])
           <= TRIM(SPLIT(p.reverse_as_path[OFFSET(i+1)], '-')[SAFE_OFFSET(0)])
      THEN CONCAT(TRIM(p.reverse_as_path[OFFSET(i)]), ' - ', TRIM(p.reverse_as_path[OFFSET(i+1)]))
      ELSE CONCAT(TRIM(p.reverse_as_path[OFFSET(i+1)]), ' - ', TRIM(p.reverse_as_path[OFFSET(i)]))
    END AS canonical_edge,
    'reverse' AS information_source,
    CASE
      WHEN LEFT(p.reverse_as_path[OFFSET(i)], 1) = '*'
        OR LEFT(p.reverse_as_path[OFFSET(i+1)], 1) = '*' THEN 'undetermined'
      WHEN TRIM(SPLIT(p.reverse_as_path[OFFSET(i)], '-')[SAFE_OFFSET(0)])
         = TRIM(SPLIT(p.reverse_as_path[OFFSET(i+1)], '-')[SAFE_OFFSET(0)]) THEN 'intradomain'
      ELSE 'interdomain'
    END AS is_interdomain,
    'non_anomalous' AS path_type
  FROM path_with_anomaly p,
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(p.reverse_as_path) - 2)) AS i
  WHERE p.is_reverse_anomaly = FALSE
    AND p.is_anomaly = FALSE
    AND CAST(p.day AS DATE) = partition_date_var;

  -- ==========================================================================
  -- 5. Initialize output + tracking tables
  -- ==========================================================================
  CREATE TEMP TABLE plausible_culprit (
    canonical_edge STRING,
    day STRING,
    information_source STRING,
    is_interdomain STRING,
    src_dst_pairs_impacted ARRAY<STRING>,
    anomalous_src_dst_pairs_impacted ARRAY<STRING>,
    paths ARRAY<STRUCT<
      path_type STRING,
      edge_count INT64,
      fraction FLOAT64,
      total_paths INT64,
      fraction_src_dst_pair FLOAT64,
      total_src_dst_pairs_in_window INT64>>,
    max_fraction_anomalous FLOAT64,
    max_fraction_src_dst_pair_anomalous FLOAT64,
    max_fraction_non_anomalous FLOAT64,
    ratio_anomaly FLOAT64,
    max_fraction_src_dst_pair_non_anomalous FLOAT64,
    fraction_anomalous_paths FLOAT64,
    partition_date DATE,
    iteration_number INT64,
    anomalies_explained_by_edge INT64,
    fraction_anomalies_explained_by_edge FLOAT64,
    cumulative_anomalies_explained INT64,
    cumulative_fraction_anomalies_explained_so_far FLOAT64
  );

  CREATE TEMP TABLE explained_src_dst_pairs (src_dst_pair STRING);

  CREATE TEMP TABLE all_anomalies AS
  SELECT DISTINCT CONCAT(fr.src_asn, ' - ', fr.src_city, ' - ', fr.dst_site) AS src_dst_pair
  FROM final_results fr
  WHERE (
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
  );

  SET total_anomalies = 0;

  -- ==========================================================================
  -- 6. Iterative greedy set-cover loop
  --    Uses _precomputed_edges — no array unnesting per iteration.
  -- ==========================================================================
  WHILE NOT done DO

    SET iteration_count = iteration_count + 1;

    -- Remaining anomalous src_dst_pairs (not yet explained)
    CREATE OR REPLACE TEMP TABLE total_anomalous_src_dst_pairs AS
    SELECT DISTINCT CONCAT(fr.src_asn, ' - ', fr.src_city, ' - ', fr.dst_site) AS src_dst_pair
    FROM final_results fr
    LEFT JOIN explained_src_dst_pairs e
      ON CONCAT(fr.src_asn, ' - ', fr.src_city, ' - ', fr.dst_site) = e.src_dst_pair
    WHERE e.src_dst_pair IS NULL
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
      );

    SET total_anomalies = (SELECT COUNT(*) FROM total_anomalous_src_dst_pairs);

    IF total_anomalies = 0 THEN
      SET done = TRUE;
      LEAVE;
    END IF;

    -- Edge ranking: filter precomputed edges, aggregate, compute fractions
    CREATE OR REPLACE TEMP TABLE paths_with_max AS
    WITH
      filtered_edges AS (
        SELECT e.*
        FROM _precomputed_edges e
        LEFT JOIN explained_src_dst_pairs ex ON e.src_dst_pair = ex.src_dst_pair
        WHERE ex.src_dst_pair IS NULL
      ),
      edge_counts AS (
        SELECT
          day, path_type, edge, information_source,
          COUNT(*) AS edge_count,
          ARRAY_AGG(DISTINCT src_dst_pair) AS src_dst_pair_impacted,
          ANY_VALUE(is_interdomain) AS is_interdomain,
          ANY_VALUE(canonical_edge) AS canonical_edge
        FROM filtered_edges
        GROUP BY day, path_type, edge, information_source
      ),
      total_paths_per_type AS (
        SELECT day, path_type, COUNT(DISTINCT id) AS total_paths
        FROM filtered_edges
        GROUP BY day, path_type
      ),
      total_sdp_per_type AS (
        SELECT day, path_type, COUNT(DISTINCT src_dst_pair) AS total_src_dst_pairs_in_window
        FROM filtered_edges
        GROUP BY day, path_type
      ),
      edge_fractions AS (
        SELECT
          ec.day, ec.path_type, ec.edge, ec.canonical_edge,
          ec.edge_count, tpc.total_paths, ec.information_source,
          ec.src_dst_pair_impacted, ec.is_interdomain,
          SAFE_DIVIDE(ec.edge_count, tpc.total_paths) AS fraction,
          tsp.total_src_dst_pairs_in_window,
          SAFE_DIVIDE(ARRAY_LENGTH(ec.src_dst_pair_impacted), tsp.total_src_dst_pairs_in_window) AS fraction_src_dst_pair
        FROM edge_counts ec
        JOIN total_paths_per_type tpc ON ec.day = tpc.day AND ec.path_type = tpc.path_type
        JOIN total_sdp_per_type tsp ON ec.day = tsp.day AND ec.path_type = tsp.path_type
      ),
      max_fractions_anomalous AS (
        SELECT
          edge, day, information_source, is_interdomain,
          MAX(fraction) AS max_fraction_anomalous,
          MAX(fraction_src_dst_pair) AS max_fraction_src_dst_pair_anomalous
        FROM edge_fractions
        WHERE path_type = 'anomalous'
        GROUP BY edge, day, information_source, is_interdomain
      ),
      max_fractions_non_anomalous AS (
        SELECT
          edge, day, information_source, is_interdomain,
          MAX(fraction) AS max_fraction_non_anomalous,
          MAX(fraction_src_dst_pair) AS max_fraction_src_dst_pair_non_anomalous
        FROM edge_fractions
        WHERE path_type = 'non_anomalous'
        GROUP BY edge, day, information_source, is_interdomain
      ),
      edge_src_dst_pairs AS (
        SELECT
          ef.edge, ef.day, ef.information_source, ef.is_interdomain,
          ARRAY_AGG(DISTINCT sdp) AS src_dst_pairs_impacted,
          ARRAY_AGG(DISTINCT sdp_anomalous.src_dst_pair IGNORE NULLS) AS anomalous_src_dst_pairs_impacted
        FROM edge_fractions ef
        CROSS JOIN UNNEST(ef.src_dst_pair_impacted) sdp
        LEFT JOIN total_anomalous_src_dst_pairs sdp_anomalous
          ON sdp = sdp_anomalous.src_dst_pair
        GROUP BY ef.edge, ef.day, ef.information_source, ef.is_interdomain
      )
    SELECT
      ef.edge,
      ef.canonical_edge,
      ef.day,
      ef.information_source,
      ef.is_interdomain,
      esdp.src_dst_pairs_impacted,
      esdp.anomalous_src_dst_pairs_impacted,
      ARRAY_AGG(STRUCT(
        ef.path_type,
        ef.edge_count,
        ef.fraction,
        ef.total_paths,
        ef.fraction_src_dst_pair,
        ef.total_src_dst_pairs_in_window
      )) AS paths,
      mfa.max_fraction_anomalous,
      mfa.max_fraction_src_dst_pair_anomalous,
      mfn.max_fraction_non_anomalous,
      SAFE_DIVIDE(mfa.max_fraction_anomalous, mfn.max_fraction_non_anomalous) AS ratio_anomaly,
      mfn.max_fraction_src_dst_pair_non_anomalous,
      SAFE_DIVIDE(
        SUM(IF(ef.path_type = 'anomalous', ef.edge_count, 0)),
        SUM(ef.edge_count)
      ) AS fraction_anomalous_paths,
      ARRAY_LENGTH(esdp.anomalous_src_dst_pairs_impacted) AS anomalies_explained_by_edge,
      SAFE_DIVIDE(
        ARRAY_LENGTH(esdp.anomalous_src_dst_pairs_impacted),
        (SELECT COUNT(*) FROM total_anomalous_src_dst_pairs)
      ) AS fraction_anomalies_explained_by_edge,
      DATE(partition_date_var) AS partition_date
    FROM edge_fractions ef
    LEFT JOIN edge_src_dst_pairs esdp
      ON ef.edge = esdp.edge AND ef.day = esdp.day
      AND ef.information_source = esdp.information_source
      AND ef.is_interdomain = esdp.is_interdomain
    LEFT JOIN max_fractions_anomalous mfa
      ON ef.edge = mfa.edge AND ef.day = mfa.day
      AND ef.information_source = mfa.information_source
      AND ef.is_interdomain = mfa.is_interdomain
    LEFT JOIN max_fractions_non_anomalous mfn
      ON ef.edge = mfn.edge AND ef.day = mfn.day
      AND ef.information_source = mfn.information_source
      AND ef.is_interdomain = mfn.is_interdomain
    GROUP BY
      ef.edge, ef.canonical_edge, ef.day, ef.information_source, ef.is_interdomain,
      esdp.src_dst_pairs_impacted, esdp.anomalous_src_dst_pairs_impacted,
      mfa.max_fraction_anomalous, mfa.max_fraction_src_dst_pair_anomalous,
      mfn.max_fraction_non_anomalous, mfn.max_fraction_src_dst_pair_non_anomalous
    HAVING
      SUM(IF(ef.path_type = 'anomalous', ef.edge_count, 0)) >= 2
      AND fraction_anomalous_paths >= 0.7
      AND edge NOT LIKE '%*%'
    ORDER BY ratio_anomaly DESC, is_interdomain DESC;

    SET num_rows = (SELECT COUNT(*) FROM paths_with_max);

    IF num_rows = 0 THEN
      SET done = TRUE;
    ELSE
      CREATE OR REPLACE TEMP TABLE top_edges AS
      WITH ranked_paths AS (
        SELECT *,
          ROW_NUMBER() OVER (ORDER BY ratio_anomaly DESC, is_interdomain DESC) AS row_num
        FROM paths_with_max
      )
      SELECT *
      FROM ranked_paths
      WHERE row_num <= FLOOR(100 / iteration_count) + 1;

      SET anomalies_explained_this_iteration = (
        SELECT SUM(anomalies_explained_by_edge) FROM top_edges
      );

      SET cumulative_anomalies_explained = cumulative_anomalies_explained + anomalies_explained_this_iteration;

      SET cumulative_fraction_anomalies_explained_so_far = SAFE_DIVIDE(
        cumulative_anomalies_explained,
        (SELECT COUNT(*) FROM all_anomalies)
      );

      INSERT INTO plausible_culprit
      SELECT
        canonical_edge, day, information_source, is_interdomain,
        src_dst_pairs_impacted, anomalous_src_dst_pairs_impacted,
        paths, max_fraction_anomalous, max_fraction_src_dst_pair_anomalous,
        max_fraction_non_anomalous, ratio_anomaly,
        max_fraction_src_dst_pair_non_anomalous, fraction_anomalous_paths,
        DATE(partition_date_var) AS partition_date,
        iteration_count AS iteration_number,
        anomalies_explained_by_edge, fraction_anomalies_explained_by_edge,
        cumulative_anomalies_explained,
        cumulative_fraction_anomalies_explained_so_far
      FROM top_edges;

      INSERT INTO explained_src_dst_pairs (src_dst_pair)
      SELECT DISTINCT src_dst_pair
      FROM top_edges, UNNEST(anomalous_src_dst_pairs_impacted) AS src_dst_pair;
    END IF;

    -- Early termination: stop when 95% of anomalies are explained
    IF cumulative_fraction_anomalies_explained_so_far >= 0.95 THEN
      SET done = TRUE;
    END IF;

    IF iteration_count >= MAX_ITERATIONS THEN
      SET done = TRUE;
    END IF;

  END WHILE;

  -- ==========================================================================
  -- 7. Post-loop: Build hyperedge summary with node-level culprit fractions
  --    Uses _base_events (already in memory) instead of re-scanning source.
  -- ==========================================================================
  INSERT INTO `mlab-collaboration.hermes_union.correlation_hyperedges_tomography`
--   CREATE TABLE IF NOT EXISTS `mlab-collaboration.hermes_union.correlation_hyperedges_tomography`
--   PARTITION BY partition_date
--   AS
  WITH all_edges_per_node AS (
    -- Forward edges
    SELECT
      fr.id,
      CASE
        WHEN TRIM(SPLIT(fwd.asn_city[OFFSET(i)], '-')[SAFE_OFFSET(0)])
             <= TRIM(SPLIT(fwd.asn_city[OFFSET(i+1)], '-')[SAFE_OFFSET(0)])
        THEN CONCAT(fwd.asn_city[OFFSET(i)], ' - ', fwd.asn_city[OFFSET(i+1)])
        ELSE CONCAT(fwd.asn_city[OFFSET(i+1)], ' - ', fwd.asn_city[OFFSET(i)])
      END AS canonical_edge,
      TRIM(SPLIT(fwd.asn_city[OFFSET(i)], '-')[SAFE_OFFSET(0)]) AS from_asn,
      TRIM(SPLIT(fwd.asn_city[OFFSET(i)], '-')[SAFE_OFFSET(1)]) AS from_metro,
      CONCAT(
        TRIM(SPLIT(fwd.asn_city[OFFSET(i)], '-')[SAFE_OFFSET(0)]), '-',
        TRIM(SPLIT(fwd.asn_city[OFFSET(i)], '-')[SAFE_OFFSET(1)])
      ) AS from_asn_metro,
      TRIM(SPLIT(fwd.asn_city[OFFSET(i+1)], '-')[SAFE_OFFSET(0)]) AS to_asn,
      TRIM(SPLIT(fwd.asn_city[OFFSET(i+1)], '-')[SAFE_OFFSET(1)]) AS to_metro,
      CONCAT(
        TRIM(SPLIT(fwd.asn_city[OFFSET(i+1)], '-')[SAFE_OFFSET(0)]), '-',
        TRIM(SPLIT(fwd.asn_city[OFFSET(i+1)], '-')[SAFE_OFFSET(1)])
      ) AS to_asn_metro
    FROM _base_events AS fr
    CROSS JOIN UNNEST(
      ARRAY(
        SELECT STRUCT(
          ARRAY_AGG(CONCAT(n.associated_asn, '-', n.place) IGNORE NULLS) AS asn_city
        )
        FROM UNNEST(fr.forward_updated_node_details) n
        WHERE n.associated_asn IS NOT NULL AND n.place IS NOT NULL
      )
    ) AS fwd
    CROSS JOIN UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(fwd.asn_city) - 2)) AS i

    UNION ALL

    -- Reverse edges
    SELECT
      fr.id,
      CASE
        WHEN TRIM(SPLIT(rev.asn_city[OFFSET(i)], '-')[SAFE_OFFSET(0)])
             <= TRIM(SPLIT(rev.asn_city[OFFSET(i+1)], '-')[SAFE_OFFSET(0)])
        THEN CONCAT(rev.asn_city[OFFSET(i)], ' - ', rev.asn_city[OFFSET(i+1)])
        ELSE CONCAT(rev.asn_city[OFFSET(i+1)], ' - ', rev.asn_city[OFFSET(i)])
      END AS canonical_edge,
      TRIM(SPLIT(rev.asn_city[OFFSET(i)], '-')[SAFE_OFFSET(0)]) AS from_asn,
      TRIM(SPLIT(rev.asn_city[OFFSET(i)], '-')[SAFE_OFFSET(1)]) AS from_metro,
      CONCAT(
        TRIM(SPLIT(rev.asn_city[OFFSET(i)], '-')[SAFE_OFFSET(0)]), '-',
        TRIM(SPLIT(rev.asn_city[OFFSET(i)], '-')[SAFE_OFFSET(1)])
      ) AS from_asn_metro,
      TRIM(SPLIT(rev.asn_city[OFFSET(i+1)], '-')[SAFE_OFFSET(0)]) AS to_asn,
      TRIM(SPLIT(rev.asn_city[OFFSET(i+1)], '-')[SAFE_OFFSET(1)]) AS to_metro,
      CONCAT(
        TRIM(SPLIT(rev.asn_city[OFFSET(i+1)], '-')[SAFE_OFFSET(0)]), '-',
        TRIM(SPLIT(rev.asn_city[OFFSET(i+1)], '-')[SAFE_OFFSET(1)])
      ) AS to_asn_metro
    FROM _base_events AS fr
    CROSS JOIN UNNEST(
      ARRAY(
        SELECT STRUCT(
          ARRAY_AGG(CONCAT(n.associated_asn, '-', n.place) IGNORE NULLS) AS asn_city
        )
        FROM UNNEST(fr.reverse_updated_node_details) n
        WHERE n.associated_asn IS NOT NULL AND n.place IS NOT NULL
      )
    ) AS rev
    CROSS JOIN UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(rev.asn_city) - 2)) AS i
  ),

  -- Node counts at three granularities: ASN-metro, ASN, metro
  all_node_counts_asn_metro AS (
    SELECT node_asn_metro, SUM(edge_count) AS edge_count
    FROM (
      SELECT from_asn_metro AS node_asn_metro, COUNT(*) AS edge_count
      FROM all_edges_per_node GROUP BY from_asn_metro
      UNION ALL
      SELECT to_asn_metro, COUNT(*) FROM all_edges_per_node GROUP BY to_asn_metro
    )
    GROUP BY node_asn_metro
  ),
  all_node_counts_asn AS (
    SELECT node_asn, SUM(edge_count) AS edge_count
    FROM (
      SELECT from_asn AS node_asn, COUNT(*) AS edge_count
      FROM all_edges_per_node GROUP BY from_asn
      UNION ALL
      SELECT to_asn, COUNT(*) FROM all_edges_per_node GROUP BY to_asn
    )
    GROUP BY node_asn
  ),
  all_node_counts_metro AS (
    SELECT node_metro, SUM(edge_count) AS edge_count
    FROM (
      SELECT from_metro AS node_metro, COUNT(*) AS edge_count
      FROM all_edges_per_node GROUP BY from_metro
      UNION ALL
      SELECT to_metro, COUNT(*) FROM all_edges_per_node GROUP BY to_metro
    )
    GROUP BY node_metro
  ),

  -- Culprit edge node parsing
  culprit_edges_per_node AS (
    SELECT
      pc.*,
      TRIM(SPLIT(left_part, '-')[SAFE_OFFSET(0)]) AS from_asn,
      TRIM(SPLIT(left_part, '-')[SAFE_OFFSET(1)]) AS from_metro,
      CONCAT(TRIM(SPLIT(left_part, '-')[SAFE_OFFSET(0)]), '-', TRIM(SPLIT(left_part, '-')[SAFE_OFFSET(1)])) AS from_asn_metro,
      TRIM(SPLIT(right_part, '-')[SAFE_OFFSET(0)]) AS to_asn,
      TRIM(SPLIT(right_part, '-')[SAFE_OFFSET(1)]) AS to_metro,
      CONCAT(TRIM(SPLIT(right_part, '-')[SAFE_OFFSET(0)]), '-', TRIM(SPLIT(right_part, '-')[SAFE_OFFSET(1)])) AS to_asn_metro
    FROM (
      SELECT
        pc.*,
        SPLIT(pc.canonical_edge, ' - ')[SAFE_OFFSET(0)] AS left_part,
        SPLIT(pc.canonical_edge, ' - ')[SAFE_OFFSET(1)] AS right_part
      FROM plausible_culprit pc
      WHERE pc.partition_date = partition_date_var
    ) AS pc
  ),

  -- Culprit node counts at three granularities
  culprit_node_counts_asn_metro AS (
    SELECT node_asn_metro, SUM(c) AS culprit_count
    FROM (
      SELECT from_asn_metro AS node_asn_metro, COUNT(*) AS c FROM culprit_edges_per_node GROUP BY from_asn_metro
      UNION ALL
      SELECT to_asn_metro, COUNT(*) FROM culprit_edges_per_node GROUP BY to_asn_metro
    )
    GROUP BY node_asn_metro
  ),
  culprit_node_counts_asn AS (
    SELECT node_asn, SUM(c) AS culprit_count
    FROM (
      SELECT from_asn AS node_asn, COUNT(*) AS c FROM culprit_edges_per_node GROUP BY from_asn
      UNION ALL
      SELECT to_asn, COUNT(*) FROM culprit_edges_per_node GROUP BY to_asn
    )
    GROUP BY node_asn
  ),
  culprit_node_counts_metro AS (
    SELECT node_metro, SUM(c) AS culprit_count
    FROM (
      SELECT from_metro AS node_metro, COUNT(*) AS c FROM culprit_edges_per_node GROUP BY from_metro
      UNION ALL
      SELECT to_metro, COUNT(*) FROM culprit_edges_per_node GROUP BY to_metro
    )
    GROUP BY node_metro
  ),

  -- Fraction of culprit edges per node
  joined_asn_metro AS (
    SELECT
      am.node_asn_metro,
      SPLIT(am.node_asn_metro, '-')[SAFE_OFFSET(0)] AS node_asn,
      SPLIT(am.node_asn_metro, '-')[SAFE_OFFSET(1)] AS node_metro,
      am.edge_count AS total_edges_for_node_asn_metro,
      IFNULL(cm.culprit_count, 0) AS culprit_edges_for_node_asn_metro,
      SAFE_DIVIDE(IFNULL(cm.culprit_count, 0), am.edge_count) AS fraction_culprit_asn_metro
    FROM all_node_counts_asn_metro am
    LEFT JOIN culprit_node_counts_asn_metro cm ON am.node_asn_metro = cm.node_asn_metro
  ),
  joined_asn AS (
    SELECT
      an.node_asn,
      an.edge_count AS total_edges_for_node_asn,
      IFNULL(ca.culprit_count, 0) AS culprit_edges_for_node_asn,
      SAFE_DIVIDE(IFNULL(ca.culprit_count, 0), an.edge_count) AS fraction_culprit_asn
    FROM all_node_counts_asn an
    LEFT JOIN culprit_node_counts_asn ca ON an.node_asn = ca.node_asn
  ),
  joined_metro AS (
    SELECT
      mn.node_metro,
      mn.edge_count AS total_edges_for_node_metro,
      IFNULL(cm.culprit_count, 0) AS culprit_edges_for_node_metro,
      SAFE_DIVIDE(IFNULL(cm.culprit_count, 0), mn.edge_count) AS fraction_culprit_metro
    FROM all_node_counts_metro mn
    LEFT JOIN culprit_node_counts_metro cm ON mn.node_metro = cm.node_metro
  ),

  -- Final join
  final_table AS (
    SELECT
      jam.node_asn_metro, jam.node_asn, jam.node_metro,
      jam.total_edges_for_node_asn_metro, jam.culprit_edges_for_node_asn_metro, jam.fraction_culprit_asn_metro,
      ja.total_edges_for_node_asn, ja.culprit_edges_for_node_asn, ja.fraction_culprit_asn,
      jm.total_edges_for_node_metro, jm.culprit_edges_for_node_metro, jm.fraction_culprit_metro
    FROM joined_asn_metro jam
    LEFT JOIN joined_asn ja ON jam.node_asn = ja.node_asn
    LEFT JOIN joined_metro jm ON jam.node_metro = jm.node_metro
  )

  SELECT
    c.canonical_edge AS edge_asn_metro,
    c.* EXCEPT (canonical_edge),
    jam_from.total_edges_for_node_asn_metro AS from_total_edges_asn_metro,
    jam_from.culprit_edges_for_node_asn_metro AS from_culprit_edges_asn_metro,
    jam_from.fraction_culprit_asn_metro AS from_fraction_culprit_asn_metro,
    jam_from.total_edges_for_node_asn AS from_total_edges_asn,
    jam_from.culprit_edges_for_node_asn AS from_culprit_edges_asn,
    jam_from.fraction_culprit_asn AS from_fraction_culprit_asn,
    jam_from.total_edges_for_node_metro AS from_total_edges_metro,
    jam_from.culprit_edges_for_node_metro AS from_culprit_edges_metro,
    jam_from.fraction_culprit_metro AS from_fraction_culprit_metro,
    jam_to.total_edges_for_node_asn_metro AS to_total_edges_asn_metro,
    jam_to.culprit_edges_for_node_asn_metro AS to_culprit_edges_asn_metro,
    jam_to.fraction_culprit_asn_metro AS to_fraction_culprit_asn_metro,
    jam_to.total_edges_for_node_asn AS to_total_edges_asn,
    jam_to.culprit_edges_for_node_asn AS to_culprit_edges_asn,
    jam_to.fraction_culprit_asn AS to_fraction_culprit_asn,
    jam_to.total_edges_for_node_metro AS to_total_edges_metro,
    jam_to.culprit_edges_for_node_metro AS to_culprit_edges_metro,
    jam_to.fraction_culprit_metro AS to_fraction_culprit_metro
  FROM culprit_edges_per_node c
  LEFT JOIN final_table AS jam_from ON c.from_asn_metro = jam_from.node_asn_metro
  LEFT JOIN final_table AS jam_to ON c.to_asn_metro = jam_to.node_asn_metro;
END

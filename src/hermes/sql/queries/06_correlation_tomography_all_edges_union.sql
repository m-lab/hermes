--------------------------------------------------------------------------------
-- HERMES (union): Correlation tomography — all edges per node
--
-- Returns all (forward + reverse) edges from events_with_as_and_geoloc
-- with the broad filter. Downloaded by Python for hyperedge computation.
--
-- Parameters: ${DAY}
--------------------------------------------------------------------------------
WITH base_events AS (
  SELECT *
  FROM `mlab-collaboration.hermes_union.events_with_as_and_geoloc`
  WHERE partition_date = '${DAY}'
    AND DATE(window_start) >= partition_date
    AND NOT EXISTS (
      SELECT 1 FROM UNNEST(reverse_updated_node_details) AS node
      WHERE node.is_interdomain_symmetry = TRUE OR node.is_fishy_type_4 = TRUE
    )
    AND NOT EXISTS (
      SELECT 1 FROM UNNEST(forward_updated_node_details) AS node
      WHERE node.distance_rtt_check = 'Above threshold'
    )
)
-- Forward edges
SELECT
  fr.id,
  CASE WHEN TRIM(SPLIT(fwd.asn_city[OFFSET(i)], '-')[SAFE_OFFSET(0)])
            <= TRIM(SPLIT(fwd.asn_city[OFFSET(i+1)], '-')[SAFE_OFFSET(0)])
    THEN CONCAT(fwd.asn_city[OFFSET(i)], ' - ', fwd.asn_city[OFFSET(i+1)])
    ELSE CONCAT(fwd.asn_city[OFFSET(i+1)], ' - ', fwd.asn_city[OFFSET(i)])
  END AS canonical_edge,
  TRIM(SPLIT(fwd.asn_city[OFFSET(i)], '-')[SAFE_OFFSET(0)]) AS from_asn,
  TRIM(SPLIT(fwd.asn_city[OFFSET(i)], '-')[SAFE_OFFSET(1)]) AS from_metro,
  CONCAT(TRIM(SPLIT(fwd.asn_city[OFFSET(i)], '-')[SAFE_OFFSET(0)]), '-',
         TRIM(SPLIT(fwd.asn_city[OFFSET(i)], '-')[SAFE_OFFSET(1)])) AS from_asn_metro,
  TRIM(SPLIT(fwd.asn_city[OFFSET(i+1)], '-')[SAFE_OFFSET(0)]) AS to_asn,
  TRIM(SPLIT(fwd.asn_city[OFFSET(i+1)], '-')[SAFE_OFFSET(1)]) AS to_metro,
  CONCAT(TRIM(SPLIT(fwd.asn_city[OFFSET(i+1)], '-')[SAFE_OFFSET(0)]), '-',
         TRIM(SPLIT(fwd.asn_city[OFFSET(i+1)], '-')[SAFE_OFFSET(1)])) AS to_asn_metro
FROM base_events AS fr
CROSS JOIN UNNEST(
  ARRAY(SELECT STRUCT(ARRAY_AGG(CONCAT(n.associated_asn, '-', n.place) IGNORE NULLS) AS asn_city)
        FROM UNNEST(fr.forward_updated_node_details) n
        WHERE n.associated_asn IS NOT NULL AND n.place IS NOT NULL)
) AS fwd
CROSS JOIN UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(fwd.asn_city) - 2)) AS i

UNION ALL

-- Reverse edges
SELECT
  fr.id,
  CASE WHEN TRIM(SPLIT(rev.asn_city[OFFSET(i)], '-')[SAFE_OFFSET(0)])
            <= TRIM(SPLIT(rev.asn_city[OFFSET(i+1)], '-')[SAFE_OFFSET(0)])
    THEN CONCAT(rev.asn_city[OFFSET(i)], ' - ', rev.asn_city[OFFSET(i+1)])
    ELSE CONCAT(rev.asn_city[OFFSET(i+1)], ' - ', rev.asn_city[OFFSET(i)])
  END AS canonical_edge,
  TRIM(SPLIT(rev.asn_city[OFFSET(i)], '-')[SAFE_OFFSET(0)]) AS from_asn,
  TRIM(SPLIT(rev.asn_city[OFFSET(i)], '-')[SAFE_OFFSET(1)]) AS from_metro,
  CONCAT(TRIM(SPLIT(rev.asn_city[OFFSET(i)], '-')[SAFE_OFFSET(0)]), '-',
         TRIM(SPLIT(rev.asn_city[OFFSET(i)], '-')[SAFE_OFFSET(1)])) AS from_asn_metro,
  TRIM(SPLIT(rev.asn_city[OFFSET(i+1)], '-')[SAFE_OFFSET(0)]) AS to_asn,
  TRIM(SPLIT(rev.asn_city[OFFSET(i+1)], '-')[SAFE_OFFSET(1)]) AS to_metro,
  CONCAT(TRIM(SPLIT(rev.asn_city[OFFSET(i+1)], '-')[SAFE_OFFSET(0)]), '-',
         TRIM(SPLIT(rev.asn_city[OFFSET(i+1)], '-')[SAFE_OFFSET(1)])) AS to_asn_metro
FROM base_events AS fr
CROSS JOIN UNNEST(
  ARRAY(SELECT STRUCT(ARRAY_AGG(CONCAT(n.associated_asn, '-', n.place) IGNORE NULLS) AS asn_city)
        FROM UNNEST(fr.reverse_updated_node_details) n
        WHERE n.associated_asn IS NOT NULL AND n.place IS NOT NULL)
) AS rev
CROSS JOIN UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(rev.asn_city) - 2)) AS i;

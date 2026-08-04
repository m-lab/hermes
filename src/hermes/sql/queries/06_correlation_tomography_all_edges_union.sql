--------------------------------------------------------------------------------
-- HERMES (union): Correlation tomography — all edges per node
--
-- Returns all (forward + reverse) edges from events_with_as_and_geoloc
-- with the broad filter. Downloaded by Python for hyperedge computation.
--
-- Each node carries its ⟨AS,metro⟩ identity (asn_city, metro-first via
-- COALESCE(metro, place)) and its IXP (ixp_arr, 'None' when the hop is not at an
-- IXP), so the Python step can aggregate culprit fractions at the IXP granularity
-- in addition to ASN, metro, and ⟨AS,metro⟩. The two ARRAY_AGGs run over the same
-- filtered hop set, so they are positionally aligned.
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
  fwd.ixp_arr[OFFSET(i)] AS from_ixp,
  TRIM(SPLIT(fwd.asn_city[OFFSET(i+1)], '-')[SAFE_OFFSET(0)]) AS to_asn,
  TRIM(SPLIT(fwd.asn_city[OFFSET(i+1)], '-')[SAFE_OFFSET(1)]) AS to_metro,
  CONCAT(TRIM(SPLIT(fwd.asn_city[OFFSET(i+1)], '-')[SAFE_OFFSET(0)]), '-',
         TRIM(SPLIT(fwd.asn_city[OFFSET(i+1)], '-')[SAFE_OFFSET(1)])) AS to_asn_metro,
  fwd.ixp_arr[OFFSET(i+1)] AS to_ixp
FROM base_events AS fr
CROSS JOIN UNNEST(
  ARRAY(SELECT STRUCT(
          ARRAY_AGG(CONCAT(n.associated_asn, '-', COALESCE(n.metro, n.place))) AS asn_city,
          ARRAY_AGG(IFNULL(n.associated_ixp, 'None')) AS ixp_arr)
        FROM UNNEST(fr.forward_updated_node_details) n
        WHERE n.associated_asn IS NOT NULL AND COALESCE(n.metro, n.place) IS NOT NULL)
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
  rev.ixp_arr[OFFSET(i)] AS from_ixp,
  TRIM(SPLIT(rev.asn_city[OFFSET(i+1)], '-')[SAFE_OFFSET(0)]) AS to_asn,
  TRIM(SPLIT(rev.asn_city[OFFSET(i+1)], '-')[SAFE_OFFSET(1)]) AS to_metro,
  CONCAT(TRIM(SPLIT(rev.asn_city[OFFSET(i+1)], '-')[SAFE_OFFSET(0)]), '-',
         TRIM(SPLIT(rev.asn_city[OFFSET(i+1)], '-')[SAFE_OFFSET(1)])) AS to_asn_metro,
  rev.ixp_arr[OFFSET(i+1)] AS to_ixp
FROM base_events AS fr
CROSS JOIN UNNEST(
  ARRAY(SELECT STRUCT(
          ARRAY_AGG(CONCAT(n.associated_asn, '-', COALESCE(n.metro, n.place))) AS asn_city,
          ARRAY_AGG(IFNULL(n.associated_ixp, 'None')) AS ixp_arr)
        FROM UNNEST(fr.reverse_updated_node_details) n
        WHERE n.associated_asn IS NOT NULL AND COALESCE(n.metro, n.place) IS NOT NULL)
) AS rev
CROSS JOIN UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(rev.asn_city) - 2)) AS i;

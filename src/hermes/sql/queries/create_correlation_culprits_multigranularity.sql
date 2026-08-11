-- Multi-granularity correlation-tomography culprits (§4.3.2 aggregation).
-- One row per selected culprit from the single mixed-granularity set-cover, which
-- chooses, per anomaly cluster, the entity (at edge / node(⟨AS,metro⟩) / AS / metro
-- / IXP granularity) that best explains the still-unexplained anomalies — coverage
-- first, gated by Fisher's-exact precision, with a distinctness demotion so a coarse
-- entity that subsumes < 2 distinct finer nodes collapses to its node.
--
-- Written by correlation_tomography.run_mixed_granularity_cover /
-- upload_multigranularity. Each anomalous src–dst pair is attributed to exactly one
-- culprit, so anomalies_explained sums to the total explained (no double counting).
--
-- Idempotent: safe to run repeatedly. Bills 0 bytes.
CREATE TABLE IF NOT EXISTS `mlab-collaboration.${DS}.correlation_culprits_multigranularity`
(
  day STRING,
  partition_date DATE,
  information_source STRING,            -- 'forward' | 'reverse'
  granularity STRING,                   -- 'edge' | 'node' | 'AS' | 'metro' | 'IXP'
  entity STRING,                        -- the culprit at that granularity (e.g. '174', 'Dallas-Texas-US', 'LINX_LON1:_Main', '174-Dallas-Texas-US', 'A - B')
  attribution_method STRING,            -- 'correlation' (mixed set-cover) | 'path_local' (singleton tail)
  demoted_from STRING,                  -- set when a coarse pick was demoted by the distinctness guard (e.g. 'AS:174'); else NULL
  iteration_number INT64,
  anomalies_explained INT64,            -- distinct anomalous src_dst_pairs this culprit explains (one-per-pair)
  cumulative_anomalies_explained INT64,
  cumulative_fraction_explained FLOAT64,
  ratio_anomaly FLOAT64,                -- (anom paths/total anom) / (healthy paths/total healthy) through the entity
  p_value FLOAT64,                      -- one-sided Fisher's-exact precision
  odds_ratio FLOAT64,
  support_anomalous INT64,              -- anomalous paths through the entity
  support_healthy INT64,               -- healthy paths through the entity
  anomalous_src_dst_pairs_impacted ARRAY<STRING>
)
PARTITION BY partition_date;

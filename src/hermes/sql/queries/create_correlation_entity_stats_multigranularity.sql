-- Per-entity anomaly stats for EVERY candidate the multi-granularity cover evaluated
-- (winners and non-winners), at all granularities. One row per
-- (partition_date, information_source, granularity, entity). Lets the dashboard zoom
-- in/out to any granularity for a flagged entity — not just the chosen culprits —
-- since the non-winning sub/super-entities (e.g. the whole AS a node was chosen over)
-- are present here with is_culprit = FALSE.
--
-- Entity naming is identical to correlation_culprits_multigranularity:
--   AS = '<asn>'; metro = '<city-region-cc>'; node = '<asn>-<metro>';
--   edge = '<node> - <node>'; IXP = '<ixp name>'.
-- Stats are over all event-day paths (the entity's overall daily footprint).
-- Written by correlation_tomography.upload_entity_stats (load job, per-partition replace).
--
-- Idempotent: safe to run repeatedly. Bills 0 bytes.
CREATE TABLE IF NOT EXISTS `mlab-collaboration.hermes_union.correlation_entity_stats_multigranularity`
(
  partition_date DATE,
  information_source STRING,            -- 'forward' | 'reverse'
  granularity STRING,                   -- 'edge' | 'node' | 'AS' | 'metro' | 'IXP'
  entity STRING,
  support_anomalous INT64,              -- anomalous paths through the entity
  support_healthy INT64,               -- healthy paths through the entity
  ratio_anomaly FLOAT64,               -- over-representation vs baseline (NOT the anomaly rate)
  p_value FLOAT64,                     -- one-sided Fisher's-exact
  odds_ratio FLOAT64,
  is_culprit BOOL                      -- TRUE if the cover selected this exact entity
)
PARTITION BY partition_date;

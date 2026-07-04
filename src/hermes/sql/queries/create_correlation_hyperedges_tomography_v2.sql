-- DDL for the redesigned correlation-tomography output table (coverage + trust
-- redesign, 2026-06-22). Same schema as the original
-- `correlation_hyperedges_tomography` plus seven new attribution/confidence
-- fields, written by `correlation_tomography.py`'s `compute_hyperedges`.
--
-- This is a NEW table so the redesigned pipeline can be validated in parallel
-- with the live one before cutover; at rollout, `06` and the dashboard are
-- pointed here (or this table is renamed over the original).
--
-- Idempotent: safe to run repeatedly. Bills 0 bytes.
CREATE TABLE IF NOT EXISTS `mlab-collaboration.hermes_union.correlation_hyperedges_tomography_v2`
(
  edge_asn_metro STRING,
  day STRING,
  information_source STRING,
  is_interdomain STRING,
  src_dst_pairs_impacted ARRAY<STRING>,
  anomalous_src_dst_pairs_impacted ARRAY<STRING>,
  paths ARRAY<STRUCT<path_type STRING, edge_count INT64, fraction FLOAT64, total_paths INT64, fraction_src_dst_pair FLOAT64, total_src_dst_pairs_in_window INT64>>,
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
  cumulative_fraction_anomalies_explained_so_far FLOAT64,
  left_part STRING,
  right_part STRING,
  from_asn STRING,
  from_metro STRING,
  from_asn_metro STRING,
  to_asn STRING,
  to_metro STRING,
  to_asn_metro STRING,
  from_total_edges_asn_metro INT64,
  from_culprit_edges_asn_metro INT64,
  from_fraction_culprit_asn_metro FLOAT64,
  from_total_edges_asn INT64,
  from_culprit_edges_asn INT64,
  from_fraction_culprit_asn FLOAT64,
  from_total_edges_metro INT64,
  from_culprit_edges_metro INT64,
  from_fraction_culprit_metro FLOAT64,
  to_total_edges_asn_metro INT64,
  to_culprit_edges_asn_metro INT64,
  to_fraction_culprit_asn_metro FLOAT64,
  to_total_edges_asn INT64,
  to_culprit_edges_asn INT64,
  to_fraction_culprit_asn FLOAT64,
  to_total_edges_metro INT64,
  to_culprit_edges_metro INT64,
  to_fraction_culprit_metro FLOAT64,
  -- IXP-granularity aggregation (associated_ixp per endpoint; 'None' when the hop
  -- is not at an IXP). Mirrors the AS / metro / ⟨AS,metro⟩ culprit-fraction fields.
  from_ixp STRING,
  to_ixp STRING,
  from_total_edges_ixp INT64,
  from_culprit_edges_ixp INT64,
  from_fraction_culprit_ixp FLOAT64,
  to_total_edges_ixp INT64,
  to_culprit_edges_ixp INT64,
  to_fraction_culprit_ixp FLOAT64,
  -- Coverage + trust redesign (2026-06-22):
  attribution_method STRING,   -- 'correlation' | 'path_local'
  confidence_tier STRING,      -- 'strong' | 'weak' | 'path_local'
  p_value FLOAT64,             -- one-sided Fisher's-exact (correlation edges)
  odds_ratio FLOAT64,
  support_anomalous INT64,
  support_healthy INT64,
  reason STRING                -- path-local: which per-hop flag fired
)
PARTITION BY partition_date;

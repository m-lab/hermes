-- Per-pair reroute/congestion verdict for temporal tomography. Idempotent; bills 0 bytes.
CREATE TABLE IF NOT EXISTS `mlab-collaboration.hermes_union.temporal_path_verdicts`
(
  partition_date DATE,
  src_dst_pair STRING,
  ip_version STRING,
  verdict STRING,            -- reroute | congestion_in_place | indeterminate
  change_dir STRING,         -- forward | reverse | NULL
  div_forward FLOAT64,
  div_reverse FLOAT64,
  changed_segment STRING,    -- reroute: most-diverted edge
  congested_segment STRING,  -- congestion: worst stable hop edge
  agrees_with_culprit BOOL
)
PARTITION BY partition_date;

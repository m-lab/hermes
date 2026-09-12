-- Idempotent schema migration for upload-throughput anomaly parity.
--
-- Keep this append order identical to create_events_explained_daily.sql and to
-- step 07's explicit INSERT list. CREATE TABLE IF NOT EXISTS cannot evolve an
-- existing table, while ALTER TABLE ADD COLUMN appends each new column.
ALTER TABLE `mlab-collaboration.${DS}.events_explained_daily`
  -- One table update operation: BigQuery rate-limits schema changes per table,
  -- so separate ALTER statements can fail halfway through a fresh rollout.
  ADD COLUMN IF NOT EXISTS baseline_median_upload_throughput FLOAT64,
  ADD COLUMN IF NOT EXISTS median_daily_upload_throughput FLOAT64,
  ADD COLUMN IF NOT EXISTS mean_daily_upload_throughput FLOAT64,
  ADD COLUMN IF NOT EXISTS anomaly_ratio_upload_throughput FLOAT64,
  ADD COLUMN IF NOT EXISTS upload_anomaly_sites INT64,
  ADD COLUMN IF NOT EXISTS total_anomalous_sites_all_signals INT64,
  ADD COLUMN IF NOT EXISTS anomaly_signals ARRAY<STRING>;

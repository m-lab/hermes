-- Idempotent schema migration for explicit client-geography provenance.
-- Run before any Step0-aware writer or the events_enriched compatibility view.
ALTER TABLE `${PROJECT}.${DS}.anomaly_counts_union`
  ADD COLUMN IF NOT EXISTS client_geo_source STRING;

ALTER TABLE `${PROJECT}.${DS}.transient_events_union`
  ADD COLUMN IF NOT EXISTS client_geo_source STRING;

ALTER TABLE `${PROJECT}.${DS}.events_with_as_and_geoloc`
  ADD COLUMN IF NOT EXISTS client_geo_source STRING;

ALTER TABLE `${PROJECT}.${DS}.giga_meter_measurements`
  ADD COLUMN IF NOT EXISTS client_geo_source STRING;

ALTER TABLE `${PROJECT}.${DS}.events_explained_daily`
  ADD COLUMN IF NOT EXISTS client_geo_source STRING;

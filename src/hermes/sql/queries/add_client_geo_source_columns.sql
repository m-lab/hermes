-- Idempotent schema migration for explicit client-geography provenance.
-- Run before any Step0-aware writer or the events_enriched compatibility view.
ALTER TABLE `mlab-collaboration.${DS}.anomaly_counts_union`
  ADD COLUMN IF NOT EXISTS client_geo_source STRING;

ALTER TABLE `mlab-collaboration.${DS}.transient_events_union`
  ADD COLUMN IF NOT EXISTS client_geo_source STRING;

ALTER TABLE `mlab-collaboration.${DS}.events_with_as_and_geoloc`
  ADD COLUMN IF NOT EXISTS client_geo_source STRING;

ALTER TABLE `mlab-collaboration.${DS}.giga_meter_measurements`
  ADD COLUMN IF NOT EXISTS client_geo_source STRING;

ALTER TABLE `mlab-collaboration.${DS}.events_explained_daily`
  ADD COLUMN IF NOT EXISTS client_geo_source STRING;

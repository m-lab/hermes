-- Schema changes for the group-identity work.
-- See docs/proposals/2026-08-group-granularity.md.
--
-- All writers now use explicit column lists. The order below is retained to
-- match existing staging schemas, but it is no longer load-bearing.
--
-- Staging may contain the two pre-review names. Rename them in place when they
-- exist so the already-computed rehearsal data is preserved; fresh datasets
-- simply add the final names.
--
-- ${DS} is the target dataset: hermes_staging to rehearse, the production
-- dataset when promoting. (Deliberately not naming prod here: the rehearsal
-- gate rejects any file containing that string, comments included.)

IF EXISTS (
  SELECT 1 FROM `mlab-collaboration.${DS}.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = 'anomaly_counts_union'
    AND column_name = 'src_group_granularity'
) AND NOT EXISTS (
  SELECT 1 FROM `mlab-collaboration.${DS}.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = 'anomaly_counts_union'
    AND column_name = 'detection_granularity'
) THEN
  ALTER TABLE `mlab-collaboration.${DS}.anomaly_counts_union`
    RENAME COLUMN src_group_granularity TO detection_granularity;
ELSE
  ALTER TABLE `mlab-collaboration.${DS}.anomaly_counts_union`
    ADD COLUMN IF NOT EXISTS detection_granularity STRING;
END IF;
ALTER TABLE `mlab-collaboration.${DS}.anomaly_counts_union`
  ADD COLUMN IF NOT EXISTS src_group_label STRING,
  ADD COLUMN IF NOT EXISTS client_geo_source STRING;

IF EXISTS (
  SELECT 1 FROM `mlab-collaboration.${DS}.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = 'transient_events_union'
    AND column_name = 'src_group_granularity'
) AND NOT EXISTS (
  SELECT 1 FROM `mlab-collaboration.${DS}.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = 'transient_events_union'
    AND column_name = 'detection_granularity'
) THEN
  ALTER TABLE `mlab-collaboration.${DS}.transient_events_union`
    RENAME COLUMN src_group_granularity TO detection_granularity;
ELSE
  ALTER TABLE `mlab-collaboration.${DS}.transient_events_union`
    ADD COLUMN IF NOT EXISTS detection_granularity STRING;
END IF;
ALTER TABLE `mlab-collaboration.${DS}.transient_events_union`
  ADD COLUMN IF NOT EXISTS src_group_label STRING,
  ADD COLUMN IF NOT EXISTS client_geo_source STRING;

-- 04 uses explicit column lists, so order is not load-bearing for these two.
IF EXISTS (
  SELECT 1 FROM `mlab-collaboration.${DS}.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = 'events_with_as_and_geoloc'
    AND column_name = 'src_group_granularity'
) AND NOT EXISTS (
  SELECT 1 FROM `mlab-collaboration.${DS}.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = 'events_with_as_and_geoloc'
    AND column_name = 'detection_granularity'
) THEN
  ALTER TABLE `mlab-collaboration.${DS}.events_with_as_and_geoloc`
    RENAME COLUMN src_group_granularity TO detection_granularity;
ELSE
  ALTER TABLE `mlab-collaboration.${DS}.events_with_as_and_geoloc`
    ADD COLUMN IF NOT EXISTS detection_granularity STRING;
END IF;
ALTER TABLE `mlab-collaboration.${DS}.events_with_as_and_geoloc`
  ADD COLUMN IF NOT EXISTS src_group_label STRING,
  ADD COLUMN IF NOT EXISTS src_metro STRING,
  ADD COLUMN IF NOT EXISTS client_geo_source STRING;

IF EXISTS (
  SELECT 1 FROM `mlab-collaboration.${DS}.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = 'giga_meter_measurements'
    AND column_name = 'src_group_granularity'
) AND NOT EXISTS (
  SELECT 1 FROM `mlab-collaboration.${DS}.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = 'giga_meter_measurements'
    AND column_name = 'detection_granularity'
) THEN
  ALTER TABLE `mlab-collaboration.${DS}.giga_meter_measurements`
    RENAME COLUMN src_group_granularity TO detection_granularity;
ELSE
  ALTER TABLE `mlab-collaboration.${DS}.giga_meter_measurements`
    ADD COLUMN IF NOT EXISTS detection_granularity STRING;
END IF;
ALTER TABLE `mlab-collaboration.${DS}.giga_meter_measurements`
  ADD COLUMN IF NOT EXISTS src_group_label STRING,
  ADD COLUMN IF NOT EXISTS src_metro STRING,
  ADD COLUMN IF NOT EXISTS client_geo_source STRING;

-- The public table appends seven columns, in exactly this order
-- (final_result: ... confidence_tier, detection_granularity, src_metro,
-- src_group_label, n_dayof, src_match_granularity, client_geo_source,
-- n_baseline).
-- Adjacent STRINGs mean a
-- transposition is type-valid and silent; the INT64s in positions 4 and 7 are the
-- only positional anchors. verify_group_identity.sql check 4 tests them.
IF EXISTS (
  SELECT 1 FROM `mlab-collaboration.${DS}.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = 'events_explained_daily'
    AND column_name = 'src_group_granularity'
) AND NOT EXISTS (
  SELECT 1 FROM `mlab-collaboration.${DS}.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = 'events_explained_daily'
    AND column_name = 'detection_granularity'
) THEN
  ALTER TABLE `mlab-collaboration.${DS}.events_explained_daily`
    RENAME COLUMN src_group_granularity TO detection_granularity;
ELSE
  ALTER TABLE `mlab-collaboration.${DS}.events_explained_daily`
    ADD COLUMN IF NOT EXISTS detection_granularity STRING;
END IF;
ALTER TABLE `mlab-collaboration.${DS}.events_explained_daily`
  ADD COLUMN IF NOT EXISTS src_metro STRING,
  ADD COLUMN IF NOT EXISTS src_group_label STRING,
  ADD COLUMN IF NOT EXISTS n_dayof INT64,
  ADD COLUMN IF NOT EXISTS client_geo_source STRING;
IF EXISTS (
  SELECT 1 FROM `mlab-collaboration.${DS}.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = 'events_explained_daily'
    AND column_name = 'attribution_granularity'
) AND NOT EXISTS (
  SELECT 1 FROM `mlab-collaboration.${DS}.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = 'events_explained_daily'
    AND column_name = 'src_match_granularity'
) THEN
  ALTER TABLE `mlab-collaboration.${DS}.events_explained_daily`
    RENAME COLUMN attribution_granularity TO src_match_granularity;
ELSE
  ALTER TABLE `mlab-collaboration.${DS}.events_explained_daily`
    ADD COLUMN IF NOT EXISTS src_match_granularity STRING;
END IF;
-- n_baseline is added LAST, on its own, and must stay last: the live tables were
-- created with client_geo_source in final position, so appending here is what keeps
-- the CREATE DDL and an ALTERed table in the same order.
ALTER TABLE `mlab-collaboration.${DS}.events_explained_daily`
  ADD COLUMN IF NOT EXISTS n_baseline INT64;

--------------------------------------------------------------------------------
-- Make the time filter mandatory on every partitioned table in ${DS}.
--
-- With require_partition_filter = TRUE, BigQuery REJECTS at plan time -- billing
-- zero bytes -- any query that does not constrain the partitioning column. The
-- unbounded scan that used to cost real money now costs an error message.
--
-- This is aimed at readers OUTSIDE this repo (dashboards, notebooks, ad-hoc `bq`
-- and console queries) as much as inside it. Every pipeline step already filters
-- on partition_date, so production SQL is unaffected; see the audit note below.
--
-- The table list is DERIVED, not hard-coded. A hard-coded list is the failure
-- mode this repo keeps hitting: a table added later is not in it, nobody
-- notices, and the one table that most needed the guard is the one without it.
-- Anything in ${DS} that is partitioned gets the option, whatever its
-- partitioning column is named. Verified against the live datasets on
-- 2026-09-23: 12 partitioned tables in hermes_union, 14 in hermes_staging, all
-- on partition_date. The derived list is what catches the ones no registry in
-- this repo mentions -- hermes_union.correlation_hyperedges_tomography (the v1
-- table, absent from DERIVED_OUTPUT_TABLES) and hermes_staging's three
-- upload_sandbox_* tables.
--
-- Unpartitioned tables are skipped: the option is meaningless there and setting
-- it is an error. In ${DS} that means the lookups place_canonical_metro and
-- giga_school_ips -- so step 04's `month_start` predicate on giga_school_ips is
-- its own choice, not something this file requires.
--
-- Idempotent -- SET OPTIONS to the same value is a no-op -- and bills 0 bytes.
--
-- REVERSAL. This is a table option, not a migration: it changes no data and no
-- schema. To lift it for one table, run
--   ALTER TABLE `mlab-collaboration.${DS}.<table>`
--     SET OPTIONS (require_partition_filter = FALSE);
--
-- AUDIT at the time this was introduced -- every in-repo reader of a ${DS} table
-- and the filter it already carries:
--   steps 02/03  merged_download_upload      partition_date BETWEEN week .. day
--   step  03     anomaly_counts_union        partition_date = day
--   step  04     transient_events_union      partition_date = day  (x7 sites)
--   step  04     giga_school_ips             month_start = month of day
--   step  04     giga_meter_measurements     partition_date BETWEEN day-7 .. day
--   steps 05/06  events_with_as_and_geoloc   partition_date = day
--   step  07     events_explained_daily      partition_date = day (DELETE)
--   step  07     correlation_hyperedges_..   partition_date = day
--   Phase D/E    (correlation_tomography.py, temporal_verdict.py)
--                                            DATE(partition_date) = day
--   init_staging merged_download_upload      partition_date BETWEEN start .. end
--
-- Parameters:
--   ${DS} dataset whose partitioned tables are constrained
--------------------------------------------------------------------------------

FOR partitioned_table IN (
  SELECT DISTINCT table_name
  FROM `mlab-collaboration.${DS}.INFORMATION_SCHEMA.COLUMNS`
  WHERE is_partitioning_column = 'YES'
  ORDER BY table_name
) DO
  EXECUTE IMMEDIATE FORMAT(
    "ALTER TABLE `mlab-collaboration.${DS}.%s` SET OPTIONS (require_partition_filter = TRUE)",
    partitioned_table.table_name
  );
END FOR;

-- Verification tail. The bootstrap discards these rows, but a hand-run of this
-- file prints exactly which tables now demand a filter -- the only direct
-- evidence that the ALTERs above landed.
SELECT
  table_name,
  option_value AS require_partition_filter
FROM `mlab-collaboration.${DS}.INFORMATION_SCHEMA.TABLE_OPTIONS`
WHERE option_name = 'require_partition_filter'
ORDER BY table_name;

-- Idempotent schema migration for the baseline measurement count.
--
-- MUST run before step 07, which names n_baseline in its INSERT column list: on a
-- table created before this column existed, that INSERT fails outright. This file is
-- in bootstrap_tables.DDL_FILES for exactly that reason -- CREATE TABLE IF NOT EXISTS
-- is a no-op on an existing table and will never add the column by itself.
--
-- n_baseline is the companion to n_dayof: measurements for the group in the trailing
-- baseline window, counted on src_group_label, the key the detector actually grouped
-- on. Consumers were re-deriving it by aggregating the whole partition per query, on a
-- coarser key than the detector used.
--
-- Appended last on purpose. The live tables were created with client_geo_source in
-- final position, and ALTER TABLE ADD COLUMN appends; keeping the CREATE DDL in the
-- same order is what stops a fresh table and a migrated one from disagreeing.
ALTER TABLE `mlab-collaboration.${DS}.events_explained_daily`
  ADD COLUMN IF NOT EXISTS n_baseline INT64;

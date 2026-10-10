-- Idempotent schema migration: the selected upload's connection id per test.
--
-- MUST run before step 01 writes it. Step 01's INSERT is positional (no column
-- list), so on a table without this column the INSERT fails outright -- which is
-- the safe direction: no silent NULLs. Existing partitions keep NULL here; step
-- 03 then falls back to the download id alone, i.e. the old behaviour.
--
-- Appended last on purpose, matching step 01's final SELECT column.
ALTER TABLE `mlab-collaboration.${DS}.merged_download_upload`
  ADD COLUMN IF NOT EXISTS upload_id STRING;

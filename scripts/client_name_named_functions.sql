-- Table functions returning a HERMES table with each test's OWN client_name.
--
-- Before 2026-10-20, step 03 stored the group's MIN(client_name) on every test that
-- matched a step-02 group (fixed in #61): 58% of test rows on 2026-09-10 carried a
-- name that was not their own. Rewriting the column costs ~62 TiB on-demand, so the
-- stored history is left as is and these functions supply the right value instead:
-- the test's own name from merged_download_upload (step 01) before the cutover, the
-- stored value from it on. Validated on 2026-09-10 against a staging run of the fixed
-- step 03: 35,338,949 of 35,338,949 tests identical, row counts unchanged.
--
--   SELECT * FROM `mlab-collaboration.hermes_union.events_with_as_and_geoloc_named`(
--     DATE '2026-09-01', DATE '2026-09-30')
--
-- Reads only id and client_name from merged_download_upload for
-- [start_date - 8 days, end_date] (step 03's 7-day lookback plus a day), ~0.25 GiB/day.
-- Create with: ${DS} = hermes_union (or hermes_staging).

CREATE OR REPLACE TABLE FUNCTION `mlab-collaboration.${DS}.events_with_as_and_geoloc_named`(
  start_date DATE, end_date DATE)
AS (
  SELECT e.* REPLACE (
    IF(e.partition_date >= DATE '2026-10-20', e.client_name, n.client_name) AS client_name)
  FROM `mlab-collaboration.${DS}.events_with_as_and_geoloc` e
  LEFT JOIN (
    SELECT id, ANY_VALUE(client_name) AS client_name
    FROM `mlab-collaboration.${DS}.merged_download_upload`
    WHERE partition_date BETWEEN DATE_SUB(start_date, INTERVAL 8 DAY) AND end_date
    GROUP BY id) n
  USING (id)
  WHERE e.partition_date BETWEEN start_date AND end_date
);

CREATE OR REPLACE TABLE FUNCTION `mlab-collaboration.${DS}.transient_events_union_named`(
  start_date DATE, end_date DATE)
AS (
  SELECT t.* REPLACE (
    IF(t.partition_date >= DATE '2026-10-20', t.client_name, n.client_name) AS client_name)
  FROM `mlab-collaboration.${DS}.transient_events_union` t
  LEFT JOIN (
    SELECT id, ANY_VALUE(client_name) AS client_name
    FROM `mlab-collaboration.${DS}.merged_download_upload`
    WHERE partition_date BETWEEN DATE_SUB(start_date, INTERVAL 8 DAY) AND end_date
    GROUP BY id) n
  USING (id)
  WHERE t.partition_date BETWEEN start_date AND end_date
);

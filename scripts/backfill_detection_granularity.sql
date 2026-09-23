-- Idempotently label historical TESTED populations with the detector regime
-- that produced them. Run group_identity_alters.sql first so the columns exist.
--
-- Substitute ${DS} with the target dataset and ${LEGACY_THROUGH_DAY} with the
-- last partition written by the legacy pipeline. Every row through that cutoff
-- came from its anomaly-group INNER JOIN and was therefore city-detected. The
-- cutoff is mandatory: after the newer LEFT-JOIN pipeline is deployed, unmatched
-- giga-meter traces legitimately have NULL detection identity and must stay NULL.
--
-- The `partition_date > DATE '1970-01-01'` predicates below are sentinels, not
-- filters. These checks are meant to span the whole table, but ${DS} tables now
-- carry require_partition_filter = TRUE, which rejects a query with no predicate
-- on the partitioning column. The sentinel satisfies that and says in one place
-- that the full scan is intended -- and it is greppable, so a genuinely
-- unbounded scan stays easy to find.
--
-- See docs/proposals/2026-08-group-granularity.md.

ASSERT (
  SELECT COUNTIF(detection_granularity NOT IN ('maxmind_city', 'city', 'metro')) = 0
  FROM `mlab-collaboration.${DS}.anomaly_counts_union`
  WHERE partition_date > DATE '1970-01-01'  -- whole table, on purpose
) AS 'anomaly_counts_union contains an unsupported detection_granularity';

ASSERT (
  SELECT COUNTIF(detection_granularity NOT IN ('maxmind_city', 'city', 'metro')) = 0
  FROM `mlab-collaboration.${DS}.transient_events_union`
  WHERE partition_date > DATE '1970-01-01'  -- whole table, on purpose
) AS 'transient_events_union contains an unsupported detection_granularity';

ASSERT (
  SELECT COUNTIF(detection_granularity NOT IN ('maxmind_city', 'city', 'metro')) = 0
  FROM `mlab-collaboration.${DS}.events_with_as_and_geoloc`
  WHERE partition_date > DATE '1970-01-01'  -- whole table, on purpose
) AS 'events_with_as_and_geoloc contains an unsupported detection_granularity';

ASSERT (
  SELECT COUNTIF(detection_granularity NOT IN ('maxmind_city', 'city', 'metro')) = 0
  FROM `mlab-collaboration.${DS}.giga_meter_measurements`
  WHERE partition_date > DATE '1970-01-01'  -- whole table, on purpose
) AS 'giga_meter_measurements contains an unsupported detection_granularity';

ASSERT (
  SELECT COUNTIF(detection_granularity NOT IN ('maxmind_city', 'city', 'metro')) = 0
  FROM `mlab-collaboration.${DS}.events_explained_daily`
  WHERE partition_date > DATE '1970-01-01'  -- whole table, on purpose
) AS 'events_explained_daily contains an unsupported detection_granularity';

UPDATE `mlab-collaboration.${DS}.anomaly_counts_union`
SET detection_granularity = 'maxmind_city', client_geo_source = 'maxmind'
WHERE detection_granularity IS NULL
  AND partition_date <= DATE '${LEGACY_THROUGH_DAY}';

UPDATE `mlab-collaboration.${DS}.transient_events_union`
SET detection_granularity = 'maxmind_city', client_geo_source = 'maxmind'
WHERE detection_granularity IS NULL
  AND partition_date <= DATE '${LEGACY_THROUGH_DAY}';

UPDATE `mlab-collaboration.${DS}.events_with_as_and_geoloc`
SET detection_granularity = 'maxmind_city', client_geo_source = 'maxmind'
WHERE detection_granularity IS NULL
  AND partition_date <= DATE '${LEGACY_THROUGH_DAY}';

UPDATE `mlab-collaboration.${DS}.giga_meter_measurements`
SET detection_granularity = 'maxmind_city', client_geo_source = 'maxmind'
WHERE detection_granularity IS NULL
  AND partition_date <= DATE '${LEGACY_THROUGH_DAY}';

UPDATE `mlab-collaboration.${DS}.events_explained_daily`
SET detection_granularity = 'maxmind_city', client_geo_source = 'maxmind'
WHERE detection_granularity IS NULL
  AND partition_date <= DATE '${LEGACY_THROUGH_DAY}';

SELECT 'anomaly_counts_union' AS table_name,
       COUNTIF(detection_granularity IS NULL) AS remaining_null,
       COUNTIF(detection_granularity = 'maxmind_city') AS maxmind_city,
       COUNTIF(detection_granularity = 'metro') AS metro
FROM `mlab-collaboration.${DS}.anomaly_counts_union`
WHERE partition_date > DATE '1970-01-01'  -- whole table, on purpose
UNION ALL
SELECT 'transient_events_union',
       COUNTIF(detection_granularity IS NULL),
       COUNTIF(detection_granularity = 'maxmind_city'),
       COUNTIF(detection_granularity = 'metro')
FROM `mlab-collaboration.${DS}.transient_events_union`
WHERE partition_date > DATE '1970-01-01'  -- whole table, on purpose
UNION ALL
SELECT 'events_with_as_and_geoloc',
       COUNTIF(detection_granularity IS NULL),
       COUNTIF(detection_granularity = 'maxmind_city'),
       COUNTIF(detection_granularity = 'metro')
FROM `mlab-collaboration.${DS}.events_with_as_and_geoloc`
WHERE partition_date > DATE '1970-01-01'  -- whole table, on purpose
UNION ALL
SELECT 'giga_meter_measurements',
       COUNTIF(detection_granularity IS NULL),
       COUNTIF(detection_granularity = 'maxmind_city'),
       COUNTIF(detection_granularity = 'metro')
FROM `mlab-collaboration.${DS}.giga_meter_measurements`
WHERE partition_date > DATE '1970-01-01'  -- whole table, on purpose
UNION ALL
SELECT 'events_explained_daily',
       COUNTIF(detection_granularity IS NULL),
       COUNTIF(detection_granularity = 'maxmind_city'),
       COUNTIF(detection_granularity = 'metro')
FROM `mlab-collaboration.${DS}.events_explained_daily`
WHERE partition_date > DATE '1970-01-01';  -- whole table, on purpose

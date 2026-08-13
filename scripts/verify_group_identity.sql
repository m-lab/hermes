-- Acceptance checks for the group-identity change.
-- Run against the REHEARSAL dataset after rebuilding one partition, comparing
-- to the untouched production partition for the same date.
-- See docs/proposals/2026-08-group-granularity.md.
--
-- Substitute ${DAY} and ${EXPECTED_GRANULARITY}. Every `bad_*` column must be 0.
-- New rehearsal rows always use IPInfo client geography.

DECLARE _expected_granularity STRING DEFAULT '${EXPECTED_GRANULARITY}';
ASSERT _expected_granularity IN ('city', 'metro')
  AS 'EXPECTED_GRANULARITY must be city or metro';

-- 1. IDENTITY PRESERVED. The detection label reaches the mapped table intact.
--    Before this change src_city was overwritten with the metro and the label
--    was unrecoverable; src_group_label must now equal what 03 wrote.
SELECT '1_identity_preserved' AS check_,
       COUNT(*)                                              AS rows_,
       COUNTIF(e.src_group_label IS NULL)                    AS bad_null_label,
       COUNTIF(e.src_group_label != t.src_city)              AS bad_label_mismatch,
       COUNTIF(
         e.detection_granularity != _expected_granularity
         OR e.client_geo_source != 'ipinfo'
       ) AS bad_provenance
FROM `mlab-collaboration.hermes_staging.events_with_as_and_geoloc` e
JOIN `mlab-collaboration.hermes_staging.transient_events_union` t
  USING (id, partition_date)
WHERE e.partition_date = DATE '${DAY}'
  AND t.src_group_label IS NOT NULL   -- untested giga traces legitimately NULL

UNION ALL

-- 2. POSITIONAL SWAP GUARD. granularity and label are both STRING and 02/03
--    INSERT by position, so a wrong ALTER order swaps them with no error.
SELECT '2_no_column_swap',
       COUNT(*),
       COUNTIF(detection_granularity != _expected_granularity),
       COUNTIF(src_group_label = _expected_granularity),
       0
FROM `mlab-collaboration.hermes_staging.transient_events_union`
WHERE partition_date = DATE '${DAY}' AND detection_granularity IS NOT NULL

UNION ALL

-- 3. DISPLAY/GROUP CONTRACT. In city mode, src_city keeps the IPInfo city
--    name. In metro mode, both src_city (the pre-public readable label) and
--    src_metro equal the exact metro detection label.
--
--    The city-mode invariant is the one the old
--    overwrite violated: 04 replaced src_city wholesale, so Benicassim became
--    Castello. src_city now takes the metro's STATE but must still lead with
--    the city name from src_group_label.
--
--    Do NOT assert src_city != src_metro. They are legitimately equal whenever
--    a city names its own metro (Auckland-AUK-NZ -> src_city
--    Auckland-Auckland-NZ == src_metro), which is the common case for the
--    cities carrying most rows. An earlier revision asserted equality (the
--    superseded legacy-alias design) and its inverse; both are wrong.
--
--    Right-parse src_group_label only: ISO subdivision codes never contain '-',
--    so City-ISO-CC is unambiguous. A label with fewer than 3 components yields
--    an empty city and is flagged -- that is a malformed label, worth seeing.
SELECT '3_display_group_contract',
       COUNT(*),
       COUNTIF(IF(
         _expected_granularity = 'metro',
         src_city != src_group_label OR src_metro != src_group_label,
         NOT STARTS_WITH(src_city, CONCAT(label_city, '-'))
       )),
       0, 0
FROM (
  SELECT src_city,
    ARRAY_TO_STRING(ARRAY(
      SELECT p FROM UNNEST(SPLIT(src_group_label, '-')) p WITH OFFSET o
      WHERE o <= ARRAY_LENGTH(SPLIT(src_group_label, '-')) - 3 ORDER BY o), '-')
      AS label_city
  FROM `mlab-collaboration.hermes_staging.events_with_as_and_geoloc`
  WHERE partition_date = DATE '${DAY}'
    AND src_group_label IS NOT NULL AND src_metro IS NOT NULL
)

UNION ALL

-- 4. PUBLIC IDENTITY CONTRACT. Every public label must join back to an actual
--    event group for the same key, and its count/provenance must be populated.
--    This detects a label/metro transposition without incorrectly assuming the
--    two strings can never be equal.
SELECT '4_public_columns_not_transposed',
       COUNT(*),
       COUNTIF(
         p.detection_granularity != _expected_granularity
         OR p.client_geo_source != 'ipinfo'
       ),
       COUNTIF(p.n_dayof IS NULL OR p.n_dayof < 1),
       COUNTIF(e.src_group_label IS NULL)
FROM `mlab-collaboration.hermes_staging.events_explained_daily` p
LEFT JOIN (
  SELECT DISTINCT src_asn, src_group_label, dst_site, ip_version, partition_date
  FROM `mlab-collaboration.hermes_staging.events_with_as_and_geoloc`
  WHERE partition_date = DATE '${DAY}' AND src_group_label IS NOT NULL
) e
  ON e.src_asn = p.src_asn
 AND e.src_group_label = p.src_group_label
 AND e.dst_site = p.dst_site
 AND e.ip_version = p.ip_version
 AND e.partition_date = p.partition_date
WHERE p.partition_date = DATE '${DAY}';

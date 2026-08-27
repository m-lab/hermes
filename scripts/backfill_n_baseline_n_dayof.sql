--------------------------------------------------------------------------------
-- ONE-OFF BACKFILL — n_baseline / n_dayof on events_explained_daily
--
-- Written 2026-08-26. NOT run against production.
--
-- WHY THIS EXISTS
-- ---------------
-- `n_baseline` and `n_dayof` are NULL on every partition written before
-- 2026-08-01. The columns arrived with the metro-era rewrite
-- (add_n_baseline_column.sql is an `ADD COLUMN IF NOT EXISTS`) and step 07 only
-- populated them going forward. Nothing was lost: the columns are derived, and
-- their input -- events_with_as_and_geoloc -- retains partitions back to
-- 2025-06-15.
--
-- This reproduces the deployed definition exactly
-- (07_translating_to_public_format_union.sql, `group_counts` CTE):
--
--     COUNTIF(DATE(window_start) >= '${DAY}') AS n_dayof,
--     COUNTIF(DATE(window_start) <  '${DAY}') AS n_baseline
--
-- THE JOIN KEY -- THE ONLY HARD PART
-- ----------------------------------
-- The counts must attach at the group the detector actually used. Three keys
-- were tried and measured before the right one; the failures are the reason
-- this file carries so much comment:
--
--   src_group_label            0%      NULL for the whole MaxMind era in BOTH
--                                      tables, and NULL never joins.
--   right-parse City-State-CC  92.3%   BOTH city and state names contain
--                                      hyphens. 'Wuppertal-Nordrhein-Westfalen-DE'
--                                      parses to 'Wuppertal-Nordrhein';
--                                      "'s-Hertogenbosch-..." breaks the other way.
--   prefix/suffix anchoring    98.6%   36 unmatched, 265 ambiguous multi-matches.
--   SAME-TRANSFORM (below)     100.000%
--
-- events_explained_daily.src_city is a LOSSY display projection of the group
-- identity. Step 07 says so outright:
--
--     "Never reconstruct or join on src_city here: display-state substitution
--      is not injective, while src_group_label is exactly what detection
--      grouped on."
--
-- For the MaxMind era that projection is SPLIT(src_city,'-')[OFFSET(0)], which
-- the pipeline's own comment notes collapses 4.17% of names (Saint-Agapit and
-- Saint-Georges both become 'Saint'). No parse inverts a non-injective map.
--
-- So do not invert it -- REAPPLY it. Applying the identical transform to the
-- events_with_as_and_geoloc side before grouping makes both sides collapse the
-- same way. The join becomes total, and where names merge the counts simply sum
-- over the same merged group the detector itself grouped on.
--
-- COST
-- ----
-- events_with_as_and_geoloc is ~68 GiB per partition, but these five columns
-- prune to ~0.9 GiB/day. May-August 2026 (116 days) measured at 193 GiB.
--
-- SAFETY
-- ------
--   * Writes production. prod-change-gate applies: run against hermes_staging
--     first (`--target staging`), compare, then promote.
--   * `WHERE t.n_baseline IS NULL` makes this idempotent and stops it from
--     overwriting values the pipeline wrote correctly. Do not remove it.
--   * Verify with verify_n_baseline_backfill.sql over a range where stored
--     values already exist, and require EXACT equality -- not a correlation.
--
-- Params: ${START_DATE}, ${END_DATE}, ${DS} (hermes_union | hermes_staging)
--------------------------------------------------------------------------------

UPDATE `mlab-collaboration.${DS}.events_explained_daily` AS t
SET
  t.n_baseline = gc.n_baseline_rc,
  t.n_dayof    = gc.n_dayof_rc
FROM (
  SELECT
    partition_date AS day,
    -- The era's own shortening, reapplied. See the header.
    SPLIT(src_city, '-')[OFFSET(0)] AS city_short,
    src_country,
    src_asn,
    dst_site,
    ip_version,
    COUNTIF(DATE(window_start) >= partition_date) AS n_dayof_rc,
    COUNTIF(DATE(window_start) <  partition_date) AS n_baseline_rc
  FROM `mlab-collaboration.${DS}.events_with_as_and_geoloc`
  WHERE partition_date BETWEEN '${START_DATE}' AND '${END_DATE}'
  GROUP BY 1, 2, 3, 4, 5, 6
) AS gc
WHERE t.partition_date BETWEEN '${START_DATE}' AND '${END_DATE}'
  -- Idempotence guard: only fill what was never populated.
  AND t.n_baseline IS NULL
  AND t.partition_date = gc.day
  AND t.src_city       = gc.city_short
  AND t.src_country    = gc.src_country
  AND t.src_asn        = gc.src_asn
  AND t.dst_site       = gc.dst_site
  AND t.ip_version     = gc.ip_version
;

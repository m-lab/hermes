--------------------------------------------------------------------------------
-- VERIFY the n_baseline / n_dayof recomputation is EXACT.
--
-- Run this over a range where the stored values ALREADY exist (i.e. on or after
-- 2026-08-01). If the recomputation reproduces those, it can be trusted on the
-- earlier partitions where nothing is stored to compare against.
--
-- Require exact equality. A correlation is not sufficient: two systematically
-- offset series correlate at 1.0 while being wrong everywhere.
--
-- Result 2026-08-26 over 2026-08-05..08:
--     matched_rows 5077 | dayof_exact 5077 | baseline_exact 5077
--     corr_dayof 1.0    | corr_baseline 1.0
--
-- Note this control uses src_group_label, which is populated in the metro era.
-- The backfill itself cannot use that key -- it is NULL for every MaxMind-era
-- partition -- which is exactly why the backfill reapplies the display
-- projection instead. See backfill_n_baseline_n_dayof.sql.
--
-- Params: ${START_DATE}, ${END_DATE}, ${DS}
--------------------------------------------------------------------------------

WITH recomputed AS (
  SELECT
    partition_date, src_asn, src_group_label, dst_site, ip_version,
    COUNTIF(DATE(window_start) >= partition_date) AS n_dayof_rc,
    COUNTIF(DATE(window_start) <  partition_date) AS n_baseline_rc
  FROM `mlab-collaboration.${DS}.events_with_as_and_geoloc`
  WHERE partition_date BETWEEN '${START_DATE}' AND '${END_DATE}'
  GROUP BY 1, 2, 3, 4, 5
),
existing AS (
  SELECT
    partition_date, src_asn, src_group_label, dst_site, ip_version,
    ANY_VALUE(n_dayof)    AS n_dayof,
    ANY_VALUE(n_baseline) AS n_baseline
  FROM `mlab-collaboration.${DS}.events_explained_daily`
  WHERE partition_date BETWEEN '${START_DATE}' AND '${END_DATE}'
  GROUP BY 1, 2, 3, 4, 5
)
SELECT
  COUNT(*)                                     AS matched_rows,
  COUNTIF(e.n_dayof    = r.n_dayof_rc)         AS dayof_exact,
  COUNTIF(e.n_baseline = r.n_baseline_rc)      AS baseline_exact,
  -- Both must equal matched_rows. Anything less means the key is wrong.
  COUNTIF(e.n_dayof    != r.n_dayof_rc)        AS dayof_mismatch,
  COUNTIF(e.n_baseline != r.n_baseline_rc)     AS baseline_mismatch,
  ROUND(CORR(e.n_dayof,    r.n_dayof_rc), 4)   AS corr_dayof,
  ROUND(CORR(e.n_baseline, r.n_baseline_rc), 4) AS corr_baseline
FROM existing e
JOIN recomputed r
  USING (partition_date, src_asn, src_group_label, dst_site, ip_version)
;

-- Is a full-snapshot load complete enough to be selected by step 04?
--
-- n          : rows in the snapshot being judged
-- neighbours : row counts of up to two snapshots on each side (same table, same
--              source), excluding the snapshot itself
--
-- A snapshot is complete when it holds at least HALF the median of its
-- neighbours. Calibrated on the data (2026-10-27):
--   * the failure it must catch is a truncated batched upload that the
--     enricher's has_rows_for_date guard then makes permanent: RouteViews v4
--     2025-10-17 holds 20,000 prefixes against ~1.05-1.08M around it (1.9%);
--   * the change it must tolerate is a genuine source shift: the IXP member list
--     fell from ~94K to ~57K on 2025-08-25 and stayed there (75% of the
--     neighbour median on that date), and 2025-12-07 / 12-11 IXP hold 44K (76%).
-- 90% would discard both of those real snapshots; 50% keeps them and still
-- rejects any truncation worse than half. A snapshot with no neighbours is
-- trivially complete.
CREATE TEMP FUNCTION snapshot_is_complete(n INT64, neighbours ARRAY<INT64>) RETURNS BOOL AS (
  ARRAY_LENGTH(IFNULL(neighbours, [])) = 0
  OR n >= 0.5 * (
    SELECT AVG(x)
    FROM (
      SELECT x, ROW_NUMBER() OVER (ORDER BY x) AS r, COUNT(*) OVER () AS c
      FROM UNNEST(neighbours) AS x
    )
    WHERE r IN (DIV(c + 1, 2), DIV(c + 2, 2))
  )
);

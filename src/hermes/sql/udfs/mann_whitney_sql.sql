-- Mann-Whitney U in SQL, on whole arrays, evaluated per row (step 02).
--
-- Replaces the JS mann_whitney_u UDF there. Same statistic: average ranks with
-- ties (rank = before + (t + 1) / 2), U = min(U1, U2), tie-corrected stdU,
-- continuity-corrected Z = min(0, U - meanU + 0.5) / stdU, two-sided normal p via
-- the same Abramowitz-Stegun 7.1.26 erf. Ranks are half-integers and every sum is
-- far below 2^53, so the result is exact in FLOAT64 whatever the summation order.
-- Verified bit-identical to the JS UDF in BigQuery (ties, shifts, NaN, empty).
-- Being SQL it has no JS memory ceiling, so it runs on the FULL arrays.
-- IEEE_DIVIDE keeps the JS Inf/NaN semantics where the UDF divided by zero.
CREATE TEMP FUNCTION erf_as(x FLOAT64) AS (
  IF(x >= 0, 1, -1) * (1 - (((((1.061405429 * IEEE_DIVIDE(1, 1 + 0.3275911 * ABS(x))
    - 1.453152027) * IEEE_DIVIDE(1, 1 + 0.3275911 * ABS(x)) + 1.421413741)
    * IEEE_DIVIDE(1, 1 + 0.3275911 * ABS(x)) - 0.284496736)
    * IEEE_DIVIDE(1, 1 + 0.3275911 * ABS(x)) + 0.254829592)
    * IEEE_DIVIDE(1, 1 + 0.3275911 * ABS(x)) * EXP(-x * x)))
);
CREATE TEMP FUNCTION normal_cdf_as(x FLOAT64) AS ((1 + erf_as(x / SQRT(2))) / 2);
CREATE TEMP FUNCTION mann_whitney_sql(a ARRAY<FLOAT64>, b ARRAY<FLOAT64>) AS ((
  SELECT IF(IFNULL(n1, 0) < 1 OR IFNULL(n2, 0) < 1,
    -- the JS UDF returned a struct of NULLs here, not a NULL struct
    STRUCT(CAST(NULL AS FLOAT64) AS U, CAST(NULL AS FLOAT64) AS Z, CAST(NULL AS FLOAT64) AS p_value,
      CAST(NULL AS FLOAT64) AS meanU, CAST(NULL AS FLOAT64) AS stdU),
    STRUCT(
    u AS U,
    IEEE_DIVIDE(LEAST(0, u - mean_u + 0.5), std_u) AS Z,
    2 * (1 - normal_cdf_as(ABS(IEEE_DIVIDE(LEAST(0, u - mean_u + 0.5), std_u)))) AS p_value,
    mean_u AS meanU,
    std_u AS stdU))
  FROM (
    SELECT n1, n2, LEAST(u1, n1 * n2 - u1) AS u, (n1 * n2) / 2 AS mean_u,
      SQRT((n1 * n2 / 12) * ((n1 + n2 + 1) - IEEE_DIVIDE(tie, (n1 + n2) * (n1 + n2 - 1)))) AS std_u
    FROM (
      SELECT n1, n2, n1 * n2 + (n1 * (n1 + 1)) / 2 - r1 AS u1, tie
      FROM (
        SELECT CAST(SUM(c) AS FLOAT64) AS n1, CAST(SUM(t - c) AS FLOAT64) AS n2,
          SUM(c * (before + (t + 1) / 2)) AS r1,
          SUM(CAST(t AS FLOAT64) * (CAST(t AS FLOAT64) * t - 1)) AS tie
        FROM (
          SELECT t, c, SUM(t) OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) - t AS before
          FROM (
            SELECT x, COUNT(*) AS t, COUNTIF(side = 1) AS c
            FROM (SELECT x, 1 AS side FROM UNNEST(a) AS x UNION ALL SELECT x, 2 FROM UNNEST(b) AS x)
            GROUP BY x)))))
));

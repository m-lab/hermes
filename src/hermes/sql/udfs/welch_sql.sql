-- Welch's t-test in SQL on whole arrays (step 02): exact full-data moments
-- (COUNT / AVG / VAR_SAMP), turned into the welch_t_test struct by the scalar JS
-- UDF welch_from_moments (requires it). Matches welch_t_test to ~1e-13 relative
-- (AVG's summation order); no array reaches JS.
CREATE TEMP FUNCTION welch_sql(a ARRAY<FLOAT64>, b ARRAY<FLOAT64>) AS (
  welch_from_moments(
    ARRAY_LENGTH(a), (SELECT AVG(x) FROM UNNEST(a) AS x), (SELECT VAR_SAMP(x) FROM UNNEST(a) AS x),
    ARRAY_LENGTH(b), (SELECT AVG(x) FROM UNNEST(b) AS x), (SELECT VAR_SAMP(x) FROM UNNEST(b) AS x))
);

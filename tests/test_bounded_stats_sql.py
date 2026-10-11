"""Step 02 hands every JS statistics UDF a bounded, deterministic sample."""

import re

from hermes.sql import loader, paths

STEP = "02_detect_anomalies_union.sql"


def _sql() -> str:
    return loader.load_query(
        STEP,
        {"DAY": "2025-03-08", "ONE_WEEK_EARLIER": "2025-03-01", "DETECTION_GRANULARITY": "metro"},
    )


def _call_args(sql: str, fn: str) -> list[str]:
    """The argument text of every call to ``fn``."""
    out = []
    for m in re.finditer(rf"\b{fn}\(", sql):
        depth, k = 1, m.end()
        while depth:
            depth += {"(": 1, ")": -1}.get(sql[k], 0)
            k += 1
        out.append(sql[m.end() : k - 1])
    return out


def test_only_wasserstein_still_takes_arrays_and_they_are_capped():
    sql = _sql()
    query = sql[sql.index("ASSERT") :]
    assert not _call_args(query, "mann_whitney_u")
    assert not _call_args(query, "welch_t_test")
    calls = _call_args(query, "wasserstein_permutation_test")
    assert len(calls) == 2
    for args in calls:
        arrays = re.findall(r"\b(?:current|baseline)_\w+_array\b", args)
        assert len(arrays) == 2 and len(re.findall(r"capped\(", args)) == 2, args


def test_mann_whitney_and_welch_run_in_sql_on_full_arrays():
    sql = _sql()
    query = sql[sql.index("ASSERT") :]
    for fn, n in (("mann_whitney_sql", 4), ("welch_sql", 3)):
        calls = _call_args(query, fn)
        assert len(calls) == n, fn
        for args in calls:
            assert "capped(" not in args and re.search(r"current_\w+_array", args), args
    assert "CREATE TEMP FUNCTION mann_whitney_sql" in sql and "before + (t + 1) / 2" in sql
    assert "CREATE TEMP FUNCTION welch_sql" in sql and "VAR_SAMP(" in sql


def test_arrays_are_built_in_hash_order_so_a_prefix_is_a_random_sample():
    sql = _sql()
    for m in re.finditer(r"ARRAY_AGG\(([^)]*?) AS (?:current|baseline)_\w+_array", sql):
        assert "ORDER BY FARM_FINGERPRINT(" in m.group(0), m.group(0)


def test_cap_and_permutations_are_declared():
    sql = _sql()
    assert "CREATE TEMP FUNCTION stats_cap() AS (20000);" in sql
    assert "CREATE TEMP FUNCTION w1_permutations() AS (999);" in sql
    assert "compute_wasserstein_p_value" not in sql[sql.index("ASSERT") :]


def test_permutation_p_value_uses_plus_one_correction():
    udf = paths.udf_path("wasserstein_permutation_test").read_text()
    body = udf[udf.index('r"""') :]
    assert "(countGe + 1) / (numPerms + 1)" in body
    assert "JSON.stringify" not in body

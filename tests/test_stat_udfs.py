"""Step 02's statistical UDFs against scipy, run locally in Node.

The JavaScript bodies of ``welch_t_test`` and ``mann_whitney_u`` (the TEMP
functions Step 02 inlines via ``@requires-udf``) are executed with Node on
deterministic samples and compared with scipy's reference results, frozen in
``golden/udf/stat_udf_reference.json`` so CI does not need scipy.

Two defects motivated this file:

* the Welch p-value's continued fraction (``betacf``) used the wrong index
  terms, overestimating p by up to ~0.2 when |t| is roughly 1 to 1.8;
* both functions returned ``p_value = 1e-10`` with zeroed fields, without
  testing, whenever either sample exceeded 20,000 values.

Regenerate the reference (needs scipy)::

    HERMES_REGEN_STAT_REFERENCE=1 pytest tests/test_stat_udfs.py
"""

from __future__ import annotations

import json
import math
import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

from hermes.sql import paths

REFERENCE = Path(__file__).parent / "golden" / "udf" / "stat_udf_reference.json"
UDF_DIR = paths.query_path("02_detect_anomalies_union.sql").parents[1] / "udfs"

GOLDEN_RATIO_FRACTION = 0.6180339887498949

#: (label, n1, n2, target |t|, decimals). The second sample is shifted so the
#: t statistic lands near the target, which puts cases on both sides of the
#: 0.05 threshold and inside the |t| in [1, 1.8] band where betacf was wrong.
CASES = [
    ("n5", 5, 5, 1.4, 3),
    ("n30", 30, 30, 1.2, 3),
    ("n200", 200, 200, 1.6, 3),
    ("n200_null", 200, 200, 0.3, 3),
    ("unequal_40_300", 40, 300, 1.5, 3),
    ("ties_500", 500, 500, 1.3, 0),
    ("n2000", 2000, 2000, 2.2, 3),
    ("n2000_border", 2000, 2000, 1.9, 3),
    ("n20001_over_cap", 20001, 20001, 1.7, 3),
    ("n25000_over_cap", 25000, 25000, 2.4, 3),
    ("n60000_over_cap", 60000, 60000, 1.1, 3),
]


def _sample(n: int, loc: float, seed: float, decimals: int) -> list[float]:
    """Deterministic, roughly uniform sample on [loc, loc + 10)."""
    return [
        round(loc + 10.0 * math.modf(seed + i * GOLDEN_RATIO_FRACTION)[0], decimals)
        for i in range(n)
    ]


def _arrays(n1: int, n2: int, target_t: float, decimals: int) -> tuple[list[float], list[float]]:
    sd = 10.0 / math.sqrt(12.0)
    shift = target_t * sd * math.sqrt(1.0 / n1 + 1.0 / n2)
    return _sample(n1, 20.0 + shift, 0.13, decimals), _sample(n2, 20.0, 0.71, decimals)


def _js_body(udf: str) -> tuple[list[str], str]:
    sql = (UDF_DIR / f"{udf}.sql").read_text(encoding="utf-8")
    params = re.search(rf"CREATE TEMP FUNCTION {udf}\((.*?)\) RETURNS", sql, re.S)
    assert params, f"{udf}.sql must define a TEMP FUNCTION named {udf}"
    names = [p.strip().split()[0] for p in params.group(1).split(",")]
    body = re.search(r'r"""(.*?)"""', sql, re.S)
    assert body
    return names, body.group(1)


def _run_js(udf: str, pairs: list[tuple[list[float], list[float]]]) -> list[dict]:
    names, body = _js_body(udf)
    script = (
        "const data = JSON.parse(require('fs').readFileSync(0, 'utf8'));\n"
        f"const f = new Function({json.dumps(names[0])}, {json.dumps(names[1])}, data.body);\n"
        "console.log(JSON.stringify(data.pairs.map(([a, b]) => f(a, b))));\n"
    )
    out = subprocess.run(
        ["node", "-e", script],
        input=json.dumps({"body": body, "pairs": pairs}),
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(out.stdout)


def _reference() -> dict:
    if os.environ.get("HERMES_REGEN_STAT_REFERENCE"):
        from scipy import stats  # only needed to regenerate

        ref = {}
        for label, n1, n2, target_t, decimals in CASES:
            a, b = _arrays(n1, n2, target_t, decimals)
            welch = stats.ttest_ind(a, b, equal_var=False)
            mw = stats.mannwhitneyu(a, b, alternative="two-sided", method="asymptotic")
            ref[label] = {
                "welch": {"t_stat": float(welch.statistic), "p_value": float(welch.pvalue)},
                "mann_whitney": {
                    "U": float(min(mw.statistic, n1 * n2 - mw.statistic)),
                    "p_value": float(mw.pvalue),
                },
            }
        REFERENCE.write_text(json.dumps(ref, indent=2) + "\n", encoding="utf-8")
    return json.loads(REFERENCE.read_text(encoding="utf-8"))


pytestmark = pytest.mark.skipif(shutil.which("node") is None, reason="needs node")


@pytest.fixture(scope="module")
def reference() -> dict:
    return _reference()


@pytest.fixture(scope="module")
def pairs() -> list[tuple[list[float], list[float]]]:
    return [_arrays(n1, n2, t, d) for _, n1, n2, t, d in CASES]


def test_welch_matches_scipy(reference, pairs):
    results = _run_js("welch_t_test", pairs)
    for (label, *_), got in zip(CASES, results, strict=True):
        want = reference[label]["welch"]
        assert got["t_stat"] == pytest.approx(want["t_stat"], rel=1e-9), label
        assert got["p_value"] == pytest.approx(want["p_value"], abs=1e-8), label


def test_mann_whitney_matches_scipy(reference, pairs):
    results = _run_js("mann_whitney_u", pairs)
    for (label, *_), got in zip(CASES, results, strict=True):
        want = reference[label]["mann_whitney"]
        assert got["U"] == want["U"], label
        # The UDF's erf approximation is accurate to ~1.5e-7.
        assert got["p_value"] == pytest.approx(want["p_value"], abs=1e-6), label


def test_large_samples_are_tested_not_short_circuited(pairs):
    """Samples over 20,000 used to return p = 1e-10 with zeroed statistics."""
    big = [p for (_, n1, n2, *_), p in zip(CASES, pairs, strict=True) if max(n1, n2) > 20000]
    assert big
    for got in _run_js("welch_t_test", big):
        assert got["t_stat"] != 0.0 and got["p_value"] != 1e-10
    for got in _run_js("mann_whitney_u", big):
        assert got["U"] != 0.0 and got["p_value"] != 1e-10


def test_step_02_inlines_the_repo_udfs_not_the_shared_routines():
    """Step 02 must run the version-controlled TEMP functions, so every image
    carries the exact detector it runs and staging cannot share a live routine
    with production (as compute_wasserstein_p_value already does)."""
    from hermes.sql import loader

    sql = paths.query_path("02_detect_anomalies_union.sql").read_text(encoding="utf-8")
    assert set(loader.required_udfs(sql)) >= {"welch_t_test", "mann_whitney_u"}
    code = "\n".join(line for line in sql.splitlines() if not line.lstrip().startswith("--"))
    assert "hermes.welchs_t_test" not in code
    assert "hermes.mann_whitney_u_test" not in code

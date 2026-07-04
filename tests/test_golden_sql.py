"""Gated golden test for the persistent BigQuery stat UDFs.

On-demand only: runs against ``mlab-collaboration.hermes`` and is skipped unless
BigQuery credentials are configured (``GOOGLE_APPLICATION_CREDENTIALS`` set, or
``HERMES_BQ_TEST=1``). The expected values in ``golden/udf/stat_udf_cases.json``
were captured from the live UDFs; only deterministic fields are asserted
(wasserstein's permutation-randomized ``p_value`` is excluded).

Run it explicitly with::

    HERMES_BQ_TEST=1 pytest tests/test_golden_sql.py -v
"""

from __future__ import annotations

import json
import math
import os
from pathlib import Path

import pytest

pytestmark = pytest.mark.bq

CASES = Path(__file__).parent / "golden" / "udf" / "stat_udf_cases.json"
PROJECT = os.environ.get("HERMES_TEST_PROJECT", "mlab-collaboration")


def _creds_available() -> bool:
    return bool(
        os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") or os.environ.get("HERMES_BQ_TEST")
    )


@pytest.fixture(scope="module")
def client():
    if not _creds_available():
        pytest.skip("BigQuery creds not configured (on-demand test)")
    from google.cloud import bigquery

    return bigquery.Client(project=PROJECT)


def _arr(xs: list[float]) -> str:
    return "[" + ",".join(repr(float(x)) for x in xs) + "]"


def _assert_close(got: dict, expected: dict, label: str) -> None:
    for key, exp in expected.items():
        actual = got[key]
        assert math.isclose(actual, exp, rel_tol=1e-9, abs_tol=1e-12), (
            f"{label}.{key}: got {actual!r}, expected {exp!r}"
        )


def test_persistent_stat_udfs_match_golden(client):
    case = json.loads(CASES.read_text())
    base = _arr(case["input"]["baseline"])
    cur = _arr(case["input"]["current"])
    nperm = int(case["input"]["num_permutations"])
    exp = case["expected"]

    sql = (
        "SELECT "
        f"`{PROJECT}`.hermes.welchs_t_test({base},{cur}) AS welch, "
        f"`{PROJECT}`.hermes.mann_whitney_u_test({base},{cur}) AS mw, "
        f"`{PROJECT}`.hermes.compute_wasserstein_p_value({base},{cur},{nperm}) AS ws"
    )
    row = list(client.query(sql).result())[0]

    _assert_close(dict(row["welch"]), exp["welch"], "welch")
    _assert_close(dict(row["mw"]), exp["mann_whitney"], "mann_whitney")
    # wasserstein: only the deterministic distance (p_value is permutation-random)
    assert math.isclose(dict(row["ws"])["distance"], exp["wasserstein"]["distance"], rel_tol=1e-9)

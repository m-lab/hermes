"""Offline golden regression for the greedy set-cover tomography.

Frozen edge fixtures live in ``tests/golden/python/edges_<date>.parquet`` with
the blessed result in ``expected_setcover_<date>.json`` (regenerate via
``python tests/golden/bless.py``). The test runs fully offline — no BigQuery.
"""

from __future__ import annotations

import glob
import json
import os

import pandas as pd
import pytest

from hermes.pipeline.correlation_tomography import run_greedy_set_cover
from tests.golden.bless import summary

GOLDEN = os.path.join(os.path.dirname(__file__), "golden", "python")
FIXTURES = sorted(glob.glob(os.path.join(GOLDEN, "edges_*.parquet")))


@pytest.mark.skipif(not FIXTURES, reason="no golden edge fixtures present")
@pytest.mark.parametrize("fixture", FIXTURES, ids=[os.path.basename(p) for p in FIXTURES])
def test_setcover_matches_golden(fixture: str) -> None:
    day = os.path.basename(fixture)[len("edges_") : -len(".parquet")]
    expected_path = os.path.join(GOLDEN, f"expected_setcover_{day}.json")
    expected = json.loads(open(expected_path).read())

    edges = pd.read_parquet(fixture)
    culprits, total = run_greedy_set_cover(edges, day_str=day)

    assert summary(culprits, total) == expected

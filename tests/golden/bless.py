"""Regenerate golden expected outputs from frozen edge fixtures.

Run intentionally — never in CI::

    python tests/golden/bless.py

For every ``tests/golden/python/edges_<date>.parquet`` it runs the greedy
set-cover and writes ``expected_setcover_<date>.json`` next to it.
"""

from __future__ import annotations

import glob
import json
import os

import pandas as pd

from hermes.pipeline.correlation_tomography import run_greedy_set_cover

GOLDEN = os.path.join(os.path.dirname(__file__), "python")


def summary(culprits: object, total: object) -> dict:
    """Deterministic, order-independent summary of a set-cover result."""
    try:
        n = len(culprits)  # type: ignore[arg-type]
    except TypeError:
        n = int(culprits)  # pragma: no cover
    return {"total_anomalies": int(total), "num_culprits": int(n)}


def bless() -> None:
    fixtures = sorted(glob.glob(os.path.join(GOLDEN, "edges_*.parquet")))
    if not fixtures:
        raise SystemExit("no edges_*.parquet fixtures found in tests/golden/python/")
    for fx in fixtures:
        day = os.path.basename(fx)[len("edges_") : -len(".parquet")]
        edges = pd.read_parquet(fx)
        culprits, total = run_greedy_set_cover(edges, day_str=day)
        out = summary(culprits, total)
        with open(os.path.join(GOLDEN, f"expected_setcover_{day}.json"), "w") as f:
            json.dump(out, f, indent=2, sort_keys=True)
        print("blessed", day, out)


if __name__ == "__main__":
    bless()

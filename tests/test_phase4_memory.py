"""Phase 4 (run_mixed_granularity_cover) must reproduce the pre-optimisation output.

The golden file was produced by the string-based implementation before it was
rewritten on integer codes (the rewrite fixed an OOM on large backfill days).
"""

import json
import math
import random
from pathlib import Path

import pandas as pd
import pytest

from hermes.pipeline.correlation_tomography import run_mixed_granularity_cover

GOLDEN = Path(__file__).parent / "golden" / "phase4" / "multigran_synthetic.json"


def synthetic_inputs(seed: int = 7) -> tuple[pd.DataFrame, pd.DataFrame]:
    rng = random.Random(seed)
    metros = [f"M{i}-Region{i % 4}-C{i % 3}" for i in range(12)] + ["Paris-Ile-de-France-FR"]
    asns = [str(100 * k) for k in range(1, 16)]
    nodes = [f"{a}-{m}" for a in asns for m in metros] + ["*", "700"]
    ixps = {
        "200-M5-Region1-C2": "Equinix-X",
        "300-Paris-Ile-de-France-FR": "France-IX",
        "400-M5-Region1-C2": "Equinix-X",
    }
    rows = []
    for p in range(240):
        sdp = f"{rng.choice(asns)} - grp{p % 60} - site{p % 9} - {'v4' if p % 3 else 'v6'}"
        info = "forward" if p % 2 else "reverse"
        anomalous_pair = p % 4 == 0
        for t in range(rng.randint(2, 6)):
            tid = f"t{p}-{t}"
            ptype = "anomalous" if anomalous_pair and t % 3 != 2 else "non_anomalous"
            hops = [rng.choice(nodes) for _ in range(rng.randint(2, 4))]
            if ptype == "anomalous" and p % 8 == 0:  # one AS at several metros -> coarse AS pick
                hops.insert(
                    1, rng.choice(["100-M1-Region1-C1", "100-M2-Region2-C2", "100-M3-Region3-C0"])
                )
            if ptype == "anomalous" and p % 8 == 4:  # one node only -> AS pick demoted to node
                hops.insert(1, "300-Paris-Ile-de-France-FR")
            if ptype == "anomalous" and p % 16 == 2:  # IXP-mapped node
                hops.insert(1, "200-M5-Region1-C2")
            for a, b in zip(hops, hops[1:], strict=False):
                rows.append((tid, sdp, info, ptype, f"{a} - {b}"))
                if rng.random() < 0.05:  # exact duplicate row
                    rows.append((tid, sdp, info, ptype, f"{a} - {b}"))
    edges = pd.DataFrame(
        rows, columns=["id", "src_dst_pair", "information_source", "path_type", "canonical_edge"]
    )
    ae = []
    for a in nodes:
        for b in nodes:
            if a != b and rng.random() < 0.05:
                ae.append((a, ixps.get(a, "None"), b, ixps.get(b, "None")))
    all_edges = pd.DataFrame(ae, columns=["from_asn_metro", "from_ixp", "to_asn_metro", "to_ixp"])
    return edges, all_edges


def _clean(o):
    if isinstance(o, float) and (math.isinf(o) or math.isnan(o)):
        return repr(o)
    if isinstance(o, dict):
        return {k: _clean(v) for k, v in o.items()}
    if isinstance(o, list):
        return [_clean(v) for v in o]
    if hasattr(o, "item"):  # numpy scalar
        return _clean(o.item())
    return o


@pytest.mark.parametrize("seed", ["7", "11", "23", "31", "47"])
def test_matches_frozen_output(seed):
    want = json.loads(GOLDEN.read_text())[seed]
    edges, all_edges = synthetic_inputs(int(seed))
    culprits, stats = run_mixed_granularity_cover(edges, all_edges, "2026-01-01")
    assert json.loads(json.dumps(_clean(culprits), sort_keys=True)) == want["culprits"]
    assert json.loads(json.dumps(_clean(stats), sort_keys=True)) == want["entity_stats"]

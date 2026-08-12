"""Pipeline entry point.

    python -m cleaning_polygons.build --build-version 2026-08-v2

Runs the stages in order and writes an intermediate artifact after each, so a
failure is inspectable rather than opaque. ``--from-stage`` resumes.
"""
from __future__ import annotations

import argparse
import json
import time

import pandas as pd

from . import build_country_voronoi as bv
from . import config as cfg
from . import export_bigquery as ex
from . import load_country_boundaries as lb
from . import load_state_boundaries as sb
from . import load_seeds as ls
from . import validate_seeds as vs
from . import validate_tessellation as vt

STAGES = ("boundaries", "states", "seeds", "validate-seeds", "tessellate", "validate", "export")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--build-version", default=cfg.BUILD_VERSION)
    ap.add_argument("--from-stage", choices=STAGES, default="boundaries")
    ap.add_argument("--upload", action="store_true", help="load metro_polygons_v2")
    ap.add_argument("--force-download", action="store_true")
    ap.add_argument(
        "--allow-gate-failure",
        action="store_true",
        help="write artifacts even if the correctness gate fails (diagnosis only)",
    )
    args = ap.parse_args()
    cfg.BUILD_VERSION = args.build_version
    start = STAGES.index(args.from_stage)

    def active(name: str) -> bool:
        return STAGES.index(name) >= start

    t0 = time.time()

    if active("boundaries"):
        print("[02] boundaries ...", flush=True)
        lb.save(lb.build_boundaries(force_download=args.force_download))
    boundaries = lb.load()
    print(f"     {len(boundaries)} country codes, {boundaries.area_km2.sum():,.0f} km2")

    if active("states"):
        print("[02b] ADM1 boundaries ...", flush=True)
        sb.save(sb.build_states(boundaries))
    states = sb.load()
    print(f"     {len(states)} ADM1 units across {states.country_code.nunique()} countries")

    if active("seeds"):
        print("[01] seeds ...", flush=True)
        seeds, seed_report = ls.build_seeds()
        ls.save(seeds)
        print(f"     {len(seeds)} seeds; matched to legacy: {seed_report['matched_to_legacy']}")

    if active("validate-seeds"):
        print("[03] validating seeds ...", flush=True)
        seeds, report = vs.run(ls.load(), boundaries)
        cfg.S03_SEED_VALIDATION.write_text(json.dumps(report, indent=2, default=str))
        seeds.to_parquet(cfg.S01_SEEDS, index=False)
        sc = report["seed_country_consistency"]
        print(
            f"     inside={sc['inside']} near_border={sc['near_border_count']} "
            f"reassigned={sc['reassigned_count']} outside={sc['clearly_outside_count']}"
        )
        needs = report["country_seed_coverage"]["countries_needing_seed"]
        if needs:
            print(f"     WARNING: {len(needs)} inhabited territories still need a seed: {needs}")

        # State assignment runs after the country fixes, since a reassigned
        # country changes which ADM1 units a seed can belong to.
        seeds, srep = sb.assign_seeds_to_states(seeds, states)
        seeds.to_parquet(cfg.S01_SEEDS, index=False)
        report["state_assignment"] = srep
        cfg.S03_SEED_VALIDATION.write_text(json.dumps(report, indent=2, default=str))
        print(
            f"     states: {srep['assigned_to_state']}/{srep['seeds']} seeds in an ADM1 unit, "
            f"{srep['states_with_seeds']} of {srep['states_total']} units seeded, "
            f"{srep['state_resolved_taken_from_admin1']} region names corrected"
        )
    seeds = ls.load()

    if active("tessellate"):
        print("[04] tessellating ...", flush=True)
        # No normalize_frame here. build_country already splits the +/-180 seam
        # per piece, and deliberately does NOT for polar pieces (they are cut on
        # the true meridian instead). Re-running it wholesale destroyed the
        # negative-longitude half of the South Polar region.
        cells, diags = bv.build_all(seeds, boundaries, states=states)
        ex.save_cells(cells)
        diags.to_csv(cfg.REPORTS / "04_build_diagnostics.csv", index=False)
        print(
            f"     {len(cells)} cells / {cells.metro_id.nunique()} metros / "
            f"{sum(len(g.geoms) for g in cells.geometry)} pieces  ({time.time()-t0:.0f}s)"
        )
    cells = ex.load_cells()

    if active("validate"):
        print("[06] validating tessellation ...", flush=True)
        report, coverage = vt.run(cells, boundaries, seeds, states=states)
        cfg.S06_VALIDATION.write_text(json.dumps(report, indent=2, default=str))
        coverage.to_csv(cfg.COUNTRY_DIAGNOSTICS, index=False)
        g = report["global"]
        print(
            f"     land {g['country_area_km2']:,.0f} km2 | uncovered {g['uncovered_km2']:,.3f} "
            f"| overlap {g['overlap_km2']:,.3f} | escape {g['escape_km2']:,.3f}"
        )
        print(
            f"     seed-ownership failures {report['seed_ownership']['failures']} | "
            f"nearest-seed mismatches {report['nearest_seed']['mismatches']} | "
            f"land misses {report['land_coverage']['unmatched']} | "
            f"interior multi-match {report['interior_uniqueness']['multi_match']}"
        )
        st = report.get("state_consistency", {})
        print(
            f"     state tier: cells {st.get('state_tier_cells', 0)} | "
            f"cross-state area {st.get('cross_state_area_km2', 0):.3f} km2 | "
            f"points in wrong state {st.get('points_in_wrong_state', 0)}/{st.get('points_tested', 0)}"
        )
        nc = report["named_cases"]
        print(
            f"     antimeridian failures {nc['antimeridian_failures']} | "
            f"arctic failures {nc['arctic_failures']} | seam double-matches {nc['seam_double_matches']}"
        )
        if not report["gate_passed"]:
            print("     GATE FAILED:")
            for p in report["gate_problems"][:25]:
                print("       -", p)
            if not args.allow_gate_failure:
                return 1
        else:
            print("     GATE PASSED")

    if active("export"):
        print("[07] export ...", flush=True)
        export = ex.to_export_frame(cells)
        export.to_parquet(cfg.S07_EXPORT, index=False)
        print(f"     {len(export)} rows -> {cfg.S07_EXPORT}")
        if args.upload:
            table = ex.upload(export)
            print(f"     uploaded -> {table}")

    print(f"done in {time.time()-t0:.0f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

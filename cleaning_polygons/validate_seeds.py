"""Stage 03 -- seed validation (plan section 4).

Nothing here silently drops or moves a seed. Every category is counted, every
individual case that changes the build is listed in the JSON report, and the two
categories that *do* mutate the seed set -- country reassignment and exact
coordinate deduplication -- are both driven by evidence rather than by a
hand-maintained list.
"""
from __future__ import annotations

import csv
import json
from pathlib import Path

import numpy as np
import pandas as pd
from shapely.geometry import Point
from shapely.strtree import STRtree

from . import config as cfg
from .geometry_utils import great_circle_km

# A seed farther than this outside its declared country is treated as a
# country-code error rather than a coastline-resolution artefact.
MISPLACED_KM = 25.0
NEAR_DUPLICATE_KM = 1.0
VERY_NEAR_DUPLICATE_KM = 0.1


def read_commented_csv(path: Path) -> list[dict]:
    """CSV reader that tolerates leading '>' comment lines."""
    if not path.exists():
        return []
    lines = [ln for ln in path.read_text(encoding="utf-8").splitlines() if not ln.startswith(">")]
    if not lines:
        return []
    return list(csv.DictReader(lines))


def load_excluded_territories() -> dict[str, str]:
    return {
        r["country_code"].strip(): r.get("reason", "")
        for r in read_commented_csv(cfg.ROOT / "excluded_territories.csv")
        if r.get("country_code")
    }


def check_invalid_coordinates(seeds: pd.DataFrame) -> tuple[pd.DataFrame, list[dict]]:
    """Section 4.1 -- reject, but report, seeds with unusable coordinates."""
    lat, lon = seeds.seed_lat, seeds.seed_lon
    bad = (
        lat.isna() | lon.isna()
        | (lat < -90) | (lat > 90)
        | (lon < -180) | (lon > 180)
    )
    rejected = seeds[bad]
    listing = rejected[["city", "country_code", "seed_lat", "seed_lon"]].to_dict("records")
    return seeds[~bad].reset_index(drop=True), listing


def check_duplicate_coordinates(seeds: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """Section 4.2 -- exact coincident seeds cannot be told apart geometrically."""
    grp = seeds.groupby(["seed_lat", "seed_lon"])
    dupes = [g for _, g in grp if len(g) > 1]
    report = {"groups": len(dupes), "rows_involved": int(sum(len(g) for g in dupes)), "detail": []}

    drop_idx: list[int] = []
    for g in dupes:
        same_metro = g.metro.nunique() == 1
        # Keep the largest-population seed; that is a stated rule, not a coin flip.
        keeper = g.sort_values(["ne_pop_max", "metro_id"], ascending=[False, True]).index[0]
        dropped = [i for i in g.index if i != keeper]
        drop_idx.extend(dropped)
        report["detail"].append(
            {
                "lat": float(g.seed_lat.iloc[0]),
                "lon": float(g.seed_lon.iloc[0]),
                "same_canonical_metro": bool(same_metro),
                "kept": str(seeds.loc[keeper, "metro"]),
                "dropped": [str(seeds.loc[i, "metro"]) for i in dropped],
            }
        )
    kept = seeds.drop(index=drop_idx).reset_index(drop=True)
    report["rows_dropped"] = len(drop_idx)
    return kept, report


def check_near_duplicates(seeds: pd.DataFrame) -> dict:
    """Section 4.3 -- report, but keep, seeds that are very close together."""
    out = {"under_100m": [], "under_1km": []}
    for cc, g in seeds.groupby("country_code"):
        if len(g) < 2:
            continue
        lons = g.seed_lon.to_numpy()
        lats = g.seed_lat.to_numpy()
        n = len(g)
        for i in range(n):
            d = great_circle_km(lons[i], lats[i], lons[i + 1 :], lats[i + 1 :])
            for off, dist in enumerate(np.atleast_1d(d)):
                if dist >= NEAR_DUPLICATE_KM:
                    continue
                j = i + 1 + off
                item = {
                    "country_code": cc,
                    "a": str(g.metro.iloc[i]),
                    "b": str(g.metro.iloc[j]),
                    "km": round(float(dist), 4),
                }
                key = "under_100m" if dist < VERY_NEAR_DUPLICATE_KM else "under_1km"
                out[key].append(item)
    out["count_under_100m"] = len(out["under_100m"])
    out["count_under_1km"] = len(out["under_1km"])
    out["under_100m"] = out["under_100m"][:50]
    out["under_1km"] = out["under_1km"][:50]
    return out


def check_seed_country_consistency(
    seeds: pd.DataFrame, boundaries
) -> tuple[pd.DataFrame, dict]:
    """Section 4.4 -- classify inside / near-border / clearly outside, and fix the last.

    A seed that is far outside its declared country *and* falls inside exactly one
    other country is a country-code error in the source, not a coastline artefact.
    Natural Earth's own ADM0NAME agrees in both cases this currently catches
    (Atafu is labelled Tokelau, Alofi is labelled Niue, yet both carry ISO_A2=NZ).
    """
    bmap = {r.country_code: r.geometry for _, r in boundaries.iterrows()}
    codes = list(bmap)
    tree = STRtree([bmap[c] for c in codes])

    inside, near_border, outside, reassigned = 0, [], [], []
    new_cc = seeds.country_code.tolist()

    for pos, s in enumerate(seeds.itertuples()):
        pt = Point(s.seed_lon, s.seed_lat)
        geom = bmap.get(s.country_code)
        if geom is None:
            outside.append({"metro": s.metro, "reason": "no boundary for declared code"})
            continue
        if geom.covers(pt):
            inside += 1
            continue

        # Degrees are only used to bucket; the reported distance is great-circle.
        nearest = geom.exterior if geom.geom_type == "Polygon" else geom
        km = float(great_circle_km(s.seed_lon, s.seed_lat, *_nearest_lonlat(geom, pt)))
        if km <= MISPLACED_KM:
            near_border.append({"metro": s.metro, "km_outside": round(km, 3)})
            continue

        containing = [
            codes[i] for i in tree.query(pt) if bmap[codes[i]].covers(pt)
        ]
        if len(containing) == 1 and containing[0] != s.country_code:
            new_cc[pos] = containing[0]
            reassigned.append(
                {
                    "metro": s.metro,
                    "from": s.country_code,
                    "to": containing[0],
                    "km_outside_declared": round(km, 1),
                    "ne_adm0name_hint": getattr(s, "ne_adm0_a3", ""),
                }
            )
        else:
            outside.append(
                {
                    "metro": s.metro,
                    "km_outside": round(km, 1),
                    "containing_countries": containing,
                }
            )
        del nearest

    fixed = seeds.copy()
    fixed["country_code"] = new_cc
    report = {
        "inside": inside,
        "near_border_count": len(near_border),
        "near_border": sorted(near_border, key=lambda r: -r["km_outside"])[:30],
        "clearly_outside_count": len(outside),
        "clearly_outside": outside[:30],
        "reassigned_count": len(reassigned),
        "reassigned": reassigned,
    }
    return fixed, report


def _nearest_lonlat(geom, pt: Point) -> tuple[float, float]:
    from shapely.ops import nearest_points

    p = nearest_points(geom, pt)[0]
    return p.x, p.y


def check_countries_without_seeds(seeds: pd.DataFrame, boundaries) -> pd.DataFrame:
    """Section 5 -- the has_boundary / seed_count / action table."""
    excluded = load_excluded_territories()
    seeded = seeds.groupby("country_code").size().to_dict()
    rows = []
    for _, b in boundaries.iterrows():
        cc = b.country_code
        n = int(seeded.get(cc, 0))
        if n > 0:
            action = "tessellate"
        elif cc in excluded:
            action = f"excluded: {excluded[cc]}"
        else:
            action = "NEEDS SEED"
        rows.append(
            {
                "country_code": cc,
                "has_boundary": True,
                "seed_count": n,
                "area_km2": round(float(b.area_km2), 2),
                "action": action,
            }
        )
    for cc, n in seeded.items():
        if cc not in set(boundaries.country_code):
            rows.append(
                {
                    "country_code": cc,
                    "has_boundary": False,
                    "seed_count": int(n),
                    "area_km2": 0.0,
                    "action": "NO BOUNDARY - seeds cannot be tessellated",
                }
            )
    return pd.DataFrame(rows).sort_values("country_code").reset_index(drop=True)


def run(seeds: pd.DataFrame, boundaries) -> tuple[pd.DataFrame, dict]:
    report: dict = {}
    seeds, report["invalid_coordinates"] = check_invalid_coordinates(seeds)
    seeds, report["seed_country_consistency"] = check_seed_country_consistency(
        seeds, boundaries
    )
    seeds, report["duplicate_coordinates"] = check_duplicate_coordinates(seeds)
    report["near_duplicates"] = check_near_duplicates(seeds)

    coverage = check_countries_without_seeds(seeds, boundaries)
    report["country_seed_coverage"] = {
        "countries_with_boundary": int(coverage.has_boundary.sum()),
        "countries_needing_seed": coverage[coverage.action == "NEEDS SEED"][
            ["country_code", "area_km2"]
        ].to_dict("records"),
        "countries_excluded": coverage[
            coverage.action.str.startswith("excluded")
        ].country_code.tolist(),
        "seeds_without_boundary": coverage[~coverage.has_boundary][
            ["country_code", "seed_count"]
        ].to_dict("records"),
    }
    coverage.to_csv(cfg.REPORTS / "country_seed_coverage.csv", index=False)
    report["final_seed_count"] = int(len(seeds))
    return seeds.reset_index(drop=True), report


def main() -> None:
    from . import load_country_boundaries as lb
    from . import load_seeds as ls

    seeds = ls.load()
    boundaries = lb.load()
    seeds, report = run(seeds, boundaries)
    cfg.S03_SEED_VALIDATION.write_text(json.dumps(report, indent=2, default=str))
    seeds.to_parquet(cfg.S01_SEEDS, index=False)
    print(json.dumps({k: v for k, v in report.items() if k != "near_duplicates"}, indent=2, default=str)[:3000])


if __name__ == "__main__":
    main()

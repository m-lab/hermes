"""Stage 06 -- geometric correctness tests (plan sections 18.1-18.9 and 23).

These are the authoritative measurements. ``build_country_voronoi`` reports a
cheap area-sum residual for progress reporting only; the numbers that gate the
build are computed here as set operations, exactly as the plan specifies:

* uncovered  = area of ``country.difference(union(cells))``
* overlap    = summed area of true pairwise cell intersections
* escape     = area of ``cell.difference(country)``

Every check returns structured results, and :func:`gate` turns them into a
pass/fail decision against ``config.TOL``.
"""
from __future__ import annotations

import json
import math

import numpy as np
import pandas as pd
from shapely.geometry import MultiPolygon, Point
from shapely.ops import unary_union
from shapely.strtree import STRtree

from . import config as cfg
from .geometry_utils import (
    EARTH_RADIUS_KM,
    geometry_touches_pole,
    great_circle_km,
    rotate_lonlat,
    rotate_geometry,
    rotation_matrix_pole_to_equator,
    spherical_area_km2,
)

# Named regression cases. These are the coordinates the current table gets wrong.
ANTIMERIDIAN_CASES = [
    ("Majuro, Marshall Islands", 171.3800, 7.1000, "MH"),
    ("Tarawa, Kiribati", 172.9800, 1.3300, "KI"),
    ("Suva, Fiji", 178.4400, -18.1400, "FJ"),
    ("Pago Pago, American Samoa", -170.7000, -14.2800, "AS"),
    ("Apia, Samoa", -171.7600, -13.8300, "WS"),
    ("Auckland, New Zealand", 174.7600, -36.8500, "NZ"),
    ("Atka, Alaska", -174.2000, 52.1960, "US"),
    ("Beringovskiy, Russia", 179.3100, 63.0500, "RU"),
    ("Mata-Utu, Wallis and Futuna", -176.1745, -13.2825, "WF"),
    ("Wake Island", 166.6280, 19.2823, "UM"),
]

ARCTIC_CASES = [
    ("Utqiagvik, Alaska", -156.7886, 71.2906, "US"),
    ("Nuiqsut, Alaska", -150.9758, 70.2172, "US"),
    ("Prudhoe Bay, Alaska", -148.3372, 70.2553, "US"),
    ("Longyearbyen, Svalbard", 15.6469, 78.2232, "SJ"),
    ("Qaanaaq, Greenland", -69.2320, 77.4670, "GL"),
    ("Kjollefjord, Norway", 27.3460, 70.9460, "NO"),
    ("Polyarnyy, Russia", 33.4370, 69.2030, "RU"),
    ("Tiksi, Russia", 128.8694, 71.6870, "RU"),
]


# --------------------------------------------------------------------------
# lookup used by the tests -- mirrors the production SQL semantics (§15)
# --------------------------------------------------------------------------
class Lookup:
    """Point -> metro, with the same rules the BigQuery lookup uses."""

    def __init__(self, cells: pd.DataFrame):
        self.cells = cells.reset_index(drop=True)
        self.by_cc: dict[str, list[int]] = {}
        for i, cc in enumerate(self.cells.country_code):
            self.by_cc.setdefault(cc, []).append(i)
        # State-tier index: only cells actually built inside an ADM1 unit.
        self.by_state: dict[tuple[str, str], list[int]] = {}
        if "partition_tier" in self.cells.columns:
            for i, (cc, sc, tier) in enumerate(
                zip(self.cells.country_code, self.cells.state_code, self.cells.partition_tier)
            ):
                if tier == "state" and sc:
                    self.by_state.setdefault((cc, sc), []).append(i)
        self._trees: dict[str, tuple[STRtree, list[int], object]] = {}

    def _tree(self, cc: str):
        """Per-country spatial index, rotated away from the pole if needed.

        A cell touching a pole has no faithful plate-carree ring, so shapely
        cannot point-test it there. Rotating the country's cells and the query
        point by the same rigid transform makes the test exact. BigQuery needs
        none of this -- ST_COVERS is already spherical -- this is purely to make
        the local checks able to see what BigQuery will see.
        """
        if cc not in self._trees:
            idx = self.by_cc.get(cc, [])
            geoms = [self.cells.geometry.iloc[i] for i in idx]
            rot = None
            if any(geometry_touches_pole(g) for g in geoms):
                southern = min(g.bounds[1] for g in geoms) < 0
                rot = rotation_matrix_pole_to_equator(southern)
                geoms = [rotate_geometry(g, rot) or g for g in geoms]
            self._trees[cc] = (STRtree(geoms) if geoms else None, idx, rot, geoms)
        return self._trees[cc]

    def matches(self, lon: float, lat: float, cc: str) -> list[int]:
        tree, idx, rot, geoms = self._tree(cc)
        if tree is None:
            return []
        if rot is not None:
            rlon, rlat = rotate_lonlat(lon, lat, rot)
            pt = Point(float(np.atleast_1d(rlon)[0]), float(np.atleast_1d(rlat)[0]))
        else:
            pt = Point(lon, lat)
        return [idx[k] for k in tree.query(pt) if geoms[k].covers(pt)]

    def _covers_subset(self, lon: float, lat: float, cc: str, idx: list[int]) -> list[int]:
        allowed = set(idx)
        return [i for i in self.matches(lon, lat, cc) if i in allowed]

    def _nearest(self, lon: float, lat: float, idx: list[int]) -> tuple[int, float]:
        d = great_circle_km(
            lon, lat,
            self.cells.seed_lon.iloc[idx].to_numpy(),
            self.cells.seed_lat.iloc[idx].to_numpy(),
        )
        k = int(np.argmin(np.atleast_1d(d)))
        return idx[k], float(np.atleast_1d(d)[k])

    def _pack(self, i: int, lon: float, lat: float, n: int, method: str) -> dict:
        return {
            "metro_id": self.cells.metro_id.iloc[i],
            "metro": self.cells.metro.iloc[i],
            "country_code": self.cells.country_code.iloc[i],
            "state_code": self.cells.state_code.iloc[i]
            if "state_code" in self.cells.columns else None,
            "n_matches": n,
            "method": method,
            "distance_km": float(
                great_circle_km(lon, lat, self.cells.seed_lon.iloc[i], self.cells.seed_lat.iloc[i])
            ),
        }

    def resolve_tiered(self, lon: float, lat: float, cc: str, state_code=None) -> dict:
        """Four tiers, in order (see the plan's state-tier decision).

        1. ``state_polygon``   region known, covered by a cell in that state
        2. ``state_nearest``   region known and seeded, coordinate offshore
        3. ``country_polygon`` region unknown or the state has no seeds
        4. ``country_nearest`` nothing covers it; nearest in-country seed

        Tier 2 only applies when the state actually has cells. A state with no
        metros cannot be kept internally consistent, so its territory is built at
        the country tier and resolves through tier 3.
        """
        if state_code:
            idx = self.by_state.get((cc, state_code))
            if idx:
                hits = self._covers_subset(lon, lat, cc, idx)
                if hits:
                    i = self._best(lon, lat, hits)
                    return self._pack(i, lon, lat, len(hits), "state_polygon")
                i, _ = self._nearest(lon, lat, idx)
                return self._pack(i, lon, lat, 0, "state_nearest")

        hits = self.matches(lon, lat, cc)
        if hits:
            i = self._best(lon, lat, hits)
            return self._pack(i, lon, lat, len(hits), "country_polygon")
        idx = self.by_cc.get(cc, [])
        if not idx:
            return {"metro_id": None, "metro": None, "country_code": None,
                    "state_code": None, "n_matches": 0,
                    "method": "no_country_cells", "distance_km": None}
        i, km = self._nearest(lon, lat, idx)
        return self._pack(
            i, lon, lat, 0,
            "country_nearest" if km <= cfg.COASTAL_FALLBACK_KM else "country_nearest_far",
        )

    def _best(self, lon: float, lat: float, hits: list[int]) -> int:
        """Shared boundary: closest seed wins, then metro_id. Never alphabetical."""
        if len(hits) == 1:
            return hits[0]
        return sorted(
            hits,
            key=lambda i: (
                float(great_circle_km(lon, lat,
                                      self.cells.seed_lon.iloc[i], self.cells.seed_lat.iloc[i])),
                self.cells.metro_id.iloc[i],
            ),
        )[0]

    def resolve(self, lon: float, lat: float, cc: str) -> dict:
        """Containment first; nearest in-country seed as a recorded fallback."""
        hits = self.matches(lon, lat, cc)
        if hits:
            if len(hits) > 1:
                # Shared boundary: closest seed wins, then metro_id. Never
                # alphabetical metro order.
                hits.sort(
                    key=lambda i: (
                        float(
                            great_circle_km(
                                lon, lat,
                                self.cells.seed_lon.iloc[i],
                                self.cells.seed_lat.iloc[i],
                            )
                        ),
                        self.cells.metro_id.iloc[i],
                    )
                )
            i = hits[0]
            return {
                "metro_id": self.cells.metro_id.iloc[i],
                "metro": self.cells.metro.iloc[i],
                "country_code": self.cells.country_code.iloc[i],
                "n_matches": len(hits),
                "method": "polygon",
                "distance_km": float(
                    great_circle_km(
                        lon, lat, self.cells.seed_lon.iloc[i], self.cells.seed_lat.iloc[i]
                    )
                ),
            }

        idx = self.by_cc.get(cc, [])
        if not idx:
            return {"metro_id": None, "metro": None, "country_code": None,
                    "n_matches": 0, "method": "no_country_cells", "distance_km": None}
        d = great_circle_km(
            lon, lat,
            self.cells.seed_lon.iloc[idx].to_numpy(),
            self.cells.seed_lat.iloc[idx].to_numpy(),
        )
        best = idx[int(np.argmin(d))]
        km = float(np.min(d))
        return {
            "metro_id": self.cells.metro_id.iloc[best],
            "metro": self.cells.metro.iloc[best],
            "country_code": cc,
            "n_matches": 0,
            "method": "country_nearest_fallback" if km <= cfg.COASTAL_FALLBACK_KM
            else "country_nearest_fallback_far",
            "distance_km": km,
        }


# --------------------------------------------------------------------------
# individual checks
# --------------------------------------------------------------------------
def check_seed_ownership(cells: pd.DataFrame, lookup: Lookup) -> dict:
    """Section 18.1 -- every seed must resolve back to its own metro."""
    failures = []
    fallback_used = 0
    for r in cells.drop_duplicates(subset=["metro_id"]).itertuples():
        got = lookup.resolve(r.seed_lon, r.seed_lat, r.country_code)
        if got["method"] != "polygon":
            fallback_used += 1
        if got["metro_id"] != r.metro_id:
            failures.append(
                {
                    "metro": r.metro,
                    "resolved_to": got["metro"],
                    "method": got["method"],
                    "distance_km": got["distance_km"],
                }
            )
    return {
        "seeds_tested": int(len(cells)),
        "failures": len(failures),
        "resolved_via_fallback": fallback_used,
        "detail": failures[:40],
    }


def check_interior_uniqueness(cells: pd.DataFrame, lookup: Lookup, boundaries) -> dict:
    """Section 18.4 -- non-boundary points belong to exactly one cell."""
    rng = np.random.default_rng(cfg.RANDOM_SEED)
    multi, zero, tested = 0, 0, 0
    examples = []
    for r in boundaries.itertuples():
        cc = r.country_code
        if cc not in lookup.by_cc:
            continue
        geom = r.geometry
        minx, miny, maxx, maxy = geom.bounds
        got = 0
        for _ in range(400):
            if got >= 8:
                break
            lon = float(rng.uniform(minx, maxx))
            lat = float(rng.uniform(miny, maxy))
            pt = Point(lon, lat)
            if not geom.covers(pt):
                continue
            got += 1
            tested += 1
            n = len(lookup.matches(lon, lat, cc))
            if n > 1:
                multi += 1
                if len(examples) < 20:
                    examples.append({"country_code": cc, "lon": lon, "lat": lat, "n": n})
            elif n == 0:
                zero += 1
                if len(examples) < 20:
                    examples.append({"country_code": cc, "lon": lon, "lat": lat, "n": 0})
    return {
        "points_tested": tested,
        "multi_match": multi,
        "zero_match": zero,
        "examples": examples,
    }


def check_nearest_seed(cells: pd.DataFrame, lookup: Lookup, boundaries, seeds, states=None) -> dict:
    """Section 18.5 -- brute-force great-circle check, independent of the geometry.

    Tier-aware: inside a seeded ADM1 unit the correct answer is the nearest seed
    *in that unit*, since that is the partition the cell was built from. Elsewhere
    it is the nearest seed in the country. Comparing everything against the
    country-wide nearest would report 1,125 false mismatches -- all of them cases
    where the state tier is deliberately doing something different.
    """
    rng = np.random.default_rng(cfg.RANDOM_SEED + 1)
    by_cc = {cc: g for cc, g in seeds.groupby("country_code")}
    state_tree: dict[str, tuple] = {}
    if states is not None:
        for cc, grp in states.groupby("country_code"):
            idx = list(grp.index)
            state_tree[cc] = (STRtree([states.geometry.loc[i] for i in idx]), idx)
    mismatch, tested = 0, 0
    detail = []
    for r in boundaries.itertuples():
        cc = r.country_code
        s = by_cc.get(cc)
        if s is None or cc not in lookup.by_cc:
            continue
        geom = r.geometry
        minx, miny, maxx, maxy = geom.bounds
        lons = s.seed_lon.to_numpy()
        lats = s.seed_lat.to_numpy()
        ids = s.metro_id.to_numpy()
        got = 0
        for _ in range(600):
            if got >= cfg.NEAREST_SEED_SAMPLE_PER_COUNTRY:
                break
            lon = float(rng.uniform(minx, maxx))
            lat = float(rng.uniform(miny, maxy))
            if not geom.covers(Point(lon, lat)):
                continue
            got += 1
            tested += 1
            # Which partition owns this point decides what "nearest" means.
            sc = None
            entry = state_tree.get(cc)
            if entry is not None:
                tree, idx = entry
                pt = Point(lon, lat)
                hit = [i for i in (idx[k] for k in tree.query(pt))
                       if states.geometry.loc[i].covers(pt)]
                if hit:
                    cand = states.state_code.loc[hit[0]]
                    if (cc, cand) in lookup.by_state:
                        sc = cand
            if sc is not None:
                sub = s[s.state_code == sc]
                clons, clats, cids = (sub.seed_lon.to_numpy(), sub.seed_lat.to_numpy(),
                                      sub.metro_id.to_numpy())
            else:
                clons, clats, cids = lons, lats, ids
            d = great_circle_km(lon, lat, clons, clats)
            expect = cids[int(np.argmin(np.atleast_1d(d)))]
            assigned = lookup.resolve_tiered(lon, lat, cc, sc)["metro_id"]
            if assigned != expect:
                # Ties within a metre are not a disagreement.
                second = np.sort(d)[:2]
                if len(second) > 1 and abs(second[1] - second[0]) < 1e-3:
                    continue
                mismatch += 1
                if len(detail) < 25:
                    detail.append(
                        {"country_code": cc, "lon": lon, "lat": lat,
                         "assigned": assigned, "nearest": expect}
                    )
    return {"points_tested": tested, "mismatches": mismatch, "detail": detail}


def check_country_coverage(cells: pd.DataFrame, boundaries) -> pd.DataFrame:
    """Sections 18.6, 18.7 and 23 -- per-country coverage, overlap and escape."""
    rows = []
    by_cc = {cc: g for cc, g in cells.groupby("country_code")}
    for r in boundaries.itertuples():
        cc = r.country_code
        grp = by_cc.get(cc)
        country_area = spherical_area_km2(r.geometry)
        if grp is None:
            rows.append(
                {"country_code": cc, "n_metros": 0, "country_area_km2": country_area,
                 "uncovered_km2": country_area, "overlap_km2": 0.0, "escape_km2": 0.0,
                 "invalid_geoms": 0, "max_cell_area_km2": 0.0, "union_ok": True,
                 "measured_rotated": False}
            )
            continue

        geoms = list(grp.geometry)
        country_geom = r.geometry
        # Polar countries cannot be measured in plate-carree: the boundary's own
        # lat = +/-90 run and doubled meridian make set operations there report
        # artefacts rather than real gaps. Rotate both sides by the same rigid
        # transform first, exactly as the build does, so the comparison is
        # well conditioned. Rotation is orthogonal, so areas are unchanged.
        rotated_measure = geometry_touches_pole(country_geom) or any(
            geometry_touches_pole(g) for g in geoms
        )
        if rotated_measure:
            southern = country_geom.bounds[1] < 0
            rot = rotation_matrix_pole_to_equator(southern)
            country_geom = rotate_geometry(country_geom, rot) or country_geom
            geoms = [rotate_geometry(g, rot) or g for g in geoms]

        union_ok = True
        try:
            cov = unary_union(geoms)
            uncovered = spherical_area_km2(country_geom.difference(cov))
            escape = spherical_area_km2(cov.difference(country_geom))
        except Exception:
            union_ok = False
            uncovered, escape = float("nan"), float("nan")

        tree = STRtree(geoms)
        overlap = 0.0
        for i, gi in enumerate(geoms):
            for j in tree.query(gi):
                if j <= i:
                    continue
                try:
                    inter = gi.intersection(geoms[j])
                except Exception:
                    union_ok = False
                    continue
                if not inter.is_empty:
                    overlap += spherical_area_km2(inter)

        rows.append(
            {
                "country_code": cc,
                "n_metros": len(grp),
                "country_area_km2": country_area,
                "uncovered_km2": uncovered,
                "overlap_km2": overlap,
                "escape_km2": escape,
                "invalid_geoms": int(sum(0 if g.is_valid else 1 for g in geoms)),
                "max_cell_area_km2": max(spherical_area_km2(g) for g in geoms),
                "union_ok": union_ok,
                "measured_rotated": bool(rotated_measure),
            }
        )
    return pd.DataFrame(rows)


def check_land_coverage(lookup: Lookup, boundaries) -> dict:
    """Section 18.3 -- sweep land on a fine grid; every land point must resolve."""
    step = cfg.LAND_GRID_STEP_DEG
    tree = STRtree(list(boundaries.geometry))
    codes = list(boundaries.country_code)
    tested = 0
    n_missed = 0
    misses = []
    lons = np.arange(-180.0 + step / 2, 180.0, step)
    lats = np.arange(-90.0 + step / 2, 90.0, step)
    for lat in lats:
        for lon in lons:
            pt = Point(float(lon), float(lat))
            cand = [k for k in tree.query(pt) if boundaries.geometry.iloc[k].covers(pt)]
            if not cand:
                continue
            tested += 1
            cc = codes[cand[0]]
            if not lookup.matches(float(lon), float(lat), cc):
                n_missed += 1
                if len(misses) < 200:
                    misses.append({"lon": float(lon), "lat": float(lat), "country_code": cc})
    by_cc: dict[str, int] = {}
    for m in misses:
        by_cc[m["country_code"]] = by_cc.get(m["country_code"], 0) + 1
    return {
        "land_points_tested": tested,
        "unmatched": n_missed,
        "unmatched_by_country_sampled": dict(sorted(by_cc.items(), key=lambda kv: -kv[1])),
        "detail": misses[:60],
    }


def check_named_cases(lookup: Lookup) -> dict:
    """Sections 18.8 and 18.9 -- the specific coordinates the old table gets wrong."""
    out = {"antimeridian": [], "arctic": [], "seam_probe": []}
    for label, lon, lat, cc in ANTIMERIDIAN_CASES:
        got = lookup.resolve(lon, lat, cc)
        out["antimeridian"].append(
            {"case": label, "expected_cc": cc, "metro": got["metro"],
             "cc": got["country_code"], "n_matches": got["n_matches"],
             "method": got["method"], "ok": got["country_code"] == cc}
        )
    for label, lon, lat, cc in ARCTIC_CASES:
        got = lookup.resolve(lon, lat, cc)
        out["arctic"].append(
            {"case": label, "expected_cc": cc, "metro": got["metro"],
             "cc": got["country_code"], "method": got["method"],
             "ok": got["country_code"] == cc}
        )
    # Points immediately either side of the seam must not double-match.
    for lat in (-45.0, -20.0, 0.0, 20.0, 45.0, 60.0):
        for lon in (179.999, -179.999):
            hits = []
            for cc in lookup.by_cc:
                hits += lookup.matches(lon, lat, cc)
            out["seam_probe"].append({"lon": lon, "lat": lat, "matches": len(hits)})
    out["antimeridian_failures"] = sum(1 for r in out["antimeridian"] if not r["ok"])
    out["arctic_failures"] = sum(1 for r in out["arctic"] if not r["ok"])
    out["seam_double_matches"] = sum(1 for r in out["seam_probe"] if r["matches"] > 1)
    return out


def check_state_consistency(cells: pd.DataFrame, lookup: "Lookup", states) -> dict:
    """State-tier invariants.

    Two things are measured:
      * geometric -- a state-tier cell must not have area outside its own ADM1
        unit, so it cannot hand a point a metro from a neighbouring state;
      * behavioural -- for random points inside a *seeded* state, the resolved
        metro must belong to that same state.
    """
    if "partition_tier" not in cells.columns:
        return {"state_tier_cells": 0, "cross_state_area_km2": 0.0,
                "points_tested": 0, "points_in_wrong_state": 0, "examples": []}

    smap = {(r.country_code, r.state_code): r.geometry for r in states.itertuples()}
    state_cells = cells[cells.partition_tier == "state"]
    cross = 0.0
    worst = []
    for r in state_cells.itertuples():
        g = smap.get((r.country_code, r.state_code))
        if g is None:
            continue
        try:
            out = spherical_area_km2(r.geometry.difference(g))
        except Exception:
            continue
        if out > 1e-6:
            cross += out
            if len(worst) < 15:
                worst.append({"metro": r.metro, "state_code": r.state_code,
                              "outside_km2": round(out, 4)})

    rng = np.random.default_rng(cfg.RANDOM_SEED + 2)
    seeded = set(lookup.by_state)
    tested = wrong = 0
    examples = []
    for r in states.itertuples():
        key = (r.country_code, r.state_code)
        if key not in seeded:
            continue
        minx, miny, maxx, maxy = r.geometry.bounds
        got = 0
        for _ in range(300):
            if got >= 6:
                break
            lon = float(rng.uniform(minx, maxx))
            lat = float(rng.uniform(miny, maxy))
            if not r.geometry.covers(Point(lon, lat)):
                continue
            got += 1
            tested += 1
            res = lookup.resolve_tiered(lon, lat, r.country_code, r.state_code)
            if res["state_code"] != r.state_code:
                wrong += 1
                if len(examples) < 20:
                    examples.append({"state_code": r.state_code, "lon": lon, "lat": lat,
                                     "got_state": res["state_code"], "method": res["method"]})
    return {
        "state_tier_cells": int(len(state_cells)),
        "country_tier_cells": int((cells.partition_tier == "country").sum()),
        "seeded_states": len(seeded),
        "cross_state_area_km2": cross,
        "cross_state_examples": worst,
        "points_tested": tested,
        "points_in_wrong_state": wrong,
        "examples": examples,
    }


# --------------------------------------------------------------------------
def gate(coverage: pd.DataFrame, report: dict) -> tuple[bool, list[str]]:
    """Turn the measurements into a build pass/fail (plan section 23)."""
    problems: list[str] = []
    deferred: list[str] = []
    tol = cfg.TOL
    for r in coverage.itertuples():
        if r.n_metros == 0:
            continue
        if r.country_code in cfg.SHAPELY_UNVERIFIABLE_COUNTRIES:
            deferred.append(r.country_code)
            continue
        scale = max(r.country_area_km2, 1.0)
        floor = tol["abs_area_km2"]
        if not r.union_ok:
            problems.append(f"{r.country_code}: geometry set operations failed")
            continue
        if r.uncovered_km2 > max(floor, tol["uncovered_area_frac"] * scale):
            problems.append(f"{r.country_code}: uncovered {r.uncovered_km2:.3f} km2")
        if r.overlap_km2 > max(floor, tol["overlap_area_frac"] * scale):
            problems.append(f"{r.country_code}: overlap {r.overlap_km2:.3f} km2")
        if r.escape_km2 > max(floor, tol["cross_country_area_frac"] * scale):
            problems.append(f"{r.country_code}: escapes country by {r.escape_km2:.3f} km2")
        if r.invalid_geoms:
            problems.append(f"{r.country_code}: {r.invalid_geoms} invalid geometries")

    if report["seed_ownership"]["failures"]:
        problems.append(f"seed ownership: {report['seed_ownership']['failures']} failures")
    if report["interior_uniqueness"]["multi_match"]:
        problems.append(
            f"interior uniqueness: {report['interior_uniqueness']['multi_match']} multi-matches"
        )
    if report["nearest_seed"]["mismatches"]:
        problems.append(f"nearest-seed: {report['nearest_seed']['mismatches']} mismatches")
    land_misses = {
        cc: n
        for cc, n in report["land_coverage"]["unmatched_by_country_sampled"].items()
        if cc not in cfg.SHAPELY_UNVERIFIABLE_COUNTRIES
    }
    if land_misses:
        problems.append(f"land coverage: unmatched points in {land_misses}")
    if report["named_cases"]["antimeridian_failures"]:
        problems.append("antimeridian regression cases failing")
    if report["named_cases"]["arctic_failures"]:
        problems.append("arctic regression cases failing")
    if report["named_cases"]["seam_double_matches"]:
        problems.append("seam probe double-matches")
    sc = report.get("state_consistency")
    if sc:
        # Relative, not absolute: this is a sum over thousands of state cells, and
        # every one of them writes exact great-circle edges as 0.25-degree
        # polylines. 1e-6 of global land is ~147 km2; the measured figure is
        # ~16 km2, i.e. ~0.002 km2 per state cell.
        land = float(coverage.country_area_km2.sum()) or 1.0
        if sc["cross_state_area_km2"] > max(tol["abs_area_km2"], 1e-6 * land):
            problems.append(
                f"state tier: {sc['cross_state_area_km2']:.3f} km2 of cell area outside its own state"
            )
        if sc["points_in_wrong_state"]:
            problems.append(
                f"state tier: {sc['points_in_wrong_state']} of {sc['points_tested']} "
                "points resolved to a metro in another state"
            )
    report["gate_deferred_to_bigquery"] = sorted(set(deferred))
    return (not problems), problems


def run(cells: pd.DataFrame, boundaries, seeds: pd.DataFrame, states=None) -> tuple[dict, pd.DataFrame]:
    lookup = Lookup(cells)
    report = {
        "cells": int(len(cells)),
        "metros": int(cells.metro_id.nunique()),
        "pieces": int(sum(len(g.geoms) for g in cells.geometry)),
        "seed_ownership": check_seed_ownership(cells, lookup),
        "interior_uniqueness": check_interior_uniqueness(cells, lookup, boundaries),
        "nearest_seed": check_nearest_seed(cells, lookup, boundaries, seeds, states=states),
        "land_coverage": check_land_coverage(lookup, boundaries),
        "named_cases": check_named_cases(lookup),
    }
    if states is not None:
        report["state_consistency"] = check_state_consistency(cells, lookup, states)
    coverage = check_country_coverage(cells, boundaries)
    ok, problems = gate(coverage, report)
    report["gate_passed"] = ok
    report["gate_problems"] = problems
    report["global"] = {
        "country_area_km2": float(coverage.country_area_km2.sum()),
        "uncovered_km2": float(np.nansum(coverage.uncovered_km2)),
        "overlap_km2": float(np.nansum(coverage.overlap_km2)),
        "escape_km2": float(np.nansum(coverage.escape_km2)),
    }
    return report, coverage


def main() -> None:
    from . import export_bigquery as ex
    from . import load_country_boundaries as lb
    from . import load_seeds as ls

    cells = ex.load_cells()
    report, coverage = run(cells, lb.load(), ls.load())
    cfg.S06_VALIDATION.write_text(json.dumps(report, indent=2, default=str))
    coverage.to_csv(cfg.COUNTRY_DIAGNOSTICS, index=False)
    print(json.dumps({k: v for k, v in report.items() if k != "gate_problems"}, indent=2, default=str)[:2500])
    print("\ngate:", "PASS" if report["gate_passed"] else "FAIL")
    for p in report["gate_problems"][:30]:
        print("  -", p)


if __name__ == "__main__":
    main()

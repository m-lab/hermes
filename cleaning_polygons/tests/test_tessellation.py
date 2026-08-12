"""Offline invariants for the metro tessellation.

No BigQuery and no network: these run against the stage artifacts in
``data/stages/`` if a build has been run, and skip cleanly otherwise. The
mathematical properties (half-space exactness, antimeridian handling, rotation
fidelity) are tested on synthetic inputs and always run.
"""
from __future__ import annotations

import math

import numpy as np
import pytest
from shapely.geometry import MultiPolygon, Point, Polygon

from cleaning_polygons import config as cfg
from cleaning_polygons.antimeridian import normalize, unwrap_lons
from cleaning_polygons.build_country_voronoi import CountryJob, build_country
from cleaning_polygons.geometry_utils import (
    EARTH_RADIUS_KM,
    GnomonicFrame,
    densify_geometry,
    great_circle_km,
    lonlat_to_xyz,
    rotate_geometry,
    rotation_matrix_pole_to_equator,
    spherical_area_km2,
    xyz_to_lonlat,
)


# --------------------------------------------------------------------- maths
def test_lonlat_roundtrip():
    lon = np.array([-179.9, -90.0, 0.0, 45.5, 179.9])
    lat = np.array([-89.0, -45.0, 0.0, 12.25, 89.0])
    got_lon, got_lat = xyz_to_lonlat(lonlat_to_xyz(lon, lat))
    assert np.allclose(got_lon, lon, atol=1e-9)
    assert np.allclose(got_lat, lat, atol=1e-9)


def test_great_circle_across_antimeridian():
    """179 and -179 are 2 degrees apart, not 358."""
    km = float(great_circle_km(179.0, 0.0, -179.0, 0.0))
    assert km == pytest.approx(2.0 * math.pi * EARTH_RADIUS_KM / 180.0, rel=1e-6)


def test_unwrap_lons_takes_shortest_steps():
    got = unwrap_lons(np.array([170.0, 179.0, -179.0, -170.0]))
    assert np.allclose(got, [170.0, 179.0, 181.0, 190.0])


def test_gnomonic_halfplane_matches_spherical_bisector():
    """The linear half-plane must agree with brute-force great-circle distance."""
    rng = np.random.default_rng(11)
    for _ in range(40):
        pts = rng.normal(size=(3, 3))
        pts /= np.linalg.norm(pts, axis=1, keepdims=True)
        if abs(np.dot(pts[0], pts[1])) > 0.999:
            continue
        frame = GnomonicFrame.about(pts[2] if np.dot(pts[2], pts[0]) > 0 else -pts[2])
        a, b, d = frame.halfplane(pts[0], pts[1])
        for _ in range(20):
            uv = rng.normal(scale=0.3, size=2)
            x = frame.inverse(uv[None, :])[0]
            side = a * uv[0] + b * uv[1] + d
            closer_to_0 = float(np.dot(x, pts[0])) >= float(np.dot(x, pts[1]))
            if abs(side) > 1e-9:
                assert (side > 0) == closer_to_0


def test_pole_rotation_preserves_area():
    """A polar polygon's area must survive the rotation round-trip.

    The ring is densified first, exactly as the pipeline densifies boundaries.
    A run along constant latitude is not a geodesic, so without densification the
    rotated copy -- whose edges are great circles -- differs by ~0.1%. That is a
    representation difference, not a rotation error.
    """
    ring = [(lon, -80.0) for lon in np.arange(-180, 181, 5.0)]
    ring += [(180.0, -90.0), (-180.0, -90.0)]
    poly = densify_geometry(MultiPolygon([Polygon(ring)]), 0.1)
    before = spherical_area_km2(poly)
    rot = rotation_matrix_pole_to_equator(True)
    there = rotate_geometry(poly, rot)
    back = rotate_geometry(there, rot.T)
    assert spherical_area_km2(there) == pytest.approx(before, rel=1e-6)
    assert spherical_area_km2(back) == pytest.approx(before, rel=1e-6)


def test_normalize_splits_a_dateline_crossing_polygon():
    poly = Polygon([(170.0, -5.0), (-170.0, -5.0), (-170.0, 5.0), (170.0, 5.0)])
    out = normalize(poly)
    assert out is not None
    assert len(out.geoms) == 2
    for g in out.geoms:
        xs = np.asarray(g.exterior.coords)[:, 0]
        assert xs.min() >= -180.0 - 1e-9 and xs.max() <= 180.0 + 1e-9
    # The short way round: 20 degrees of longitude, not 340. Compare against the
    # closed-form area of a 20 x 10 degree lat/lon cell straddling the equator.
    expected = (
        EARTH_RADIUS_KM**2
        * math.radians(20.0)
        * 2.0
        * math.sin(math.radians(5.0))
    )
    assert spherical_area_km2(out) == pytest.approx(expected, rel=1e-3)


# ------------------------------------------------------- tessellation itself
def _square(lon0, lat0, size=10.0) -> MultiPolygon:
    """A lat/lon box, densified the way the pipeline densifies real boundaries.

    Without densification the top and bottom edges are constant-latitude runs,
    which are not geodesics, so the tessellation (whose edges are great circles)
    would tile a slightly smaller region than ``spherical_area_km2`` reports for
    the plate-carree ring -- a ~0.2% mismatch that is representation, not error.
    """
    box = Polygon(
        [(lon0, lat0), (lon0 + size, lat0), (lon0 + size, lat0 + size), (lon0, lat0 + size)]
    )
    return densify_geometry(MultiPolygon([box]), 0.1)


def _seeds(rows) -> "object":
    import pandas as pd

    return pd.DataFrame(
        [
            {
                "metro_id": f"m_{i}",
                "metro": f"C{i}-R-XX",
                "city": f"C{i}",
                "state_resolved": "R",
                "state_iso2": "R",
                "country_code": "XX",
                "seed_lon": lon,
                "seed_lat": lat,
                "ne_pop_max": 1000,
                "legacy_cell_population": None,
                "seed_source": "test",
            }
            for i, (lon, lat) in enumerate(rows)
        ]
    )


def test_single_seed_takes_whole_territory():
    geom = _square(0.0, 0.0)
    recs, diag = build_country(CountryJob("XX", geom, _seeds([(5.0, 5.0)])))
    assert diag["case"] == "single_seed"
    assert len(recs) == 1
    assert spherical_area_km2(recs[0]["geometry"]) == pytest.approx(
        spherical_area_km2(geom), rel=1e-6
    )


def test_two_seeds_split_on_the_spherical_bisector():
    geom = _square(0.0, 0.0)
    recs, diag = build_country(CountryJob("XX", geom, _seeds([(2.5, 5.0), (7.5, 5.0)])))
    assert diag["case"] == "two_seeds_bisector"
    assert len(recs) == 2
    total = sum(spherical_area_km2(r["geometry"]) for r in recs)
    assert total == pytest.approx(spherical_area_km2(geom), rel=1e-5)
    # Symmetric seeds about lon 5 give equal halves.
    a, b = (spherical_area_km2(r["geometry"]) for r in recs)
    assert a == pytest.approx(b, rel=1e-3)


def test_cells_partition_without_overlap_and_respect_nearest_seed():
    rng = np.random.default_rng(3)
    pts = [(float(rng.uniform(1, 9)), float(rng.uniform(1, 9))) for _ in range(12)]
    geom = _square(0.0, 0.0)
    recs, _ = build_country(CountryJob("XX", geom, _seeds(pts)))
    total = sum(spherical_area_km2(r["geometry"]) for r in recs)
    assert total == pytest.approx(spherical_area_km2(geom), rel=1e-4)

    for i, ri in enumerate(recs):
        for rj in recs[i + 1 :]:
            inter = ri["geometry"].intersection(rj["geometry"])
            assert spherical_area_km2(inter) < 1e-6

    lons = np.array([p[0] for p in pts])
    lats = np.array([p[1] for p in pts])
    for _ in range(150):
        lon, lat = float(rng.uniform(0.2, 9.8)), float(rng.uniform(0.2, 9.8))
        d = great_circle_km(lon, lat, lons, lats)
        order = np.argsort(d)
        if d[order[1]] - d[order[0]] < 1e-3:
            continue  # on a bisector
        want = f"m_{int(order[0])}"
        got = [r["metro_id"] for r in recs if r["geometry"].covers(Point(lon, lat))]
        assert got == [want], (lon, lat, got, want)


def test_seeds_lie_inside_their_own_cells_across_the_dateline():
    geom = densify_geometry(
        MultiPolygon(
            [Polygon([(170.0, -5.0), (180.0, -5.0), (180.0, 5.0), (170.0, 5.0)]),
             Polygon([(-180.0, -5.0), (-170.0, -5.0), (-170.0, 5.0), (-180.0, 5.0)])]
        ),
        0.1,
    )
    pts = [(173.0, 0.0), (178.0, 2.0), (-178.0, -2.0), (-172.0, 1.0)]
    recs, _ = build_country(CountryJob("XX", geom, _seeds(pts)))
    assert len(recs) == len(pts)
    for r in recs:
        assert r["geometry"].covers(Point(r["seed_lon"], r["seed_lat"])), r["metro"]
    total = sum(spherical_area_km2(r["geometry"]) for r in recs)
    assert total == pytest.approx(spherical_area_km2(geom), rel=1e-4)


def test_duplicate_metro_ids_are_collapsed_not_double_counted():
    """Two seeds with one canonical identity must not emit the cell twice."""
    import pandas as pd

    s = _seeds([(3.0, 5.0), (7.0, 5.0)])
    s.loc[1, "metro_id"] = s.loc[0, "metro_id"]
    recs, diag = build_country(CountryJob("XX", _square(0.0, 0.0), s))
    assert diag["duplicate_metro_ids_dropped"] == 1
    assert len(recs) == 1


# ------------------------------------------------------- built artifacts
def _load_cells():
    if not cfg.S05_CELLS_NORM.exists():
        pytest.skip("no build artifacts; run `python -m cleaning_polygons.build` first")
    import geopandas as gpd

    return gpd.read_parquet(cfg.S05_CELLS_NORM)


def test_built_cells_are_valid_positive_geometry():
    cells = _load_cells()
    assert len(cells) > 0
    assert cells.geometry.is_valid.all()
    for g in cells.geometry:
        assert g.geom_type == "MultiPolygon"
        # Positive geometry: a real cell is a small fraction of the globe, unlike
        # the old inverted polygons whose area was ~the whole sphere.
        assert spherical_area_km2(g) < 0.25 * 4 * math.pi * EARTH_RADIUS_KM**2


def test_built_cells_stay_within_longitude_range():
    for g in _load_cells().geometry:
        for p in g.geoms:
            xs = np.asarray(p.exterior.coords)[:, 0]
            assert xs.min() >= -180.0 - 1e-6
            assert xs.max() <= 180.0 + 1e-6


def test_state_tier_cells_carry_a_state_code():
    cells = _load_cells()
    if "partition_tier" not in cells.columns:
        pytest.skip("build predates the state tier")
    state = cells[cells.partition_tier == "state"]
    assert len(state) > 0
    assert state.state_code.notna().all()
    country = cells[cells.partition_tier == "country"]
    assert country.state_code.isna().all()


def test_metro_strings_have_no_placeholder_or_whitespace_damage():
    cells = _load_cells()
    metros = cells.metro.astype(str)
    assert not metros.str.contains("  ").any(), "double space leaked into a metro key"
    assert not metros.str.fullmatch(r".*-nan-.*").any()
    assert not metros.str.contains("-99").any()
    # The state_iso2-only regression put 37.85% of live metros into this form.
    assert (metros.str.contains("-NA-").mean()) < 0.01

"""Stage 05 -- antimeridian normalisation.

Cells are computed on the sphere, where +180 and -180 are the same meridian and
nothing special happens there. The seam only appears when the result is written
out as lon/lat rings. This module does that conversion correctly:

1. Unwrap each ring's longitudes by accumulating *shortest* steps, so a ring that
   crosses the dateline becomes continuous in an extended longitude space
   (e.g. 170 -> 190) instead of jumping 179 -> -179.
2. Split the unwrapped ring on every 360-degree strip boundary and shift each
   piece back into [-180, 180].
3. Emit a MULTIPOLYGON.

The failure this prevents is exactly the one in the current table, where two
polygons both claimed a band around 180 and the enrichment SQL picked between
them alphabetically. Longitude differences are never taken as ``abs(l1 - l2)``.
"""
from __future__ import annotations

import numpy as np
from shapely.geometry import MultiPolygon, Polygon, box
from shapely.ops import unary_union

from .geometry_utils import as_multipolygon, clean

_EPS = 1e-9


def unwrap_lons(lons: np.ndarray) -> np.ndarray:
    """Make a longitude sequence continuous by accumulating shortest steps."""
    lons = np.asarray(lons, dtype=float)
    if len(lons) == 0:
        return lons
    steps = np.diff(lons)
    steps = ((steps + 180.0) % 360.0) - 180.0
    return np.concatenate([[lons[0]], lons[0] + np.cumsum(steps)])


def _unwrap_ring(coords) -> np.ndarray:
    arr = np.asarray(coords, dtype=float)
    lon = unwrap_lons(arr[:, 0])
    return np.column_stack([lon, arr[:, 1]])


def ring_touches_pole(coords, tol: float = 1e-6) -> bool:
    arr = np.asarray(coords, dtype=float)
    return bool(np.any(np.abs(np.abs(arr[:, 1]) - 90.0) < tol))


def split_polygon_at_antimeridian(poly: Polygon) -> list[Polygon]:
    """Return pieces of ``poly`` each contained in [-180, 180] longitude."""
    shell = _unwrap_ring(poly.exterior.coords)
    holes = [_unwrap_ring(r.coords) for r in poly.interiors]
    unwrapped = clean(Polygon(shell, holes))
    if unwrapped.is_empty:
        return []

    minx, _, maxx, _ = unwrapped.bounds
    k_lo = int(np.floor((minx + 180.0) / 360.0))
    k_hi = int(np.floor((maxx + 180.0 - _EPS) / 360.0))

    if k_lo == k_hi == 0 and -180.0 - _EPS <= minx and maxx <= 180.0 + _EPS:
        return [p for p in _as_polys(unwrapped)]

    pieces: list[Polygon] = []
    for k in range(k_lo, k_hi + 1):
        strip = box(-180.0 + 360.0 * k, -90.0, 180.0 + 360.0 * k, 90.0)
        part = unwrapped.intersection(strip)
        if part.is_empty:
            continue
        for p in _as_polys(part):
            if k == 0:
                pieces.append(p)
            else:
                pieces.append(_shift_lon(p, -360.0 * k))
    return [p for p in pieces if not p.is_empty]


def _as_polys(geom) -> list[Polygon]:
    if geom.is_empty:
        return []
    if geom.geom_type == "Polygon":
        return [geom]
    if geom.geom_type in ("MultiPolygon", "GeometryCollection"):
        out = []
        for g in geom.geoms:
            out.extend(_as_polys(g))
        return out
    return []


def _shift_lon(poly: Polygon, delta: float) -> Polygon:
    def _r(coords):
        arr = np.asarray(coords, dtype=float)
        return list(zip((arr[:, 0] + delta).tolist(), arr[:, 1].tolist()))

    return Polygon(_r(poly.exterior.coords), [_r(r.coords) for r in poly.interiors])


def normalize(geom) -> MultiPolygon | None:
    """Split a (Multi)Polygon at +/-180 and return clean positive geometry."""
    mp = as_multipolygon(geom)
    if mp is None:
        return None
    pieces: list[Polygon] = []
    for poly in mp.geoms:
        if poly.is_empty:
            continue
        pieces.extend(split_polygon_at_antimeridian(poly))
    if not pieces:
        return None
    # Snap numerical drift at the seam so adjacent cells do not gain slivers.
    fixed = []
    for p in pieces:
        p = clean(p)
        if p.is_empty:
            continue
        fixed.extend(_as_polys(p))
    if not fixed:
        return None
    if len(fixed) == 1:
        return as_multipolygon(fixed[0])
    try:
        return as_multipolygon(clean(unary_union(fixed)))
    except Exception:
        # Union can fail where many pieces meet near a pole. The pieces are
        # already disjoint by construction, so keeping them apart is correct.
        return MultiPolygon(fixed)


def normalize_frame(df, geometry_col: str = "geometry"):
    """Apply :func:`normalize` across a cell frame, dropping empties."""
    out_geoms = []
    keep = []
    for idx, geom in df[geometry_col].items():
        norm = normalize(geom)
        if norm is None or norm.is_empty:
            continue
        out_geoms.append(norm)
        keep.append(idx)
    result = df.loc[keep].copy()
    result[geometry_col] = out_geoms
    return result.reset_index(drop=True)

"""Spherical geometry helpers.

Everything here works on unit vectors, never on raw lon/lat arithmetic. The one
place a plane is used is the *gnomonic* frame, which is chosen precisely because
great circles map to straight lines under it -- so a spherical half-space
becomes an exact planar half-plane and no approximation enters the tessellation.
"""
from __future__ import annotations

import math
from dataclasses import dataclass

import numpy as np
from shapely.geometry import LinearRing, MultiPolygon, Polygon
from shapely.ops import unary_union

EARTH_RADIUS_KM = 6371.0088
EARTH_AREA_KM2 = 4.0 * math.pi * EARTH_RADIUS_KM**2  # 510_065_621 km^2


# --------------------------------------------------------------------------
# lon/lat <-> unit vectors
# --------------------------------------------------------------------------
def lonlat_to_xyz(lon, lat) -> np.ndarray:
    """(lon, lat) in degrees -> unit vectors, shape (..., 3)."""
    lon_r = np.radians(np.asarray(lon, dtype=float))
    lat_r = np.radians(np.asarray(lat, dtype=float))
    cos_lat = np.cos(lat_r)
    return np.stack(
        [cos_lat * np.cos(lon_r), cos_lat * np.sin(lon_r), np.sin(lat_r)], axis=-1
    )


def xyz_to_lonlat(vecs: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Unit vectors -> (lon, lat) in degrees, lon in [-180, 180]."""
    v = np.asarray(vecs, dtype=float)
    norm = np.linalg.norm(v, axis=-1, keepdims=True)
    v = v / np.where(norm == 0.0, 1.0, norm)
    lat = np.degrees(np.arcsin(np.clip(v[..., 2], -1.0, 1.0)))
    lon = np.degrees(np.arctan2(v[..., 1], v[..., 0]))
    return lon, lat


def great_circle_km(lon1, lat1, lon2, lat2) -> np.ndarray:
    """Great-circle distance in km. Never compares longitudes numerically."""
    a = lonlat_to_xyz(lon1, lat1)
    b = lonlat_to_xyz(lon2, lat2)
    dot = np.clip(np.sum(a * b, axis=-1), -1.0, 1.0)
    return EARTH_RADIUS_KM * np.arccos(dot)


def angular_sep_rad(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    dot = np.clip(np.sum(np.asarray(a) * np.asarray(b), axis=-1), -1.0, 1.0)
    return np.arccos(dot)


# --------------------------------------------------------------------------
# gnomonic frame
# --------------------------------------------------------------------------
@dataclass
class GnomonicFrame:
    """Central-perspective frame about unit vector ``centre``.

    A plane point ``(u, v)`` denotes the sphere direction
    ``normalize(centre + u * e1 + v * e2)``. Great circles through the near
    hemisphere are straight lines in ``(u, v)``, exactly.
    """

    centre: np.ndarray
    e1: np.ndarray
    e2: np.ndarray

    @classmethod
    def about(cls, centre: np.ndarray) -> "GnomonicFrame":
        c = np.asarray(centre, dtype=float)
        c = c / np.linalg.norm(c)
        # Pick a helper axis that is not parallel to c, so e1 is well defined.
        helper = np.array([0.0, 0.0, 1.0])
        if abs(float(np.dot(c, helper))) > 0.9:
            helper = np.array([1.0, 0.0, 0.0])
        e1 = np.cross(helper, c)
        e1 /= np.linalg.norm(e1)
        e2 = np.cross(c, e1)
        e2 /= np.linalg.norm(e2)
        return cls(centre=c, e1=e1, e2=e2)

    def forward(self, vecs: np.ndarray) -> np.ndarray:
        """Unit vectors -> (u, v). Requires the near hemisphere."""
        v = np.atleast_2d(np.asarray(vecs, dtype=float))
        w = v @ self.centre
        if np.any(w <= 1e-9):
            raise ValueError("gnomonic projection requires the near hemisphere")
        return np.stack([(v @ self.e1) / w, (v @ self.e2) / w], axis=-1)

    def inverse(self, uv: np.ndarray) -> np.ndarray:
        """(u, v) -> unit vectors."""
        uv = np.atleast_2d(np.asarray(uv, dtype=float))
        vecs = (
            self.centre[None, :]
            + uv[:, 0:1] * self.e1[None, :]
            + uv[:, 1:2] * self.e2[None, :]
        )
        return vecs / np.linalg.norm(vecs, axis=1, keepdims=True)

    def halfplane(self, p_i: np.ndarray, p_j: np.ndarray) -> tuple[float, float, float]:
        """Coefficients of "closer to p_i than to p_j" as ``A*u + B*v + D >= 0``.

        The spherical condition is ``x . (p_i - p_j) >= 0``. Substituting
        ``x ~ centre + u*e1 + v*e2`` (positive scaling, so the inequality
        direction is preserved) gives an exact linear form.
        """
        w = np.asarray(p_i, dtype=float) - np.asarray(p_j, dtype=float)
        return float(self.e1 @ w), float(self.e2 @ w), float(self.centre @ w)


def spherical_centroid(vecs: np.ndarray) -> np.ndarray:
    v = np.atleast_2d(np.asarray(vecs, dtype=float))
    m = v.mean(axis=0)
    n = np.linalg.norm(m)
    if n < 1e-12:  # antipodally balanced; fall back to first vertex
        return v[0] / np.linalg.norm(v[0])
    return m / n


# --------------------------------------------------------------------------
# convex half-plane clipping (Sutherland-Hodgman)
# --------------------------------------------------------------------------
def clip_convex_halfplane(
    ring: np.ndarray, a: float, b: float, d: float, eps: float = 0.0
) -> np.ndarray:
    """Clip a convex ring (N,2), open (no repeated last point), to a*u+b*v+d>=0."""
    if len(ring) == 0:
        return ring
    vals = a * ring[:, 0] + b * ring[:, 1] + d
    if np.all(vals >= -eps):
        return ring
    if np.all(vals <= eps):
        return np.empty((0, 2))
    out = []
    n = len(ring)
    for i in range(n):
        j = (i + 1) % n
        vi, vj = vals[i], vals[j]
        if vi >= -eps:
            out.append(ring[i])
        if (vi > eps and vj < -eps) or (vi < -eps and vj > eps):
            t = vi / (vi - vj)
            out.append(ring[i] + t * (ring[j] - ring[i]))
    if len(out) < 3:
        return np.empty((0, 2))
    return np.asarray(out)


def ring_max_radius(ring: np.ndarray, frame: GnomonicFrame, p_i: np.ndarray) -> float:
    """Max angular distance (radians) from ``p_i`` to any vertex of ``ring``."""
    if len(ring) == 0:
        return 0.0
    vecs = frame.inverse(ring)
    return float(np.max(angular_sep_rad(vecs, p_i[None, :])))


# --------------------------------------------------------------------------
# densification and shapely bridges
# --------------------------------------------------------------------------
def densify_ring_uv(ring: np.ndarray, frame: GnomonicFrame, step_deg: float) -> np.ndarray:
    """Insert intermediate points so each edge spans <= ``step_deg`` on the sphere.

    Edges are straight in the gnomonic plane, i.e. exact great-circle arcs, so
    this only refines the polyline used to *represent* them in lon/lat.
    """
    if len(ring) < 2:
        return ring
    step = math.radians(step_deg)
    vecs = frame.inverse(ring)
    out = []
    n = len(ring)
    for i in range(n):
        j = (i + 1) % n
        out.append(ring[i])
        sep = float(angular_sep_rad(vecs[i], vecs[j]))
        k = int(sep // step)
        if k >= 1:
            for m in range(1, k + 1):
                t = m / (k + 1)
                out.append(ring[i] + t * (ring[j] - ring[i]))
    return np.asarray(out)


def densify_lonlat_ring(coords, step_deg: float):
    """Densify a plate-carree ring so geodesic and straight-line readings agree.

    Longitude deltas are unwrapped, never taken as ``abs(lon1 - lon2)``.
    """
    pts = list(coords)
    if len(pts) < 2:
        return pts
    out = []
    for i in range(len(pts) - 1):
        (x0, y0), (x1, y1) = pts[i], pts[i + 1]
        out.append((x0, y0))
        dx = ((x1 - x0 + 180.0) % 360.0) - 180.0
        dy = y1 - y0
        n = int(max(abs(dx), abs(dy)) // step_deg)
        for m in range(1, n + 1):
            t = m / (n + 1)
            out.append((x0 + t * dx, y0 + t * dy))
    out.append(pts[-1])
    return out


def densify_geometry(geom, step_deg: float):
    """Densify every ring of a (Multi)Polygon in lon/lat."""
    if geom is None or geom.is_empty:
        return geom

    def _poly(p: Polygon) -> Polygon:
        shell = densify_lonlat_ring(p.exterior.coords, step_deg)
        holes = [densify_lonlat_ring(r.coords, step_deg) for r in p.interiors]
        return Polygon(shell, holes)

    if geom.geom_type == "Polygon":
        return _poly(geom)
    return MultiPolygon([_poly(p) for p in geom.geoms if not p.is_empty])


def as_multipolygon(geom) -> MultiPolygon | None:
    """Normalise to MultiPolygon, dropping non-areal debris."""
    if geom is None or geom.is_empty:
        return None
    if geom.geom_type == "Polygon":
        return MultiPolygon([geom])
    if geom.geom_type == "MultiPolygon":
        return geom
    if geom.geom_type == "GeometryCollection":
        polys = [g for g in geom.geoms if g.geom_type in ("Polygon", "MultiPolygon")]
        if not polys:
            return None
        merged = unary_union(polys)
        return as_multipolygon(merged)
    return None


def clean(geom):
    """Repair self-intersections without changing topology meaningfully."""
    if geom is None or geom.is_empty:
        return geom
    if not geom.is_valid:
        geom = geom.buffer(0)
    return geom


# --------------------------------------------------------------------------
# spherical area
# --------------------------------------------------------------------------
def _ring_spherical_excess(lonlat: np.ndarray) -> float:
    """Signed spherical area (steradians) of a ring given as (N,2) lon/lat degrees."""
    if len(lonlat) < 3:
        return 0.0
    lon = np.radians(lonlat[:, 0])
    lat = np.radians(lonlat[:, 1])
    lon2 = np.roll(lon, -1)
    lat2 = np.roll(lat, -1)
    # Unwrap longitude differences so the antimeridian is not a discontinuity.
    dlon = np.mod(lon2 - lon + np.pi, 2 * np.pi) - np.pi
    return float(np.sum(dlon * (2.0 + np.sin(lat) + np.sin(lat2))) / 2.0)


def spherical_area_km2(geom) -> float:
    """Area on the sphere in km^2, valid across the antimeridian and poles."""
    mp = as_multipolygon(geom)
    if mp is None:
        return 0.0
    total = 0.0
    for poly in mp.geoms:
        shell = abs(_ring_spherical_excess(np.asarray(poly.exterior.coords)))
        holes = sum(abs(_ring_spherical_excess(np.asarray(r.coords))) for r in poly.interiors)
        total += shell - holes
    return total * EARTH_RADIUS_KM**2


def ring_is_ccw(coords) -> bool:
    return LinearRing(coords).is_ccw


# --------------------------------------------------------------------------
# rigid rotation of the sphere
# --------------------------------------------------------------------------
def rotation_matrix_pole_to_equator(southern: bool) -> np.ndarray:
    """Rotation taking the given pole onto (lon 0, lat 0).

    Polar country outlines are unusable as plate-carree polygons: a pole is a
    single point that the format has to draw as a whole parallel, and the +/-180
    meridian has to be traversed twice. Both artefacts vanish under a rigid
    rotation that moves the pole to the equator -- the region becomes an ordinary
    blob around (0, 0) with no seam and no degenerate parallel. The rotation is a
    pure orthogonal transform of unit vectors, so it introduces no distortion.
    """
    pole = np.array([0.0, 0.0, -1.0 if southern else 1.0])
    target = np.array([1.0, 0.0, 0.0])
    axis = np.cross(pole, target)
    norm = np.linalg.norm(axis)
    if norm < 1e-12:
        return np.eye(3)
    axis = axis / norm
    angle = math.acos(float(np.clip(np.dot(pole, target), -1.0, 1.0)))
    k = np.array(
        [[0.0, -axis[2], axis[1]], [axis[2], 0.0, -axis[0]], [-axis[1], axis[0], 0.0]]
    )
    return np.eye(3) + math.sin(angle) * k + (1.0 - math.cos(angle)) * (k @ k)


def rotate_lonlat(lon, lat, rot: np.ndarray):
    vecs = lonlat_to_xyz(lon, lat)
    return xyz_to_lonlat(np.atleast_2d(vecs) @ rot.T)


def rotate_geometry(geom, rot: np.ndarray):
    """Rotate a lon/lat (Multi)Polygon, repairing degenerate polar artefacts.

    ``buffer(0)`` after rotation is what discards the collapsed ``lat = +/-90``
    run and the doubled meridian: they become zero-area spikes, which is exactly
    what a repair removes.
    """
    def _ring(coords):
        arr = np.asarray(coords, dtype=float)
        lon, lat = rotate_lonlat(arr[:, 0], arr[:, 1], rot)
        return list(zip(np.atleast_1d(lon).tolist(), np.atleast_1d(lat).tolist()))

    def _poly(p: Polygon):
        try:
            return Polygon(_ring(p.exterior.coords), [_ring(r.coords) for r in p.interiors])
        except Exception:
            return None

    src = [geom] if geom.geom_type == "Polygon" else list(geom.geoms)
    out = []
    for p in src:
        if p.geom_type != "Polygon" or p.is_empty:
            continue
        q = _poly(p)
        if q is None:
            continue
        q = q.buffer(0)
        if q.is_empty:
            continue
        if q.geom_type == "Polygon":
            out.append(q)
        else:
            out.extend(g for g in q.geoms if g.geom_type == "Polygon" and not g.is_empty)
    if not out:
        return None
    return MultiPolygon(out)


def geometry_touches_pole(geom, tol_deg: float = 0.01) -> bool:
    mp = as_multipolygon(geom)
    if mp is None:
        return False
    for p in mp.geoms:
        arr = np.asarray(p.exterior.coords)
        if np.any(np.abs(arr[:, 1]) >= 90.0 - tol_deg):
            return True
    return False

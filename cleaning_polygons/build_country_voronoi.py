"""Stage 04 -- country-constrained spherical Voronoi tessellation.

Method
------
For every country ``c`` with seed set ``S_c`` and land geometry ``G_c``, the cell
of seed ``i`` is

    cell_i = { x in G_c : d_gc(x, s_i) <= d_gc(x, s_j)  for all s_j in S_c }

computed directly: the country's own territory is repartitioned among the
country's own seeds. No global Voronoi is built and then clipped.

Why this is exact
-----------------
Great-circle distance is monotone in the dot product, so for unit vectors
``p_i``, ``p_j`` the condition "closer to i than to j" is the linear half-space
``x . (p_i - p_j) >= 0``, whose boundary is a great circle.

Under a *gnomonic* (central-perspective) projection about a unit vector
``centre``, the sphere direction of a plane point ``(u, v)`` is proportional to
``centre + u*e1 + v*e2``. Substituting into the linear condition and dropping the
positive scale factor leaves ``A*u + B*v + D >= 0`` -- an exact planar half-plane.
So a spherical Voronoi cell is exactly a convex polygon in the gnomonic plane,
and great circles are exactly straight lines there. Nothing is linearised, and
Euclidean lon/lat distance is never used anywhere.

Two coordinate hazards, handled structurally rather than patched
---------------------------------------------------------------
*Antimeridian.* On the sphere +180 and -180 are the same meridian and nothing
happens there. Each country gets its own gnomonic frame centred on its own
territory, so the seam is not a boundary of the computation. It reappears only
when writing lon/lat rings, which ``antimeridian.py`` handles as a final step.

*Poles.* A pole is a single point that plate-carree must draw as an entire
parallel, and a polar outline traverses the +/-180 meridian twice. Both are format
artefacts, and both vanish under a rigid rotation moving the pole to (lon 0,
lat 0). Polar countries are therefore rotated first, tessellated as ordinary
mid-latitude blobs, and rotated back. The rotation is orthogonal and so
distortion-free -- verified on Antarctica, whose area round-trips to the km.
"""
from __future__ import annotations

import math
from dataclasses import dataclass

import numpy as np
import pandas as pd
from shapely.geometry import MultiPolygon, Polygon
from shapely.ops import unary_union

from . import antimeridian
from . import config as cfg
from .geometry_utils import (
    GnomonicFrame,
    angular_sep_rad,
    as_multipolygon,
    clean,
    clip_convex_halfplane,
    densify_ring_uv,
    geometry_touches_pole,
    lonlat_to_xyz,
    ring_max_radius,
    rotate_geometry,
    rotation_matrix_pole_to_equator,
    spherical_area_km2,
    spherical_centroid,
    xyz_to_lonlat,
)

IDENTITY = np.eye(3)


@dataclass
class CountryJob:
    country_code: str
    geometry: MultiPolygon
    seeds: pd.DataFrame


@dataclass
class _Cluster:
    """A group of country components sharing one gnomonic frame."""

    polys: list
    rot: np.ndarray  # rotation applied before projecting; identity for most


# --------------------------------------------------------------------------
def _make_clusters(geom: MultiPolygon) -> list[_Cluster]:
    """Split a country into groups that each fit inside one gnomonic frame.

    A gnomonic frame only spans the near hemisphere, so components more than
    ``MAX_COMPONENT_ANGULAR_RADIUS_DEG`` apart are separated. If any component
    touches a pole the whole country is rotated first, so no cluster ever
    contains one.
    """
    polys = list(geom.geoms) if geom.geom_type == "MultiPolygon" else [geom]
    polar = [p for p in polys if geometry_touches_pole(p)]

    if polar:
        southern = bool(
            np.mean([np.asarray(p.exterior.coords)[:, 1].mean() for p in polar]) < 0
        )
        rot = rotation_matrix_pole_to_equator(southern)
        # Rotate the whole country, not just the polar component, so nearby
        # islands still compete for territory in the same frame.
        rotated = rotate_geometry(MultiPolygon(polys), rot)
        if rotated is not None:
            return [_Cluster(grp, rot) for grp in _group_by_frame(list(rotated.geoms))]

    return [_Cluster(grp, IDENTITY) for grp in _group_by_frame(polys)]


def _group_by_frame(polys: list) -> list[list]:
    if not polys:
        return []
    reps = np.asarray(
        [
            lonlat_to_xyz(p.representative_point().x, p.representative_point().y).reshape(3)
            for p in polys
        ]
    )
    limit = math.radians(cfg.MAX_COMPONENT_ANGULAR_RADIUS_DEG) * 0.9
    groups: list[list[int]] = []
    centres: list[np.ndarray] = []
    for idx in np.argsort([-p.area for p in polys]):
        for k in range(len(groups)):
            members = groups[k] + [int(idx)]
            cand = spherical_centroid(reps[members])
            if float(np.max(angular_sep_rad(reps[members], cand[None, :]))) < limit:
                groups[k], centres[k] = members, cand
                break
        else:
            groups.append([int(idx)])
            centres.append(reps[int(idx)])
    return [[polys[i] for i in grp] for grp in groups]


def _cell_ring(
    frame: GnomonicFrame,
    seed_xyz: np.ndarray,
    i: int,
    start_ring: np.ndarray,
    order: np.ndarray,
) -> np.ndarray:
    """Convex cell of seed ``i`` within ``start_ring``, as a gnomonic ring.

    ``order`` lists the other seeds by increasing angular distance, which permits
    an exact early exit: once the cell lies within angular radius ``R`` of
    ``p_i``, a seed farther than ``2R`` cannot have a bisector reaching it, and
    nor can any seed beyond it.
    """
    ring = start_ring
    p_i = seed_xyz[i]
    for j in order:
        if len(ring) < 3:
            return np.empty((0, 2))
        d_ij = float(angular_sep_rad(p_i, seed_xyz[j]))
        if d_ij > 0.0 and d_ij > 2.0 * ring_max_radius(ring, frame, p_i):
            break
        a, b, d = frame.halfplane(p_i, seed_xyz[j])
        ring = clip_convex_halfplane(ring, a, b, d)
    return ring


def _true_meridian_halfplanes(frame: GnomonicFrame, rot: np.ndarray):
    """Half-plane coefficients for the two sides of the true +/-180 meridian.

    That meridian lies in the plane with normal (0, 1, 0) in true coordinates;
    under the pole rotation the normal becomes ``rot @ (0,1,0)``. Cutting a polar
    cell on it, in the gnomonic plane where great circles are straight lines,
    leaves pieces that each sit in one longitude hemisphere and none of which
    encloses the pole -- so rotating back and writing lon/lat rings is lossless.
    Without this cut the unwrap-and-split step shaved a wedge off the South Pole,
    measured at 209,000 km2 (1.7% of Antarctica) before the fix.
    """
    n = rot @ np.array([0.0, 1.0, 0.0])
    return [
        (float(frame.e1 @ n), float(frame.e2 @ n), float(frame.centre @ n)),
        (-float(frame.e1 @ n), -float(frame.e2 @ n), -float(frame.centre @ n)),
    ]


def _split_on_true_meridian(geom_uv, frame: GnomonicFrame, rot, rotated: bool):
    """Cut a rotated cell on the true +/-180 meridian; pass others through."""
    if not rotated:
        return [geom_uv]
    minu, minv, maxu, maxv = geom_uv.bounds
    span = 10.0 * max(maxu - minu, maxv - minv, 1.0)
    big = np.array(
        [[minu - span, minv - span], [maxu + span, minv - span],
         [maxu + span, maxv + span], [minu - span, maxv + span]]
    )
    out = []
    for a, b, d in _true_meridian_halfplanes(frame, rot):
        ring = clip_convex_halfplane(big, a, b, d)
        if len(ring) < 3:
            continue
        part = clean(Polygon(ring).intersection(geom_uv))
        if not part.is_empty:
            out.append(part)
    return out or [geom_uv]


def _project(polys: list, frame: GnomonicFrame):
    out = []
    for p in polys:
        try:
            shell = frame.forward(lonlat_to_xyz(*_xy(p.exterior.coords)))
            holes = [frame.forward(lonlat_to_xyz(*_xy(r.coords))) for r in p.interiors]
        except ValueError:
            continue
        q = clean(Polygon(shell, holes))
        if q.is_empty:
            continue
        # clean() can split a self-touching ring into a MultiPolygon, so flatten
        # before collecting -- MultiPolygon() rejects nested multi-parts.
        out.extend(as_multipolygon(q).geoms)
    if not out:
        return None
    return out[0] if len(out) == 1 else clean(unary_union(out))


def _xy(coords):
    arr = np.asarray(coords)
    return arr[:, 0], arr[:, 1]


def _unproject(geom, frame: GnomonicFrame):
    def _ring(coords):
        lon, lat = xyz_to_lonlat(frame.inverse(np.asarray(coords)))
        return list(zip(np.atleast_1d(lon).tolist(), np.atleast_1d(lat).tolist()))

    polys = []
    for p in [geom] if geom.geom_type == "Polygon" else list(geom.geoms):
        if p.geom_type != "Polygon" or p.is_empty:
            continue
        try:
            q = Polygon(_ring(p.exterior.coords), [_ring(r.coords) for r in p.interiors])
        except Exception:
            continue
        if not q.is_empty:
            polys.append(q)
    return MultiPolygon(polys) if polys else None


# --------------------------------------------------------------------------
def build_country(job: CountryJob) -> tuple[list[dict], dict]:
    """Tessellate one partition -- a whole country, or one ADM1 unit."""
    cc = job.country_code
    seeds = job.seeds.drop_duplicates(subset=["metro_id"], keep="first").reset_index(drop=True)
    n_dropped = len(job.seeds) - len(seeds)
    n_seeds = len(seeds)
    country_area = spherical_area_km2(job.geometry)

    diag = {
        "country_code": cc,
        "seed_count": n_seeds,
        "duplicate_metro_ids_dropped": n_dropped,
        "country_area_km2": country_area,
        "n_clusters": 0,
        "rotated_for_pole": False,
        "number_of_output_pieces": 0,
        "number_of_metros": 0,
        "sum_cell_area_km2": 0.0,
        "area_sum_residual_km2": 0.0,
        "max_cell_area_km2": 0.0,
        "geometry_valid": True,
        "case": "",
    }
    if n_seeds == 0:
        diag["case"] = "no_seeds"
        return [], diag

    diag["case"] = (
        "single_seed" if n_seeds == 1
        else "two_seeds_bisector" if n_seeds == 2
        else "halfspace_intersection"
    )

    seed_xyz_true = lonlat_to_xyz(seeds.seed_lon.to_numpy(), seeds.seed_lat.to_numpy())
    clusters = _make_clusters(job.geometry)
    diag["n_clusters"] = len(clusters)
    diag["rotated_for_pole"] = any(not np.allclose(c.rot, IDENTITY) for c in clusters)

    per_metro: dict[str, list] = {}
    for cluster in clusters:
        rot = cluster.rot
        rotated = not np.allclose(rot, IDENTITY)
        seed_xyz = seed_xyz_true @ rot.T if rotated else seed_xyz_true

        verts = np.vstack([np.asarray(p.exterior.coords) for p in cluster.polys])
        vert_xyz = lonlat_to_xyz(verts[:, 0], verts[:, 1])
        centre = spherical_centroid(vert_xyz)
        radius = float(np.max(angular_sep_rad(vert_xyz, centre[None, :])))
        if radius >= math.radians(cfg.MAX_COMPONENT_ANGULAR_RADIUS_DEG):
            raise RuntimeError(
                f"{cc}: cluster angular radius {math.degrees(radius):.1f} deg exceeds the "
                "gnomonic limit; lower MAX_COMPONENT_ANGULAR_RADIUS_DEG to split further"
            )
        frame = GnomonicFrame.about(centre)

        cluster_uv = _project(cluster.polys, frame)
        if cluster_uv is None or cluster_uv.is_empty:
            continue

        # Case A of the plan: one seed takes the whole territory. Routed through
        # the same machinery so a polar single-seed country is rotated too.
        if n_seeds == 1:
            for sub_uv in _split_on_true_meridian(cluster_uv, frame, rot, rotated):
                piece = _unproject(sub_uv, frame)
                if piece is not None:
                    _stash(per_metro, seeds.iloc[0].metro_id, piece, rot, rotated)
            continue

        minu, minv, maxu, maxv = cluster_uv.bounds
        pad = 0.05 * max(maxu - minu, maxv - minv, 1e-6)
        start_ring = np.array(
            [
                [minu - pad, minv - pad],
                [maxu + pad, minv - pad],
                [maxu + pad, maxv + pad],
                [minu - pad, maxv + pad],
            ]
        )
        sep = angular_sep_rad(seed_xyz[:, None, :], seed_xyz[None, :, :])
        for i in range(n_seeds):
            order = np.argsort(sep[i])
            order = order[order != i]
            ring = _cell_ring(frame, seed_xyz, i, start_ring, order)
            if len(ring) < 3:
                continue
            cell_uv = clean(Polygon(densify_ring_uv(ring, frame, cfg.CELL_DENSIFY_DEG)))
            if cell_uv.is_empty:
                continue
            piece_uv = clean(cell_uv.intersection(cluster_uv))
            if piece_uv.is_empty:
                continue
            for sub_uv in _split_on_true_meridian(piece_uv, frame, rot, rotated):
                piece = _unproject(sub_uv, frame)
                if piece is None or piece.is_empty:
                    continue
                _stash(per_metro, seeds.iloc[i].metro_id, piece, rot, rotated)

    records: list[dict] = []
    total = 0.0
    for _, row in seeds.iterrows():
        parts = [p for p in per_metro.get(row.metro_id, []) if not p.is_empty]
        if not parts:
            continue
        mp = MultiPolygon(parts)
        if not mp.is_valid:
            repaired = as_multipolygon(clean(mp))
            if repaired is not None and not repaired.is_empty:
                mp = repaired
        area = spherical_area_km2(mp)
        if area < cfg.MIN_PIECE_AREA_KM2:
            continue
        total += area
        diag["max_cell_area_km2"] = max(diag["max_cell_area_km2"], area)
        records.append(_record(row, cc, mp, n_seeds))

    diag["number_of_metros"] = len(records)
    diag["number_of_output_pieces"] = sum(len(r["geometry"].geoms) for r in records)
    diag["sum_cell_area_km2"] = total
    # Indicative only: a ring cut exactly on the +/-180 meridian has an edge with
    # dlon = +/-180, whose sign is ambiguous, so this residual can be nonzero on an
    # exact tessellation. validate_tessellation computes the authoritative figures
    # via set operations, which is what plan sections 18.6 and 23 specify.
    diag["area_sum_residual_km2"] = total - country_area
    diag["geometry_valid"] = all(r["geometry"].is_valid for r in records)
    return records, diag


def _stash(store: dict, metro_id: str, piece: MultiPolygon, rot, rotated: bool) -> None:
    """Rotate back if needed, then file the pieces.

    Rotated (polar) pieces have already been cut on the true +/-180 meridian by
    :func:`_split_on_true_meridian`, so they cannot cross the seam and must NOT go
    through the unwrap-and-split path. Their rings touch the pole, where longitude
    is degenerate, so a step of exactly +/-180 appears between consecutive
    vertices; ``((dlon + 180) % 360) - 180`` maps +180 to -180, the unwrap then
    walks the wrong way round, and the piece is destroyed. That is what removed
    the entire negative-longitude half of the South Polar region (69 of 4,000
    sampled points, ~209,000 km2) in the previous build.
    """
    if rotated:
        piece = rotate_geometry(piece, rot.T)
        if piece is None:
            return
        store.setdefault(metro_id, []).extend(
            g for g in as_multipolygon(piece).geoms if not g.is_empty
        )
        return
    norm = antimeridian.normalize(piece)
    if norm is None or norm.is_empty:
        return
    store.setdefault(metro_id, []).extend(g for g in norm.geoms if not g.is_empty)


def _record(row, cc: str, mp: MultiPolygon, n_seeds: int) -> dict:
    return {
        "metro_id": row.metro_id,
        "metro": row.metro,
        "city": row.city,
        "state_resolved": row.state_resolved,
        "state_iso2": row.state_iso2,
        "country_code": cc,
        "seed_lat": float(row.seed_lat),
        "seed_lon": float(row.seed_lon),
        "seed_pop_max": int(getattr(row, "ne_pop_max", 0) or 0),
        "legacy_cell_population": getattr(row, "legacy_cell_population", None),
        "seed_source": getattr(row, "seed_source", "") or "",
        "source_seed_count_country": int(n_seeds),
        "geometry": mp,
    }


_EMPTY_DIAG = {
    "n_state_partitions": 0,
    "seeds_in_state_tier": 0,
    "seeds_country_tier_only": 0,
    "residual_area_km2": 0.0,
    "seed_count": 0,
    "duplicate_metro_ids_dropped": 0,
    "country_area_km2": 0.0,
    "n_clusters": 0,
    "rotated_for_pole": False,
    "number_of_output_pieces": 0,
    "number_of_metros": 0,
    "sum_cell_area_km2": 0.0,
    "area_sum_residual_km2": 0.0,
    "max_cell_area_km2": 0.0,
    "geometry_valid": True,
}


def build_all(seeds: pd.DataFrame, boundaries, states=None) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Tessellate every country, hierarchically.

    Two tiers, so that state consistency holds wherever the data supports it:

    * **state tier** -- every ADM1 unit that has at least one seed is
      repartitioned among *its own* seeds. A cell built here cannot cross a state
      border, so a coordinate in that state can only ever receive a metro in that
      state.
    * **country tier** -- whatever territory is left (ADM1 units with no seeds,
      and any land the ADM1 layer does not cover) is repartitioned among *all* the
      country's seeds. A state with no metros cannot be kept internally
      consistent, so those points fall back to the nearest in-country metro and
      the tier is recorded rather than hidden.

    The two tiers are disjoint and together tile the country exactly, so the
    country-level invariant is unchanged: no cell ever leaves its country.
    """
    bmap = {r.country_code: r.geometry for _, r in boundaries.iterrows()}
    states_by_cc: dict[str, list] = {}
    if states is not None:
        for cc, grp in states.groupby("country_code"):
            states_by_cc[cc] = list(grp.itertuples())

    records: list[dict] = []
    diags: list[dict] = []

    for cc, grp in seeds.groupby("country_code"):
        geom = bmap.get(cc)
        if geom is None:
            diags.append({**_EMPTY_DIAG, "country_code": cc, "seed_count": len(grp),
                          "case": "no_boundary"})
            continue

        country_geom = as_multipolygon(geom)
        polar = geometry_touches_pole(country_geom)
        state_rows = states_by_cc.get(cc, [])
        has_state_col = "state_code" in grp.columns

        used_state_geoms = []
        n_state_partitions = 0
        n_state_seeds = 0

        # ---- state tier
        if state_rows and has_state_col and not polar:
            by_code = {st.state_code: st for st in state_rows}
            for sc, sgrp in grp.groupby("state_code", dropna=True):
                st = by_code.get(sc)
                if st is None or sgrp.empty:
                    continue
                sgeom = as_multipolygon(st.geometry)
                if sgeom is None or sgeom.is_empty:
                    continue
                recs, sdiag = build_country(CountryJob(cc, sgeom, sgrp))
                for r in recs:
                    r["state_code"] = sc
                    r["state_name"] = st.state_name
                    r["partition_tier"] = "state"
                records.extend(recs)
                used_state_geoms.append(st.geometry)
                n_state_partitions += 1
                n_state_seeds += len(sgrp)

        # ---- country tier over whatever the state tier did not claim
        residual = country_geom
        if used_state_geoms:
            try:
                residual = as_multipolygon(
                    clean(country_geom.difference(clean(unary_union(used_state_geoms))))
                )
            except Exception:
                residual = country_geom
                used_state_geoms = []
        if residual is not None and not residual.is_empty:
            recs, cdiag = build_country(CountryJob(cc, residual, grp))
            for r in recs:
                r["state_code"] = None
                r["state_name"] = None
                r["partition_tier"] = "country"
            records.extend(recs)
        else:
            cdiag = {**_EMPTY_DIAG, "country_code": cc, "case": "fully_state_partitioned"}

        diags.append(
            {
                **_EMPTY_DIAG,
                "country_code": cc,
                "seed_count": len(grp),
                "case": cdiag.get("case", ""),
                "country_area_km2": spherical_area_km2(country_geom),
                "rotated_for_pole": polar,
                "n_state_partitions": n_state_partitions,
                "seeds_in_state_tier": n_state_seeds,
                "seeds_country_tier_only": len(grp) - n_state_seeds,
                "residual_area_km2": spherical_area_km2(residual) if residual is not None else 0.0,
                "number_of_metros": sum(
                    1 for r in records if r["country_code"] == cc
                ),
            }
        )

    seeded = set(seeds.country_code)
    for cc, geom in bmap.items():
        if cc in seeded:
            continue
        diags.append({**_EMPTY_DIAG, "country_code": cc, "case": "no_seeds",
                      "country_area_km2": spherical_area_km2(geom)})

    df = pd.DataFrame(records)
    for col in ("state_code", "state_name", "partition_tier"):
        if col not in df.columns:
            df[col] = None
    return df, pd.DataFrame(diags)

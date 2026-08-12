"""Stage 02b -- ADM1 (state / province / region) boundaries.

The country tier alone still lets a coordinate in one state be handed a metro in
the neighbouring state, whenever that seed happens to be nearer. This adds the
finer partition: within a country, each ADM1 unit that has at least one seed is
repartitioned among *its own* seeds.

Source: Natural Earth 10m ``admin_1_states_provinces`` (4,596 units, 251
countries, ISO 3166-2 present on every row). Same provenance as the seeds, so
ADM1 identity lines up with the ``state_resolved`` already in the metro key.

A seed's state is decided by **geometric containment**, not by name matching.
Matching ``ADM1NAME`` against ``admin_1.name`` only agrees for 90.9% of seeds --
the shortfall is spelling and vintage drift, not real disagreement -- so names
are used to corroborate and report, never to decide.
"""
from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import Point
from shapely.ops import unary_union
from shapely.strtree import STRtree

from . import config as cfg
from .geometry_utils import as_multipolygon, clean, densify_geometry, spherical_area_km2
from .normalize_country_codes import load_country_code_overrides

S02B_STATES = cfg.DATA_STAGES / "02b_state_boundaries_normalized.parquet"


def _cc_from_row(row, overrides: dict[str, str]) -> str | None:
    """Country code for an ADM1 unit.

    The ISO 3166-2 code carries its own country prefix (``AR-E`` -> ``AR``),
    which is the most reliable signal available on this layer.
    """
    iso = row.get("iso_3166_2")
    if iso and isinstance(iso, str) and "-" in iso:
        cc = iso.split("-", 1)[0].strip()
        if len(cc) == 2:
            return overrides.get(cc, cc)
    from .normalize_country_codes import NE_A3_TO_CC

    a3 = str(row.get("adm0_a3") or "").strip()
    if a3 in NE_A3_TO_CC:
        cc = NE_A3_TO_CC[a3]
        return overrides.get(cc, cc)
    return None


def build_states(country_boundaries: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """One row per ADM1 unit, clipped to its country's land geometry."""
    path = cfg.DATA_RAW / "ne_10m_admin_1_states_provinces.zip"
    if not path.exists():
        import requests

        url = f"{cfg.NE_BASE}/cultural/ne_10m_admin_1_states_provinces.zip"
        resp = requests.get(url, timeout=600)
        resp.raise_for_status()
        path.write_bytes(resp.content)

    a1 = gpd.read_file(f"zip://{path}")
    overrides = load_country_code_overrides()
    a1 = a1.copy()
    a1["country_code"] = [_cc_from_row(r, overrides) for _, r in a1.iterrows()]
    a1 = a1[a1.country_code.notna()]

    cmap = {r.country_code: r.geometry for _, r in country_boundaries.iterrows()}
    rows = []
    for (cc, code), grp in a1.groupby(["country_code", "iso_3166_2"]):
        cgeom = cmap.get(cc)
        if cgeom is None:
            continue
        geom = clean(unary_union(list(grp.geometry)))
        if geom is None or geom.is_empty:
            continue
        # Clip to the country's own land so the state tier can never introduce
        # territory the country tier does not have.
        try:
            # as_multipolygon drops the LineString/Point debris a polygon-polygon
            # intersection leaves along shared borders.
            geom = as_multipolygon(clean(geom.intersection(cgeom)))
        except Exception:
            continue
        if geom is None or geom.is_empty:
            continue
        rows.append(
            {
                "country_code": cc,
                "state_code": code,
                "state_name": str(grp.iloc[0].get("name") or ""),
                "area_km2": spherical_area_km2(geom),
                "geometry": clean(densify_geometry(geom, cfg.BOUNDARY_DENSIFY_DEG)),
            }
        )
    out = gpd.GeoDataFrame(rows, geometry="geometry", crs="EPSG:4326")
    out = out.sort_values(["country_code", "state_code"]).reset_index(drop=True)
    return _make_states_disjoint(out)


def _make_states_disjoint(states: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Ensure a country's ADM1 units do not overlap each other.

    Natural Earth's admin_1 layer overlaps itself where territory is disputed --
    Pakistan's Azad Kashmir and Gilgit-Baltistan against neighbouring claims being
    the largest case, 707 km2. Overlapping partitions would produce overlapping
    cells, so shared area is assigned to the lexicographically first state_code.
    Deterministic and reported, rather than resolved by whichever row came first.
    """
    keep, removed = [], []
    for cc, grp in states.groupby("country_code"):
        claimed = None
        for r in grp.sort_values("state_code").itertuples():
            geom = r.geometry
            if claimed is not None:
                try:
                    trimmed = as_multipolygon(clean(geom.difference(claimed)))
                except Exception:
                    trimmed = as_multipolygon(geom)
                if trimmed is None or trimmed.is_empty:
                    removed.append({"country_code": cc, "state_code": r.state_code,
                                    "reason": "fully contained in an earlier state"})
                    continue
                lost = spherical_area_km2(geom) - spherical_area_km2(trimmed)
                if lost > 1.0:
                    removed.append({"country_code": cc, "state_code": r.state_code,
                                    "overlap_km2": round(lost, 3)})
                geom = trimmed
            claimed = geom if claimed is None else clean(unary_union([claimed, geom]))
            keep.append({"country_code": cc, "state_code": r.state_code,
                         "state_name": r.state_name,
                         "area_km2": spherical_area_km2(geom), "geometry": geom})
    if removed:
        pd.DataFrame(removed).to_csv(cfg.REPORTS / "state_overlaps_resolved.csv", index=False)
    return gpd.GeoDataFrame(keep, geometry="geometry", crs="EPSG:4326").reset_index(drop=True)


def assign_seeds_to_states(seeds: pd.DataFrame, states: gpd.GeoDataFrame) -> tuple[pd.DataFrame, dict]:
    """Attach ``state_code`` to each seed by containment within its own country.

    Seeds that fall in no state polygon (coastline mismatch, or a country with no
    ADM1 coverage) are left unassigned and simply compete at the country tier.
    """
    by_cc: dict[str, tuple[STRtree, list[int]]] = {}
    for cc, grp in states.groupby("country_code"):
        idx = list(grp.index)
        by_cc[cc] = (STRtree([states.geometry.loc[i] for i in idx]), idx)

    codes: list[str | None] = []
    name_agrees = 0
    name_differs: list[dict] = []
    unassigned = 0
    for s in seeds.itertuples():
        entry = by_cc.get(s.country_code)
        if entry is None:
            codes.append(None)
            unassigned += 1
            continue
        tree, idx = entry
        pt = Point(s.seed_lon, s.seed_lat)
        hit = [i for i in (idx[k] for k in tree.query(pt)) if states.geometry.loc[i].covers(pt)]
        if not hit:
            codes.append(None)
            unassigned += 1
            continue
        i = hit[0]
        codes.append(states.state_code.loc[i])
        ne_name = getattr(s, "state_resolved", None)
        if ne_name and str(states.state_name.loc[i]) == str(ne_name):
            name_agrees += 1
        elif ne_name:
            if len(name_differs) < 40:
                name_differs.append(
                    {
                        "metro": s.metro,
                        "ne_adm1name": str(ne_name),
                        "containing_state": str(states.state_name.loc[i]),
                        "state_code": states.state_code.loc[i],
                    }
                )

    out = seeds.copy()
    out["state_code"] = codes

    # The containing ADM1 unit is also the better *name* authority. Natural
    # Earth's populated-places layer ships mojibake in ADM1NAME -- "MUdenine" for
    # Médenine, "Kasssrine" for Kassérine, "Vi?n Bi" for Điện Biên -- and those
    # corrupted strings are in the live metro keys today. The admin_1 layer is
    # clean and carries ISO 3166-2, so state_resolved and state_iso2 are taken
    # from the polygon the seed actually sits in. Seeds outside every state
    # polygon keep whatever they had.
    from .load_seeds import metro_id_for, metro_string

    name_by_code = {r.state_code: r.state_name for r in states.itertuples()}
    renamed = 0
    new_resolved, new_iso2 = [], []
    for prev_name, prev_iso2, sc in zip(out.state_resolved, out.state_iso2, out.state_code):
        if sc and sc in name_by_code:
            nm = name_by_code[sc]
            iso2 = sc.split("-", 1)[1] if "-" in sc else prev_iso2
            if str(nm) != str(prev_name):
                renamed += 1
            new_resolved.append(nm)
            new_iso2.append(iso2)
        else:
            new_resolved.append(prev_name)
            new_iso2.append(prev_iso2)
    out["state_resolved"] = new_resolved
    out["state_iso2"] = new_iso2
    out["metro"] = [
        metro_string(r.city, r.state_resolved, r.state_iso2, r.country_code)
        for r in out.itertuples()
    ]
    out["metro_id"] = [
        metro_id_for(r.city, r.state_resolved, r.state_iso2, r.country_code)
        for r in out.itertuples()
    ]

    report = {
        "seeds": int(len(seeds)),
        "assigned_to_state": int(len(seeds) - unassigned),
        "unassigned_compete_at_country_tier": int(unassigned),
        "ne_adm1name_agrees_with_containing_state": int(name_agrees),
        "state_resolved_taken_from_admin1": int(renamed),
        "ne_adm1name_differs_examples": name_differs,
        "states_total": int(len(states)),
        "states_with_seeds": int(out.state_code.nunique(dropna=True)),
        "metro_id_collisions_after_rename": int(len(out) - out.metro_id.nunique()),
    }
    return out, report


def save(gdf: gpd.GeoDataFrame) -> Path:
    gdf.to_parquet(S02B_STATES, index=False)
    return S02B_STATES


def load() -> gpd.GeoDataFrame:
    return gpd.read_parquet(S02B_STATES)


def main() -> None:
    from . import load_country_boundaries as lb

    states = build_states(lb.load())
    save(states)
    print(f"states: {len(states)} ADM1 units across {states.country_code.nunique()} countries")
    print(f"  -> {S02B_STATES}")


if __name__ == "__main__":
    main()

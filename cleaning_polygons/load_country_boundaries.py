"""Stage 02 -- load and normalise country/territory boundaries.

Natural Earth ``admin_0_map_units`` is the primary source rather than
``admin_0_countries`` because the latter folds the French overseas departments
(GF, GP, MQ, RE, YT) into FR and Svalbard (SJ) into NO, while HERMES/IPInfo
geolocation reports those as distinct country codes. Verified 2026-08-10:
map_units carries all six separately, admin_0_countries carries none of them.
"""
from __future__ import annotations

import io
import zipfile
from pathlib import Path

import geopandas as gpd
import pandas as pd
import requests
from shapely.ops import unary_union

from . import config as cfg
from .geometry_utils import clean, densify_geometry, spherical_area_km2
from .normalize_country_codes import (
    NE_SOVEREIGN_TO_CC,
    boundary_cc_from_row,
    load_country_code_overrides,
)


def download_natural_earth(force: bool = False) -> dict[str, Path]:
    """Fetch the Natural Earth archives into ``data/raw``."""
    paths: dict[str, Path] = {}
    for name, url in cfg.NE_FILES.items():
        dest = cfg.DATA_RAW / f"ne_10m_{name}.zip"
        paths[name] = dest
        if dest.exists() and not force:
            continue
        resp = requests.get(url, timeout=300)
        resp.raise_for_status()
        # Fail loudly on a truncated or HTML error body rather than at read time.
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            bad = zf.testzip()
            if bad is not None:
                raise OSError(f"corrupt archive from {url}: {bad}")
        dest.write_bytes(resp.content)
    return paths


def _read(path: Path) -> gpd.GeoDataFrame:
    return gpd.read_file(f"zip://{path}")


def build_boundaries(force_download: bool = False) -> gpd.GeoDataFrame:
    """Return one row per country/territory code with a cleaned geometry."""
    paths = download_natural_earth(force=force_download)
    overrides = load_country_code_overrides()

    frames = []
    for key, source_rank in (("admin_0_map_units", 0), ("admin_0_countries", 1)):
        gdf = _read(paths[key])
        gdf = gdf.copy()
        gdf["boundary_cc"] = [boundary_cc_from_row(r, overrides) for _, r in gdf.iterrows()]
        gdf["source_layer"] = key
        gdf["source_rank"] = source_rank
        gdf["ne_name"] = gdf.get("NAME_EN", gdf.get("NAME"))
        frames.append(gdf[["boundary_cc", "ne_name", "source_layer", "source_rank", "geometry"]])

    allrows = pd.concat(frames, ignore_index=True)
    allrows = allrows[allrows.boundary_cc.notna() & (allrows.boundary_cc != "")]

    # map_units wins; admin_0_countries only supplies codes map_units lacks.
    have = set(allrows.loc[allrows.source_rank == 0, "boundary_cc"])
    keep = allrows[(allrows.source_rank == 0) | (~allrows.boundary_cc.isin(have))]

    records = []
    for cc, grp in keep.groupby("boundary_cc"):
        geom = clean(unary_union(list(grp.geometry)))
        if geom is None or geom.is_empty:
            continue
        # No pole clipping: build_country_voronoi rotates polar countries so a
        # pole is an ordinary interior point, which needs no territory excised.
        dens = clean(densify_geometry(geom, cfg.BOUNDARY_DENSIFY_DEG))
        records.append(
            {
                "country_code": cc,
                "ne_name": sorted({str(n) for n in grp.ne_name if n})[:1] or [""],
                "source_layer": ",".join(sorted(set(grp.source_layer))),
                "n_components": len(list(dens.geoms)) if dens.geom_type == "MultiPolygon" else 1,
                "area_km2": spherical_area_km2(dens),
                "geometry": dens,
            }
        )
    out = gpd.GeoDataFrame(records, geometry="geometry", crs="EPSG:4326")
    out["ne_name"] = out["ne_name"].apply(lambda v: v[0] if isinstance(v, list) else v)
    return out.sort_values("country_code").reset_index(drop=True)


def save(gdf: gpd.GeoDataFrame) -> Path:
    gdf.to_parquet(cfg.S02_BOUNDARIES, index=False)
    return cfg.S02_BOUNDARIES


def load() -> gpd.GeoDataFrame:
    return gpd.read_parquet(cfg.S02_BOUNDARIES)


def main() -> None:
    gdf = build_boundaries()
    save(gdf)
    print(f"boundaries: {len(gdf)} country codes -> {cfg.S02_BOUNDARIES}")
    print(f"  total land area {gdf.area_km2.sum():,.0f} km2")
    print(f"  sovereign fallbacks available for {len(NE_SOVEREIGN_TO_CC)} NE sovereignty codes")


if __name__ == "__main__":
    main()

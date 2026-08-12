"""Stage 07 -- emit ``metro_polygons_v2`` to BigQuery.

Geometry is written as ordinary positive polygons. Nothing downstream needs
``NOT ST_CONTAINS`` any more: the lookup is ``ST_COVERS(polygon, point)``.
``ST_COVERS`` rather than ``ST_CONTAINS`` because Voronoi cells share their
boundaries, and a point lying exactly on one should still match.
"""
from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import pandas as pd

from . import config as cfg
from .geometry_utils import spherical_area_km2

SCHEMA_FIELDS = [
    ("metro_id", "STRING"),
    ("metro", "STRING"),
    ("city", "STRING"),
    ("state_resolved", "STRING"),
    ("state_iso2", "STRING"),
    ("country_code", "STRING"),
    ("state_code", "STRING"),
    ("state_name", "STRING"),
    ("partition_tier", "STRING"),
    ("seed_lat", "FLOAT64"),
    ("seed_lon", "FLOAT64"),
    ("polygon", "GEOGRAPHY"),
    ("seed_pop_max", "INT64"),
    ("cell_population", "INT64"),
    ("legacy_cell_population", "INT64"),
    ("piece_id", "INT64"),
    ("geometry_area_km2", "FLOAT64"),
    ("source_seed_count_country", "INT64"),
    ("seed_source", "STRING"),
    ("build_version", "STRING"),
]


def save_cells(cells: pd.DataFrame) -> Path:
    gdf = gpd.GeoDataFrame(cells, geometry="geometry", crs="EPSG:4326")
    gdf.to_parquet(cfg.S05_CELLS_NORM, index=False)
    return cfg.S05_CELLS_NORM


def load_cells() -> gpd.GeoDataFrame:
    return gpd.read_parquet(cfg.S05_CELLS_NORM)


def to_export_frame(cells: pd.DataFrame, one_row_per_piece: bool = False) -> pd.DataFrame:
    """Build the export frame.

    Default is one MULTIPOLYGON row per metro, which keeps the lookup a single
    ``ST_COVERS``. ``one_row_per_piece`` splits disconnected pieces into their own
    rows (still one ``metro_id``), which the plan allows and which is useful if a
    consumer wants per-piece areas.
    """
    rows = []
    for r in cells.itertuples():
        geoms = list(r.geometry.geoms) if one_row_per_piece else [r.geometry]
        for k, g in enumerate(geoms):
            rows.append(
                {
                    "metro_id": r.metro_id,
                    "metro": r.metro,
                    "city": r.city,
                    "state_resolved": r.state_resolved,
                    "state_iso2": r.state_iso2,
                    "country_code": r.country_code,
                    "state_code": getattr(r, "state_code", None),
                    "state_name": getattr(r, "state_name", None),
                    "partition_tier": getattr(r, "partition_tier", "country"),
                    "seed_lat": float(r.seed_lat),
                    "seed_lon": float(r.seed_lon),
                    "wkt": g.wkt,
                    "seed_pop_max": int(getattr(r, "seed_pop_max", 0) or 0),
                    "legacy_cell_population": _int_or_none(
                        getattr(r, "legacy_cell_population", None)
                    ),
                    "piece_id": k,
                    "geometry_area_km2": float(spherical_area_km2(g)),
                    "source_seed_count_country": int(r.source_seed_count_country),
                    "seed_source": getattr(r, "seed_source", "") or "",
                    "build_version": cfg.BUILD_VERSION,
                }
            )
    return pd.DataFrame(rows)


def _int_or_none(v):
    try:
        if v is None or pd.isna(v):
            return None
        return int(v)
    except (TypeError, ValueError):
        return None


def upload(export: pd.DataFrame, table: str | None = None, replace: bool = True) -> str:
    """Load the frame into BigQuery, converting WKT to GEOGRAPHY server-side.

    The staging hop exists because the Python client cannot serialise a
    GEOGRAPHY column directly; WKT lands in a temp table and one ``CREATE OR
    REPLACE`` promotes it via ``ST_GEOGFROMTEXT``. ``make_valid => TRUE`` repairs
    residual ring-orientation noise; anything structurally wrong has already been
    caught by validate_tessellation.
    """
    from google.cloud import bigquery

    table = table or cfg.NEW_POLYGON_TABLE
    staging = f"{table}_staging"
    client = bigquery.Client(project=cfg.BQ_PROJECT)

    job = client.load_table_from_dataframe(
        export,
        staging,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            schema=[
                bigquery.SchemaField("metro_id", "STRING"),
                bigquery.SchemaField("metro", "STRING"),
                bigquery.SchemaField("city", "STRING"),
                bigquery.SchemaField("state_resolved", "STRING"),
                bigquery.SchemaField("state_iso2", "STRING"),
                bigquery.SchemaField("country_code", "STRING"),
                bigquery.SchemaField("state_code", "STRING"),
                bigquery.SchemaField("state_name", "STRING"),
                bigquery.SchemaField("partition_tier", "STRING"),
                bigquery.SchemaField("seed_lat", "FLOAT64"),
                bigquery.SchemaField("seed_lon", "FLOAT64"),
                bigquery.SchemaField("wkt", "STRING"),
                bigquery.SchemaField("seed_pop_max", "INT64"),
                bigquery.SchemaField("legacy_cell_population", "INT64"),
                bigquery.SchemaField("piece_id", "INT64"),
                bigquery.SchemaField("geometry_area_km2", "FLOAT64"),
                bigquery.SchemaField("source_seed_count_country", "INT64"),
                bigquery.SchemaField("seed_source", "STRING"),
                bigquery.SchemaField("build_version", "STRING"),
            ],
        ),
    )
    job.result()

    promote = f"""
    CREATE TABLE `{table}`
    CLUSTER BY country_code, state_code AS
    SELECT
      metro_id, metro, city, state_resolved, state_iso2, country_code,
      state_code, state_name, partition_tier,
      seed_lat, seed_lon,
      ST_GEOGFROMTEXT(wkt, make_valid => TRUE) AS polygon,
      seed_pop_max,
      CAST(NULL AS INT64) AS cell_population,
      legacy_cell_population,
      piece_id, geometry_area_km2, source_seed_count_country,
      seed_source, build_version
    FROM `{staging}`
    """
    # BigQuery refuses CREATE OR REPLACE when the clustering spec changes, so drop
    # first. Safe because the staging table already holds the full new contents.
    client.delete_table(table, not_found_ok=True)
    client.query(promote).result()
    client.delete_table(staging, not_found_ok=True)
    return table


def main() -> None:
    cells = load_cells()
    export = to_export_frame(cells)
    export.to_parquet(cfg.S07_EXPORT, index=False)
    print(f"export rows: {len(export)} -> {cfg.S07_EXPORT}")
    print(f"  wkt bytes total: {export.wkt.str.len().sum() / 2**20:.1f} MiB")
    print(f"  run `python -m cleaning_polygons.export_bigquery --upload` to load {cfg.NEW_POLYGON_TABLE}")


if __name__ == "__main__":
    import sys

    if "--upload" in sys.argv:
        cells = load_cells()
        export = to_export_frame(cells)
        table = upload(export)
        print(f"uploaded {len(export)} rows -> {table}")
    else:
        main()

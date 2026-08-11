"""Configuration for the country-constrained metro Voronoi build."""
from __future__ import annotations

import os
from pathlib import Path

# ---------------------------------------------------------------- paths
ROOT = Path(__file__).resolve().parent
DATA_RAW = ROOT / "data" / "raw"
DATA_STAGES = ROOT / "data" / "stages"
REPORTS = ROOT / "reports"
for _d in (DATA_RAW, DATA_STAGES, REPORTS):
    _d.mkdir(parents=True, exist_ok=True)

# Stage artifacts (§22 of the plan).
S01_SEEDS = DATA_STAGES / "01_seeds_clean.parquet"
S02_BOUNDARIES = DATA_STAGES / "02_country_boundaries_normalized.parquet"
S03_SEED_VALIDATION = REPORTS / "03_seed_validation.json"
S04_CELLS_RAW = DATA_STAGES / "04_country_cells_raw.parquet"
S05_CELLS_NORM = DATA_STAGES / "05_country_cells_normalized.parquet"
S06_VALIDATION = REPORTS / "06_validation_report.json"
S07_EXPORT = DATA_STAGES / "07_bigquery_export.parquet"
S08_HERMES_VALIDATION = REPORTS / "08_hermes_validation.json"
COUNTRY_DIAGNOSTICS = REPORTS / "country_diagnostics.csv"

# ---------------------------------------------------------------- inputs
# Natural Earth 10m. Verified 2026-08-10 to be the upstream source of
# `mlab-collaboration.hermes.metro_polygons_with_population`: 7,342 populated
# places vs 7,301 polygons, with NAME/ADM1NAME/ISO_A2 matching
# city/state_resolved/country_code exactly.
NE_BASE = "https://naciscdn.org/naturalearth/10m"
NE_FILES = {
    "populated_places": f"{NE_BASE}/cultural/ne_10m_populated_places.zip",
    "admin_0_countries": f"{NE_BASE}/cultural/ne_10m_admin_0_countries.zip",
    "admin_0_map_units": f"{NE_BASE}/cultural/ne_10m_admin_0_map_units.zip",
    "land": f"{NE_BASE}/physical/ne_10m_land.zip",
}

SEED_ADDITIONS_CSV = ROOT / "seed_additions.csv"
COUNTRY_CODE_OVERRIDES_CSV = ROOT / "country_code_overrides.csv"

# ---------------------------------------------------------------- BigQuery
BQ_PROJECT = os.environ.get("HERMES_BQ_PROJECT", "mlab-collaboration")
BQ_DATASET = os.environ.get("HERMES_BQ_DATASET", "hermes")
OLD_POLYGON_TABLE = f"{BQ_PROJECT}.{BQ_DATASET}.metro_polygons_with_population"
NEW_POLYGON_TABLE = f"{BQ_PROJECT}.{BQ_DATASET}.metro_polygons_v2"
IP_GEOLOC_TABLE = f"{BQ_PROJECT}.{BQ_DATASET}.unified_ip_to_geoloc"
WORLDPOP_TABLE = "bigquery-public-data.worldpop.population_grid_1km"

BUILD_VERSION = os.environ.get("METRO_BUILD_VERSION", "2026-08-v2")

# ---------------------------------------------------------------- geometry
# A gnomonic (central-perspective) projection maps great circles to straight
# lines *exactly*, which is what makes the spherical half-space intersection an
# exact planar half-plane intersection. It diverges past 90 deg from the centre,
# so each connected country component must fit inside this angular radius.
MAX_COMPONENT_ANGULAR_RADIUS_DEG = 75.0

# Country outlines are stored as plate-carree vertex lists but BigQuery reads
# them as geodesics. Densify long edges so the two interpretations agree.
BOUNDARY_DENSIFY_DEG = 0.25

# Cell boundaries are exact great circles; densify before emitting lon/lat so
# the WKT edges track the true geodesic.
CELL_DENSIFY_DEG = 0.25

# Drop slivers produced by floating-point intersection noise.
MIN_PIECE_AREA_KM2 = 1e-4

# ---------------------------------------------------------------- tolerances
# Build fails if any of these are exceeded (§23).
TOL = {
    # Per country: land not assigned to any metro, as a fraction of country area.
    "uncovered_area_frac": 1e-6,
    # Per country: sum(cell areas) - country area, as a fraction of country area.
    "overlap_area_frac": 1e-6,
    # Per cell: area lying outside its own country, as a fraction of cell area.
    "cross_country_area_frac": 1e-6,
    # Absolute floor. Cell edges are exact great circles, but they are *written*
    # as 0.25-degree polylines, so a cell can bulge or fall short of a coastline
    # by a sliver. Measured worst case across all 248 territories is NZ at
    # 0.6 km2 on 268,104 km2 (2e-6). 2 km2 covers that without masking a real
    # defect: the smallest tessellated territory is 4 km2 (Tokelau).
    "abs_area_km2": 2.0,
}

# Territories whose geometry cannot be verified with planar shapely operations.
# A cell touching a pole has no faithful plate-carree ring, so difference/union
# there measure format artefacts rather than real gaps. The tessellation itself is
# computed in a rotated frame where the pole is an ordinary interior point, and
# these countries are verified in BigQuery instead, whose ST_COVERS/ST_AREA are
# natively spherical (see validate_hermes_rows.verify_polar_countries). Listing a
# country here defers its geometric gate; it does not skip it.
SHAPELY_UNVERIFIABLE_COUNTRIES = {"AQ"}

# Offshore fallback radius for coordinates that miss every polygon (§16).
COASTAL_FALLBACK_KM = 100.0

# Validation sampling.
LAND_GRID_STEP_DEG = 0.25
NEAREST_SEED_SAMPLE_PER_COUNTRY = 40
RANDOM_SEED = 20260810

"""scripts/staging_validate_day.py must exercise the staging geolocation table.

The HOIHO A/B (step 04-07 with the frozen geolocation, then again after a HOIHO
refresh, on the same 02/03 output) is only meaningful if step 04 reads
``hermes_staging.geolocation`` -- the script used to fall through to the
production table via the loader's defaults.
"""

from __future__ import annotations

import datetime as dt
import importlib.util
from pathlib import Path

from hermes.pipeline import union

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "staging_validate_day.py"


def _load():
    spec = importlib.util.spec_from_file_location("staging_validate_day", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_params_route_geolocation_and_polygons_to_staging():
    params = _load().build_params(dt.date(2026, 11, 21))
    assert params["GEOLOCATION"] == union.geolocation_for(union.STAGING_DATASET)
    assert params["METRO_POLYGONS"] == union.metro_polygons_for(union.STAGING_DATASET)
    assert params["DS"] == union.STAGING_DATASET
    assert params["DAY"] == "2026-11-21"


def test_hoiho_stage_and_single_det_run_are_offered():
    parser = _load().build_parser()
    args = parser.parse_args(["--date", "2026-11-21", "--suffix", "x", "--stage", "hoiho"])
    assert args.stage == "hoiho"
    args = parser.parse_args(["--date", "2026-11-21", "--suffix", "x", "--det-runs", "1"])
    assert args.det_runs == 1
    assert parser.parse_args(["--date", "2026-11-21", "--suffix", "x"]).det_runs == 2

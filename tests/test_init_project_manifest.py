"""scripts/init_project.sh must create every table the pipeline steps touch.

A fresh project gets exactly the tables in ``scripts/schemas/tables.tsv``. A step
that starts reading a table missing from it would fail only for outside users,
never in M-Lab's deployment, so the gap would go unnoticed without this check.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

from hermes.sql import loader

ROOT = Path(__file__).resolve().parents[1]
SCHEMAS = ROOT / "scripts" / "schemas"

#: Created by bootstrap_tables.py rather than init_project.sh.
_BOOTSTRAP_VIEWS = {"hermes_union.events_enriched"}


def _manifest() -> list[list[str]]:
    lines = (SCHEMAS / "tables.tsv").read_text().splitlines()
    return [line.split("\t") for line in lines if line and not line.startswith("#")]


def _tables_read_by_steps() -> set[str]:
    """``dataset.table`` names in the numbered pipeline steps, comments stripped."""
    names: set[str] = set()
    for path in sorted((loader.paths.sql_root() / "queries").glob("0*.sql")):
        sql = loader.load_query(path.name, {"PROJECT": "P", "DS": "hermes_union"})
        sql = re.sub(r"--[^\n]*", "", sql)
        names |= {
            f"{ds}.{t}"
            for ds, t in re.findall(r"`(?:P\.)?(hermes_union|hermes|ix_data)\.(\w+)`", sql)
        }
    return names - {"hermes_union.INFORMATION_SCHEMA"}


def test_manifest_rows_are_well_formed():
    for row in _manifest():
        assert len(row) == 6, row
        ds, table, part_field, _clustering, require_filter, populated_by = row
        assert ds in {"hermes", "ix_data", "hermes_union"}
        assert require_filter in {"true", "false"}
        assert populated_by


def test_every_manifest_table_has_a_matching_schema():
    for ds, table, part_field, clustering, *_ in _manifest():
        fields = json.loads((SCHEMAS / f"{ds}.{table}.json").read_text())
        names = {f["name"] for f in fields}
        if part_field != "-":
            assert part_field in names, f"{ds}.{table}: no partition column {part_field}"
        if clustering != "-":
            assert set(clustering.split(",")) <= names, f"{ds}.{table}: bad clustering"


def test_no_orphan_schema_files():
    listed = {f"{ds}.{table}.json" for ds, table, *_ in _manifest()}
    assert {p.name for p in SCHEMAS.glob("*.json")} == listed


def test_manifest_covers_every_table_the_steps_touch():
    listed = {f"{ds}.{table}" for ds, table, *_ in _manifest()}
    missing = _tables_read_by_steps() - listed - _BOOTSTRAP_VIEWS
    assert missing == set(), f"add to scripts/schemas/tables.tsv: {sorted(missing)}"


def test_site_to_state_seed_matches_its_schema():
    header = (SCHEMAS / "data" / "hermes.site_to_state.csv").read_text().splitlines()[0]
    fields = json.loads((SCHEMAS / "hermes.site_to_state.json").read_text())
    assert header.split(",") == [f["name"] for f in fields]

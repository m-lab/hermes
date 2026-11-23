"""HERMES_PROJECT must retarget every query and table, and default to M-Lab's.

An outside evaluator runs the pipeline in their own Google Cloud project. A single
query that still names ``mlab-collaboration`` would fail there with a 403 -- or,
worse, read M-Lab's tables while writing theirs. ``PROJECT`` is resolved at
import, so the override cases run in a fresh interpreter.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from hermes import settings
from hermes.sql import loader

SRC = Path(__file__).resolve().parents[1] / "src"
QUERY_FILES = sorted(p.name for p in (loader.paths.sql_root() / "queries").glob("*.sql"))

#: Persistent-UDF install scripts, run by hand once per deployment, never by the
#: pipeline (which inlines its UDFs as temporary functions).
_LEGACY_UDF_INSTALLERS = {"mann_whitney_u_test.sql", "welchs_t_test.sql"}

_RENDER_ALL = """
import json
from hermes.pipeline import union
from hermes.sql import loader
from hermes.enrichment.utils.common import BaseEnrichment
names = sorted(p.name for p in (loader.paths.sql_root() / "queries").glob("*.sql"))
out = {n: loader.load_query(n, {"DAY": "2026-11-01"}) for n in names}
out["union.tables"] = repr([union.FINAL_OUTPUT_TABLE, union.GIGA_OUTPUT_TABLE,
                            union.DELETE_TABLES, union.metro_polygons_for("hermes_union")])
print(json.dumps(out))
"""


def _render_in(project: str | None) -> dict[str, str]:
    env = {k: v for k, v in os.environ.items() if k != "HERMES_PROJECT"}
    if project is not None:
        env["HERMES_PROJECT"] = project
    env["PYTHONPATH"] = str(SRC)
    done = subprocess.run(
        [sys.executable, "-c", _RENDER_ALL], env=env, capture_output=True, text=True, check=True
    )
    return json.loads(done.stdout)


def test_default_project_is_mlab_collaboration():
    assert settings.resolve_project({}) == "mlab-collaboration"
    assert settings.resolve_project({"HERMES_PROJECT": "  "}) == "mlab-collaboration"


def test_override_is_used():
    assert settings.resolve_project({"HERMES_PROJECT": "my-own-project"}) == "my-own-project"


@pytest.mark.parametrize("bad", ["bad`proj", "a b", "x", "proj;DROP"])
def test_malformed_override_rejected(bad):
    with pytest.raises(ValueError, match="HERMES_PROJECT"):
        settings.resolve_project({"HERMES_PROJECT": bad})


def test_no_query_file_hardcodes_the_project():
    root = loader.paths.sql_root()
    offenders = [
        p.relative_to(root).as_posix()
        for p in sorted(root.rglob("*.sql"))
        if p.name not in _LEGACY_UDF_INSTALLERS and "mlab-collaboration" in p.read_text()
    ]
    assert offenders == [], "use ${PROJECT} instead of the literal project id"


def test_no_python_module_hardcodes_the_project():
    offenders = [
        p.relative_to(SRC).as_posix()
        for p in sorted((SRC / "hermes").rglob("*.py"))
        if p.name != "settings.py"
        and any(
            "mlab-collaboration" in line and "help=" not in line
            for line in p.read_text().splitlines()
        )
    ]
    assert offenders == [], "derive the project from hermes.settings.PROJECT"


def test_override_retargets_everything():
    rendered = _render_in("example-proj")
    leaks = sorted(k for k, sql in rendered.items() if "mlab-collaboration" in sql)
    unsubstituted = sorted(k for k, sql in rendered.items() if "${PROJECT}" in sql)
    assert leaks == []
    assert unsubstituted == []
    assert "example-proj.hermes_union.events_explained_daily" in rendered["union.tables"]


def test_measurement_lab_sources_are_never_retargeted():
    rendered = _render_in("example-proj")
    assert "measurement-lab.ndt.ndt7_union" in rendered["01_merge_upload_download_union.sql"]


def test_default_render_names_mlab_collaboration():
    rendered = _render_in(None)
    assert "mlab-collaboration.hermes_union.events_explained_daily" in rendered["union.tables"]
    assert not any("${PROJECT}" in sql for sql in rendered.values())

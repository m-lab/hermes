"""Tests for IXP companion-repo path resolution.

The three paths the IXP snapshot generator needs were hardcoded to one laptop's
layout, so the refresh could only ever run on that machine -- IXP membership was
last refreshed 2026-06-08. Both collectors also advertised
``python_executable`` / ``output_dir`` arguments and then silently ignored them.
"""

from __future__ import annotations

import os

from hermes.enrichment.peeringdb_ixp.paths import (
    DEFAULT_IXP_PYTHON,
    DEFAULT_IXP_REPO,
    resolve_ixp_paths,
)

IXP_ENV = (
    "HERMES_IXP_REPO",
    "HERMES_IXP_WRAPPER",
    "HERMES_IXP_OUTPUT_DIR",
    "HERMES_IXP_PYTHON",
)


def _clear(monkeypatch):
    for name in IXP_ENV:
        monkeypatch.delenv(name, raising=False)


def test_defaults_match_the_historical_laptop_layout(monkeypatch):
    # Unchanged behaviour when nothing is configured, so existing runs still work.
    _clear(monkeypatch)
    wrapper, python_exe, out_dir = resolve_ixp_paths()
    assert wrapper == os.path.join(DEFAULT_IXP_REPO, "scripts", "wrapper.py")
    assert out_dir == os.path.join(DEFAULT_IXP_REPO, "scripts", "data")
    assert python_exe == DEFAULT_IXP_PYTHON


def test_repo_env_var_moves_wrapper_and_output_together(monkeypatch):
    _clear(monkeypatch)
    monkeypatch.setenv("HERMES_IXP_REPO", "/srv/mpl")
    wrapper, _, out_dir = resolve_ixp_paths()
    assert wrapper == os.path.join("/srv/mpl", "scripts", "wrapper.py")
    assert out_dir == os.path.join("/srv/mpl", "scripts", "data")


def test_individual_env_vars_override_the_derived_paths(monkeypatch):
    _clear(monkeypatch)
    monkeypatch.setenv("HERMES_IXP_REPO", "/srv/mpl")
    monkeypatch.setenv("HERMES_IXP_WRAPPER", "/opt/w.py")
    monkeypatch.setenv("HERMES_IXP_OUTPUT_DIR", "/data/snaps")
    monkeypatch.setenv("HERMES_IXP_PYTHON", "/usr/bin/python3")
    assert resolve_ixp_paths() == ("/opt/w.py", "/usr/bin/python3", "/data/snaps")


def test_explicit_arguments_beat_the_environment(monkeypatch):
    # The regression: these arguments existed on both collectors and were ignored.
    _clear(monkeypatch)
    monkeypatch.setenv("HERMES_IXP_WRAPPER", "/env/w.py")
    monkeypatch.setenv("HERMES_IXP_OUTPUT_DIR", "/env/out")
    monkeypatch.setenv("HERMES_IXP_PYTHON", "/env/py")
    assert resolve_ixp_paths(
        python_executable="/arg/py",
        output_dir="/arg/out",
        wrapper_script_path="/arg/w.py",
    ) == ("/arg/w.py", "/arg/py", "/arg/out")


def test_constructing_a_collector_creates_no_directories(monkeypatch, tmp_path):
    """Constructing a collector must not create the snapshot directory.

    It used to call os.makedirs(output_dir) in __init__, which stamped an empty
    ~/Documents/GitHub/missing-peering-links/scripts/data tree onto hermes-ec2 --
    0 files, looking like a checkout that had never been there.
    """
    from hermes.enrichment.peeringdb_ixp.ixp_collector import IXPCollector
    from hermes.enrichment.peeringdb_ixp.ixp_collector_ipv6 import IXPCollectorIPv6

    _clear(monkeypatch)
    monkeypatch.setattr("google.cloud.bigquery.Client", lambda project=None: object())

    for cls in (IXPCollector, IXPCollectorIPv6):
        target = tmp_path / cls.__name__ / "scripts" / "data"
        monkeypatch.setenv("HERMES_IXP_REPO", str(tmp_path / cls.__name__))
        collector = cls()
        assert collector.output_dir == str(target)
        assert not target.exists(), f"{cls.__name__}.__init__ created {target}"


def test_collectors_honour_explicit_paths(monkeypatch):
    from hermes.enrichment.peeringdb_ixp.ixp_collector import IXPCollector
    from hermes.enrichment.peeringdb_ixp.ixp_collector_ipv6 import IXPCollectorIPv6

    _clear(monkeypatch)
    monkeypatch.setattr("google.cloud.bigquery.Client", lambda project=None: object())
    for cls in (IXPCollector, IXPCollectorIPv6):
        c = cls(python_executable="/arg/py", output_dir="/arg/out", wrapper_script_path="/arg/w.py")
        assert (c.wrapper_script_path, c.python_executable, c.output_dir) == (
            "/arg/w.py",
            "/arg/py",
            "/arg/out",
        )

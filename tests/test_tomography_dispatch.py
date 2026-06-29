import datetime as dt

import pytest

from hermes.pipeline import tomography


def test_python_backend_calls_v2(monkeypatch):
    called = {}
    monkeypatch.setattr(
        "hermes.pipeline.correlation_tomography.run_correlation_tomography",
        lambda date, project_id, **kw: called.setdefault("ok", True),
    )
    tomography.run_tomography(dt.date(2026, 5, 20), backend="python", project_id="p")
    assert called["ok"]


def test_unknown_backend_raises():
    with pytest.raises(ValueError):
        tomography.run_tomography(dt.date(2026, 5, 20), backend="bigquery", project_id="p")

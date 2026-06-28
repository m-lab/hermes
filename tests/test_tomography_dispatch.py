import datetime as dt

import pytest

from hermes.pipeline import tomography


def test_python_backend_calls_hybrid(monkeypatch):
    called = {}
    monkeypatch.setattr(
        tomography,
        "_run_python",
        lambda date, **kw: called.setdefault("py", (date, kw)),
    )
    tomography.run_tomography(dt.date(2026, 5, 20), backend="python", project_id="p")
    assert called["py"][0] == dt.date(2026, 5, 20)


def test_bigquery_backend_calls_sql(monkeypatch):
    called = {}
    monkeypatch.setattr(
        tomography,
        "_run_bigquery",
        lambda date, **kw: called.setdefault("bq", (date, kw)),
    )
    tomography.run_tomography(dt.date(2026, 5, 20), backend="bigquery", project_id="p")
    assert called["bq"][0] == dt.date(2026, 5, 20)


def test_unknown_backend_raises():
    with pytest.raises(ValueError):
        tomography.run_tomography(dt.date(2026, 5, 20), backend="nope", project_id="p")

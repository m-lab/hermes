import datetime as dt

import pytest

from hermes.pipeline import correlation_tomography, tomography


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


def test_phase_d_query_can_be_retargeted_to_staging(monkeypatch):
    monkeypatch.setattr(
        correlation_tomography.loader,
        "load_query",
        lambda name, params: (
            "SELECT * FROM `mlab-collaboration.hermes_union.events_with_as_and_geoloc` "
            "JOIN `mlab-collaboration.hermes_union.place_canonical_metro` USING (place)"
        ),
    )
    sql = correlation_tomography._load_dataset_query("query.sql", {}, dataset="hermes_staging")
    assert "mlab-collaboration.hermes_staging.events_with_as_and_geoloc" in sql
    assert "mlab-collaboration.hermes_union.events_with_as_and_geoloc" not in sql
    assert "mlab-collaboration.hermes_union.place_canonical_metro" in sql


def test_phase_d_rejects_invalid_dataset_names():
    with pytest.raises(ValueError):
        correlation_tomography._dataset_table("events", dataset="bad.dataset")


def test_all_edges_query_omits_unused_high_cardinality_strings():
    """Phase D must not retain per-measurement strings its consumers never read."""
    sql = correlation_tomography.loader.load_query(
        "06_correlation_tomography_all_edges_union.sql", {"DAY": "2026-08-07"}
    )

    assert "fr.id" not in sql
    assert "AS canonical_edge" not in sql
    assert "SELECT *" not in sql
    assert "AS from_asn_metro" in sql
    assert "AS to_asn_metro" in sql

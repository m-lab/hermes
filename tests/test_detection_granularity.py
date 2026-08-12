"""Pipeline-mode guards for city- versus metro-keyed anomaly detection."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest
from scripts import build_staging_sql

from hermes.pipeline import union
from hermes.sql import loader

PARAMS = {
    "DAY": "2026-08-07",
    "ONE_WEEK_EARLIER": "2026-07-31",
}


@pytest.mark.parametrize("value", union.DETECTION_GRANULARITIES)
def test_parse_detection_granularity_accepts_supported_values(value):
    assert union.parse_detection_granularity(value) == value


def test_parse_detection_granularity_rejects_unknown_value():
    with pytest.raises(ValueError, match="Unsupported detection granularity"):
        union.parse_detection_granularity("state")


@pytest.mark.parametrize(
    "step", ["02_detect_anomalies_union.sql", "03_build_transient_events_union.sql"]
)
@pytest.mark.parametrize("granularity", union.DETECTION_GRANULARITIES)
def test_detection_steps_resolve_the_mode_placeholder(step, granularity):
    sql = loader.load_query(step, {**PARAMS, "DETECTION_GRANULARITY": granularity})
    assert "${DETECTION_GRANULARITY}" not in sql
    assert f"DEFAULT '{granularity}'" in sql


def test_run_sql_steps_passes_metro_to_loader(monkeypatch):
    captured = {}

    monkeypatch.setattr(union, "step_already_done", lambda *args: False)
    monkeypatch.setattr(
        union.loader,
        "load_query",
        lambda name, params: captured.update(name=name, params=params) or "SELECT 1",
    )
    monkeypatch.setattr(union, "execute_query", lambda *args: None)

    result = union._run_sql_steps(
        date(2026, 8, 7),
        "mlab-collaboration",
        ["02_detect_anomalies_union.sql"],
        skip_data_check=True,
        detection_granularity="metro",
    )

    assert result == "Success: 2026-08-07"
    assert captured["params"]["DETECTION_GRANULARITY"] == "metro"


@pytest.mark.parametrize(
    ("present", "expected"),
    [([], False), (["metro"], True)],
)
def test_granularity_aware_resume_accepts_only_the_requested_mode(
    monkeypatch, present, expected
):
    rows = [SimpleNamespace(granularity=value) for value in present]
    query_job = SimpleNamespace(result=lambda: rows)
    client = SimpleNamespace(query=lambda *args, **kwargs: query_job)
    monkeypatch.setattr(union.bigquery, "Client", lambda **kwargs: client)

    assert (
        union.step_already_done(
            "billing-project",
            "data-project.dataset.table",
            "2026-08-07",
            "metro",
        )
        is expected
    )


def test_granularity_aware_resume_rejects_a_mixed_or_conflicting_partition(monkeypatch):
    rows = [SimpleNamespace(granularity="maxmind_city")]
    query_job = SimpleNamespace(result=lambda: rows)
    client = SimpleNamespace(query=lambda *args, **kwargs: query_job)
    monkeypatch.setattr(union.bigquery, "Client", lambda **kwargs: client)

    with pytest.raises(RuntimeError, match="refusing to append metro"):
        union.step_already_done(
            "billing-project",
            "data-project.dataset.table",
            "2026-08-07",
            "metro",
        )


def test_legacy_flag_backfill_is_idempotent_and_preserves_untested_rows():
    sql = (
        Path(__file__).resolve().parents[1]
        / "scripts"
        / "backfill_detection_granularity.sql"
    ).read_text(encoding="utf-8")
    assert "SET detection_granularity = 'maxmind_city'" in sql
    assert "WHERE detection_granularity IS NULL" in sql
    assert sql.count("partition_date <= DATE '${LEGACY_THROUGH_DAY}'") == 5
    assert "SET detection_granularity = 'metro'" not in sql


def test_staging_builder_emits_every_numbered_sql_stage(tmp_path):
    assert build_staging_sql.build("2026-08-07", "hermes_staging", tmp_path, "metro") == 0
    assert {path.name for path in tmp_path.iterdir()} == set(build_staging_sql.STEPS)

    temporal = (tmp_path / "05_temporal_tomography_union.sql").read_text(encoding="utf-8")
    assert "mlab-collaboration.hermes_staging.temporal_correlations" in temporal
    assert "mlab-collaboration.hermes_union.temporal_correlations" not in temporal


def test_acceptance_query_is_parameterized_for_both_detection_modes():
    sql = (
        Path(__file__).resolve().parents[1] / "scripts" / "verify_group_identity.sql"
    ).read_text(encoding="utf-8")
    assert "DEFAULT '${EXPECTED_GRANULARITY}'" in sql
    assert "detection_granularity != _expected_granularity" in sql

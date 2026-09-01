"""Regression contracts for repeatable model decisions and public output."""

from __future__ import annotations

import pandas as pd

from hermes.pipeline.correlation_tomography import _deterministic_mode
from hermes.sql import loader, paths


def test_detector_inlines_seeded_wasserstein_and_stable_trimming():
    sql = loader.load_query(
        "02_detect_anomalies_union.sql",
        {"DAY": "2026-08-26", "DETECTION_GRANULARITY": "metro"},
    )

    assert "CREATE TEMP FUNCTION compute_wasserstein_p_value" in sql
    assert "Math.random" not in sql
    assert "seededRng(fnv1a(seedMaterial))" in sql
    assert "ORDER BY FARM_FINGERPRINT(measurement_id), measurement_id" in sql
    assert "ORDER BY (SELECT NULL)" not in sql
    assert "`hermes.compute_wasserstein_p_value`" not in sql
    assert "COALESCE(city_ip_info, '') ASC" in sql
    assert "COALESCE(metro, '') ASC" in sql
    assert "APPROX_QUANTILES" not in sql


def test_wasserstein_seed_is_invariant_to_input_array_order():
    udf = paths.udf_path("compute_wasserstein_p_value").read_text()
    assert "canonicalWeekly = weeklyData.slice().sort" in udf
    assert "canonicalDaily = dailyData.slice().sort" in udf
    assert "JSON.stringify([canonicalWeekly, canonicalDaily, numPerms])" in udf


def test_public_output_orders_observed_ips_and_avoids_any_value():
    sql = loader.load_query("07_translating_to_public_format_union.sql", {"DAY": "2026-08-26"})
    assert "ARRAY_AGG(DISTINCT fr.src ORDER BY fr.src)" in sql
    assert "ANY_VALUE(" not in sql


def test_mode_breaks_frequency_ties_by_value():
    values = pd.Series(["zeta", "alpha", "zeta", "alpha"])
    assert _deterministic_mode(values) == "alpha"

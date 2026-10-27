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

    assert "CREATE TEMP FUNCTION wasserstein_permutation_test" in sql
    assert "Math.random" not in sql
    # seeded from SQL (group + day + metric), not from the data
    assert "seededRng(Number(seed))" in sql
    assert "w1_seed(src_asn, src_city, dst_site, ip_version, 'download')" in sql
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


def _q(name: str) -> str:
    return loader.load_query(name, {"DAY": "2026-10-14", "DETECTION_GRANULARITY": "metro"})


def test_detector_summarizes_the_current_day_once_per_detection_group():
    """Medians, differences and flags must describe the same group the baseline and
    the statistical tests describe, not a split of it (2026-10 identity fix)."""
    sql = _q("02_detect_anomalies_union.sql")
    base = sql[sql.index("\nCurrentDayAggregatedBase AS (") :]
    base = base[: base.index("\n),")]
    for col in ("src_country", "src_asn_name", "src_state", "dst_asn", "dst_country", "dst_city"):
        assert f"gd.d.{col} AS {col}" in base
    assert "JOIN GroupDescriptors gd" in base
    descriptors = sql[sql.index("\nGroupDescriptors AS (") :]
    assert "ORDER BY n DESC, src_state, src_country, src_asn_name, dst_city" in descriptors
    assert "AVG(CAST(ndt.client.Geo.Latitude AS NUMERIC))" in base


def test_transient_events_is_reproducible():
    sql = _q("03_build_transient_events_union.sql")
    assert "APPROX_QUANTILES" not in sql
    assert "PERCENTILE_DISC(x, 0.50) OVER() FROM UNNEST(rtts) x LIMIT 1" in sql
    assert "raw.date DESC,\n      raw.id" in sql
    # refuses a split group instead of mixing its rows column by column
    guard = sql.index("ASSERT NOT EXISTS")
    assert guard < sql.index("INSERT INTO `mlab-collaboration.hermes_union.transient_events_union`")
    assert "GROUP BY src_group_label, src_asn, dst_site, ip_version" in sql[guard:]


def test_all_edges_orders_hops_before_pairing_neighbours():
    sql = _q("06_correlation_tomography_all_edges_union.sql")
    assert sql.count("COALESCE(n.metro, n.place)) ORDER BY n.ttl) AS asn_city") == 2
    assert sql.count("IFNULL(n.associated_ixp, 'None') ORDER BY n.ttl) AS ixp_arr") == 2

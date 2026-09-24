"""Contract tests for the canonical HERMES compatibility view."""

from hermes.sql import loader


def _view_sql(ds: str = "hermes_union") -> str:
    """The view SQL with `--` comment lines dropped.

    The header comment legitimately names the old `hermes.events_enriched` and
    the retired ${PUBLISHED_DS}; the tests are about the statements.
    """
    sql = loader.load_query("create_events_enriched.sql", {"DS": ds})
    return "\n".join(line for line in sql.splitlines() if not line.lstrip().startswith("--"))


def test_view_is_published_beside_the_table_it_reads():
    """The view moved from `hermes` into `hermes_union`.

    Source and publication dataset are now the same ${DS}, so a staging
    bootstrap cannot publish a staging view over production rows.
    """
    assert "`mlab-collaboration.hermes_union.events_enriched`" in _view_sql()

    staging = _view_sql("hermes_staging")
    assert "`mlab-collaboration.hermes_staging.events_enriched`" in staging
    assert "`mlab-collaboration.hermes_staging.events_with_as_and_geoloc`" in staging
    assert "mlab-collaboration.hermes_union." not in staging
    assert "${PUBLISHED_DS}" not in staging


def test_view_uses_stable_nested_contract():
    sql = _view_sql()

    assert "`mlab-collaboration.hermes_union.events_enriched`" in sql
    assert "`mlab-collaboration.hermes_union.events_with_as_and_geoloc`" in sql
    assert "AS client" in sql
    assert "AS server" in sql
    assert "AS performance" in sql
    assert "AS server_to_client_path" in sql
    assert "AS client_to_server_path" in sql
    assert "AS quality" in sql
    assert "e.src AS client_ip" in sql
    assert "e.dst AS server_ip" in sql


def test_view_normalizes_legacy_group_provenance_without_rewriting_history():
    sql = _view_sql()

    assert "detection_granularity = 'maxmind_city'" in sql
    assert "END AS grouping_granularity" in sql
    assert "COALESCE(src_group_label, src_city)" in sql
    assert "client_geo_source" in sql
    assert "'maxmind'" in sql
    assert "'ipinfo'" in sql


def test_forward_and_reverse_hops_share_canonical_names():
    sql = _view_sql()

    for field in (
        "AS ip",
        "AS rtt_ms",
        "AS asn",
        "AS as_name",
        "AS country_code",
        "AS geo_score",
        "AS segment_distance_km",
        "AS remaining_distance_km",
        "AS propagation_speed_km_s",
        "AS facilities",
    ):
        assert sql.count(field) >= 2, field

    assert "AS revtr_hop_type" in sql
    assert "AS uses_interdomain_symmetry" in sql
    assert "baseline_consistency_flag" in sql


def test_path_summaries_are_symmetric():
    sql = _view_sql()

    for prefix in ("forward", "reverse"):
        assert f"{prefix}_total_hop_count" in sql
        assert f"{prefix}_responsive_hop_count" in sql
        assert f"{prefix}_geolocated_hop_count" in sql

    assert sql.count("AS geolocation_coverage") == 2
    assert sql.count("AS geodesic_distance_km") == 2
    assert sql.count("AS detour_ratio") == 2
    assert sql.count("AS as_path") == 2
    assert sql.count("AS country_path") == 2
    assert sql.count("AS metro_path") == 2
    assert sql.count("AS ixp_path") == 2


def test_path_directions_match_the_measurement_techniques():
    sql = _view_sql()

    assert "'server_to_client' AS direction" in sql
    assert "'scamper' AS measurement_method" in sql
    assert "'client_to_server' AS direction" in sql
    assert "'reverse_traceroute' AS measurement_method" in sql
    assert sql.count("ORDER BY hop.ttl") >= 2
    assert sql.count("SUM(hop.segment_distance_km)") == 2


def test_legacy_fiber_value_is_not_mislabeled_as_a_speed():
    sql = _view_sql()

    assert sql.count("200000.0") == 2
    assert sql.count("AS propagation_speed_km_s") == 2
    assert sql.count("AS fiber_lower_bound_rtt_ms") == 2


def test_anomaly_flags_are_not_called_counts():
    """`anomaly_*_count` is a 0/1 verdict flag, not a count of measurements.

    Exposing it as `rtt_count` invited readers to treat it as "how many
    measurements were anomalous". It is `SUM()` of a per-group boolean, which is
    why every predicate in this repo reads `>= 0.5`. The view states that
    contract as a BOOL. See docs/reference/anomaly-fields.md.
    """
    sql = _view_sql()

    for signal in ("rtt", "download", "upload"):
        assert f"AS {signal}_significant" in sql, signal
        assert f"AS {signal}_anomalous_sample_fraction" in sql, signal
        # the misleading names must not come back
        assert f"AS {signal}_count" not in sql, signal

    # Exposed as a boolean, using the same threshold the pipeline predicates use.
    assert "anomaly_rtt_count >= 0.5 AS rtt_significant" in sql
    assert "anomaly_throughput_count >= 0.5 AS download_significant" in sql
    assert "anomaly_upload_throughput_count >= 0.5 AS upload_significant" in sql

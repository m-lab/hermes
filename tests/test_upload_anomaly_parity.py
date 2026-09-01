"""Contracts for promoting upload throughput to a first-class anomaly signal."""

from hermes.sql import loader


def _sql(name: str) -> str:
    params = {"DAY": "2026-08-26"} if name[0].isdigit() else {}
    return loader.load_query(name, params)


def test_upload_detector_requires_upload_specific_sample_power():
    sql = _sql("02_detect_anomalies_union.sql")

    assert "ARRAY_LENGTH(current_upload_throughput_array) AS current_upload_sample_count" in sql
    assert "ARRAY_LENGTH(baseline_upload_throughput_array) AS baseline_upload_sample_count" in sql
    assert "st.current_upload_sample_count >= 10" in sql
    assert "st.baseline_upload_sample_count >= 25" in sql


def test_temporal_prevalence_classifies_each_direction_with_its_own_signal():
    sql = _sql("05_temporal_edge_prevalences_union.sql")

    assert "AS is_forward_anomaly" in sql
    assert "AS is_reverse_anomaly" in sql
    assert "is_forward_anomaly AS is_anomaly, 'forward' AS direction" in sql
    assert "is_reverse_anomaly AS is_anomaly, 'reverse' AS direction" in sql
    assert "anomaly_ratio_upload_throughput" in sql


def test_forward_only_temporal_tomography_does_not_absorb_upload():
    sql = _sql("05_temporal_tomography_union.sql")

    assert "anomaly_ratio_upload_throughput" not in sql


def test_correlation_tomography_and_fallback_are_upload_directional():
    prepare = _sql("06_correlation_tomography_prepare_union.sql")
    fallback = _sql("06_correlation_tomography_unexplained_hops_union.sql")

    assert "AS is_forward_anomaly" in prepare
    assert "AS is_reverse_anomaly" in prepare
    assert "anomaly_ratio_upload_throughput" in prepare
    assert "is_forward_anomaly" in fallback
    assert "is_reverse_anomaly" in fallback
    assert "WHERE is_forward_anomaly" in fallback
    assert "WHERE is_reverse_anomaly" in fallback


def test_correlation_pair_identity_includes_ip_family():
    prepare = _sql("06_correlation_tomography_prepare_union.sql")
    fallback = _sql("06_correlation_tomography_unexplained_hops_union.sql")
    public = _sql("07_translating_to_public_format_union.sql")

    for sql in (prepare, fallback, public):
        assert "src_group_label, ' - '," in sql
        assert "dst_site, ' - '," in sql
        assert "ip_version" in sql

    assert "ocd.pair_ip_version = ta.ip_version" in public
    assert "ocd.information_source = 'forward'" in public
    assert "ocd.information_source = 'reverse'" in public


def test_public_formatter_parses_pair_keys_from_both_ends():
    sql = _sql("07_translating_to_public_format_union.sql")

    assert "AS pair_src_asn" in sql
    assert "AS pair_src_group_label" in sql
    assert "AS pair_dst_site" in sql
    assert "AS pair_ip_version" in sql
    assert "ARRAY_TO_STRING(" in sql
    assert "ARRAY_SLICE(" in sql


def test_public_formatter_recovers_upload_only_pairs_and_names_the_signals():
    sql = _sql("07_translating_to_public_format_union.sql")

    assert "fr.median_upload_throughput < fr.baseline_median_upload_throughput" in sql
    assert "AS baseline_median_upload_throughput" in sql
    assert "AS median_daily_upload_throughput" in sql
    assert "AS mean_daily_upload_throughput" in sql
    assert "AS anomaly_ratio_upload_throughput" in sql
    assert "AS anomaly_signals" in sql
    assert "AS is_upload_anomaly" in sql
    assert "COUNTIF(is_upload_anomaly = 1) AS upload_anomaly_sites" in sql
    assert "AS total_anomalous_sites_all_signals" in sql

    # Keep the legacy denominator stable across the rollout date.
    assert (
        "COUNTIF(is_latency_anomaly = 1 OR is_throughput_anomaly = 1) "
        "AS total_anomalous_sites" in sql
    )


def test_public_upload_columns_have_one_append_order_for_create_and_migration():
    ddl = _sql("create_events_explained_daily.sql")
    migration = _sql("add_upload_anomaly_columns.sql")
    columns = (
        "baseline_median_upload_throughput",
        "median_daily_upload_throughput",
        "mean_daily_upload_throughput",
        "anomaly_ratio_upload_throughput",
        "upload_anomaly_sites",
        "total_anomalous_sites_all_signals",
        "anomaly_signals",
    )

    for sql in (ddl, migration):
        positions = [sql.index(column) for column in columns]
        assert positions == sorted(positions)

    # A single table metadata update avoids BigQuery's schema-update rate limit
    # and remains safe after a partially completed older migration.
    assert sum(line.lstrip().startswith("ALTER TABLE") for line in migration.splitlines()) == 1
    assert migration.count("ADD COLUMN IF NOT EXISTS") == len(columns)

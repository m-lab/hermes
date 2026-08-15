"""Detection granularity must stay consistent from 02 through to the public table.

02 groups by (src_asn, src_city, dst_site, ip_version), where src_city is the
selected client-group label: an IPInfo City-Region-Country triple or a
canonical metro. 04 used to REPLACE src_city in place with a
coarser metro-polygon label, so every stage after it silently re-grouped at a
granularity nobody tested at: 22.78% of keys covered several tested populations,
06 built hyperedge pair strings on the collapsed label, and 07's >=10 day-of
sufficiency gate counted whole metros rather than the group that fired.

Now: src_group_label carries the exact key 02 grouped on and everything keys on
it; src_city is a readable label; src_metro is the rollup;
detection_granularity records the grouping shape; client_geo_source records
the provider.

See docs/proposals/2026-08-group-granularity.md and its HANDOVER companion.
"""

import re
from pathlib import Path

import pytest

QUERIES = Path(__file__).resolve().parents[1] / "src" / "hermes" / "sql" / "queries"

STEPS = {
    "02": "02_detect_anomalies_union.sql",
    "03": "03_build_transient_events_union.sql",
    "04": "04_mapping_union.sql",
    "05_temporal": "05_temporal_tomography_union.sql",
    "05_prevalences": "05_temporal_edge_prevalences_union.sql",
    "06": "06_correlation_tomography_prepare_union.sql",
    "06_unexplained": "06_correlation_tomography_unexplained_hops_union.sql",
    "07": "07_translating_to_public_format_union.sql",
}


def _sql(name: str) -> str:
    return (QUERIES / name).read_text()


@pytest.fixture(scope="module")
def step02() -> str:
    return _sql(STEPS["02"])


@pytest.fixture(scope="module")
def step03() -> str:
    return _sql(STEPS["03"])


@pytest.fixture(scope="module")
def step04() -> str:
    return _sql(STEPS["04"])


@pytest.fixture(scope="module")
def step06() -> str:
    return _sql(STEPS["06"])


@pytest.fixture(scope="module")
def step05_temporal() -> str:
    return _sql(STEPS["05_temporal"])


@pytest.fixture(scope="module")
def step05_prevalences() -> str:
    return _sql(STEPS["05_prevalences"])


@pytest.fixture(scope="module")
def step06_unexplained() -> str:
    return _sql(STEPS["06_unexplained"])


@pytest.fixture(scope="module")
def step07() -> str:
    return _sql(STEPS["07"])


# ---------------------------------------------------------------------------
# The regression guard. This is the whole point of the change.
# ---------------------------------------------------------------------------
def test_04_never_overwrites_the_detection_label(step04):
    """No REPLACE may target src_group_label.

    Reintroducing it would re-collapse every downstream stage -- tomography
    attribution and 07's sufficiency gate included -- with no error to notice.
    """
    assert not re.search(r"REPLACE\s*\([^)]*\bAS\s+src_group_label\b", step04, re.S)


def test_04_no_longer_writes_the_metro_into_src_city(step04):
    """The specific historical bug: src_city := COALESCE(metro, src_city)."""
    assert not re.search(
        r"REPLACE\s*\(\s*COALESCE\(\s*sm\.metro\s*,\s*m\.src_city\s*\)\s*AS\s+src_city",
        step04,
    ), "04 must not overwrite src_city with the metro; the metro belongs in src_metro"


def test_04_emits_the_metro_as_an_annotation(step04):
    assert "COALESCE(sm.metro, m.src_city)" in step04
    assert "m.detection_granularity = 'metro', m.src_group_label" in step04


def test_04_giga_dedup_uses_the_immutable_identity_with_a_legacy_bridge(step04):
    assert "g.src_group_label = _mapping_result.src_group_label" in step04
    assert "OR g.src_group_label IS NULL" in step04
    assert "g.src_city = _mapping_result.src_city" not in step04


# ---------------------------------------------------------------------------
# Identity originates at 02 and is carried, never synthesised.
# ---------------------------------------------------------------------------
def test_02_declares_granularity_and_the_grouping_key(step02):
    assert "DECLARE _detection_granularity STRING DEFAULT '${DETECTION_GRANULARITY}'" in step02
    assert re.search(r"_detection_granularity\s+AS\s+detection_granularity", step02)
    assert re.search(r"\bsrc_city\s+AS\s+src_group_label\b", step02)
    assert "'ipinfo' AS client_geo_source" in step02


def test_02_resolves_metro_before_statistical_grouping(step02):
    """The metro must be settled before any statistic is computed.

    It used to be resolved inside 02 by a spatial join (_source_metro_lookup +
    ST_COVERS). Phase A0 now resolves every client IP to a metro in
    unified_src_ip_to_geoloc, so 02 reads it rather than computing it and does no
    spatial join at all. The invariant is unchanged; only where it is satisfied
    moved. Asserting on the *outcome* rather than the mechanism.
    """
    assert "unified_src_ip_to_geoloc" in step02, "metro must come from Phase A0"
    # Check the DDL, not the bare name -- a comment explaining the removal is fine.
    assert "CREATE TEMP TABLE _source_metro_lookup" not in step02, (
        "02 must no longer resolve metro itself"
    )
    assert "ST_COVERS(mp.polygon" not in step02, "02 must no longer do a spatial join"
    assert "MeasurementsWithGroup AS" in step02
    assert "ndt.detection_src_city AS src_city" in step02
    assert "PARTITION BY src_asn, src_city, dst_site, ip_version" in step02


def test_02_client_geography_comes_from_ipinfo_not_maxmind(step02):
    """MaxMind's client.Geo is replaced wholesale, not blended.

    The struct is rewritten in place so every downstream reference reads IPInfo;
    the guard is that no MaxMind value can survive into a group label. A blended
    COALESCE would silently reintroduce MaxMind's country-centroid fallback,
    which is what made Tokyo 53.3% and Paris 55.9% synthetic under metro grouping.
    """
    for field in (
        "city_ip_info",
        "region_ip_info",
        "country_ip_info",
        "lat_ip_info",
        "lon_ip_info",
    ):
        assert f"g.{field}" in step02, f"{field} must feed the rewritten client.Geo"
    assert "AS Geo" in step02 and "AS client" in step02, "client.Geo must be replaced in place"
    assert "COALESCE(g.city_ip_info, ndt.client.Geo.City)" not in step02, (
        "must not fall back to MaxMind"
    )


def test_03_reattaches_raw_measurements_with_the_selected_group(step03):
    assert "AND detection_granularity = _detection_granularity" in step03
    assert step03.count("ndt.detection_src_city = a.src_group_label") == 2
    assert "ndt.detection_src_city AS src_city" in step03


def test_03_carries_identity_without_synthesising_it(step03):
    """A giga trace with no AnomalyCounts match was never in a tested group.

    src_city gets an IF(a.src_asn IS NULL, <scamper fallback>, ...) there; the
    identity columns must not, or the row asserts a test that never ran.
    """
    for col in ("detection_granularity", "src_group_label"):
        assert f"ANY_VALUE(a.{col})" in step03
        assert not re.search(rf"IF\(\s*a\.src_asn IS NULL[^)]*\)\s*AS\s+{col}\b", step03)
    assert step03.count("'ipinfo'                           AS client_geo_source") == 2


# ---------------------------------------------------------------------------
# Every number must be computed at the granularity the flag names.
# ---------------------------------------------------------------------------
def test_06_keys_pair_strings_on_the_detection_group(step06):
    """Otherwise Phase D attributes metro-collapsed groups."""
    assert "CONCAT(fr.src_asn, ' - ', fr.src_group_label, ' - ', fr.dst_site)" in step06
    assert "CONCAT(fr.src_asn, ' - ', fr.src_city, ' - ', fr.dst_site)" not in step06


def test_path_local_uses_the_same_pair_identity_as_the_main_prepare(
    step06_unexplained,
):
    """Python intersects the two query outputs by exact src_dst_pair string."""
    assert "CONCAT(src_asn, ' - ', src_group_label, ' - ', dst_site)" in step06_unexplained
    assert "CONCAT(src_asn, ' - ', src_city, ' - ', dst_site)" not in step06_unexplained


def test_temporal_consumers_key_on_the_detection_group(step05_temporal, step05_prevalences):
    assert "CONCAT(src_asn, ' - ', src_group_label, ' - ', dst_site)" in step05_temporal
    assert "GROUP BY src_asn, src_group_label, dst_site" in step05_temporal
    assert "CONCAT(src_asn, ' - ', src_group_label, ' - ', dst_site)" in step05_prevalences
    assert "CONCAT(src_asn, ' - ', src_city, ' - ', dst_site)" not in step05_prevalences


def test_07_sufficiency_gate_counts_the_tested_population(step07):
    """The >=10 day-of gate must group by the detection key, not the metro.

    Counting whole metros let a 6-measurement group qualify on its neighbours'
    totals -- the motivating Castello case.
    """
    assert re.search(
        r"COUNTIF\(DATE\(window_start\) >= '\$\{DAY\}'\) AS n_dayof,\s*\n"
        r"\s*COUNTIF\(DATE\(window_start\) <\s+'\$\{DAY\}'\) AS n_baseline\s*\n"
        r"\s*FROM[^\n]*\n\s*WHERE[^\n]*\n\s*GROUP BY "
        r"src_asn, src_group_label, dst_site, ip_version",
        step07,
    )


def test_07_counts_both_windows_in_one_pass(step07):
    """n_dayof and n_baseline must come from a single scan of the partition.

    events_with_as_and_geoloc is the pipeline's largest table; splitting the two
    windows into separate CTEs would double the scan for no benefit.
    """
    assert "dayof_counts" not in step07, "renamed to group_counts; it counts both windows"
    # Both counts are defined in one CTE, so adding n_baseline must not have added a
    # reference to the partition. 7 is the count before n_baseline existed.
    assert step07.count("events_with_as_and_geoloc") == 7
    assert step07.count("COUNTIF(DATE(window_start)") == 2


def test_07_does_not_group_or_join_on_src_city(step07):
    """src_city is a display label; keying on it would re-merge 45 label pairs."""
    assert not re.search(r"GROUP BY[^\n]*\bsrc_city\b", step07)
    assert "= combined.src_city" not in step07


# ---------------------------------------------------------------------------
# Compatibility with hyperedges built before the change.
# ---------------------------------------------------------------------------
def test_07_accepts_hyperedges_keyed_either_way(step07):
    """Historical hyperedges key pair strings on the metro.

    A hyperedge's content (from_asn/to_asn/edge_asn_metro) is an intermediary hop
    pair and is granularity-independent, so the historical corpus stays valid and
    needs no Phase D re-run -- but 07 must accept both vocabularies or every
    event falls to `unresolved` with NULL attribution.
    """
    assert "= ta.src_group_label" in step07
    assert "= ta.src_metro" in step07


def test_07_unresolved_mirrors_the_two_way_match(step07):
    """Else a pair matched on the metro form appears in BOTH branches."""
    assert step07.count("CONCAT(src_asn, ' - ', src_group_label, ' - ', dst_site) NOT IN") == 1
    assert step07.count("CONCAT(src_asn, ' - ', src_metro, ' - ', dst_site) NOT IN") == 1


def test_07_resolved_takes_the_label_from_the_matched_row(step07):
    """Not from the pair string.

    A metro-keyed hyperedge would otherwise put a METRO in src_group_label, and
    the INNER JOIN to anomaly_summary (keyed on real labels) would silently drop
    every resolved row -- indistinguishable from the join not matching at all.
    """
    assert re.search(
        r"COALESCE\(ta\.src_group_label,\s*\n?\s*SPLIT\(src_dst_str, ' - '\)"
        r"\[SAFE_OFFSET\(1\)\]\) AS src_group_label",
        step07,
    )


def test_07_records_which_source_label_vocabulary_matched(step07):
    assert re.search(
        r"IF\(SPLIT\(src_dst_str, ' - '\)\[SAFE_OFFSET\(1\)\] = ta\.src_group_label,"
        r"\s*ta\.detection_granularity, 'metro'\) AS src_match_granularity",
        step07,
    )
    assert "CAST(NULL AS STRING) AS src_match_granularity" in step07


# ---------------------------------------------------------------------------
# Display-name parsing.
# ---------------------------------------------------------------------------
def test_07_derives_the_display_city_from_the_parseable_label(step07):
    """City-State-CC is not parseable from either end.

    City names contain '-' (Saint-Agapit, Vila-real), so left-parsing truncates
    4.17% of names and collapses Saint-Agapit and Saint-Georges both to "Saint".
    Full state names also contain '-' (Nordrhein-Westfalen, Zuid-Holland), so
    right-parsing src_city bleeds state into city (Bergkamen-Nordrhein). Only
    src_group_label (City-ISO-CC) is right-parseable, because ISO subdivision
    codes never contain '-'.
    """
    assert "SPLIT(src_city, '-')[SAFE_OFFSET(0)]" not in step07
    assert re.search(r"UNNEST\(SPLIT\(src_group_label, '-'\)\)", step07)
    assert "detection_granularity = 'metro'" in step07
    assert "ENDS_WITH(src_group_label, CONCAT('-', src_state, '-', src_country))" in step07


def test_07_emits_the_resolved_state_without_prefixing_the_country(step07):
    """The dashboard receives country and state as separate fields."""
    assert "CONCAT(src_country, '-', src_state) AS src_state" not in step07
    assert re.search(
        r"src_match_granularity,\s*\n\s*-- `src_country`.*?\n\s*src_state,",
        step07,
        re.S,
    )


# ---------------------------------------------------------------------------
# Public table contract.
# ---------------------------------------------------------------------------
def test_07_public_table_is_joinable_and_filterable(step07):
    """Without these the public table cannot be tied back to a tested group.

    Unambiguous now because 07 keys on the label: one row is one population.
    """
    for col in (
        "src_group_label",
        "n_dayof",
        "n_baseline",
        "detection_granularity",
        "src_metro",
        "src_match_granularity",
        "client_geo_source",
    ):
        assert col in step07


def test_schema_evolving_writers_use_explicit_column_lists(step02, step03, step07):
    for table, sql in (
        ("anomaly_counts_union", step02),
        ("transient_events_union", step03),
        ("events_explained_daily", step07),
    ):
        assert re.search(rf"INSERT INTO `[^`]+\.{table}`\s*\(", sql), table


def test_public_bootstrap_ddl_contains_the_identity_contract():
    ddl = _sql("create_events_explained_daily.sql")
    for col in (
        "detection_granularity STRING",
        "src_metro STRING",
        "src_group_label STRING",
        "n_dayof INT64",
        "n_baseline INT64",
        "src_match_granularity STRING",
        "client_geo_source STRING",
    ):
        assert col in ddl


def test_no_superseded_columns_survive():
    """src_group_id / src_city_maxmind were dropped: redundant with src_city."""
    for name in STEPS.values():
        sql = _sql(name)
        for dead in ("src_group_id", "src_city_maxmind"):
            assert dead not in sql, f"{name} still references {dead}"


def test_no_pre_review_granularity_names_survive():
    for name in STEPS.values():
        sql = _sql(name)
        for old in ("src_group_granularity", "attribution_granularity"):
            assert old not in sql, f"{name} still references {old}"


@pytest.mark.parametrize("name", sorted(STEPS.values()))
def test_each_step_points_at_the_design_doc(name):
    assert "group-granularity" in _sql(name)

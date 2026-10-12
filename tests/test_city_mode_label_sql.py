"""City-mode group labels must not repeat the region and country."""

import pytest

from hermes.sql import loader


@pytest.mark.parametrize(
    "step", ["02_detect_anomalies_union.sql", "03_build_transient_events_union.sql"]
)
def test_city_label_appends_suffix_only_when_absent(step):
    sql = loader.load_query(
        step,
        {"DAY": "2025-08-26", "ONE_WEEK_EARLIER": "2025-08-19", "DETECTION_GRANULARITY": "city"},
    )
    suffix = "CONCAT('-', g.region_ip_info, '-', g.country_ip_info)"
    assert f"IF(ENDS_WITH(g.city_ip_info, {suffix})," in sql

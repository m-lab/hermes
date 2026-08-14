"""Client geolocation must cover both address families and the whole baseline window.

Both defects were found by the first Phase A0 staging run, which showed transient
events down 70% against production. Neither is visible in a schema check or a
row-count sanity test — both produce well-formed output over a silently truncated
population.

1. Enrichment writes IPv4 and IPv6 client geolocation to *separate* tables, because
   the enricher is instantiated once per family with its own ``tables`` dict. Steps
   02/03 read only the IPv4 table, so every IPv6 client got a NULL group label and
   left detection: 45.6% of distinct client IPs and 36.8% of measurements on
   2026-08-07.

2. Phase A0's lookback was sized to the batch span, but step 02's baseline spans
   ``BASELINE_DAYS`` before the target day and groups those days by client geography
   too. Baseline days were geolocated at 17-24% of measurements while the target day
   was at 100%, so the comparison was not like-for-like.
"""

from __future__ import annotations

import inspect
import re

import pytest

from hermes.pipeline import union
from hermes.sql.loader import load_query

DETECTION_STEPS = [
    "02_detect_anomalies_union.sql",
    "03_build_transient_events_union.sql",
]


@pytest.mark.parametrize("step", DETECTION_STEPS)
def test_detection_reads_both_client_geoloc_families(step):
    sql = load_query(step, {"DAY": "2026-08-07", "ONE_WEEK_EARLIER": "2026-07-31"})
    assert "unified_src_ip_to_geoloc`" in sql, f"{step} lost the IPv4 client geoloc table"
    assert "unified_src_ip_to_geoloc_ipv6`" in sql, (
        f"{step} does not read the IPv6 client geoloc table — every IPv6 client "
        "would be grouped under NULL and dropped from detection"
    )


@pytest.mark.parametrize("step", DETECTION_STEPS)
def test_client_geoloc_union_is_disjoint_not_joined(step):
    """UNION ALL is correct because an address belongs to exactly one family."""
    sql = load_query(step, {"DAY": "2026-08-07", "ONE_WEEK_EARLIER": "2026-07-31"})
    block = sql[
        sql.index("unified_src_ip_to_geoloc`") : sql.index("unified_src_ip_to_geoloc_ipv6`")
    ]
    assert "UNION ALL" in block


def test_baseline_days_is_shared_between_sql_params_and_phase_a0():
    """One constant, so the SQL window and A0's lookback cannot drift apart."""
    assert union.BASELINE_DAYS == 7

    params_src = inspect.getsource(union._run_sql_steps)
    assert "timedelta(days=BASELINE_DAYS)" in params_src, (
        "ONE_WEEK_EARLIER must derive from BASELINE_DAYS, not a bare literal"
    )
    assert not re.search(r"timedelta\(days=7\)", params_src)


def test_phase_a0_lookback_covers_the_baseline_window():
    src = inspect.getsource(union.run_dates)
    assert "+ BASELINE_DAYS" in src, (
        "Phase A0's lookback must extend past the batch to cover step 02's baseline; "
        "otherwise baseline days are grouped on a fraction of their traffic"
    )

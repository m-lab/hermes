"""Step 04's GIGA subset uses the same school-IP rule as the production correction.

scripts/correct_giga_client_name.py (run on prod 2025-11-01..2026-10-19) keeps a row
whose IP is listed for ANY month the trace window DAY-8..DAY touches. Step 04 used
only DAY's month, so re-running an early-month date (e.g. a backfill) would drop the
late-previous-month school traces that the correction kept (~1,000 rows on 2026-10-03).
"""

import re
from pathlib import Path

from hermes.sql import loader

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "correct_giga_client_name.py"


def _giga_predicate() -> str:
    sql = loader.load_query("04_mapping_union.sql", {"DAY": "2026-10-03", "DS": "hermes_union"})
    tail = sql[sql.index("INSERT INTO `mlab-collaboration.hermes_union.giga_meter_measurements`") :]
    return tail[
        tail.index("FROM _mapping_result") : tail.index("-- Write each measurement exactly ONCE")
    ]


def test_school_list_spans_every_month_of_the_trace_window():
    pred = _giga_predicate()
    assert "client_name = 'giga-meter'" in pred
    assert re.search(
        r"month_start BETWEEN DATE_TRUNC\(DATE_SUB\(CAST\('2026-10-03' AS DATE\), "
        r"INTERVAL 8 DAY\), MONTH\)\s+"
        r"AND DATE_TRUNC\(CAST\('2026-10-03' AS DATE\), MONTH\)",
        pred,
    )
    assert "month_start = DATE_TRUNC" not in pred


def test_window_matches_the_correction_script():
    src = SCRIPT.read_text()
    assert "OWN_LOOKBACK_DAYS = 8" in src
    assert "BETWEEN DATE_TRUNC(DATE_SUB(@d, INTERVAL {OWN_LOOKBACK_DAYS} DAY), MONTH)" in src

"""Tests for the anomaly-detection baseline-coverage check."""

from __future__ import annotations

import datetime as dt

from hermes.pipeline.union import baseline_fill_dates, dates_missing_baseline

DAY = dt.date(2026, 6, 9)
# The 7-day baseline window for June 9 is June 2..June 8.
WINDOW = {DAY - dt.timedelta(days=i) for i in range(1, 8)}


def test_isolated_date_has_empty_baseline():
    # Only June 1 present (8 days before -> outside the window): nothing usable.
    missing = dates_missing_baseline([DAY], {dt.date(2026, 6, 1)})
    assert missing[DAY] == 7


def test_full_baseline_present():
    missing = dates_missing_baseline([DAY], set(WINDOW))
    assert missing[DAY] == 0


def test_thin_baseline_counts_only_present():
    # 2 of the 7 window days present.
    present = {dt.date(2026, 6, 8), dt.date(2026, 6, 7)}
    assert dates_missing_baseline([DAY], present)[DAY] == 5


def test_scheduled_earlier_dates_do_not_count_as_present():
    # Two contiguous run dates, but neither is yet in the source. Phase A is
    # parallel, so an earlier scheduled date is NOT a usable baseline.
    d1, d2 = dt.date(2026, 6, 9), dt.date(2026, 6, 10)
    missing = dates_missing_baseline([d1, d2], set())
    assert missing[d1] == 7
    assert missing[d2] == 7


def test_fill_isolated_date_returns_target_plus_window():
    # Nothing present: must fill the target AND its 7-day window = 8 dates.
    fill = baseline_fill_dates([DAY], set())
    assert fill == sorted({DAY} | WINDOW)
    assert len(fill) == 8
    assert min(fill) == dt.date(2026, 6, 2)  # DAY - 7
    assert max(fill) == DAY


def test_fill_excludes_present_days():
    # Window already present -> only the target needs filling.
    fill = baseline_fill_dates([DAY], set(WINDOW))
    assert fill == [DAY]


def test_fill_contiguous_range_dedupes_overlapping_windows():
    targets = [dt.date(2026, 6, 8), dt.date(2026, 6, 9)]
    # Present through June 1; June 2-9 (targets + their windows back to June 1) missing.
    present = {dt.date(2026, 5, 25) + dt.timedelta(days=i) for i in range(8)}  # May25-Jun1
    fill = baseline_fill_dates(targets, present)
    # needed = June1..June9 minus present(<=June1) = June2..June9
    assert fill == [dt.date(2026, 6, d) for d in range(2, 10)]

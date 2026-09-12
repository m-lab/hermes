"""Tests for RouteViews refresh failure reporting and the IPv6 date fallback.

Both behaviours were absent, and together they let the IPv6 BGP table silently
stop refreshing: CAIDA publishes rv6 with a lag, the IPv6 downloader only ever
tried the exact requested date, and the failure was logged but never surfaced --
so the job exited 0 and reported success every time.
"""

from __future__ import annotations

import pytest

from hermes.enrichment.routeviews.enricher_ipv6 import RouteViewsEnricherIPv6
from hermes.pipeline import refresh_topology_tables as rtt


def _bare_v6() -> RouteViewsEnricherIPv6:
    """An enricher instance without touching BigQuery or the filesystem."""
    e = object.__new__(RouteViewsEnricherIPv6)
    e.cache_dir = "/nonexistent"
    e.base_url = "https://example.invalid/"
    return e


# --- IPv6 nearby-date fallback -------------------------------------------------


def test_v6_uses_exact_date_when_available(monkeypatch):
    e = _bare_v6()
    monkeypatch.setattr(
        RouteViewsEnricherIPv6,
        "_try_download_for_date",
        lambda self, d: "/tmp/rv6" if d == "20260912" else None,
    )
    assert e.download_routeviews_dataset("2026-09-12") == ("/tmp/rv6", "2026-09-12")


def test_v6_falls_back_to_the_previous_day(monkeypatch):
    # The real failure: CAIDA had not published rv6 for 2026-09-12 yet, but
    # 2026-09-11 was there. IPv4 found it; IPv6 gave up.
    e = _bare_v6()
    monkeypatch.setattr(
        RouteViewsEnricherIPv6,
        "_try_download_for_date",
        lambda self, d: "/tmp/rv6" if d == "20260911" else None,
    )
    assert e.download_routeviews_dataset("2026-09-12") == ("/tmp/rv6", "2026-09-11")


def test_v6_prefers_earlier_dates_over_later_ones(monkeypatch):
    e = _bare_v6()
    monkeypatch.setattr(
        RouteViewsEnricherIPv6,
        "_try_download_for_date",
        lambda self, d: "/tmp/rv6" if d in ("20260911", "20260913") else None,
    )
    _, actual = e.download_routeviews_dataset("2026-09-12")
    assert actual == "2026-09-11"


def test_v6_gives_up_outside_the_window(monkeypatch):
    e = _bare_v6()
    monkeypatch.setattr(RouteViewsEnricherIPv6, "_try_download_for_date", lambda self, d: None)
    assert e.download_routeviews_dataset("2026-09-12", max_days_lookback=3) is None


def test_v6_accepts_undashed_dates(monkeypatch):
    e = _bare_v6()
    monkeypatch.setattr(
        RouteViewsEnricherIPv6,
        "_try_download_for_date",
        lambda self, d: "/tmp/rv6" if d == "20260912" else None,
    )
    assert e.download_routeviews_dataset("20260912") == ("/tmp/rv6", "2026-09-12")


def test_v6_process_date_returns_false_when_nothing_downloads(monkeypatch):
    e = _bare_v6()
    monkeypatch.setattr(
        RouteViewsEnricherIPv6, "download_routeviews_dataset", lambda self, d, m=7: None
    )
    assert e.process_date("2026-09-12") is False


def test_v6_process_date_stamps_the_date_the_data_came_from(monkeypatch):
    seen = {}
    e = _bare_v6()
    monkeypatch.setattr(
        RouteViewsEnricherIPv6,
        "download_routeviews_dataset",
        lambda self, d, m=7: ("/tmp/rv6", "2026-09-11"),
    )
    monkeypatch.setattr(
        RouteViewsEnricherIPv6,
        "process_routeviews_data",
        lambda self, path, date: seen.setdefault("date", date) and [],
    )
    monkeypatch.setattr(RouteViewsEnricherIPv6, "upload_to_bigquery", lambda self, data: True)
    monkeypatch.setattr(
        RouteViewsEnricherIPv6, "has_rows_for_date", lambda self, t, d, source=None: False
    )
    e.project_id = "proj"
    e.process_date("2026-09-12")
    # Must be the file's date, not the requested one -- the closest-snapshot rule
    # in 04_mapping_union.sql compares partition_date against ${DAY}.
    assert seen["date"] == "2026-09-11"


# --- failure propagation -------------------------------------------------------


class _Enricher:
    """Stand-in whose process_date result is fixed by the constructor arg."""

    def __init__(self, result):
        self._result = result

    def __call__(self, _project_id):
        return self

    def process_date(self, _date, force=False):
        return self._result


def test_refresh_bgp_raises_when_ipv6_fails(monkeypatch):
    monkeypatch.setattr(rtt, "RouteViewsEnricher", _Enricher(True))
    monkeypatch.setattr(rtt, "RouteViewsEnricherIPv6", _Enricher(False))
    with pytest.raises(RuntimeError, match="IPv6"):
        rtt.refresh_bgp("2026-09-12", "proj", do_v4=True, do_v6=True)


def test_refresh_bgp_raises_when_ipv4_fails(monkeypatch):
    monkeypatch.setattr(rtt, "RouteViewsEnricher", _Enricher(False))
    monkeypatch.setattr(rtt, "RouteViewsEnricherIPv6", _Enricher(True))
    with pytest.raises(RuntimeError, match="IPv4"):
        rtt.refresh_bgp("2026-09-12", "proj", do_v4=True, do_v6=True)


def test_refresh_bgp_is_quiet_when_both_succeed(monkeypatch):
    monkeypatch.setattr(rtt, "RouteViewsEnricher", _Enricher(True))
    monkeypatch.setattr(rtt, "RouteViewsEnricherIPv6", _Enricher(True))
    rtt.refresh_bgp("2026-09-12", "proj", do_v4=True, do_v6=True)


def test_refresh_bgp_ignores_families_not_requested(monkeypatch):
    # do_v6=False must not turn a broken v6 into a failure.
    monkeypatch.setattr(rtt, "RouteViewsEnricher", _Enricher(True))
    monkeypatch.setattr(rtt, "RouteViewsEnricherIPv6", _Enricher(False))
    rtt.refresh_bgp("2026-09-12", "proj", do_v4=True, do_v6=False)


# --- upload result -------------------------------------------------------------


def test_empty_upload_is_not_success():
    # An empty parse must not be reported as a successful refresh.
    assert RouteViewsEnricherIPv6.upload_to_bigquery(_bare_v6(), []) is False


# --- re-run guard ---------------------------------------------------------------


def _guarded_v6(has_rows: bool, uploaded: list):
    """A v6 enricher whose date resolves fine and whose upload is recorded."""
    e = _bare_v6()
    e.project_id = "proj"
    e.has_rows_for_date = lambda table, date, source=None: has_rows  # type: ignore[method-assign]
    e.download_routeviews_dataset = lambda d, m=7: ("/tmp/rv6", "2026-09-11")  # type: ignore[method-assign]
    e.process_routeviews_data = lambda path, date: [{"ip_prefix": "x", "asn": 1}]  # type: ignore[method-assign]
    e.upload_to_bigquery = lambda data: uploaded.append(data) or True  # type: ignore[method-assign,func-returns-value]
    return e


def test_existing_date_is_skipped_not_re_uploaded():
    # The 2026-09-12 incident: running the refresh twice in one evening appended
    # a second copy of the same snapshot (1,131,314 duplicate v4 rows).
    uploaded: list = []
    assert _guarded_v6(has_rows=True, uploaded=uploaded).process_date("2026-09-12") is True
    assert uploaded == [], "must not upload when the date already has rows"


def test_missing_date_is_uploaded():
    uploaded: list = []
    assert _guarded_v6(has_rows=False, uploaded=uploaded).process_date("2026-09-12") is True
    assert len(uploaded) == 1


def test_force_overrides_the_guard():
    uploaded: list = []
    e = _guarded_v6(has_rows=True, uploaded=uploaded)
    assert e.process_date("2026-09-12", force=True) is True
    assert len(uploaded) == 1, "force=True must re-upload (for a deliberate re-ingest)"


def test_guard_is_checked_on_the_resolved_date_not_the_requested_one(monkeypatch):
    # The fallback means requested != stamped. Guarding on the requested date
    # would miss a collision with the snapshot actually being written.
    seen = {}
    e = _bare_v6()
    e.project_id = "proj"
    e.has_rows_for_date = lambda table, date, source=None: seen.setdefault("date", date) and False  # type: ignore[method-assign]
    e.download_routeviews_dataset = lambda d, m=7: ("/tmp/rv6", "2026-09-11")  # type: ignore[method-assign]
    e.process_routeviews_data = lambda path, date: []  # type: ignore[method-assign]
    e.upload_to_bigquery = lambda data: True  # type: ignore[method-assign]
    e.process_date("2026-09-12")
    assert seen["date"] == "2026-09-11"


def test_guard_restricts_to_the_routeviews_source():
    # The tables also hold IXP rows for the same dates; the guard must not treat
    # an IXP row as proof the BGP snapshot is present.
    seen = {}
    e = _bare_v6()
    e.project_id = "proj"
    e.has_rows_for_date = lambda table, date, source=None: (
        seen.update(  # type: ignore[method-assign]
            {"source": source, "table": table}
        )
        or False
    )
    e.download_routeviews_dataset = lambda d, m=7: ("/tmp/rv6", "2026-09-11")  # type: ignore[method-assign]
    e.process_routeviews_data = lambda path, date: []  # type: ignore[method-assign]
    e.upload_to_bigquery = lambda data: True  # type: ignore[method-assign]
    e.process_date("2026-09-12")
    assert seen["source"] == "RouteViews"
    assert seen["table"] == "proj.hermes.unified_ip_to_as_ipv6"

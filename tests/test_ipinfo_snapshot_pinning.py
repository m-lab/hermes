"""A run must geolocate with the IPInfo dump closest to the date it is processing.

IPInfo geolocation is a point-in-time assertion about where an address is. Taking the
newest dump for a backfill silently reassigns year-old traffic to wherever the address
space moved *since* — an address reallocated between countries lands in the wrong
country for every historical measurement, and nothing in the output says so.

Pinning does not make historical geolocation correct; no 2025 dump exists locally
(the oldest is 2026-06-12). It makes it *least-wrong and stated*: the chosen dump is
recorded per row, so the gap between measurement and geolocation is queryable.
"""

from __future__ import annotations

import json
import os
from datetime import date
from unittest import mock

from hermes.enrichment.ipinfo.enricher import IPInfoEnricher, closest_snapshot, latest_snapshot


def _snap(cache: str, day: str) -> str:
    path = os.path.join(cache, f"ipinfo_{day}.snapshot")
    with open(path, "w") as fh:
        fh.write("x")
    return path


def test_closest_snapshot_picks_nearest_not_newest(tmp_path):
    cache = str(tmp_path)
    old = _snap(cache, "2026-06-12")
    _snap(cache, "2026-08-15")

    # A July 2025 backfill: every dump is later, so the nearest is the oldest.
    assert closest_snapshot(cache, date(2025, 7, 31)) == old
    # A nightly: the nearest is today's.
    assert closest_snapshot(cache, date(2026, 8, 15)).endswith("ipinfo_2026-08-15.snapshot")


def test_closest_snapshot_spans_both_directions(tmp_path):
    cache = str(tmp_path)
    _snap(cache, "2026-06-01")
    mid = _snap(cache, "2026-07-01")
    _snap(cache, "2026-09-01")
    assert closest_snapshot(cache, date(2026, 6, 28)) == mid


def test_closest_snapshot_tie_breaks_to_earlier(tmp_path):
    """Equidistant dumps must not resolve by filesystem order."""
    cache = str(tmp_path)
    earlier = _snap(cache, "2026-07-01")
    _snap(cache, "2026-07-11")
    assert closest_snapshot(cache, date(2026, 7, 6)) == earlier


def test_snapshot_helpers_ignore_unparseable_names(tmp_path):
    cache = str(tmp_path)
    with open(os.path.join(cache, "ip_info.checksums"), "w") as fh:
        fh.write("{}")
    with open(os.path.join(cache, "ipinfo_partial.snapshot"), "w") as fh:
        fh.write("x")
    assert closest_snapshot(cache, date(2026, 8, 1)) is None
    assert latest_snapshot(cache) is None

    good = _snap(cache, "2026-08-11")
    assert closest_snapshot(cache, date(2026, 8, 1)) == good


def _enricher(cache: str, target: date | None) -> IPInfoEnricher:
    e = IPInfoEnricher.__new__(IPInfoEnricher)
    e.cache_dir = cache
    e.ipinfo_token = "token"
    e.snapshot_date = target
    return e


def test_backfill_pins_to_oldest_dump_not_todays(tmp_path):
    cache = str(tmp_path)
    oldest = _snap(cache, "2026-06-12")
    _snap(cache, "2026-08-15")
    checksums = {"checksums": {"md5": "m", "sha1": "s", "sha256": "SAME"}}
    with open(os.path.join(cache, "ip_info.checksums"), "w") as fh:
        json.dump(checksums, fh)

    response = mock.Mock()
    response.json.return_value = checksums
    with (
        mock.patch("hermes.enrichment.ipinfo.enricher.requests.get", return_value=response),
        mock.patch("hermes.enrichment.ipinfo.enricher.subprocess.run"),
    ):
        path = _enricher(cache, date(2025, 7, 31))._download_ipinfo_database()

    assert path == oldest, "a 2025 backfill must not use the 2026-08 dump"


def test_nightly_behaviour_is_unchanged(tmp_path):
    """With no target date the run still uses the newest dump."""
    cache = str(tmp_path)
    _snap(cache, "2026-06-12")
    newest = _snap(cache, "2026-08-15")
    checksums = {"checksums": {"md5": "m", "sha1": "s", "sha256": "SAME"}}
    with open(os.path.join(cache, "ip_info.checksums"), "w") as fh:
        json.dump(checksums, fh)

    response = mock.Mock()
    response.json.return_value = checksums
    with (
        mock.patch("hermes.enrichment.ipinfo.enricher.requests.get", return_value=response),
        mock.patch("hermes.enrichment.ipinfo.enricher.subprocess.run"),
        mock.patch("hermes.enrichment.ipinfo.enricher.datetime") as dt,
    ):
        dt.now.return_value.date.return_value = date(2026, 8, 15)
        dt.now.return_value.strftime.return_value = "2026-08-15"
        path = _enricher(cache, None)._download_ipinfo_database()

    assert path == newest

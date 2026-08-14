"""The IPInfo snapshot path must always name a file that exists.

`_download_ipinfo_database` downloads only when IPInfo's published checksum differs
from the local one, but names the destination for *today* unconditionally. When
IPInfo has not republished since the last local download, nothing is written and the
returned path points at a file that was never created. `maxminddb.open_database` then
fails, `self.reader` is set to None, and every lookup silently returns empty.

This was not hypothetical: it took out Phase A0 of the 2026-08-12 04:26 UTC staging
run. Production had never hit it because the nightly runs at 15:00 UTC, after the
08:18 UTC refresh — so the bug was invisible for as long as nothing ran off-schedule.
"""

from __future__ import annotations

import os
from unittest import mock

from hermes.enrichment.ipinfo.enricher import IPInfoEnricher, latest_snapshot


def _snap(cache: str, day: str) -> str:
    path = os.path.join(cache, f"ipinfo_{day}.snapshot")
    with open(path, "w") as fh:
        fh.write("x")
    return path


def test_latest_snapshot_orders_by_filename_not_ctime(tmp_path):
    """A stale file touched recently must not outrank a newer one.

    Ordering by `getctime` is what the code used to do; restoring a backup or
    `docker cp`-ing an old snapshot silently selects months-old geolocation data.
    """
    cache = str(tmp_path)
    old = _snap(cache, "2026-01-05")
    new = _snap(cache, "2026-08-11")
    # Make the OLD file the most recently changed inode.
    os.utime(old, (2_000_000_000, 2_000_000_000))

    assert latest_snapshot(cache) == new


def test_latest_snapshot_ignores_non_snapshot_files(tmp_path):
    cache = str(tmp_path)
    with open(os.path.join(cache, "ip_info.checksums"), "w") as fh:
        fh.write("{}")
    assert latest_snapshot(cache) is None

    expected = _snap(cache, "2026-08-11")
    assert latest_snapshot(cache) == expected


def test_unchanged_checksum_falls_back_to_existing_snapshot(tmp_path):
    """The regression itself: no new publication must not yield a missing path."""
    cache = str(tmp_path)
    existing = _snap(cache, "2026-08-11")
    checksums = {"checksums": {"md5": "m", "sha1": "s", "sha256": "IDENTICAL"}}
    with open(os.path.join(cache, "ip_info.checksums"), "w") as fh:
        import json

        json.dump(checksums, fh)

    enricher = IPInfoEnricher.__new__(IPInfoEnricher)
    enricher.cache_dir = cache
    enricher.ipinfo_token = "token"

    response = mock.Mock()
    response.json.return_value = checksums
    with mock.patch("hermes.enrichment.ipinfo.enricher.requests.get", return_value=response):
        with mock.patch("hermes.enrichment.ipinfo.enricher.subprocess.run") as run:
            path = enricher._download_ipinfo_database()

    run.assert_not_called()  # checksum matched, so no download was attempted
    assert path == existing
    assert os.path.exists(path), "returned path must name a file that exists"

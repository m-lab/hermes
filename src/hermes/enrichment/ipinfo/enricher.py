import json
import os
import subprocess
from datetime import UTC, date, datetime
from typing import Any

import maxminddb
import requests

from hermes.enrichment.utils.common import BaseEnrichment, logger


def latest_snapshot(cache_dir: str) -> str | None:
    """Newest ``ipinfo_YYYY-MM-DD.snapshot`` present in ``cache_dir``, or None.

    Ordered by the date **in the filename**, not ``getctime``: ctime reflects
    when the inode was last changed on this host, so a restored backup, a
    ``docker cp`` or a touch reorders the snapshots and can select a months-old
    database while a current one sits next to it. The filename is the only
    record of which day the data describes.
    """
    snapshots = sorted(_snapshot_dates(cache_dir))
    return os.path.join(cache_dir, snapshots[-1][1]) if snapshots else None


def _snapshot_dates(cache_dir: str) -> list[tuple[date, str]]:
    """``(date, filename)`` for every parseable snapshot in ``cache_dir``.

    Files whose name does not carry a valid date are skipped rather than raising:
    the cache also holds ``ip_info.checksums`` and partial ``wget`` output.
    """
    out: list[tuple[date, str]] = []
    for f in os.listdir(cache_dir):
        if not (f.startswith("ipinfo_") and f.endswith(".snapshot")):
            continue
        try:
            out.append((date.fromisoformat(f[len("ipinfo_") : -len(".snapshot")]), f))
        except ValueError:
            continue
    return out


def closest_snapshot(cache_dir: str, target: date) -> str | None:
    """Snapshot nearest in time to ``target``, or None if the cache is empty.

    IPInfo geolocation is a point-in-time assertion about where an address is. Using
    today's dump to place addresses seen a year ago silently reassigns them to
    wherever the space moved *since*, which is a fabricated answer for the date
    being processed -- an address reallocated from one country to another lands in
    the wrong country for every historical measurement.

    So a run is pinned to the dump closest to the date it is processing. For a
    nightly that is today's dump, exactly as before. For a backfill it is the oldest
    dump still on disk, which is the least-wrong available answer and, crucially, a
    *stated* one: the chosen filename is recorded per row so the gap between the
    measurement and the geolocation is queryable rather than invisible.

    Ties (equidistant dumps either side) resolve to the earlier one, so the choice
    does not depend on filesystem ordering.
    """
    snapshots = _snapshot_dates(cache_dir)
    if not snapshots:
        return None
    best = min(snapshots, key=lambda ds: (abs((ds[0] - target).days), ds[0]))
    return os.path.join(cache_dir, best[1])


class IPInfoEnricher(BaseEnrichment):
    def __init__(self, project_id: str = "mlab-collaboration", snapshot_date: date | None = None):
        """Initialize IPInfo enricher.

        ``snapshot_date`` is the date being *processed*. The MMDB used is the dump
        closest to it (see :func:`closest_snapshot`), not simply the newest one, so a
        backfill does not geolocate old traffic with new data. Defaults to today,
        which reproduces the previous behaviour for a nightly run.
        """
        super().__init__(project_id)
        self.ipinfo_token = os.getenv("IPINFO_TOKEN")
        self.ipinfo_db_path = None
        #: Basename of the MMDB actually used, recorded per row for provenance.
        self.snapshot_name: str | None = None
        self.snapshot_date = snapshot_date
        self.state_mapping = {}
        if not self.ipinfo_token:
            logger.warning(
                "IPINFO_TOKEN not found in environment variables. IPInfo lookups will be disabled."
            )
        else:
            self.ipinfo_db_path = self._download_ipinfo_database()
            self.snapshot_name = os.path.basename(self.ipinfo_db_path)
            try:
                self.reader = maxminddb.open_database(self.ipinfo_db_path)
            except Exception as e:
                logger.error(f"Failed to open IPInfo database: {e}")
                self.reader = None

    def _download_ipinfo_database(self) -> str:
        """Download and manage IPInfo database."""
        checksum_file = f"{self.cache_dir}/ip_info.checksums"
        if os.path.exists(checksum_file):
            with open(checksum_file) as f:
                current_checksums = json.load(f)
        else:
            current_checksums = {"checksums": {"md5": "", "sha1": "", "sha256": ""}}

        today_str = datetime.now(UTC).strftime("%Y-%m-%d")
        geolocation_ofile = f"{self.cache_dir}/ipinfo_{today_str}.snapshot"

        try:
            new_checksums = requests.get(
                f"https://ipinfo.io/data/standard_location.mmdb/checksums?token={self.ipinfo_token}"
            ).json()

            if new_checksums["checksums"]["sha256"] != current_checksums["checksums"]["sha256"]:
                ipinfo_url = (
                    f"https://ipinfo.io/data/standard_location.mmdb?token={self.ipinfo_token}"
                )
                cmd = ["wget", ipinfo_url, "-O", geolocation_ofile]
                logger.info(f"Running {' '.join(cmd)}")
                subprocess.run(cmd, check=True)

                with open(checksum_file, "w") as f:
                    json.dump(new_checksums, f)
        except Exception as e:
            logger.error(f"Error downloading IPInfo database: {e}")

        # The download above is conditional on the checksum having changed, but
        # `geolocation_ofile` is named for *today* unconditionally. When IPInfo has
        # not republished since the last local download, nothing is written and this
        # path names a file that does not exist -- `open_database` then fails and
        # every lookup silently returns empty. That is why this check is on the
        # normal path and not only in the `except` branch above: the failure mode is
        # a no-op download, not an error. Hit on 2026-08-12 at 04:26 UTC, before the
        # daily 08:18 refresh; production never saw it only because the nightly runs
        # at 15:00 UTC, after the refresh.
        if not os.path.exists(geolocation_ofile):
            fallback = latest_snapshot(self.cache_dir)
            if fallback:
                logger.warning(
                    "IPInfo snapshot %s absent (no new publication today); falling back to %s",
                    os.path.basename(geolocation_ofile),
                    os.path.basename(fallback),
                )
                geolocation_ofile = fallback

        # The cache is now as fresh as it can be; choose the dump closest to the date
        # being processed rather than the newest one. For a nightly these are the same
        # file. For a backfill they are not, and taking the newest would place old
        # addresses using new data -- see closest_snapshot().
        target = self.snapshot_date or datetime.now(UTC).date()
        pinned = closest_snapshot(self.cache_dir, target)
        if pinned and pinned != geolocation_ofile:
            gap = abs((date.fromisoformat(os.path.basename(pinned)[7:-9]) - target).days)
            logger.warning(
                "IPInfo pinned to %s for target date %s (%d day gap) instead of %s — "
                "geolocation is as of the dump, NOT as of the measurement",
                os.path.basename(pinned),
                target,
                gap,
                os.path.basename(geolocation_ofile),
            )
            geolocation_ofile = pinned

        return geolocation_ofile

    def get_geolocation(self, ip: str) -> dict[str, Any]:
        """Get geolocation data from IPInfo."""
        geo_data = {
            "lat": None,
            "lon": None,
            "city": None,
            "country": None,
            "region": None,
            "score": 80,
        }

        if self.reader:
            try:
                response = self.reader.get(ip)
                if response:
                    # print(response)
                    geo_data.update(
                        {
                            "lat": response.get("lat"),
                            "lon": response.get("lng"),
                            "city": response.get("city"),
                            "country": response.get("country"),
                            "region": response.get("region"),
                        }
                    )
            except Exception as e:
                logger.warning(f"IPInfo lookup failed for {ip}: {e}")

        return geo_data

    def fill_state_mapping(self) -> None:
        """Fill state mapping from IPInfo database."""
        if not self.reader:
            logger.warning("IPInfo reader is not initialized. Cannot fill state mapping.")
            return
        for ip in self.reader.get_all():
            response = self.reader.get(ip)
            if not response:
                continue
            # Assuming response contains 'region', 'country', and 'region_code'
            if "region" in response and "country" in response and "region_code" in response:
                self.state_mapping[
                    response.get("region", "NA") + "-" + response.get("country", "NA")
                ] = response.get("region_code", "NA")


def test_ipinfo_geolocation():
    enricher = IPInfoEnricher()
    test_ip = "194.183.152.39"
    result = enricher.get_geolocation(test_ip)

    print(f"Geolocation for {test_ip}: {result}")

    assert result["lat"] is not None, "Latitude should not be None"
    assert result["lon"] is not None, "Longitude should not be None"
    assert result["city"] is not None, "City should not be None"
    assert result["country"] is not None, "Country should not be None"
    assert result["region"] is not None, "Region should not be None"
    assert isinstance(result["score"], int), "Score should be an integer"


if __name__ == "__main__":
    # test_ipinfo_geolocation()
    enricher = IPInfoEnricher()
    enricher.fill_state_mapping()
    print(enricher.state_mapping)

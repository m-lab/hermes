import json
import os
import subprocess
from datetime import UTC, datetime
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
    snapshots = sorted(
        f for f in os.listdir(cache_dir) if f.startswith("ipinfo_") and f.endswith(".snapshot")
    )
    return os.path.join(cache_dir, snapshots[-1]) if snapshots else None


class IPInfoEnricher(BaseEnrichment):
    def __init__(self, project_id: str = "mlab-collaboration"):
        """Initialize IPInfo enricher."""
        super().__init__(project_id)
        self.ipinfo_token = os.getenv("IPINFO_TOKEN")
        self.ipinfo_db_path = None
        self.state_mapping = {}
        if not self.ipinfo_token:
            logger.warning(
                "IPINFO_TOKEN not found in environment variables. IPInfo lookups will be disabled."
            )
        else:
            self.ipinfo_db_path = self._download_ipinfo_database()
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

        date = datetime.now(UTC).strftime("%Y-%m-%d")
        geolocation_ofile = f"{self.cache_dir}/ipinfo_{date}.snapshot"

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

import json
import os
import subprocess
from datetime import UTC, datetime
from typing import Any

import maxminddb
import requests

from hermes.enrichment.utils.common import BaseEnrichment, logger


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
            # Find latest snapshot if download fails
            latest_snapshot = max(
                [f for f in os.listdir(self.cache_dir) if f.startswith("ipinfo_")],
                key=lambda x: os.path.getctime(os.path.join(self.cache_dir, x)),
            )
            geolocation_ofile = os.path.join(self.cache_dir, latest_snapshot)

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

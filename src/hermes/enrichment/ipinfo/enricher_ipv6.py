import ipaddress
import json
import os
import subprocess
from datetime import UTC, datetime
from typing import Any

import maxminddb
import requests

from hermes.enrichment.ipinfo.enricher import latest_snapshot
from hermes.enrichment.utils.common import BaseEnrichment, logger


class IPInfoEnricherIPv6(BaseEnrichment):
    def __init__(self, project_id: str = "mlab-collaboration"):
        """Initialize IPInfo enricher for IPv6 addresses."""
        super().__init__(project_id)
        self.ipinfo_token = os.getenv("IPINFO_TOKEN")
        self.ipinfo_db_path = None

        if not self.ipinfo_token:
            logger.warning(
                "IPINFO_TOKEN not found in environment variables. IPv6 IPInfo lookups will be disabled."
            )
        else:
            self.ipinfo_db_path = self._download_ipinfo_database()
            try:
                self.reader = maxminddb.open_database(self.ipinfo_db_path)
            except Exception as e:
                logger.error(f"Failed to open IPv6 IPInfo database: {e}")
                self.reader = None

    def _download_ipinfo_database(self) -> str:
        """Use existing IPInfo database (same for IPv4 and IPv6)."""
        # Look for existing IPInfo database file
        existing = latest_snapshot(self.cache_dir)
        if existing:
            logger.info(f"Using existing IPInfo database for IPv6: {existing}")
            return existing

        # If no existing file, download one (same as IPv4)
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
                logger.info(f"Downloading IPInfo database (shared for IPv4/IPv6): {' '.join(cmd)}")
                subprocess.run(cmd, check=True)

                with open(checksum_file, "w") as f:
                    json.dump(new_checksums, f)
        except Exception as e:
            logger.error(f"Error downloading IPInfo database: {e}")

        # Same conditional-download hazard as the IPv4 enricher; see the comment there.
        if not os.path.exists(geolocation_ofile):
            fallback = latest_snapshot(self.cache_dir)
            if fallback:
                geolocation_ofile = fallback

        return geolocation_ofile

    def is_ipv6(self, ip: str) -> bool:
        """Check if the given IP is IPv6."""
        try:
            ip_obj = ipaddress.ip_address(ip)
            return isinstance(ip_obj, ipaddress.IPv6Address)
        except ValueError:
            return False

    def get_geolocation(self, ip: str) -> dict[str, Any]:
        """Get geolocation data from IPInfo for IPv6 addresses."""
        # Verify this is an IPv6 address
        if not self.is_ipv6(ip):
            logger.warning(f"IP {ip} is not IPv6, skipping IPv6 enricher")
            return None

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
                    logger.debug(f"IPv6 IPInfo response for {ip}: {response}")
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
                logger.warning(f"IPv6 IPInfo lookup failed for {ip}: {e}")

        return geo_data

    def get_geolocation_batch(self, ips: list) -> dict[str, dict[str, Any]]:
        """Get geolocation data for a batch of IPv6 addresses."""
        results = {}

        for ip in ips:
            if self.is_ipv6(ip):
                results[ip] = self.get_geolocation(ip)
            else:
                logger.debug(f"Skipping non-IPv6 address: {ip}")

        return results


def test_ipinfo_ipv6_geolocation():
    enricher = IPInfoEnricherIPv6()

    # Test with IPv6 addresses
    test_ips = [
        "2001:4860:4860::8888",  # Google DNS
        "2606:4700:4700::1111",  # Cloudflare DNS
        "2a00:1450:4001:81b::200e",  # Google IPv6
    ]

    for test_ip in test_ips:
        result = enricher.get_geolocation(test_ip)
        print(f"IPv6 Geolocation for {test_ip}: {result}")

        if result:
            assert result["lat"] is not None, "Latitude should not be None"
            assert result["lon"] is not None, "Longitude should not be None"
            assert result["city"] is not None, "City should not be None"
            assert result["country"] is not None, "Country should not be None"
            assert result["region"] is not None, "Region should not be None"
            assert isinstance(result["score"], int), "Score should be an integer"


if __name__ == "__main__":
    test_ipinfo_ipv6_geolocation()

import datetime
import ipaddress
import os
from typing import Any

from hermes.enrichment.utils.common import BaseEnrichment, logger


class RIPEIPMapEnricherIPv6(BaseEnrichment):
    def __init__(self, project_id: str = "mlab-collaboration"):
        """Initialize the RIPE IPMap enricher for IPv6 addresses."""
        super().__init__(project_id)
        self.cache_dir = os.path.join(self.cache_dir, "ripe_ipmap")
        self.min_score = 30

        # Initialize IPv6 mappings
        self.continent_by_ipv6 = {}
        self.country_by_ipv6 = {}
        self.city_by_ipv6 = {}
        self.geo_by_ipv6 = {}
        self.state_by_ipv6 = {}
        self.score_by_ipv6 = {}

        # Track loaded date ranges
        self.loaded_start_date = None
        self.loaded_end_date = None

        # Initialize
        self._ensure_cache_dir()

    def _ensure_cache_dir(self) -> None:
        """Ensure the cache directory exists."""
        os.makedirs(self.cache_dir, exist_ok=True)

    def is_ipv6(self, ip: str) -> bool:
        """Check if the given IP is IPv6."""
        try:
            ip_obj = ipaddress.ip_address(ip)
            return isinstance(ip_obj, ipaddress.IPv6Address)
        except ValueError:
            return False

    def get_geolocation(self, ip: str, date: str = None) -> dict[str, Any]:
        """Get geolocation data for an IPv6 address."""
        # Verify this is an IPv6 address
        if not self.is_ipv6(ip):
            logger.warning(f"IP {ip} is not IPv6, skipping IPv6 enricher")
            return None

        if not date:
            date = datetime.datetime.now().strftime("%Y-%m-%d")

        logger.info(f"IPv6 geolocation lookup for {ip} (not yet implemented)")
        return None

    def get_geolocation_batch(self, ips: list[str], date: str = None) -> dict[str, dict[str, Any]]:
        """Get geolocation data for a batch of IPv6 addresses."""
        results = {}

        # Filter for IPv6 addresses only
        ipv6_ips = [ip for ip in ips if self.is_ipv6(ip)]

        if not ipv6_ips:
            logger.info("No IPv6 addresses in batch")
            return results

        # Get geolocation for each IPv6 address
        for ip in ipv6_ips:
            results[ip] = self.get_geolocation(ip, date)

        return results


def test_ripe_ipv6_lookup():
    enricher = RIPEIPMapEnricherIPv6()

    # Test with IPv6 addresses
    test_ips = [
        "2001:4860:4860::8888",  # Google DNS
        "2606:4700:4700::1111",  # Cloudflare DNS
    ]

    for test_ip in test_ips:
        result = enricher.get_geolocation(test_ip)
        print(f"IPv6 RIPE lookup for {test_ip}: {result}")


if __name__ == "__main__":
    test_ripe_ipv6_lookup()

import bz2
import csv
import datetime
import os
import re
from typing import Any

import wget

from hermes.enrichment.utils.common import BaseEnrichment, logger


class RIPEIPMapEnricher(BaseEnrichment):
    def __init__(self, project_id: str = "mlab-collaboration", cache_dir: str = None):
        """Initialize the RIPE IPMap enricher."""
        super().__init__(project_id)
        if cache_dir:
            self.cache_dir = cache_dir
        else:
            self.cache_dir = os.path.join(self.cache_dir, "ripe_ipmap")
        self.min_score = 30

        # Initialize data structures
        self.country_by_iso2 = {}
        self.continent_by_iso2 = {}
        self.continent_name_by_code = {}

        # Initialize IP mappings
        self.continent_by_ip = {}
        self.country_by_ip = {}
        self.city_by_ip = {}
        self.geo_by_ip = {}
        self.state_by_ip = {}
        self.score_by_ip = {}

        # Track loaded date ranges
        self.loaded_start_date = None
        self.loaded_end_date = None

        # Initialize
        self._ensure_cache_dir()
        self._load_country_codes()

    def _ensure_cache_dir(self) -> None:
        """Ensure the cache directory exists."""
        os.makedirs(self.cache_dir, exist_ok=True)

    def _load_country_codes(self) -> None:
        """Load country and continent codes from ISO file."""
        iso_file = os.path.join(self.cache_dir, "iso_code_2")
        if not os.path.exists(iso_file):
            raise FileNotFoundError(f"ISO codes file not found at {iso_file}")

        with open(iso_file) as f:
            reader = csv.reader(f, delimiter=",", quotechar='"')
            next(reader, None)  # Skip header
            for line in reader:
                continent_name = line[0]
                continent_code = line[1]
                self.continent_name_by_code[continent_code] = continent_name
                country_name = line[2].split(",")[0]
                country_iso2 = line[3]
                self.country_by_iso2[country_iso2] = country_name
                self.continent_by_iso2[country_iso2] = continent_code

    def _download_ripe_ipmap(self, date: datetime.date) -> str:
        """Download and decompress RIPE IPMap data for a given date."""
        date_format = "%Y-%m-%d"
        ipmap_file = f"geolocations_{date.strftime(date_format)}.csv.bz2"
        ipmap_url = f"https://ftp.ripe.net/ripe/ipmap/{ipmap_file}"
        file_path = os.path.join(self.cache_dir, ipmap_file)
        decompressed_path = file_path[:-4]  # Remove .bz2

        # Download if not exists
        if not os.path.exists(file_path):
            logger.info(f"Downloading RIPE IPMap data for {date}")
            wget.download(ipmap_url, out=self.cache_dir)

        # Decompress if not exists
        if not os.path.exists(decompressed_path):
            logger.info(f"Decompressing RIPE IPMap data for {date}")
            with bz2.BZ2File(file_path) as compressed:
                data = compressed.read()
                with open(decompressed_path, "wb") as f:
                    f.write(data)

        return decompressed_path

    def _load_ripe_ipmap_data(self, start_date: datetime.date, end_date: datetime.date) -> None:
        """Load and process RIPE IPMap data for a date range."""
        # Check if we already have this date range loaded
        if (
            self.loaded_start_date
            and self.loaded_end_date
            and start_date >= self.loaded_start_date
            and end_date <= self.loaded_end_date
        ):
            return

        # Clear existing data if we need to load a new range
        if (
            self.loaded_start_date
            and self.loaded_end_date
            and (start_date < self.loaded_start_date or end_date > self.loaded_end_date)
        ):
            logger.info("Clearing cached RIPE IPMap data for new date range")
            self.continent_by_ip.clear()
            self.country_by_ip.clear()
            self.city_by_ip.clear()
            self.geo_by_ip.clear()
            self.state_by_ip.clear()
            self.score_by_ip.clear()

        # Get all RIPE files in date range
        ripe_files = [
            f
            for f in os.listdir(self.cache_dir)
            if f.endswith(".csv") and f.startswith("geolocations_")
        ]

        date_pattern = re.compile(r"\d{4}-\d{2}-\d{2}")
        city_by_ip_candidate = {}

        for ripe_file in sorted(ripe_files):
            # Check date is within range
            match = date_pattern.search(ripe_file)
            if not match:
                continue

            file_date = datetime.datetime.strptime(match.group(), "%Y-%m-%d").date()
            if not (start_date <= file_date <= end_date):
                continue

            logger.info(f"Loading RIPE IPMap file {ripe_file}")
            file_path = os.path.join(self.cache_dir, ripe_file)

            with open(file_path) as f:
                for line in f:
                    tokens = line.split(",")

                    if len(tokens) < 10:
                        tokens = line.split("\t")
                        # logger.warning(f"Skipping malformed line: {line.strip()}")
                        # continue
                    # Handle Washington special case
                    if tokens[1] == "WASHINGTON":
                        line = line.replace("Washington,", "Washington").replace(
                            "WASHINGTON,", "WASHINGTON"
                        )
                        tokens = line.split(",")

                    try:
                        (
                            ip,
                            city_code,
                            city,
                            state,
                            country,
                            country_code_iso2,
                            country_code_iso3,
                            lat,
                            long,
                            score,
                        ) = tokens
                        ip = ip.split("/")[0]

                        if not city:
                            continue

                        lat, long = float(lat), float(long)
                        score = int(float(score.strip("\n")))

                        if score >= self.min_score:
                            if state:
                                city_by_ip_candidate.setdefault(ip, []).append(
                                    f"{city}-{state}-{country_code_iso2}"
                                )
                            else:
                                city_by_ip_candidate.setdefault(ip, []).append(
                                    f"{city}-{country_code_iso2}"
                                )
                            self.country_by_ip[ip] = country_code_iso2
                            self.geo_by_ip[ip] = (lat, long)
                            self.score_by_ip[ip] = score
                            self.state_by_ip[ip] = state

                            if country_code_iso2 in self.continent_by_iso2:
                                self.continent_by_ip[ip] = self.continent_by_iso2[country_code_iso2]

                    except (ValueError, IndexError) as e:
                        logger.warning(f"Error processing line: {line.strip()}, error: {str(e)}")
                        continue

        # Process city candidates
        for ip, cities in city_by_ip_candidate.items():
            self.city_by_ip[ip] = cities[0]  # Take first city for now

        # Update loaded date range
        self.loaded_start_date = start_date
        self.loaded_end_date = end_date
        logger.info(f"Loaded RIPE IPMap data for date range {start_date} to {end_date}")

    def get_geolocation(self, ip: str, date: str = None) -> dict[str, Any]:
        """Get geolocation data for an IP address."""
        # if not date:
        #     date = datetime.datetime.now().strftime("%Y-%m-%d")
        #
        # start_date = datetime.datetime.strptime(date, "%Y-%m-%d").date() - datetime.timedelta(days=8)
        # end_date = datetime.datetime.strptime(date, "%Y-%m-%d").date()
        #
        # # Download any missing files
        # current_date = start_date
        # while current_date < end_date:
        #     self._download_ripe_ipmap(current_date)
        #     current_date += datetime.timedelta(days=1)
        #
        # # Load data if needed
        # self._load_ripe_ipmap_data(start_date, end_date)

        if ip not in self.geo_by_ip:
            return None

        lat, lon = self.geo_by_ip[ip]
        return {
            "city": self.city_by_ip.get(ip),
            "country": self.country_by_ip.get(ip),
            "continent": self.continent_by_ip.get(ip),
            "lat": lat,
            "lon": lon,
            "state": self.state_by_ip.get(ip),
            "score": self.score_by_ip.get(ip),
        }

    def process_date_range(self, start_date: str, end_date: str) -> None:
        """Process RIPE IPMap data for a date range."""
        start = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
        end = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()

        # Download data for each date in range
        current = start
        while current <= end:
            self._download_ripe_ipmap(current)
            current += datetime.timedelta(days=1)

        # Load the data
        self._load_ripe_ipmap_data(start, end)
        logger.info(f"Processed RIPE IPMap data for date range {start_date} to {end_date}")


def test_ripe_lookup():
    """Test RIPE IPMap lookup for a known IP address."""
    enricher = RIPEIPMapEnricher()
    test_ip = "1.0.0.222"  # A random Columbia public IP
    logger.info(f"Testing RIPE IPMap lookup for {test_ip}")
    result = enricher.get_geolocation(test_ip)

    if result:
        print("RIPE IPMap Lookup Result:")
        for key, value in result.items():
            print(f"{key}: {value}")
    else:
        print("Failed to get geolocation from RIPE IPMap.")


def test_ripeipmap_enricher():
    print("Testing RIPEIPMapEnricher for both IPv4 and IPv6 geolocation...")
    enricher = RIPEIPMapEnricher(cache_dir="cache/")
    test_ips = [
        "8.8.8.8",  # IPv4 (Google DNS)
        "1.1.1.1",  # IPv4 (Cloudflare DNS)
        "2001:4860:4860::8888",  # IPv6 (Google DNS)
        "2606:4700:4700::1111",  # IPv6 (Cloudflare DNS)
    ]
    for ip in test_ips:
        result = enricher.get_geolocation(ip)
        print(f"IP: {ip}")
        if result:
            print(f"  City: {result.get('city')}")
            print(f"  Country: {result.get('country')}")
            print(f"  Lat: {result.get('lat')}")
            print(f"  Lon: {result.get('lon')}")
            print(f"  Score: {result.get('score')}")
        else:
            print("  No geolocation found.")
    print("Test complete.")


if __name__ == "__main__":
    test_ripeipmap_enricher()

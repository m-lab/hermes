import ipaddress
import json
import os
import pickle
import time
from datetime import datetime
from typing import Any

import requests
from google.cloud import bigquery
from tqdm import tqdm

from hermes.enrichment.utils.common import BaseEnrichment, logger

HOIHO_URL = "https://api.hoiho.caida.org/lookups"


def load_pickle(file_path):
    """Load and return the object stored in a pickle file."""
    try:
        with open(file_path, "rb") as file:
            data = pickle.load(file)
        logger.info(f"IPv6 pickle file loaded successfully: {file_path}")
        return data
    except FileNotFoundError:
        logger.warning(f"IPv6 pickle file not found: {file_path}. Returning an empty dictionary.")
        return {}
    except pickle.UnpicklingError as e:
        logger.error(f"Invalid IPv6 pickle file: {file_path}. Error: {e}")
        raise


def dump_pickle(obj, file_path):
    """Serialize a Python object and save it to a pickle file."""
    try:
        with open(file_path, "wb") as file:
            pickle.dump(obj, file)
        logger.info(f"IPv6 object successfully saved to: {file_path}")
    except Exception as e:
        logger.error(f"Error saving IPv6 pickle file: {file_path}. Error: {e}")
        raise


class HOIHOEnricherIPv6(BaseEnrichment):
    def __init__(self, project_id: str = "mlab-collaboration"):
        """Initialize HOIHO enricher for IPv6 addresses."""
        super().__init__(project_id)

        # API configuration
        self.hoiho_url = HOIHO_URL
        self.req_size = 2000  # Number of domains to query in one batch
        self.sleep_time = 1  # Sleep time between API calls

        self.hoiho_cache_path = os.path.join(self.cache_dir, "hoiho_output_ipv6.pkl")
        # Load existing cache or initialize an empty cache
        if os.path.exists(self.hoiho_cache_path):
            logger.info(f"Loading IPv6 HOIHO cache from: {self.hoiho_cache_path}")
            self.hoiho_cache = load_pickle(self.hoiho_cache_path)
        else:
            logger.warning(
                f"No existing IPv6 cache found at: {self.hoiho_cache_path}. Initializing a new one."
            )
            self.hoiho_cache = {}
            dump_pickle(self.hoiho_cache, self.hoiho_cache_path)

    def is_ipv6(self, ip: str) -> bool:
        """Check if the given IP is IPv6."""
        try:
            ip_obj = ipaddress.ip_address(ip)
            return isinstance(ip_obj, ipaddress.IPv6Address)
        except ValueError:
            return False

    def query_hoiho(self, rdns_list: list[str]) -> dict[str, Any]:
        """Query HOIHO API in batches for IPv6 hostnames."""
        rdns_list = list(set(rdns_list))
        hoiho_responses = {}

        for i in tqdm(range((len(rdns_list) // self.req_size) + 1), desc="Querying IPv6 HOIHO API"):
            batch = rdns_list[i * self.req_size : (i + 1) * self.req_size]
            if not batch:
                continue

            logger.info(f"Processing IPv6 batch {i + 1}/{(len(rdns_list) // self.req_size) + 1}")
            try:
                response = requests.post(self.hoiho_url, json=batch)
                if response.status_code == 200 and "matches" in response.json():
                    matches = response.json().get("matches", [])
                    logger.info(f"IPv6 batch {i + 1}: Retrieved {len(matches)} matches.")
                    for match in matches:
                        hoiho_responses[match["hostname"].lower()] = match
                else:
                    logger.warning(
                        f"IPv6 batch {i + 1}: Failed to retrieve matches. Status: {response.status_code}"
                    )
            except requests.RequestException as e:
                logger.error(f"IPv6 batch {i + 1}: Request failed. Error: {e}")
            time.sleep(self.sleep_time)

        return hoiho_responses

    def enrich_hoiho_info(self, rdns_cache: dict[str, str]) -> dict[str, Any]:
        """Enrich HOIHO information for given IPv6 addresses."""
        # Filter for IPv6 addresses only
        ipv6_rdns_cache = {
            ip: hostname.strip().lower().rstrip(".")
            for ip, hostname in rdns_cache.items()
            if self.is_ipv6(ip) and hostname
        }

        if not ipv6_rdns_cache:
            logger.info("No IPv6 addresses in rDNS cache")
            return {}

        missing_domains = list(set(ipv6_rdns_cache.values()) - set(self.hoiho_cache))
        if missing_domains:
            logger.info(f"Querying HOIHO API for {len(missing_domains)} missing IPv6 domains.")
            responses = self.query_hoiho(missing_domains)

            # Normalize keys before caching
            normalized_responses = {k.lower().rstrip("."): v for k, v in responses.items()}
            self.hoiho_cache.update(normalized_responses)
            dump_pickle(self.hoiho_cache, self.hoiho_cache_path)

        # Build mapping of rDNS → HOIHO info
        hoiho_info = {
            rdns: self.hoiho_cache[rdns]
            for rdns in ipv6_rdns_cache.values()
            if rdns in self.hoiho_cache
        }

        logger.info(
            f"Generated HOIHO info for {len(hoiho_info)} out of {len(ipv6_rdns_cache)} IPv6 domains."
        )
        return hoiho_info

    def get_rdns_for_ipv6(self, ips: list[str], date: str = None) -> dict[str, str]:
        """Get rDNS mappings for IPv6 addresses from BigQuery."""
        # Filter for IPv6 addresses only
        ipv6_ips = [ip for ip in ips if self.is_ipv6(ip)]

        if not ipv6_ips:
            return {}

        if not date:
            date = datetime.now().strftime("%Y-%m-%d")

        # Query BigQuery for rDNS data
        query = f"""
        SELECT ip_address, hostname
        FROM `{self.project_id}.hermes.unified_ip_to_rdns_ipv6`
        WHERE ip_address IN UNNEST(@ips)
          AND partition_date = @date
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("ips", "STRING", ipv6_ips),
                bigquery.ScalarQueryParameter("date", "DATE", date),
            ]
        )

        rdns_cache = {}
        try:
            for row in self.client.query(query, job_config=job_config).result():
                if row.hostname:  # Only include entries with hostnames
                    rdns_cache[row.ip_address] = row.hostname
        except Exception as e:
            logger.error(f"Error querying rDNS for IPv6 addresses: {e}")

        return rdns_cache

    def process_ipv6_batch(self, ips: list[str], date: str = None) -> dict[str, Any]:
        """Process a batch of IPv6 addresses to get HOIHO information."""
        # Get rDNS mappings
        rdns_cache = self.get_rdns_for_ipv6(ips, date)

        if not rdns_cache:
            logger.info("No rDNS mappings found for IPv6 addresses")
            return {}

        # Enrich with HOIHO information
        hoiho_info = self.enrich_hoiho_info(rdns_cache)

        return hoiho_info

    def upload_hoiho_data(self, hoiho_data: dict[str, Any], date: str) -> None:
        """Upload HOIHO data for IPv6 addresses to BigQuery."""
        if not hoiho_data:
            logger.info("No IPv6 HOIHO data to upload")
            return

        rows = []
        for hostname, info in hoiho_data.items():
            rows.append(
                {"hostname": hostname, "hoiho_data": json.dumps(info), "partition_date": date}
            )

        # Insert in batches
        batch_size = 1000
        for i in range(0, len(rows), batch_size):
            batch = rows[i : i + batch_size]
            errors = self.client.insert_rows_json(
                self.client.dataset("hermes").table("geolocation"), batch
            )
            if not errors:
                logger.info(f"IPv6 HOIHO batch {i // batch_size + 1} inserted successfully")
            else:
                logger.error(f"IPv6 HOIHO batch {i // batch_size + 1} encountered errors: {errors}")


def test_hoiho_ipv6():
    enricher = HOIHOEnricherIPv6()

    # Test IPv6 detection
    test_ips = [
        "2001:4860:4860::8888",  # IPv6
        "8.8.8.8",  # IPv4
        "2606:4700:4700::1111",  # IPv6
    ]

    for ip in test_ips:
        is_ipv6 = enricher.is_ipv6(ip)
        print(f"IP {ip} is IPv6: {is_ipv6}")

    # Test rDNS lookup
    test_rdns = {
        "2001:4860:4860::8888": "dns.google.",
        "2606:4700:4700::1111": "one.one.one.one.",
    }

    hoiho_info = enricher.enrich_hoiho_info(test_rdns)
    print(f"HOIHO info for IPv6: {len(hoiho_info)} entries")


if __name__ == "__main__":
    test_hoiho_ipv6()

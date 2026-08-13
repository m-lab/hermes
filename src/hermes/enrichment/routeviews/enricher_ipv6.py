import gzip
import ipaddress
import os
import shutil
from datetime import datetime
from typing import Any

import requests
from google.cloud import bigquery

from hermes.enrichment.utils.common import BaseEnrichment, logger


class RouteViewsEnricherIPv6(BaseEnrichment):
    def __init__(self, project_id: str = "mlab-collaboration"):
        """Initialize RouteViews enricher for IPv6 addresses."""
        super().__init__(project_id)

        # Initialize cache directory and path
        self.cache_dir = os.path.join(self.cache_dir, "routeviews_ipv6")
        os.makedirs(self.cache_dir, exist_ok=True)

        # Base URL for RouteViews IPv6 data
        self.base_url = "https://publicdata.caida.org/datasets/routing/routeviews6-prefix2as/"

    def is_ipv6_prefix(self, prefix: str) -> bool:
        """Check if the given prefix is IPv6."""
        try:
            # Extract IP part from prefix (remove /mask)
            ip_part = prefix.split("/")[0]
            ip_obj = ipaddress.ip_address(ip_part)
            return isinstance(ip_obj, ipaddress.IPv6Address)
        except (ValueError, IndexError):
            return False

    def download_routeviews_dataset(self, target_date: str) -> str | None:
        """Downloads the routeviews dataset for the given date.

        Args:
            target_date: The date of the data to be crawled in YYYY-MM-DD format.

        Returns:
            The path to the downloaded file, or None if download failed.
        """
        # Check if file already exists
        matches = [
            file for file in os.listdir(self.cache_dir) if f"routeviews-rv6-{target_date}" in file
        ]
        if matches:
            matched_path = os.path.join(self.cache_dir, matches[0])
            logger.info("Routeview IPv6 file %s already exists", matched_path)
            return matched_path

        # Format date for URL
        if "-" in target_date:
            target_date = target_date.replace("-", "")

        # Construct URL components
        target_year = target_date[0:4]
        target_month = target_date[4:6]
        hours_possible = [f"{hour}00".zfill(4) for hour in range(0, 24, 1)]

        # Try downloading for each hour
        for hour in hours_possible:
            try:
                filename = f"routeviews-rv6-{target_date}-{hour}.pfx2as.gz"
                url = f"{self.base_url}{target_year}/{target_month}/{filename}"
                logger.info(f"Attempting to download IPv6 file from {url}")

                response = requests.get(url, stream=True)
                file_path = os.path.join(self.cache_dir, filename)

                if response.status_code == 200:
                    with open(file_path, "wb") as f:
                        for chunk in response.iter_content(chunk_size=1024):
                            f.write(chunk)

                    # Unzip the file
                    logger.info("Unzipping %s", file_path)
                    unzipped_file_path = file_path.replace(".gz", "")
                    with gzip.open(file_path, "rb") as f_in:
                        with open(unzipped_file_path, "wb") as f_out:
                            shutil.copyfileobj(f_in, f_out)

                    # Remove zipped file
                    logger.info("Removing zipped file %s", file_path)
                    os.remove(file_path)

                    return unzipped_file_path
                else:
                    logger.info(
                        "Failed to download the IPv6 file. The file might not exist or there"
                        " could be a network issue."
                    )
            except Exception as e:
                logger.error(f"An error occurred during IPv6 download: {e}")
                continue

        logger.error("Failed to download the IPv6 file. Please try again later.")
        return None

    def process_routeviews_data(self, file_path: str, date: str) -> list[dict[str, Any]]:
        """Process RouteViews data and prepare it for BigQuery insertion, filtering for IPv6 only.

        Args:
            file_path: Path to the RouteViews data file
            date: Date of the data in YYYY-MM-DD format

        Returns:
            List of dictionaries containing processed IPv6 data for BigQuery
        """
        rows_to_insert = []

        with open(file_path) as f:
            for line in f:
                if line.startswith("#") or line.startswith("\n"):
                    continue

                # Parse RouteViews format
                parts = line.split("\t")
                if len(parts) >= 3:
                    prefix = parts[0]
                    mask = parts[1]
                    asn = parts[2].split(",")[0]  # Take first ASN if multiple

                    # Skip private ASNs
                    try:
                        asn_int = int(asn)
                        if (64512 <= asn_int <= 65534) or (4200000000 <= asn_int):
                            continue
                    except ValueError:
                        continue

                    # Only process IPv6 prefixes
                    if not self.is_ipv6_prefix(prefix):
                        continue

                    rows_to_insert.append(
                        {
                            "ip_prefix": f"{prefix}/{mask}",
                            "asn": asn_int,
                            "source": "RouteViews",
                            "ixp": None,
                            "partition_date": date,
                        }
                    )

        logger.info(f"Processed {len(rows_to_insert)} IPv6 prefixes from RouteViews")
        return rows_to_insert

    def upload_to_bigquery(self, data: list[dict[str, Any]]) -> None:
        """Upload processed RouteViews IPv6 data to BigQuery.

        Args:
            data: List of dictionaries containing data to upload
        """
        if not data:
            logger.info("No IPv6 data to upload")
            return

        # Insert in batches
        batch_size = 10000
        for i in range(0, len(data), batch_size):
            batch = data[i : i + batch_size]
            errors = self.client.insert_rows_json(
                self.client.dataset("hermes").table("unified_ip_to_as_ipv6"), batch
            )
            if not errors:
                logger.info(f"IPv6 batch {i // batch_size + 1} inserted successfully")
            else:
                logger.error(f"IPv6 batch {i // batch_size + 1} encountered errors: {errors}")

    def process_date(self, date: str, dst_dir: str | None = None) -> None:
        """Process RouteViews IPv6 data for a specific date.

        Args:
            date: Date to process in YYYY-MM-DD format
            dst_dir: Optional directory to store downloaded files. If not provided,
                    uses the default cache directory.
        """
        # Set the destination directory
        if dst_dir is not None:
            self.cache_dir = dst_dir
            os.makedirs(self.cache_dir, exist_ok=True)

        # Download the dataset
        file_path = self.download_routeviews_dataset(date)
        if not file_path:
            logger.error(f"Failed to download RouteViews IPv6 data for {date}")
            return

        # Process the data
        data = self.process_routeviews_data(file_path, date)

        # Upload to BigQuery
        self.upload_to_bigquery(data)

    def get_as_for_ip(self, ip: str, date: str = None) -> int | None:
        """Get ASN for a specific IPv6 address.

        Args:
            ip: IPv6 address to look up
            date: Date for the lookup (optional)

        Returns:
            ASN if found, None otherwise
        """
        if not self.is_ipv6_prefix(ip):
            return None

        if not date:
            date = datetime.now().strftime("%Y-%m-%d")

        # Query BigQuery for the ASN
        query = f"""
        SELECT asn
        FROM `{self.project_id}.hermes.unified_ip_to_as_ipv6`
        WHERE partition_date = @date
          AND NET.IP_IN_NET(CAST(@ip AS STRING), ip_prefix)
        ORDER BY NET.IP_NET_MASK(ip_prefix) DESC
        LIMIT 1
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("date", "DATE", date),
                bigquery.ScalarQueryParameter("ip", "STRING", ip),
            ]
        )

        try:
            result = self.client.query(query, job_config=job_config).result()
            for row in result:
                return row.asn
        except Exception as e:
            logger.error(f"Error querying ASN for IPv6 {ip}: {e}")

        return None


def test_routeviews_ipv6():
    enricher = RouteViewsEnricherIPv6()

    # Test IPv6 prefix detection
    test_prefixes = [
        "2001:4860:4860::/32",  # IPv6
        "8.8.8.0/24",  # IPv4
        "2606:4700:4700::/48",  # IPv6
    ]

    for prefix in test_prefixes:
        is_ipv6 = enricher.is_ipv6_prefix(prefix)
        print(f"Prefix {prefix} is IPv6: {is_ipv6}")

    # Test AS lookup for IPv6
    test_ips = [
        "2001:4860:4860::8888",  # Google DNS
        "2606:4700:4700::1111",  # Cloudflare DNS
    ]

    for ip in test_ips:
        asn = enricher.get_as_for_ip(ip)
        print(f"ASN for {ip}: {asn}")


if __name__ == "__main__":
    test_routeviews_ipv6()

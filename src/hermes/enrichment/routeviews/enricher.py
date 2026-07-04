import gzip
import os
import shutil
from datetime import datetime, timedelta
from typing import Any

import requests

from hermes.enrichment.utils.common import BaseEnrichment, logger


class RouteViewsEnricher(BaseEnrichment):
    def __init__(self, project_id: str = "mlab-collaboration"):
        """Initialize RouteViews enricher."""
        super().__init__(project_id)

        # Initialize cache directory and path
        self.cache_dir = os.path.join(self.cache_dir, "routeviews")
        os.makedirs(self.cache_dir, exist_ok=True)

        # Base URL for RouteViews data
        self.base_url = "https://publicdata.caida.org/datasets/routing/routeviews-prefix2as/"

    def _try_download_for_date(self, date_str: str) -> str | None:
        """Helper method to try downloading RouteViews data for a specific date.

        Args:
            date_str: Date in YYYYMMDD format (no dashes)

        Returns:
            Path to downloaded file if successful, None otherwise
        """
        # Check if file already exists in cache
        matches = [
            file
            for file in os.listdir(self.cache_dir)
            if f"routeviews-rv2-{date_str}" in file and not file.endswith(".gz")
        ]
        if matches:
            matched_path = os.path.join(self.cache_dir, matches[0])
            logger.info(f"Routeview file {matched_path} already exists in cache")
            return matched_path

        # Construct URL components
        target_year = date_str[0:4]
        target_month = date_str[4:6]
        hours_possible = [f"{hour}00".zfill(4) for hour in range(0, 24, 1)]

        # Try downloading for each hour
        for hour in hours_possible:
            try:
                filename = f"routeviews-rv2-{date_str}-{hour}.pfx2as.gz"
                url = f"{self.base_url}{target_year}/{target_month}/{filename}"
                logger.info(f"Attempting to download file from {url}")

                response = requests.get(url, stream=True, timeout=30)
                file_path = os.path.join(self.cache_dir, filename)

                if response.status_code == 200:
                    with open(file_path, "wb") as f:
                        for chunk in response.iter_content(chunk_size=1024):
                            f.write(chunk)

                    # Unzip the file
                    logger.info(f"Unzipping {file_path}")
                    unzipped_file_path = file_path.replace(".gz", "")
                    with gzip.open(file_path, "rb") as f_in:
                        with open(unzipped_file_path, "wb") as f_out:
                            shutil.copyfileobj(f_in, f_out)

                    # Remove zipped file
                    logger.info(f"Removing zipped file {file_path}")
                    os.remove(file_path)

                    return unzipped_file_path
                else:
                    logger.debug(f"File not found at {url} (status: {response.status_code})")
            except Exception as e:
                logger.debug(f"Error downloading from {url}: {e}")
                continue

        return None

    def download_routeviews_dataset(
        self, target_date: str, max_days_lookback: int = 7
    ) -> tuple[str, str] | None:
        """Downloads the routeviews dataset for the given date, falling back to closest available date.

        Args:
            target_date: The date of the data to be crawled in YYYY-MM-DD format.
            max_days_lookback: Maximum number of days to look back/forward for available data (default: 7)

        Returns:
            Tuple of (file_path, actual_date_used) if successful, or None if no data found.
            actual_date_used is in YYYY-MM-DD format.
        """
        # Parse target date
        if "-" in target_date:
            target_date_obj = datetime.strptime(target_date, "%Y-%m-%d")
            target_date_formatted = target_date.replace("-", "")
        else:
            target_date_obj = datetime.strptime(target_date, "%Y%m%d")
            target_date_formatted = target_date

        # Try exact date first
        logger.info(f"Attempting to download RouteViews data for exact date: {target_date}")
        result = self._try_download_for_date(target_date_formatted)
        if result:
            logger.info(f"Successfully downloaded RouteViews data for {target_date}")
            return (result, target_date)

        # If exact date fails, try nearby dates (preferring earlier dates)
        logger.info(f"No data found for {target_date}, searching for closest available date...")

        # Try dates in order: -1, -2, ..., -max_days_lookback, then +1, +2, ..., +max_days_lookback
        date_offsets = list(range(-1, -max_days_lookback - 1, -1)) + list(
            range(1, max_days_lookback + 1)
        )

        for offset in date_offsets:
            candidate_date_obj = target_date_obj + timedelta(days=offset)
            candidate_date = candidate_date_obj.strftime("%Y-%m-%d")
            candidate_date_formatted = candidate_date_obj.strftime("%Y%m%d")

            logger.info(f"Trying date {candidate_date} (offset: {offset:+d} days)")
            result = self._try_download_for_date(candidate_date_formatted)
            if result:
                logger.info(
                    f"Found RouteViews data for {candidate_date} (closest available, {abs(offset)} day(s) from target)"
                )
                return (result, candidate_date)

        logger.error(
            f"Failed to find RouteViews data for {target_date} or nearby dates (within {max_days_lookback} days)"
        )
        return None

    def process_routeviews_data(self, file_path: str, date: str) -> list[dict[str, Any]]:
        """Process RouteViews data and prepare it for BigQuery insertion.

        Args:
            file_path: Path to the RouteViews data file
            date: Date of the data in YYYY-MM-DD format

        Returns:
            List of dictionaries containing processed data for BigQuery
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

                    rows_to_insert.append(
                        {
                            "ip_prefix": f"{prefix}/{mask}",
                            "asn": asn_int,
                            "source": "RouteViews",
                            "ixp": None,
                            "partition_date": date,
                        }
                    )

        return rows_to_insert

    def upload_to_bigquery(self, data: list[dict[str, Any]]) -> None:
        """Upload processed RouteViews data to BigQuery.

        Args:
            data: List of dictionaries containing data to upload
        """
        if not data:
            logger.info("No data to upload")
            return

        # Insert in batches
        batch_size = 10000
        for i in range(0, len(data), batch_size):
            batch = data[i : i + batch_size]
            errors = self.client.insert_rows_json(
                self.client.dataset("hermes").table("unified_ip_to_as"), batch
            )
            if not errors:
                logger.info(f"Batch {i // batch_size + 1} inserted successfully")
            else:
                logger.error(f"Batch {i // batch_size + 1} encountered errors: {errors}")

    def process_date(
        self, date: str, dst_dir: str | None = None, max_days_lookback: int = 7
    ) -> None:
        """Process RouteViews data for a specific date, falling back to closest available date.

        Args:
            date: Date to process in YYYY-MM-DD format
            dst_dir: Optional directory to store downloaded files. If not provided,
                    uses the default cache directory.
            max_days_lookback: Maximum number of days to look back/forward for available data (default: 7)
        """
        # Set the destination directory
        if dst_dir is not None:
            self.cache_dir = dst_dir
            os.makedirs(self.cache_dir, exist_ok=True)

        # Download the dataset (with fallback to closest available date)
        result = self.download_routeviews_dataset(date, max_days_lookback)
        if not result:
            logger.error(f"Failed to download RouteViews data for {date} or nearby dates")
            return

        file_path, actual_date = result

        # Log if we used a different date than requested
        if actual_date != date:
            logger.warning(f"Using RouteViews data from {actual_date} (requested: {date})")

        # Process the data (using the actual date found, not the requested date)
        data = self.process_routeviews_data(file_path, actual_date)

        # Upload to BigQuery
        self.upload_to_bigquery(data)

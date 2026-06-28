import ipaddress
import logging
import os
import subprocess
from datetime import datetime

from google.cloud import bigquery
from tqdm import tqdm

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class IXPCollectorIPv6:
    def __init__(
        self,
        project_id: str = "mlab-collaboration",
        python_executable: str | None = None,
        output_dir: str | None = None,
        batch_size: int = 1000,
    ):
        """
        Initialize the IXP collector for IPv6 addresses.

        Args:
            project_id (str): Google Cloud project ID
            python_executable (str): Path to Python executable
            output_dir (str): Directory for output files
            batch_size (int): Number of rows to insert in each batch
        """
        self.project_id = project_id
        self.batch_size = batch_size
        self.client = bigquery.Client(project=project_id)

        self.wrapper_script_path = os.path.join(
            os.path.expanduser("~"),
            "Documents",
            "GitHub",
            "missing-peering-links",
            "scripts",
            "wrapper.py",
        )

        self.python_executable = os.path.join(
            os.path.expanduser("~"), "miniforge3", "envs", "missing-peering-links", "bin", "python"
        )

        self.output_dir = os.path.join(
            os.path.expanduser("~"),
            "Documents",
            "GitHub",
            "missing-peering-links",
            "scripts",
            "data",
        )
        # Create output directory if it doesn't exist
        os.makedirs(self.output_dir, exist_ok=True)

        # Define BigQuery tables for IPv6
        self.members_table = f"{project_id}.ix_data.ixp_members_ipv6"
        self.unified_table = f"{project_id}.hermes.unified_ip_to_as_ipv6"

    def is_ipv6(self, ip: str) -> bool:
        """Check if the given IP is IPv6."""
        try:
            ip_obj = ipaddress.ip_address(ip)
            return isinstance(ip_obj, ipaddress.IPv6Address)
        except ValueError:
            return False

    def add_to_unified_mapping(self, ipv6: str, asn: int, ixp_name: str, date: str) -> dict:
        """Create a row for the unified_ip_to_as_ipv6 mapping.

        Args:
            ipv6 (str): IPv6 address
            asn (int): ASN number
            ixp_name (str): Name of the IXP
            date (str): Date in YYYY-MM-DD format

        Returns:
            Dict: Row for unified_ip_to_as_ipv6 table
        """
        return {
            "ip_prefix": f"{ipv6}/128",  # Add as /128 for exact IPv6 IP mapping
            "asn": asn,
            "source": "IXP",
            "ixp": ixp_name,
            "partition_date": date,
        }

    def run_wrapper_script(self, yesterday=None) -> bool:
        """
        Run the wrapper script to generate IXP member data for IPv6, only if the output file does not exist.

        Returns:
            bool: True if successful, False otherwise
        """
        if not os.path.exists(self.wrapper_script_path):
            logger.error(f"Wrapper script not found: {self.wrapper_script_path}")
            return False

        # Determine the expected output file name
        if yesterday is None:
            from datetime import datetime, timedelta

            yesterday = (datetime.today() - timedelta(days=1)).strftime("%Y%m%d")
        file_name = f"merged-members-gen-{yesterday}.txt"
        file_path = os.path.join(self.output_dir, file_name)

        if os.path.exists(file_path):
            logger.info(
                f"Output file already exists for {yesterday}: {file_path}. Skipping wrapper script."
            )
            return True

        try:
            logger.info(f"Running wrapper script for IPv6: {self.wrapper_script_path}")
            result = subprocess.run(
                [self.python_executable, self.wrapper_script_path],
                check=True,
                capture_output=True,
                text=True,
            )
            logger.info(f"Wrapper script output: {result.stdout}")
            return True

        except subprocess.CalledProcessError as e:
            logger.error(f"Error running wrapper script: {e}")
            logger.error(f"Error output: {e.stderr}")
            return False

    def get_latest_data_file(self, yesterday) -> str | None:
        """
        Get the path to the latest data file for IPv6.

        Returns:
            Optional[str]: Path to the latest data file, or None if not found
        """
        file_name = f"merged-members-gen-{yesterday}_ipv6.txt"
        file_path = os.path.join(self.output_dir, file_name)

        if not os.path.exists(file_path):
            logger.warning(f"IPv6 data file not found: {file_path}")
            return None

        logger.info(f"Found IPv6 data file: {file_path}")
        return file_path

    def process_data_file(self, file_path: str) -> tuple[list[dict], list[dict]]:
        """
        Process the data file and prepare rows for BigQuery insertion, filtering for IPv6 only.

        Args:
            file_path (str): Path to the data file

        Returns:
            Tuple[List[Dict], List[Dict]]: Lists of rows to insert for members and unified tables
        """
        members_rows = []
        unified_rows = []

        # Extract partition date from filename
        partition_date_str = os.path.basename(file_path).split("-")[-1][:8]
        partition_date = datetime.strptime(partition_date_str, "%Y%m%d").strftime("%Y-%m-%d")

        logger.info(f"Processing IPv6 data file with partition date: {partition_date}")

        with open(file_path) as f:
            for line in f:
                if line.startswith("#") or not line.strip():
                    continue

                parts = line.strip().split("\t")
                if len(parts) != 3:
                    continue

                ip, asn, name = parts

                # Only process IPv6 addresses
                if not self.is_ipv6(ip):
                    continue

                try:
                    asn_int = int(asn)
                except ValueError:
                    print(f"Invalid row: {line.strip()}")
                    continue

                # Add to members table
                members_rows.append(
                    {"asn": asn_int, "ipv6": ip, "name": name, "partition_date": partition_date}
                )

                # Add to unified mapping
                unified_rows.append(self.add_to_unified_mapping(ip, asn_int, name, partition_date))

        logger.info(f"Processed {len(members_rows)} IPv6 rows from data file")
        return members_rows, unified_rows

    def insert_to_bigquery(self, members_rows: list[dict], unified_rows: list[dict]) -> bool:
        """
        Insert rows into BigQuery tables in batches for IPv6.

        Args:
            members_rows (List[Dict]): List of rows for members table
            unified_rows (List[Dict]): List of rows for unified table

        Returns:
            bool: True if successful, False otherwise
        """
        # Process members table
        members_batches = [
            members_rows[i : i + self.batch_size]
            for i in range(0, len(members_rows), self.batch_size)
        ]
        members_success = True

        for batch in tqdm(members_batches, desc="Inserting into IPv6 members table"):
            errors = self.client.insert_rows_json(self.members_table, batch)
            if errors:
                logger.error(f"Error inserting batch into IPv6 members table: {errors}")
                members_success = False

        # Process unified table
        unified_batches = [
            unified_rows[i : i + self.batch_size]
            for i in range(0, len(unified_rows), self.batch_size)
        ]
        unified_success = True

        for batch in tqdm(unified_batches, desc="Inserting into IPv6 unified table"):
            errors = self.client.insert_rows_json(self.unified_table, batch)
            if errors:
                logger.error(f"Error inserting batch into IPv6 unified table: {errors}")
                unified_success = False

        return members_success and unified_success

    def collect_ixp_data(self, yesterday) -> bool:
        """
        Collect IXP data for IPv6 addresses.

        Args:
            yesterday (str): Yesterday's date in YYYYMMDD format

        Returns:
            bool: True if successful, False otherwise
        """
        logger.info(f"Starting IPv6 IXP data collection for {yesterday}")

        # Run the wrapper script (only if output file doesn't exist)
        if not self.run_wrapper_script(yesterday):
            logger.error("Failed to run wrapper script")
            return False

        # Get the latest data file
        file_path = self.get_latest_data_file(yesterday)
        if not file_path:
            logger.error("Failed to get data file")
            return False

        # Process the data file
        members_rows, unified_rows = self.process_data_file(file_path)

        if not members_rows and not unified_rows:
            logger.warning("No IPv6 data to insert")
            return True

        # Insert into BigQuery
        success = self.insert_to_bigquery(members_rows, unified_rows)

        if success:
            logger.info(f"Successfully collected IPv6 IXP data for {yesterday}")
        else:
            logger.error(f"Failed to collect IPv6 IXP data for {yesterday}")

        return success

    def add_ixp_addresses_as_prefixes(
        self, ixp_name: str, ipv6_addresses: list[str], date: str
    ) -> list[dict]:
        """
        Add IXP IPv6 addresses as /128 prefixes to the unified mapping.

        Args:
            ixp_name (str): Name of the IXP
            ipv6_addresses (List[str]): List of IPv6 addresses
            date (str): Date in YYYY-MM-DD format

        Returns:
            List[Dict]: List of rows for unified table
        """
        rows = []

        for ipv6 in ipv6_addresses:
            if self.is_ipv6(ipv6):
                # For IXP addresses, we typically don't have ASN info
                # So we'll use a placeholder or skip ASN
                rows.append(
                    {
                        "ip_prefix": f"{ipv6}/128",
                        "asn": None,  # IXP addresses might not have associated ASN
                        "source": "IXP",
                        "ixp": ixp_name,
                        "partition_date": date,
                    }
                )

        return rows


def test_ixp_collector_ipv6():
    collector = IXPCollectorIPv6()

    # Test IPv6 detection
    test_ips = [
        "2001:4860:4860::8888",  # IPv6
        "8.8.8.8",  # IPv4
        "2606:4700:4700::1111",  # IPv6
    ]

    for ip in test_ips:
        is_ipv6 = collector.is_ipv6(ip)
        print(f"IP {ip} is IPv6: {is_ipv6}")

    # Test adding IXP addresses as prefixes
    ixp_addresses = [
        "2001:7f8:1::a500:1:1",
        "2001:7f8:1::a500:1:2",
        "2606:4700:4700::1111",
    ]

    rows = collector.add_ixp_addresses_as_prefixes("Test IXP", ixp_addresses, "2025-01-15")
    print(f"Generated {len(rows)} IPv6 IXP prefix rows")


if __name__ == "__main__":
    test_ixp_collector_ipv6()

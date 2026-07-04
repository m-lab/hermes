import logging
import os
import subprocess
import sys
from datetime import datetime, timedelta

from google.cloud import bigquery
from tqdm import tqdm

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class IXPCollector:
    def __init__(
        self,
        project_id: str = "mlab-collaboration",
        python_executable: str | None = None,
        output_dir: str | None = None,
        batch_size: int = 1000,
    ):
        """
        Initialize the IXP collector.

        Args:
            project_id (str): Google Cloud project ID
            wrapper_script_path (str): Path to the wrapper script
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

        # Define BigQuery tables
        self.members_table = f"{project_id}.ix_data.ixp_members"
        self.unified_table = f"{project_id}.hermes.unified_ip_to_as"

    def add_to_unified_mapping(self, ipv4: str, asn: int, ixp_name: str, date: str) -> dict:
        """Create a row for the unified_ip_to_as mapping.

        Args:
            ipv4 (str): IPv4 address
            asn (int): ASN number
            ixp_name (str): Name of the IXP
            date (str): Date in YYYY-MM-DD format

        Returns:
            Dict: Row for unified_ip_to_as table
        """
        return {
            "ip_prefix": f"{ipv4}/32",  # Add as /32 for exact IP mapping
            "asn": asn,
            "source": "IXP",
            "ixp": ixp_name,
            "partition_date": date,
        }

    def run_wrapper_script(self) -> bool:
        """
        Run the wrapper script to generate IXP member data.

        Returns:
            bool: True if successful, False otherwise
        """
        if not os.path.exists(self.wrapper_script_path):
            logger.error(f"Wrapper script not found: {self.wrapper_script_path}")
            return False

        try:
            logger.info(f"Running wrapper script: {self.wrapper_script_path}")
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
        Get the path to the latest data file.

        Returns:
            Optional[str]: Path to the latest data file, or None if not found
        """
        file_name = f"merged-members-gen-{yesterday}.txt"
        file_path = os.path.join(self.output_dir, file_name)

        if not os.path.exists(file_path):
            logger.warning(f"Data file not found: {file_path}")
            return None

        logger.info(f"Found data file: {file_path}")
        return file_path

    def process_data_file(self, file_path: str) -> tuple[list[dict], list[dict]]:
        """
        Process the data file and prepare rows for BigQuery insertion.

        Args:
            file_path (str): Path to the data file

        Returns:
            Tuple[List[Dict], List[Dict], List[Dict]]: Lists of rows to insert for members, mapping, and unified tables
        """
        members_rows = []
        unified_rows = []

        # Extract partition date from filename
        partition_date_str = os.path.basename(file_path).split("-")[-1][:8]
        partition_date = datetime.strptime(partition_date_str, "%Y%m%d").strftime("%Y-%m-%d")

        logger.info(f"Processing data file with partition date: {partition_date}")

        with open(file_path) as f:
            for line in f:
                if line.startswith("#") or not line.strip():
                    continue

                parts = line.strip().split("\t")
                if len(parts) != 3:
                    continue

                ipv4, asn, name = parts
                try:
                    asn_int = int(asn)
                except ValueError:
                    print(f"Invalid row: {line.strip()}")
                    continue
                # Add to members table
                members_rows.append(
                    {"asn": asn_int, "ipv4": ipv4, "name": name, "partition_date": partition_date}
                )

                # Add to unified mapping
                unified_rows.append(
                    self.add_to_unified_mapping(ipv4, asn_int, name, partition_date)
                )

        logger.info(f"Processed {len(members_rows)} rows from data file")
        return members_rows, unified_rows

    def insert_to_bigquery(self, members_rows: list[dict], unified_rows: list[dict]) -> bool:
        """
        Insert rows into BigQuery tables in batches.

        Args:
            members_rows (List[Dict]): List of rows for members table
            mapping_rows (List[Dict]): List of rows for mapping table
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

        for batch in tqdm(members_batches, desc="Inserting into members table"):
            errors = self.client.insert_rows_json(self.members_table, batch)
            if errors:
                logger.error(f"Error inserting batch into members table: {errors}")
                members_success = False

        # Process unified table
        unified_batches = [
            unified_rows[i : i + self.batch_size]
            for i in range(0, len(unified_rows), self.batch_size)
        ]
        unified_success = True

        for batch in tqdm(unified_batches, desc="Inserting into unified table"):
            errors = self.client.insert_rows_json(self.unified_table, batch)
            if errors:
                logger.error(f"Error inserting batch into unified table: {errors}")
                unified_success = False

        return members_success and unified_success

    def collect_ixp_data(self, yesterday) -> bool:
        """
        Main method to collect and process IXP data.

        Returns:
            bool: True if successful, False otherwise
        """
        # Check if data file exists, if not run wrapper script
        file_path = self.get_latest_data_file(yesterday)
        if file_path is None:
            if not self.run_wrapper_script():
                return False
            file_path = self.get_latest_data_file(yesterday)
            if file_path is None:
                return False

        # Process data file
        members_rows, unified_rows = self.process_data_file(file_path)
        if not members_rows or not unified_rows:
            logger.error("No valid rows found in data file")
            return False

        # Insert into BigQuery
        if not self.insert_to_bigquery(members_rows, unified_rows):
            logger.error("Failed to insert data into BigQuery")
            return False

        logger.info(f"Successfully processed and inserted {len(members_rows)} rows")
        return True


def update_ixp_data(date) -> bool:
    """
    Update IXP data as part of the HERMES enrichment pipeline.

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        collector = IXPCollector()
        if isinstance(date, str):
            date = datetime.strptime(date, "%Y-%m-%d")
        elif not isinstance(date, datetime):
            raise ValueError("Date must be a string in YYYY-MM-DD format or a datetime object")

        yesterday = (date - timedelta(days=1)).strftime("%Y%m%d")
        success = collector.collect_ixp_data(yesterday)

        if not success:
            logger.error("Failed to update IXP data")
            return False

        logger.info("IXP data update completed successfully")
        return True

    except Exception as e:
        logger.error(f"Error updating IXP data: {str(e)}")
        return False


if __name__ == "__main__":
    success = update_ixp_data()
    sys.exit(0 if success else 1)

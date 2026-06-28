import logging
import os
import time
from datetime import datetime, timedelta

from dotenv import load_dotenv
from google.cloud import bigquery

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class BaseEnrichment:
    def __init__(self, project_id: str = "mlab-collaboration"):
        """Initialize base enrichment class."""
        self.project_id = project_id
        self.client = bigquery.Client(project=project_id)

        # Define table names
        self.tables = {
            "rdns": "mlab-collaboration.hermes.unified_ip_to_rdns",
            "geolocation": "mlab-collaboration.hermes.geolocation",
            "ip_to_geoloc": "mlab-collaboration.hermes.unified_ip_to_geoloc",
            "ixp": "mlab-collaboration.hermes.ixp_data",
            "transient_events": "mlab-collaboration.hermes.transient_events",
        }

        # Initialize cache directory — use HERMES_CACHE_DIR env var if set,
        # otherwise fall back to the hermes_enrichment/cache/ directory.
        self.cache_dir = os.environ.get(
            "HERMES_CACHE_DIR", os.path.join(os.path.dirname(os.path.dirname(__file__)), "cache")
        )
        os.makedirs(self.cache_dir, exist_ok=True)

    def get_unique_ips(self, date: str) -> list:
        """Get unique IPs from the transient events table for the previous month before the given date."""
        start_timer = time.time()

        if not isinstance(date, datetime):
            end_date = datetime.strptime(date, "%Y-%m-%d")
        else:
            end_date = date
        start_date = (end_date - timedelta(days=30)).strftime("%Y-%m-%d")
        date_str = end_date.strftime("%Y-%m-%d")

        query = f"""
        SELECT DISTINCT addr
        FROM `{self.tables["transient_events"]}`,
        UNNEST(node_details) AS node
        WHERE partition_date BETWEEN '{start_date}' AND '{date_str}'

        UNION DISTINCT

        SELECT DISTINCT hop_ip AS addr
        FROM `{self.tables["transient_events"]}`,
        UNNEST(reverse_node_details) AS node
        WHERE partition_date BETWEEN '{start_date}' AND '{date_str}'
        """

        results = [row.addr for row in self.client.query(query).result()]

        elapsed_time = time.time() - start_timer
        logger.info(f"get_unique_ips executed in {elapsed_time:.2f} seconds")

        return results

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

    def has_rows_for_date(self, table: str, date: str, source: str | None = None) -> bool:
        """Whether `table` already holds rows for `date`, optionally from one source.

        A re-run guard. The RouteViews enrichers append with ``insert_rows_json``
        and do no dedup, so processing the same date twice simply doubles it --
        which is what happened on 2026-09-12, leaving 1,131,314 duplicate rows in
        the IPv4 table after the refresh ran twice in one evening.

        Deliberately a ``COUNT(*)`` query and **not**
        ``INFORMATION_SCHEMA.PARTITIONS``: these tables are streamed into, and
        PARTITIONS cannot see rows still in the streaming buffer. It reports an
        empty partition for up to ~90 minutes after a perfectly successful
        upload, so using it here would wave a duplicate straight through --
        precisely the case this guard exists to stop.

        Args:
            table: Fully-qualified table name, ``project.dataset.table``.
            date: Partition date to check, ``YYYY-MM-DD``.
            source: Optional ``source`` column value to restrict the check to.

        Returns:
            True if at least one matching row already exists.
        """
        where = "partition_date = @date"
        params = [bigquery.ScalarQueryParameter("date", "DATE", date)]
        if source is not None:
            where += " AND source = @source"
            params.append(bigquery.ScalarQueryParameter("source", "STRING", source))

        query = f"SELECT COUNT(*) AS n FROM `{table}` WHERE {where}"  # noqa: S608
        job = self.client.query(query, job_config=bigquery.QueryJobConfig(query_parameters=params))
        return next(iter(job.result())).n > 0

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

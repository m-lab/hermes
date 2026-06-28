import json
import os
import subprocess
from datetime import datetime, timedelta

from google.cloud import bigquery

from hermes.enrichment.utils.common import BaseEnrichment, logger


def parse_zdns_result(result: dict, date: str) -> list[dict]:
    """Convert one parsed zDNS JSON record into rDNS rows.

    Handles the current zdns schema, which nests the per-query result under
    ``results.PTR`` (with ``status``, ``data.answers`` and ``timestamp`` inside),
    and falls back to the legacy layout where those keys are top-level.

    Parameters
    ----------
    result
        One parsed JSON object from a zdns output line.
    date
        Partition date label (``YYYY-MM-DD``).

    Returns
    -------
    list of dict
        Rows ready for upload, each with ``ip_address``, ``hostname``,
        ``timestamp`` and ``partition_date``. ``hostname`` is ``None`` when there
        is no successful PTR answer.
    """
    name = result["name"]
    ptr = result.get("results", {}).get("PTR")
    if ptr is None:
        ptr = result  # legacy zdns layout: status/data/timestamp at top level
    timestamp = ptr.get("timestamp")
    data = ptr.get("data") or {}
    answers = data.get("answers") if ptr.get("status") == "NOERROR" else None

    base = {"ip_address": name, "timestamp": timestamp, "partition_date": date}
    if not answers:
        return [{**base, "hostname": None}]
    return [
        {**base, "hostname": answer.get("answer") if answer.get("type") == "PTR" else None}
        for answer in answers
    ]


class ZDNSEnricher(BaseEnrichment):
    def __init__(self, project_id: str = "mlab-collaboration"):
        """Initialize zDNS enricher."""
        super().__init__(project_id)
        # Use ZDNS_PATH env var if set (e.g. in Docker), otherwise
        # fall back to the relative path in the project tree.
        self.zdns_path = os.environ.get(
            "ZDNS_PATH",
            os.path.join(
                os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
                "wrapper_automation",
                "rDNS_processing",
                "zdns",
            ),
        )
        self.cache_dir = os.path.join(self.cache_dir, "zdns")
        os.makedirs(self.cache_dir, exist_ok=True)

        # Configuration matching running_rdns_mapping.sh
        self.dns_servers = "1.1.1.1"
        self.threads = 1000
        self.batch_size = 100000

    def process_rdns(self, date: str) -> None:
        """Process rDNS lookups for IPs from transient events that need updates."""
        logger.info(f"Processing rDNS for date: {date}")

        # Calculate date thresholds (one month ago and one month ahead)
        current_date = datetime.strptime(date, "%Y-%m-%d")
        month_ago = current_date - timedelta(days=30)
        month_ago_str = month_ago.strftime("%Y-%m-%d")
        month_ahead = current_date + timedelta(days=30)
        month_ahead_str = month_ahead.strftime("%Y-%m-%d")
        start_date = month_ago_str
        end_date = month_ahead_str

        # Get IPs that need updates using a single SQL query
        # Filter out '*' (star hops), private/reserved IPs, and NULL addresses
        query = f"""
        WITH latest_rdns AS (
          SELECT ip_address, MAX(partition_date) AS partition_date
          FROM `{self.tables["rdns"]}`
          GROUP BY ip_address
        ),
        unique_ips AS (
          SELECT DISTINCT addr
          FROM `{self.tables["transient_events"]}`,
               UNNEST(node_details) AS node
          WHERE partition_date BETWEEN @start_date AND @end_date
            AND addr IS NOT NULL
            AND addr != '*'
            AND NOT REGEXP_CONTAINS(addr, ':')  -- Only IPv4 addresses

          UNION DISTINCT

          SELECT DISTINCT hop_ip AS addr
          FROM `{self.tables["transient_events"]}`,
               UNNEST(reverse_node_details) AS node
          WHERE partition_date BETWEEN @start_date AND @end_date
            AND hop_ip IS NOT NULL
            AND hop_ip != '*'
            AND NOT REGEXP_CONTAINS(hop_ip, ':')  -- Only IPv4 addresses
        ),
        public_ips AS (
          SELECT addr FROM unique_ips
          WHERE NOT REGEXP_CONTAINS(addr, r'^10\\..*')
            AND NOT REGEXP_CONTAINS(addr, r'^192\\.168\\..*')
            AND NOT REGEXP_CONTAINS(addr, r'^172\\.(1[6-9]|2[0-9]|3[0-1])\\..*')
            AND NOT REGEXP_CONTAINS(addr, r'^100\\.(6[4-9]|[7-9][0-9]|1[0-2][0-7])\\..*')
        )
        SELECT DISTINCT i.addr AS ip_address
        FROM public_ips i
        LEFT JOIN latest_rdns r
          ON i.addr = r.ip_address
        WHERE r.ip_address IS NULL               -- IPs not in RDNS table
           OR r.partition_date < @month_ago      -- IPs with outdated RDNS info
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
                bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
                bigquery.ScalarQueryParameter("month_ago", "DATE", month_ago_str),
            ]
        )

        ips_to_update = [
            row.ip_address for row in self.client.query(query, job_config=job_config).result()
        ]

        if not ips_to_update:
            logger.info("No new IPs to process for rDNS")
            return

        logger.info(f"Found {len(ips_to_update)} IPs that need rDNS lookup")

        # Process new IPs in batches
        for i in range(0, len(ips_to_update), self.batch_size):
            batch = ips_to_update[i : i + self.batch_size]
            self._process_batch(batch, date)

    def _get_existing_rdns(self) -> dict[str, str]:
        """Get existing rDNS mappings from BigQuery with their partition dates."""
        existing_rdns = {}
        query = f"""
        SELECT ip_address, partition_date 
        FROM `{self.tables["rdns"]}`
        WHERE partition_date IS NOT NULL
        """
        for row in self.client.query(query).result():
            existing_rdns[row.ip_address] = row.partition_date.strftime("%Y-%m-%d")
        return existing_rdns

    def _process_batch(self, ips: list[str], date: str) -> None:
        """Process a batch of IPs using zDNS."""
        # Create input file for zDNS
        input_file = os.path.join(self.cache_dir, f"zdns_input_{date}.txt")
        with open(input_file, "w") as f:
            for ip in ips:
                f.write(f"{ip}\n")

        # Run zDNS with parameters matching running_rdns_mapping.sh
        output_file = os.path.join(self.cache_dir, f"zdns_output_{date}.json")
        cmd = [
            self.zdns_path,
            "PTR",
            "--input-file",
            input_file,
            "--output-file",
            output_file,
            "--name-servers",
            self.dns_servers,
            "--threads",
            str(self.threads),
        ]

        try:
            logger.info(f"Running zDNS command: {' '.join(cmd)}")
            subprocess.run(cmd, check=True, capture_output=True, text=True)
            self._process_zdns_output(output_file, date)

        except subprocess.CalledProcessError as e:
            logger.error(f"zDNS command failed: {e}")
            logger.error(f"Error output: {e.stderr}")
        finally:
            # Clean up temporary files
            if os.path.exists(input_file):
                os.remove(input_file)
            if os.path.exists(output_file):
                os.remove(output_file)

    def _process_zdns_output(self, output_file: str, date: str) -> None:
        """Process zDNS output and upload to BigQuery."""
        rows = []
        with open(output_file) as f:
            for line in f:
                try:
                    result = json.loads(line)
                    rows.extend(parse_zdns_result(result, date))
                except (json.JSONDecodeError, KeyError, IndexError) as e:
                    logger.warning(f"Error processing zDNS result: {e}")
        if rows:
            job_config = bigquery.LoadJobConfig(
                schema=[
                    bigquery.SchemaField("ip_address", "STRING"),
                    bigquery.SchemaField("hostname", "STRING"),
                    bigquery.SchemaField("timestamp", "TIMESTAMP"),
                    bigquery.SchemaField("partition_date", "DATE"),
                ],
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            )

            table_ref = self.client.dataset("hermes").table("unified_ip_to_rdns")
            job = self.client.load_table_from_json(rows, table_ref, job_config=job_config)
            job.result()
            logger.info(f"Uploaded {len(rows)} rDNS mappings to BigQuery")

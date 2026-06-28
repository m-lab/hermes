import ipaddress
import json
import os
import subprocess
from datetime import datetime, timedelta

from google.cloud import bigquery

from hermes.enrichment.utils.common import BaseEnrichment, logger


class ZDNSEnricherIPv6(BaseEnrichment):
    def __init__(self, project_id: str = "mlab-collaboration"):
        """Initialize zDNS enricher for IPv6 addresses."""
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
        self.cache_dir = os.path.join(self.cache_dir, "zdns_ipv6")
        os.makedirs(self.cache_dir, exist_ok=True)

        # Configuration for IPv6 rDNS lookups
        self.dns_servers = "1.1.1.1"  # Keep Cloudflare DNS for IPv6
        self.threads = 1000
        self.batch_size = 100000

    def is_ipv6(self, ip: str) -> bool:
        """Check if the given IP is IPv6."""
        try:
            ip_obj = ipaddress.ip_address(ip)
            return isinstance(ip_obj, ipaddress.IPv6Address)
        except ValueError:
            return False

    def process_rdns(self, date: str) -> None:
        """Process rDNS lookups for IPv6 addresses from transient events that need updates."""
        logger.info(f"Processing IPv6 rDNS for date: {date}")

        # Calculate date thresholds (one month ago and one month ahead)
        current_date = datetime.strptime(date, "%Y-%m-%d")
        month_ago = current_date - timedelta(days=30)
        month_ago_str = month_ago.strftime("%Y-%m-%d")
        month_ahead = current_date + timedelta(days=30)
        month_ahead_str = month_ahead.strftime("%Y-%m-%d")
        start_date = month_ago_str
        end_date = month_ahead_str

        # Get IPv6 addresses that need updates using a single SQL query
        # Filter out '*', NULL, and link-local/reserved IPv6
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
            AND REGEXP_CONTAINS(addr, ':')  -- Only IPv6 addresses

          UNION DISTINCT

          SELECT DISTINCT hop_ip AS addr
          FROM `{self.tables["transient_events"]}`,
               UNNEST(reverse_node_details) AS node
          WHERE partition_date BETWEEN @start_date AND @end_date
            AND hop_ip IS NOT NULL
            AND hop_ip != '*'
            AND REGEXP_CONTAINS(hop_ip, ':')  -- Only IPv6 addresses
        )
        SELECT DISTINCT i.addr AS ip_address
        FROM unique_ips i
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

        # Filter for IPv6 addresses only
        ipv6_ips = [ip for ip in ips_to_update if self.is_ipv6(ip)]

        if not ipv6_ips:
            logger.info("No new IPv6 addresses to process for rDNS")
            return

        logger.info(f"Found {len(ipv6_ips)} IPv6 addresses that need rDNS lookup")

        # Process new IPv6 addresses in batches
        for i in range(0, len(ipv6_ips), self.batch_size):
            batch = ipv6_ips[i : i + self.batch_size]
            self._process_batch(batch, date)

    def _process_batch(self, ips: list[str], date: str) -> None:
        """Process a batch of IPv6 addresses using zDNS."""
        # Create input file for zDNS
        input_file = os.path.join(self.cache_dir, f"zdns_ipv6_input_{date}.txt")
        with open(input_file, "w") as f:
            for ip in ips:
                f.write(f"{ip}\n")

        # Run zDNS with IPv6-specific parameters
        output_file = os.path.join(self.cache_dir, f"zdns_ipv6_output_{date}.json")
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
            logger.info(f"Running IPv6 zDNS command: {' '.join(cmd)}")
            subprocess.run(cmd, check=True, capture_output=True, text=True)
            self._process_zdns_output(output_file, date)

        except subprocess.CalledProcessError as e:
            logger.error(f"IPv6 zDNS command failed: {e}")
            logger.error(f"Error output: {e.stderr}")
        finally:
            # Clean up temporary files
            if os.path.exists(input_file):
                os.remove(input_file)
            if os.path.exists(output_file):
                os.remove(output_file)

    def _process_zdns_output(self, output_file: str, date: str) -> None:
        """Process zDNS output for IPv6 addresses and upload to BigQuery."""
        rows = []
        with open(output_file) as f:
            for line in f:
                try:
                    result = json.loads(line)
                    if result.get("status") == "NOERROR" and result.get("data", {}).get("answers"):
                        for answer in result["data"]["answers"]:
                            if answer.get("type") == "PTR":
                                rows.append(
                                    {
                                        "ip_address": result["name"],
                                        "hostname": answer["answer"],
                                        "timestamp": result["timestamp"],
                                        "partition_date": date,
                                    }
                                )
                            else:
                                rows.append(
                                    {
                                        "ip_address": result["name"],
                                        "hostname": None,
                                        "timestamp": result["timestamp"],
                                        "partition_date": date,
                                    }
                                )
                    else:
                        rows.append(
                            {
                                "ip_address": result["name"],
                                "hostname": None,
                                "timestamp": result["timestamp"],
                                "partition_date": date,
                            }
                        )
                except (json.JSONDecodeError, KeyError, IndexError) as e:
                    logger.warning(f"Error processing IPv6 zDNS result: {e}")
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

            # Use IPv6-specific table
            table_ref = self.client.dataset("hermes").table("unified_ip_to_rdns_ipv6")
            job = self.client.load_table_from_json(rows, table_ref, job_config=job_config)
            job.result()
            logger.info(f"Uploaded {len(rows)} IPv6 rDNS mappings to BigQuery")

    def get_rdns_batch(self, ips: list[str]) -> dict[str, str]:
        """Get rDNS mappings for a batch of IPv6 addresses."""
        # Filter for IPv6 addresses only
        ipv6_ips = [ip for ip in ips if self.is_ipv6(ip)]

        if not ipv6_ips:
            return {}

        # Query existing rDNS data
        query = f"""
        SELECT ip_address, hostname
        FROM `{self.tables["rdns"]}_ipv6`
        WHERE ip_address IN UNNEST(@ips)
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ArrayQueryParameter("ips", "STRING", ipv6_ips)]
        )

        results = {}
        for row in self.client.query(query, job_config=job_config).result():
            results[row.ip_address] = row.hostname

        return results


def test_zdns_ipv6():
    enricher = ZDNSEnricherIPv6()

    # Test with IPv6 addresses
    test_ips = [
        "2a00:1450:4001:81b::200e",  # Google IPv6
    ]

    # Test IPv6 detection
    for ip in test_ips:
        is_ipv6 = enricher.is_ipv6(ip)
        print(f"IP {ip} is IPv6: {is_ipv6}")

    # Test actual zDNS lookup for IPv6 addresses
    print("\nRunning zDNS lookups for IPv6 addresses...")

    # Create a temporary input file for zDNS
    import os
    import tempfile

    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as input_file:
        for ip in test_ips:
            input_file.write(f"{ip}\n")
        input_file_path = input_file.name

    # Create temporary output file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as output_file:
        output_file_path = output_file.name

    try:
        # Run zDNS command
        cmd = [
            enricher.zdns_path,
            "PTR",
            "--input-file",
            input_file_path,
            "--output-file",
            output_file_path,
            "--name-servers",
            enricher.dns_servers,
            "--threads",
            str(enricher.threads),
        ]

        print(f"Running zDNS command: {' '.join(cmd)}")
        subprocess.run(cmd, check=True, capture_output=True, text=True)
        print("zDNS command completed successfully")

        # Process the output
        print("\nzDNS results:")
        with open(output_file_path) as f:
            for line in f:
                try:
                    result_data = json.loads(line)
                    ip = result_data.get("name", "Unknown")
                    status = result_data.get("status", "Unknown")

                    if status == "NOERROR" and result_data.get("data", {}).get("answers"):
                        answers = result_data["data"]["answers"]
                        ptr_records = [ans["answer"] for ans in answers if ans.get("type") == "PTR"]
                        if ptr_records:
                            print(f"  {ip} -> {ptr_records[0]}")
                        else:
                            print(f"  {ip} -> No PTR record found")
                    else:
                        print(f"  {ip} -> {status}")

                except json.JSONDecodeError as e:
                    print(f"  Error parsing JSON: {e}")

    except subprocess.CalledProcessError as e:
        print(f"zDNS command failed: {e}")
        print(f"Error output: {e.stderr}")
    except FileNotFoundError:
        print(f"zDNS executable not found at: {enricher.zdns_path}")
        print("Please ensure zDNS is installed and the path is correct")
    finally:
        # Clean up temporary files
        if os.path.exists(input_file_path):
            os.remove(input_file_path)
        if os.path.exists(output_file_path):
            os.remove(output_file_path)

    # Test batch rDNS lookup from BigQuery (if available)
    print("\nTesting BigQuery rDNS lookup:")
    rdns_results = enricher.get_rdns_batch(test_ips)
    print(f"BigQuery rDNS results: {rdns_results}")


if __name__ == "__main__":
    test_zdns_ipv6()

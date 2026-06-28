#!/usr/bin/env python3

import argparse
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Any

from google.cloud import bigquery
from tqdm import tqdm

from hermes.enrichment.hoiho.enricher import HOIHOEnricher
from hermes.enrichment.hoiho.enricher_ipv6 import HOIHOEnricherIPv6

# IPv4 enrichers
from hermes.enrichment.ipinfo.enricher import IPInfoEnricher

# IPv6 enrichers
from hermes.enrichment.ipinfo.enricher_ipv6 import IPInfoEnricherIPv6
from hermes.enrichment.peeringdb_ixp.ixp_collector import update_ixp_data
from hermes.enrichment.peeringdb_ixp.ixp_collector_ipv6 import IXPCollectorIPv6
from hermes.enrichment.ripe_ipmap.enricher import RIPEIPMapEnricher
from hermes.enrichment.routeviews import RouteViewsEnricher
from hermes.enrichment.routeviews.enricher_ipv6 import RouteViewsEnricherIPv6
from hermes.enrichment.utils.common import logger
from hermes.enrichment.zdns.enricher import ZDNSEnricher
from hermes.enrichment.zdns.enricher_ipv6 import ZDNSEnricherIPv6
from hermes.sql import paths


class HermesEnrichment:
    def __init__(self, project_id: str = "mlab-collaboration", ipv6: bool = False):
        """Initialize the Hermes enrichment pipeline."""
        self.project_id = project_id
        self.ipv6 = ipv6
        self.client = bigquery.Client(project=project_id)

        # Define table names based on IPv6 flag
        if ipv6:
            self.tables = {
                "rdns": "mlab-collaboration.hermes.unified_ip_to_rdns_ipv6",
                "geolocation": "mlab-collaboration.hermes.geolocation",
                "ip_to_geoloc": "mlab-collaboration.hermes.unified_ip_to_geoloc_ipv6",
                "ixp": "mlab-collaboration.hermes.ixp_data_ipv6",
                "transient_events": "mlab-collaboration.hermes.transient_events_ipv6",
            }
        else:
            self.tables = {
                "rdns": "mlab-collaboration.hermes.unified_ip_to_rdns",
                "geolocation": "mlab-collaboration.hermes.geolocation",
                "ip_to_geoloc": "mlab-collaboration.hermes.unified_ip_to_geoloc",
                "ixp": "mlab-collaboration.hermes.ixp_data",
                "transient_events": "mlab-collaboration.hermes.transient_events",
            }

        # Initialize enrichers based on IPv6 flag
        if ipv6:
            logger.info("Initializing IPv6 enrichers")
            self.ipinfo = IPInfoEnricherIPv6(project_id)
            self.zdns = ZDNSEnricherIPv6(project_id)
            self.hoiho = HOIHOEnricherIPv6(project_id)
            self.ripe_ipmap = RIPEIPMapEnricher(project_id)  # Always use the same RIPEIPMapEnricher
            self.routeviews = RouteViewsEnricherIPv6(project_id)
            self.ixp_collector = IXPCollectorIPv6(project_id)
        else:
            logger.info("Initializing IPv4 enrichers")
            self.ipinfo = IPInfoEnricher(project_id)
            self.zdns = ZDNSEnricher(project_id)
            self.hoiho = HOIHOEnricher(project_id)
            self.ripe_ipmap = RIPEIPMapEnricher(project_id)
            self.routeviews = RouteViewsEnricher(project_id)

    def process_geolocation(self, date: str) -> None:
        """Process geolocation data for IPs from transient events that need updates."""
        logger.info(f"Processing {'IPv6' if self.ipv6 else 'IPv4'} geolocation for date: {date}")

        # Calculate date threshold (one month ago)
        current_date = datetime.strptime(date, "%Y-%m-%d")
        month_ago = current_date - timedelta(days=30)
        # change this after the next run back to 30
        month_ago_str = month_ago.strftime("%Y-%m-%d")
        start_date = month_ago_str

        # Get IPs that need updates using a single SQL query
        if self.ipv6:
            # IPv6 query - look for addresses with colons
            query = f"""
            WITH latest_geoloc AS (
                  SELECT ip_address, MAX(partition_date) AS partition_date
                  FROM `{self.tables["ip_to_geoloc"]}`
                  GROUP BY ip_address
                ),
            unique_ips AS (
                SELECT DISTINCT addr
                FROM `{self.tables["transient_events"]}`,
                UNNEST(node_details) AS node
                WHERE partition_date BETWEEN '{start_date}' AND '{date}'
                  AND REGEXP_CONTAINS(addr, ':')  -- Only IPv6 addresses

                UNION DISTINCT

                SELECT DISTINCT hop_ip AS addr
                FROM `{self.tables["transient_events"]}`,
                UNNEST(reverse_node_details) AS node
                WHERE partition_date BETWEEN '{start_date}' AND '{date}'
                  AND REGEXP_CONTAINS(hop_ip, ':')  -- Only IPv6 addresses
            )
            SELECT DISTINCT i.addr AS ip_address
            FROM unique_ips i
            LEFT JOIN latest_geoloc g
                ON i.addr = g.ip_address
            WHERE g.ip_address IS NULL  -- IPs not in the table
               OR g.partition_date < '{month_ago_str}'  -- IPs with old data
            """
        else:
            # IPv4 query - look for addresses without colons
            query = f"""
            WITH latest_geoloc AS (
              SELECT ip_address, MAX(partition_date) AS partition_date
              FROM `{self.tables["ip_to_geoloc"]}`
              GROUP BY ip_address
            ),
            unique_ips AS (
              SELECT DISTINCT addr
              FROM `{self.tables["transient_events"]}`,
              UNNEST(node_details) AS node
              WHERE partition_date BETWEEN '{start_date}' AND '{date}'
                AND NOT REGEXP_CONTAINS(addr, ':')  -- Only IPv4 addresses
            
              UNION DISTINCT
            
              SELECT DISTINCT hop_ip AS addr
              FROM `{self.tables["transient_events"]}`,
              UNNEST(reverse_node_details) AS node
              WHERE partition_date BETWEEN '{start_date}' AND '{date}'
                AND NOT REGEXP_CONTAINS(hop_ip, ':')  -- Only IPv4 addresses
            ),
            public_ips AS (
              SELECT *
              FROM unique_ips
              WHERE NOT REGEXP_CONTAINS(addr, r'^10\..*')
                AND NOT REGEXP_CONTAINS(addr, r'^192\.168\..*')
                AND NOT REGEXP_CONTAINS(addr, r'^172\.(1[6-9]|2[0-9]|3[0-1])\..*')
            )
            SELECT DISTINCT i.addr AS ip_address
            FROM public_ips i
            LEFT JOIN latest_geoloc g
              ON i.addr = g.ip_address
            WHERE g.ip_address IS NULL  -- IPs not in the table
               OR g.partition_date < '{month_ago_str}'  -- IPs with old data
            """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
                bigquery.ScalarQueryParameter("date", "DATE", date),
                bigquery.ScalarQueryParameter("month_ago", "DATE", month_ago_str),
            ]
        )
        print(query)
        ips_to_update = [
            row.ip_address for row in self.client.query(query, job_config=job_config).result()
        ]

        logger.info(
            f"{len(ips_to_update)} {'IPv6' if self.ipv6 else 'IPv4'} IPs need geolocation update"
        )

        # PRE-LOAD RIPE IPMap data once for the date range
        if ips_to_update:
            # Calculate the date range for RIPE IPMap (8 days before the target date)
            ripe_start_date = (current_date - timedelta(days=8)).strftime("%Y-%m-%d")
            ripe_end_date = date
            logger.info(
                f"Pre-loading RIPE IPMap data for date range {ripe_start_date} to {ripe_end_date}"
            )
            self.ripe_ipmap._load_ripe_ipmap_data(
                datetime.strptime(ripe_start_date, "%Y-%m-%d").date(),
                datetime.strptime(ripe_end_date, "%Y-%m-%d").date(),
            )

        new_geo = {}

        # Parallel IP lookups
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {executor.submit(self._get_geolocation_data, ip): ip for ip in ips_to_update}
            for future in tqdm(as_completed(futures), total=len(futures)):
                ip = futures[future]
                try:
                    geo_data = future.result()
                    if geo_data:
                        new_geo[ip] = geo_data
                except Exception as e:
                    logger.error(f"Error processing geolocation for {ip}: {e}")

        # Upload results
        if new_geo:
            self._upload_geolocation_data(new_geo, date)
            self._update_metro_for_geolocation_table()

    def process_hoiho_geolocation(self, date) -> None:
        """Process HOIHO geolocation data based on rDNS hostnames."""
        logger.info(f"Processing {'IPv6' if self.ipv6 else 'IPv4'} HOIHO geolocation data")

        # take one month before today as our limit for the rDNS data
        month_ago = (datetime.strptime(date, "%Y-%m-%d") - timedelta(days=30)).strftime("%Y-%m-%d")
        # Get rDNS data from BigQuery
        query = f"""
        SELECT DISTINCT ip_address, hostname
        FROM `{self.tables["rdns"]}` 
        WHERE partition_date > '{month_ago}' AND hostname IS NOT NULL
        """
        df_query = self.client.query(query).to_dataframe()

        # Convert to rDNS cache (mapping of ip_address to hostname)
        rdns_cache = {}
        for _, row in df_query.iterrows():
            ip = row["ip_address"]
            hostname = row["hostname"][:-1]  # Remove trailing dot
            rdns_cache[ip] = hostname

        logger.info(f"Loaded rDNS cache with {len(rdns_cache)} IP addresses")

        # Get HOIHO data
        # self.hoiho.enrich_hoiho_info(rdns_cache)
        normalized_cache = {}
        for hostname, match_data in self.hoiho.hoiho_cache.items():
            normalized_hostname = hostname.strip().lower().rstrip(".")
            if normalized_hostname:
                normalized_cache[normalized_hostname] = match_data
        # Process HOIHO data

        # Get existing hostnames from BigQuery table
        existing_hostnames = self._get_existing_hoiho_hostnames()
        logger.info(f"Found {len(existing_hostnames)} existing hostnames in table")

        # Normalize existing hostnames for comparison
        normalized_existing = {h.strip().lower().rstrip(".") for h in existing_hostnames if h}

        # Find hostnames in cache that are not in the table
        missing_hostnames = set(normalized_cache.keys()) - normalized_existing
        logger.info(f"Found {len(missing_hostnames)} hostnames in cache that are not in the table")

        if not missing_hostnames:
            logger.info("No new HOIHO data to insert - all cache entries are already in the table")
            return

        # Process HOIHO data for missing entries (first pass - without metro)
        data_to_insert = []
        for hostname in missing_hostnames:
            match_data = normalized_cache[hostname]

            # Map fields according to new schema
            processed_data = {
                "hostname": hostname,
                "lat": match_data.get("lat"),
                "lon": match_data.get("lng"),  # Map 'lng' from HOIHO to 'lon' in schema
                "place": match_data.get("place"),
                "cc": match_data.get("cc"),
                "state": match_data.get("st"),  # Map 'st' from HOIHO to 'state' in schema
                "metro": None,  # Will be computed below
                "code": None,  # Will be extracted from hostname if needed
                "locode": match_data.get("locode"),
                "domain": None,  # Will be extracted from hostname if needed
                "match_strs": match_data.get("match_strs", []),  # REPEATED field
                "match_meanings": match_data.get("match_meanings", []),  # REPEATED field
                "clli": match_data.get("clli"),
            }

            # Handle None values for nullable fields
            # For REPEATED fields, ensure they're lists (empty list if None)
            if processed_data["match_strs"] is None:
                processed_data["match_strs"] = []
            if processed_data["match_meanings"] is None:
                processed_data["match_meanings"] = []

            # Add country code to place if both exist
            if processed_data["place"] and processed_data["cc"]:
                processed_data["place"] = f"{processed_data['place']}-{processed_data['cc']}"

            data_to_insert.append(processed_data)

        logger.info(f"Prepared {len(data_to_insert)} records to insert")

        # Insert data in batches
        if data_to_insert:
            self._upload_hoiho_geolocation_data(data_to_insert)
            self._update_metro_for_hoiho_geolocation_table()
        else:
            logger.info("No new HOIHO data to insert")

    def _update_metro_for_hoiho_geolocation_table(self) -> None:
        """Update metro field for all entries in geolocation table using spatial join."""
        logger.info(f"Computing metro for {'IPv6' if self.ipv6 else 'IPv4'} geolocation table")

        # Resolve the metro SQL from packaged data (hermes.sql.paths).
        sql_file = paths.query_path("enrich_geolocation_add_metro.sql")
        if not sql_file.exists():
            logger.error(f"SQL file not found: {sql_file}")
            return

        # Read the SQL query
        query = sql_file.read_text()

        # Replace table name for IPv6 if needed
        table_name = f"hermes.geolocation{'_ipv6' if self.ipv6 else ''}"
        query = query.replace(
            "`mlab-collaboration.hermes.geolocation`", f"`{self.project_id}.{table_name}`"
        )

        # Also update the project reference if needed
        query = query.replace(
            "mlab-collaboration.hermes.metro_polygons_with_population",
            f"{self.project_id}.hermes.metro_polygons_with_population",
        )

        try:
            logger.info("Executing metro computation query...")
            job = self.client.query(query)
            job.result()  # Wait for the query to complete
            logger.info(
                f"Successfully computed metro for {'IPv6' if self.ipv6 else 'IPv4'} geolocation table"
            )
        except Exception as e:
            logger.error(f"Error computing metro: {e}")
            raise

    def _update_metro_for_geolocation_table(self) -> None:
        """Update metro field for all entries in unified_ip_to_geoloc table using spatial join."""
        logger.info(
            f"Computing metro for {'IPv6' if self.ipv6 else 'IPv4'} unified_ip_to_geoloc table"
        )

        # Resolve the metro SQL from packaged data (hermes.sql.paths).
        sql_file = paths.query_path("enrich_ip_geoloc_add_metro.sql")
        if not sql_file.exists():
            logger.error(f"SQL file not found: {sql_file}")
            return

        # Read the SQL query
        query = sql_file.read_text()

        # Replace table name for IPv6 if needed
        table_suffix = "_ipv6" if self.ipv6 else ""
        table_name = f"hermes.unified_ip_to_geoloc{table_suffix}"
        query = query.replace(
            "`mlab-collaboration.hermes.unified_ip_to_geoloc`", f"`{self.project_id}.{table_name}`"
        )
        query = query.replace("`hermes.unified_ip_to_geoloc`", f"`{self.project_id}.{table_name}`")

        # Also update the project reference if needed
        query = query.replace(
            "mlab-collaboration.hermes.metro_polygons_with_population",
            f"{self.project_id}.hermes.metro_polygons_with_population",
        )

        # For IPv6, add PARTITION BY partition_date clause
        if self.ipv6:
            # Replace "CREATE OR REPLACE TABLE ... AS" with "CREATE OR REPLACE TABLE ... PARTITION BY partition_date AS"
            query = query.replace(
                f"CREATE OR REPLACE TABLE `{self.project_id}.{table_name}` AS",
                f"CREATE OR REPLACE TABLE `{self.project_id}.{table_name}`\nPARTITION BY partition_date AS",
            )
            print("-----IPv6 query-----")
            print(query)

        try:
            logger.info("Executing metro computation query...")
            job = self.client.query(query)
            job.result()  # Wait for the query to complete
            logger.info(
                f"Successfully computed metro for {'IPv6' if self.ipv6 else 'IPv4'} unified_ip_to_geoloc table"
            )
        except Exception as e:
            logger.error(f"Error computing metro: {e}")
            raise

    def _upload_hoiho_geolocation_data(self, data: list[dict[str, Any]]) -> None:
        """Upload HOIHO geolocation data to BigQuery."""
        table_ref = self.client.dataset("hermes").table("geolocation")

        _schema = [
            bigquery.SchemaField("hostname", "STRING"),
            bigquery.SchemaField("lat", "FLOAT"),
            bigquery.SchemaField("lon", "FLOAT"),
            bigquery.SchemaField("place", "STRING"),
            bigquery.SchemaField("cc", "STRING"),
            bigquery.SchemaField("state", "STRING"),
            bigquery.SchemaField("metro", "STRING"),
            bigquery.SchemaField("code", "STRING"),
            bigquery.SchemaField("locode", "STRING"),
            bigquery.SchemaField("domain", "STRING"),
            bigquery.SchemaField("match_strs", "STRING", mode="REPEATED"),
            bigquery.SchemaField("match_meanings", "STRING", mode="REPEATED"),
            bigquery.SchemaField("clli", "STRING"),
        ]

        # Insert in batches of 10000
        batch_size = 10000
        for i in range(0, len(data), batch_size):
            batch = data[i : i + batch_size]
            errors = self.client.insert_rows_json(table_ref, batch)
            if not errors:
                logger.info(f"Batch {i // batch_size + 1} inserted successfully")
            else:
                logger.error(f"Batch {i // batch_size + 1} encountered errors: {errors}")

    def _get_existing_hoiho_hostnames(self) -> set:
        """Get existing hostnames from the HOIHO geolocation table."""
        query = f"""
        SELECT hostname
        FROM `{self.tables["geolocation"]}`
        """
        results = self.client.query(query).to_dataframe()
        return set(results["hostname"])

    def _get_geolocation_data(self, ip: str) -> dict[str, Any]:
        """Get geolocation data from IPInfo and RIPE IPMap sources (merge whatever is available)."""

        # Initialize fields with None
        data = {
            "city": None,
            "country": None,
            "lat": None,
            "lon": None,
            "score": None,
            "city_ip_info": None,
            "country_ip_info": None,
            "lat_ip_info": None,
            "lon_ip_info": None,
            "region_ip_info": None,
            "metro": None,
            "polygon": None,
            "rank": None,
        }

        # Try IPInfo
        ipinfo_data = self.ipinfo.get_geolocation(ip)
        if ipinfo_data:
            city = ipinfo_data.get("city")
            region = ipinfo_data.get("region")
            country = ipinfo_data.get("country")

            if city and region:
                data["city_ip_info"] = f"{city}-{region}-{country}"
            elif city:
                data["city_ip_info"] = f"{city}-{country}"
            else:
                data["city_ip_info"] = None

            data["country_ip_info"] = country
            data["lat_ip_info"] = ipinfo_data.get("lat")
            data["lon_ip_info"] = ipinfo_data.get("lon")
            data["region_ip_info"] = region

        # Try RIPE IPMap
        ripe_data = self.ripe_ipmap.get_geolocation(ip)
        if ripe_data:
            data["city"] = ripe_data.get("city")
            data["country"] = ripe_data.get("country")
            data["lat"] = ripe_data.get("lat")
            data["lon"] = ripe_data.get("lon")
            data["score"] = ripe_data.get("score")

        return data

    def _upload_geolocation_data(self, geo_data: dict[str, dict[str, Any]], date: str) -> None:
        """Upload geolocation data to BigQuery."""
        rows = [
            {
                "ip_address": ip,
                "city": data["city"],
                "country": data["country"],
                "lat": data["lat"],
                "lon": data["lon"],
                "score": data["score"],
                "city_ip_info": data["city_ip_info"],
                "country_ip_info": data["country_ip_info"],
                "region_ip_info": data["region_ip_info"],
                "lat_ip_info": data["lat_ip_info"],
                "lon_ip_info": data["lon_ip_info"],
                "metro": data["metro"],
                "polygon": data["polygon"],
                "partition_date": date,
                "rank": data["rank"],
            }
            for ip, data in geo_data.items()
        ]

        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("ip_address", "STRING"),
                bigquery.SchemaField("city", "STRING"),
                bigquery.SchemaField("country", "STRING"),
                bigquery.SchemaField("lat", "FLOAT"),
                bigquery.SchemaField("lon", "FLOAT"),
                bigquery.SchemaField("score", "FLOAT"),
                bigquery.SchemaField("city_ip_info", "STRING"),
                bigquery.SchemaField("country_ip_info", "STRING"),
                bigquery.SchemaField("region_ip_info", "STRING"),
                bigquery.SchemaField("lat_ip_info", "FLOAT"),
                bigquery.SchemaField("lon_ip_info", "FLOAT"),
                bigquery.SchemaField("metro", "STRING"),
                bigquery.SchemaField("polygon", "GEOGRAPHY"),
                bigquery.SchemaField("partition_date", "DATE"),
                bigquery.SchemaField("rank", "INTEGER"),
            ],
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )

        table_ref = self.client.dataset("hermes").table(
            "unified_ip_to_geoloc" + ("_ipv6" if self.ipv6 else "")
        )
        job = self.client.load_table_from_json(rows, table_ref, job_config=job_config)
        job.result()
        logger.info(
            f"Uploaded {len(rows)} {'IPv6' if self.ipv6 else 'IPv4'} geolocation mappings to BigQuery"
        )


def main():
    parser = argparse.ArgumentParser(description="Hermes Data Enrichment Pipeline")
    parser.add_argument("--date", type=str, help="Date to process (YYYY-MM-DD)")
    parser.add_argument("--start-date", type=str, help="Start date for date range (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, help="End date for date range (YYYY-MM-DD)")
    parser.add_argument(
        "--skip-ixp", action="store_true", help="Skip IXP data collection", default=False
    )
    parser.add_argument(
        "--ipv6", action="store_true", help="Process IPv6 data instead of IPv4", default=False
    )
    parser.add_argument(
        "--account",
        type=str,
        default=None,
        help="Google Cloud account to use (e.g. user@example.com)",
    )
    parser.add_argument(
        "--project",
        type=str,
        default="mlab-collaboration",
        help="Google Cloud project ID (default: mlab-collaboration)",
    )
    args = parser.parse_args()

    if args.account:
        logger.info(f"Switching gcloud account to {args.account}")
        acct_result = subprocess.run(
            ["gcloud", "config", "set", "account", args.account], capture_output=True, text=True
        )
        if acct_result.returncode != 0:
            logger.error(f"Failed to switch account: {acct_result.stderr}")
            sys.exit(1)

        proj_result = subprocess.run(
            ["gcloud", "config", "set", "project", args.project], capture_output=True, text=True
        )
        if proj_result.returncode != 0:
            logger.error(f"Failed to set project: {proj_result.stderr}")
            sys.exit(1)

        # Refresh application-default credentials for the selected account
        _auth_result = subprocess.run(
            ["gcloud", "auth", "application-default", "set-quota-project", args.project],
            capture_output=True,
            text=True,
        )

        logger.info(f"Active gcloud account: {args.account}, project: {args.project}")

    enrichment = HermesEnrichment(project_id=args.project, ipv6=args.ipv6)

    if args.date:
        dates = [args.date]
    elif args.start_date and args.end_date:
        start = datetime.strptime(args.start_date, "%Y-%m-%d")
        end = datetime.strptime(args.end_date, "%Y-%m-%d")
        dates = [
            (start + timedelta(days=x)).strftime("%Y-%m-%d")
            for x in range(0, (end - start).days + 1)
        ]
    else:
        # Default to today's date
        today = datetime.today().strftime("%Y-%m-%d")
        logger.info(f"No date provided, defaulting to today: {today}")
        dates = [today]

    for date in dates:
        logger.info(f"Processing {'IPv6' if args.ipv6 else 'IPv4'} date: {date}")

        # Step 1: Process RouteViews data
        logger.info(f"Step 1: Processing {'IPv6' if args.ipv6 else 'IPv4'} RouteViews data")
        enrichment.routeviews.process_date(date)

        # Step 2: Process geolocation (IPInfo and RIPE IPMap)
        logger.info(
            f"Step 2: Processing {'IPv6' if args.ipv6 else 'IPv4'} geolocation (IPInfo and RIPE IPMap)"
        )
        enrichment.process_geolocation(date)

        # Step 3 & 4: rDNS + HOIHO — skip for dates more than 90 days in
        # the past (lookups would not return the hostnames that were valid
        # at that time anyway)
        cutoff_str = (datetime.today() - timedelta(days=90)).strftime("%Y-%m-%d")
        if date >= cutoff_str:
            logger.info(f"Step 3: Processing {'IPv6' if args.ipv6 else 'IPv4'} rDNS lookups")
            enrichment.zdns.process_rdns(date)

            logger.info(f"Step 4: Processing {'IPv6' if args.ipv6 else 'IPv4'} HOIHO geolocation")
            enrichment.process_hoiho_geolocation(date)
        else:
            logger.info(f"Steps 3-4: Skipping rDNS/HOIHO for {date} (>90 days in the past)")

    # Step 5: Update IXP data (if not skipped)
    if not args.skip_ixp:
        logger.info(f"Step 5: Updating {'IPv6' if args.ipv6 else 'IPv4'} IXP data")
        if args.date:
            today = datetime.strptime(args.date, "%Y-%m-%d")
        else:
            today = datetime.today()

        if args.ipv6:
            # Use IPv6 IXP collector
            ixp_collector = IXPCollectorIPv6()
            today = today.strftime("%Y%m%d")
            # yesterday = (today - timedelta(days=1)).strftime('%Y%m%d')
            if not ixp_collector.collect_ixp_data(today):
                logger.error("Failed to update IPv6 IXP data")
                return
        else:
            # Use IPv4 IXP collector
            if not update_ixp_data(today):
                logger.error("Failed to update IPv4 IXP data")
                return

        logger.info(f"{'IPv6' if args.ipv6 else 'IPv4'} IXP data update completed successfully")


if __name__ == "__main__":
    main()

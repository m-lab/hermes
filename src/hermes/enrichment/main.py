#!/usr/bin/env python3

import argparse
import re
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date as date_cls
from datetime import datetime, timedelta
from string import Template
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
from hermes.sql import loader, paths

#: IPs resolved and uploaded per batch. Caps peak memory at ~1 GB regardless of run
#: size; see the chunk loop in process_geolocation.
GEOLOC_UPLOAD_CHUNK = 250_000


class HermesEnrichment:
    def __init__(
        self,
        project_id: str = "mlab-collaboration",
        ipv6: bool = False,
        snapshot_date: date_cls | None = None,
    ):
        """Initialize the Hermes enrichment pipeline.

        ``snapshot_date`` is the date being processed; it pins the IPInfo MMDB to
        the dump closest to that date instead of the newest one, so a backfill does
        not geolocate historical traffic with present-day data.
        """
        self.project_id = project_id
        self.ipv6 = ipv6
        self.snapshot_date = snapshot_date
        self.client = bigquery.Client(project=project_id)

        # Define table names based on IPv6 flag
        if ipv6:
            self.tables = {
                "rdns": "mlab-collaboration.hermes.unified_ip_to_rdns_ipv6",
                "geolocation": "mlab-collaboration.hermes.geolocation",
                "ip_to_geoloc": "mlab-collaboration.hermes.unified_ip_to_geoloc_ipv6",
                # Source/client IPs live in their own table. They churn hard
                # (86% of client IPs are new each day) while topology IPs are
                # stable, so keeping them apart lets retention, access control
                # and the nightly metro MERGE stay independent.
                "src_ip_to_geoloc": "mlab-collaboration.hermes.unified_src_ip_to_geoloc_ipv6",
                "ixp": "mlab-collaboration.hermes.ixp_data_ipv6",
                "transient_events": "mlab-collaboration.hermes.transient_events_ipv6",
            }
        else:
            self.tables = {
                "rdns": "mlab-collaboration.hermes.unified_ip_to_rdns",
                "geolocation": "mlab-collaboration.hermes.geolocation",
                "ip_to_geoloc": "mlab-collaboration.hermes.unified_ip_to_geoloc",
                # Source/client IPs live in their own table. They churn hard
                # (86% of client IPs are new each day) while topology IPs are
                # stable, so keeping them apart lets retention, access control
                # and the nightly metro MERGE stay independent.
                "src_ip_to_geoloc": "mlab-collaboration.hermes.unified_src_ip_to_geoloc",
                "ixp": "mlab-collaboration.hermes.ixp_data",
                "transient_events": "mlab-collaboration.hermes.transient_events",
            }

        # Initialize enrichers based on IPv6 flag
        if ipv6:
            logger.info("Initializing IPv6 enrichers")
            self.ipinfo = IPInfoEnricherIPv6(project_id, snapshot_date=snapshot_date)
            self.zdns = ZDNSEnricherIPv6(project_id)
            self.hoiho = HOIHOEnricherIPv6(project_id)
            self.ripe_ipmap = RIPEIPMapEnricher(project_id)  # Always use the same RIPEIPMapEnricher
            self.routeviews = RouteViewsEnricherIPv6(project_id)
            self.ixp_collector = IXPCollectorIPv6(project_id)
        else:
            logger.info("Initializing IPv4 enrichers")
            self.ipinfo = IPInfoEnricher(project_id, snapshot_date=snapshot_date)
            self.zdns = ZDNSEnricher(project_id)
            self.hoiho = HOIHOEnricher(project_id)
            self.ripe_ipmap = RIPEIPMapEnricher(project_id)
            self.routeviews = RouteViewsEnricher(project_id)

    #: Which candidate-IP population a geolocation run covers.
    SOURCES = ("topology", "clients")

    def _geoloc_table(self, source: str) -> str:
        """Destination/staleness table for a candidate population."""
        if source not in self.SOURCES:
            raise ValueError(f"Unknown source {source!r}; choose one of: {', '.join(self.SOURCES)}")
        return self.tables["src_ip_to_geoloc" if source == "clients" else "ip_to_geoloc"]

    def process_geolocation(
        self,
        date: str,
        lookback_days: int = 30,
        source: str = "topology",
        dataset: str = "hermes_union",
    ) -> None:
        """Process geolocation data for IPs from transient events that need updates.

        Parameters
        ----------
        date
            Target date, ``YYYY-MM-DD``.
        lookback_days
            How far back to *collect candidate IPs* from ``transient_events``.
            This must span every date the caller intends to map, because
            enrichment runs once per batch (Phase B) while step 04 maps every
            date. For a single-date run 0 is correct and much cheaper: measured
            by dry run, this scan costs ~3.3 GiB for 1 day, ~20.5 GiB for 7 and
            ~86.4 GiB for 30 — it is the dominant cost of enrichment, not the
            lookup-table join (~2.1 GiB).
        source
            ``"topology"`` for hop IPs, ``"clients"`` for client IPs.
        dataset
            Operational dataset to collect client IPs from. Only used by
            ``source="clients"``; ``"topology"`` reads ``transient_events``, whose
            name is already overridden by the caller.

        Notes
        -----
        The collection window is deliberately separate from the 30-day
        *staleness* threshold below. They previously shared one variable, which
        forced a 30-day scan on every nightly run even though a nightly only
        needs the target day. Staleness stays at 30 days: an IP is re-enriched
        when its stored geolocation is older than that. Narrowing the collection
        window does not weaken it — an IP present in the batch's traffic is still
        a candidate, and an IP absent from it does not need enriching for this run.
        """
        logger.info(f"Processing {'IPv6' if self.ipv6 else 'IPv4'} geolocation for date: {date}")

        # For topology IPs a missing IPInfo reader degrades the run: the hop keeps
        # whatever geolocation the other enrichers supply. For client IPs it does
        # not degrade, it silently empties the pipeline -- steps 02/03 take client
        # geography *only* from this table, so every measurement would be grouped
        # under NULL and detection would compare nothing against nothing. A run that
        # cannot geolocate clients must stop, not produce an empty answer.
        if source == "clients" and getattr(self.ipinfo, "reader", None) is None:
            raise RuntimeError(
                "IPInfo reader unavailable "
                f"(db path: {getattr(self.ipinfo, 'ipinfo_db_path', None)}) — refusing to "
                "run client geolocation, which would leave every measurement ungrouped."
            )

        current_date = datetime.strptime(date, "%Y-%m-%d")
        # Staleness threshold: stored geolocation older than this is refetched.
        month_ago_str = (current_date - timedelta(days=30)).strftime("%Y-%m-%d")
        # Candidate-collection window: only needs to cover the batch being processed.
        start_date = (current_date - timedelta(days=lookback_days)).strftime("%Y-%m-%d")

        geoloc_table = self._geoloc_table(source)

        # The candidate query has one source-specific part -- `unique_ips`. The
        # staleness join, the RFC1918 filter and the final SELECT are shared.
        if source == "clients":
            # Client IPs come from merged_download_upload, which step 01 writes, so
            # this must run after 01 and before 02/03 -- collecting them from the
            # raw NDT tables instead would duplicate step 01's 122.66 GiB scan.
            #
            # The dataset is a parameter because a --target staging run must collect
            # its clients from the staging merged table. Hardcoding hermes_union here
            # made a staging A0 read production, which defeats the isolation the
            # --target flag exists to provide -- staging happens to hold a copy of the
            # same partitions today, so the leak was invisible in the output.
            ipv6_pred = "" if self.ipv6 else "NOT "
            query = rf"""
            WITH latest_geoloc AS (
              -- Only rows that actually carry a location count as covering an IP.
              -- A chunk that dies mid-run leaves rows with no geolocation; without
              -- this they look fresh and are never retried.
              SELECT ip_address, MAX(partition_date) AS partition_date
              FROM `{geoloc_table}`
              WHERE COALESCE(lat_ip_info, lat) IS NOT NULL
              GROUP BY ip_address
            ),
            unique_ips AS (
              SELECT DISTINCT client_ip AS addr
              FROM `mlab-collaboration.{dataset}.merged_download_upload`
              WHERE partition_date BETWEEN '{start_date}' AND '{date}'
                AND client_ip IS NOT NULL
                AND {ipv6_pred}REGEXP_CONTAINS(client_ip, ':')
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
            WHERE g.ip_address IS NULL
               OR g.partition_date < '{month_ago_str}'
            """
        elif self.ipv6:
            # IPv6 query - look for addresses with colons
            query = f"""
            WITH latest_geoloc AS (
                  -- See the client branch: ungeolocated rows must not count.
                  SELECT ip_address, MAX(partition_date) AS partition_date
                  FROM `{geoloc_table}`
                  WHERE COALESCE(lat_ip_info, lat) IS NOT NULL
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
            query = rf"""
            WITH latest_geoloc AS (
              -- Only rows that actually carry a location count as covering an IP.
              -- A chunk that dies mid-run leaves rows with no geolocation; without
              -- this they look fresh and are never retried.
              SELECT ip_address, MAX(partition_date) AS partition_date
              FROM `{geoloc_table}`
              WHERE COALESCE(lat_ip_info, lat) IS NOT NULL
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

        # Chunked so peak memory tracks the chunk, not the run. Resolving everything
        # first kept the candidate list, the futures, `new_geo` and the `rows` copy
        # alive at once -- OOM at 7.07M IPs / 28 GB. Safe because the load is
        # WRITE_APPEND, so N loads equal one big load.
        uploaded = 0
        total = len(ips_to_update)
        for start in range(0, total, GEOLOC_UPLOAD_CHUNK):
            batch = ips_to_update[start : start + GEOLOC_UPLOAD_CHUNK]
            new_geo: dict[str, dict[str, Any]] = {}

            with ThreadPoolExecutor(max_workers=3) as executor:
                futures = {executor.submit(self._get_geolocation_data, ip): ip for ip in batch}
                for future in tqdm(
                    as_completed(futures),
                    total=len(futures),
                    desc=f"geoloc {start + len(batch)}/{total}",
                ):
                    ip = futures[future]
                    try:
                        geo_data = future.result()
                        if geo_data:
                            new_geo[ip] = geo_data
                    except Exception as e:
                        logger.error(f"Error processing geolocation for {ip}: {e}")

            if new_geo:
                self._upload_geolocation_data(new_geo, date, source)
                uploaded += len(new_geo)

        # Once, after every chunk: the MERGE is partition-scoped, so per-chunk runs
        # would re-scan for nothing.
        if uploaded:
            logger.info(f"Uploaded {uploaded:,} of {total:,} candidate IPs; resolving metros")
            self._update_metro_for_geolocation_table(date, source)

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

        # The metro polygon table is named once, in hermes.sql.loader.DEFAULT_PARAMS.
        # These files are read directly rather than through loader.load_query, so
        # substitute the placeholder here and keep the two paths in agreement.
        query = Template(query).safe_substitute(loader.DEFAULT_PARAMS)

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

    def _update_metro_for_geolocation_table(self, date: str, source: str = "topology") -> None:
        """Resolve ``metro`` for the rows just inserted, scoped to one partition.

        Parameters
        ----------
        date
            The partition enrichment just wrote, ``YYYY-MM-DD``. Required: the SQL
            is a partition-scoped MERGE, not a whole-table rebuild. It used to be
            ``CREATE OR REPLACE TABLE ... AS SELECT ... FROM itself``, rewriting all
            99.5M rows through a spatial join on every run -- which does not
            survive adding ~2.2M client IPs/day.
        """
        target = self._geoloc_table(source)
        logger.info(
            f"Computing metro for {'IPv6' if self.ipv6 else 'IPv4'} "
            f"{target.rsplit('.', 1)[1]} partition {date} (source={source})"
        )

        # Resolve the metro SQL from packaged data (hermes.sql.paths).
        sql_file = paths.query_path("enrich_ip_geoloc_add_metro.sql")
        if not sql_file.exists():
            logger.error(f"SQL file not found: {sql_file}")
            return

        # Read the SQL query
        query = sql_file.read_text()

        # Retarget the MERGE. The file names the IPv4 topology table; the same
        # statement serves IPv6 and the source-IP tables because all four share a
        # schema, so only the name changes.
        retargeted = f"`{self.project_id}.{target.split('.', 1)[1]}`"
        query = query.replace("`mlab-collaboration.hermes.unified_ip_to_geoloc`", retargeted)
        query = query.replace("`hermes.unified_ip_to_geoloc`", retargeted)

        # The metro polygon table is named once, in hermes.sql.loader.DEFAULT_PARAMS.
        # These files are read directly rather than through loader.load_query, so
        # substitute the placeholder here and keep the two paths in agreement.
        query = Template(query).safe_substitute({**loader.DEFAULT_PARAMS, "DAY": date})

        # No IPv6 special-casing any more. This used to inject
        # "PARTITION BY partition_date" into the CREATE OR REPLACE, because the v6
        # table was partitioned and the v4 one was not. Both are now partitioned at
        # creation and the statement is a partition-scoped MERGE, so there is
        # nothing to rewrite.

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

    def _upload_geolocation_data(
        self, geo_data: dict[str, dict[str, Any]], date: str, source: str = "topology"
    ) -> None:
        """Upload geolocation data to BigQuery."""
        snapshot_name = getattr(self.ipinfo, "snapshot_name", None)
        snapshot_day = None
        if snapshot_name:
            found = re.search(r"(\d{4}-\d{2}-\d{2})", snapshot_name)
            snapshot_day = found.group(1) if found else None

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
                # Which IPInfo dump produced this row. Without it a backfilled
                # partition is indistinguishable from a contemporaneous one.
                #
                # The DATE form is the join key steps 02/03 match on. partition_date
                # cannot serve that purpose: it is the RUN's target date, so a batched
                # run stamps every IP in a 20-day lookback with the same value (3.59M
                # rows landed on 2025-07-31 covering traffic from 07-11 onward). The
                # dump date is a property of the geolocation itself and is unaffected
                # by how runs happened to be batched.
                "geoloc_snapshot": snapshot_name,
                "geoloc_snapshot_date": snapshot_day,
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
                bigquery.SchemaField("geoloc_snapshot", "STRING"),
                bigquery.SchemaField("geoloc_snapshot_date", "DATE"),
            ],
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )

        table_ref = self._geoloc_table(source).split(".", 1)[1]
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

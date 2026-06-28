import io
import json
import logging
import os
from datetime import datetime

import pandas as pd
import requests
from google.cloud import bigquery
from tqdm import tqdm

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class ASMetadataEnricher:
    def __init__(self, project_id: str = "mlab-collaboration", batch_size: int = 1000):
        """Initialize AS metadata enricher.

        Args:
            project_id (str): Google Cloud project ID
            batch_size (int): Number of rows to insert in each batch
        """
        self.project_id = project_id
        self.batch_size = batch_size
        self.client = bigquery.Client(project=project_id)

        # Define BigQuery table
        self.table_ref = self.client.dataset("hermes").table("as_metadata")

        # Define schema
        self.schema = [
            bigquery.SchemaField("asn", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("asnName", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("rank", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField(
                "organization",
                "RECORD",
                mode="NULLABLE",
                fields=[
                    bigquery.SchemaField("orgId", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("orgName", "STRING", mode="NULLABLE"),
                ],
            ),
            bigquery.SchemaField("cliqueMember", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("seen", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("longitude", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("latitude", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField(
                "cone",
                "RECORD",
                mode="NULLABLE",
                fields=[
                    bigquery.SchemaField("numberAsns", "INTEGER", mode="NULLABLE"),
                    bigquery.SchemaField("numberPrefixes", "INTEGER", mode="NULLABLE"),
                    bigquery.SchemaField("numberAddresses", "INTEGER", mode="NULLABLE"),
                ],
            ),
            bigquery.SchemaField(
                "country",
                "RECORD",
                mode="NULLABLE",
                fields=[
                    bigquery.SchemaField("iso", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
                ],
            ),
            bigquery.SchemaField(
                "asnDegree",
                "RECORD",
                mode="NULLABLE",
                fields=[
                    bigquery.SchemaField("provider", "INTEGER", mode="NULLABLE"),
                    bigquery.SchemaField("peer", "INTEGER", mode="NULLABLE"),
                    bigquery.SchemaField("customer", "INTEGER", mode="NULLABLE"),
                    bigquery.SchemaField("total", "INTEGER", mode="NULLABLE"),
                    bigquery.SchemaField("transit", "INTEGER", mode="NULLABLE"),
                    bigquery.SchemaField("sibling", "INTEGER", mode="NULLABLE"),
                ],
            ),
            bigquery.SchemaField(
                "announcing",
                "RECORD",
                mode="NULLABLE",
                fields=[
                    bigquery.SchemaField("numberPrefixes", "INTEGER", mode="NULLABLE"),
                    bigquery.SchemaField("numberAddresses", "INTEGER", mode="NULLABLE"),
                ],
            ),
            bigquery.SchemaField("PeeringDB_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("PeeringDB_traffic", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("PeeringDB_ratio", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("PeeringDB_infotype", "STRING", mode="NULLABLE"),
            bigquery.SchemaField(
                "facilities",
                "RECORD",
                mode="NULLABLE",
                fields=[
                    bigquery.SchemaField("name", "STRING", mode="REPEATED"),
                    bigquery.SchemaField("city_cc", "STRING", mode="REPEATED"),
                ],
            ),
            bigquery.SchemaField(
                "user_population",
                "RECORD",
                mode="NULLABLE",
                fields=[
                    bigquery.SchemaField("country", "STRING", mode="REPEATED"),
                    bigquery.SchemaField("samples", "INTEGER", mode="REPEATED"),
                    bigquery.SchemaField("users", "INTEGER", mode="REPEATED"),
                    bigquery.SchemaField(" % of country", "FLOAT", mode="REPEATED"),
                    bigquery.SchemaField(" % of internet", "FLOAT", mode="REPEATED"),
                ],
            ),
            bigquery.SchemaField("partition_date", "DATE", mode="NULLABLE"),  # Added date field
        ]

    def fetch_apnic_eyeballs(self, date: str) -> pd.DataFrame:
        """Fetch APNIC eyeballs data for a specific date.

        Args:
            date (str): Date in YYYY/MM/DD format

        Returns:
            pd.DataFrame: Processed APNIC eyeballs data
        """
        url = f"https://stats.labs.apnic.net/cgi-bin/aspop?w=120&d={date}&f=c"

        try:
            response = requests.get(url)
            response.raise_for_status()

            df = pd.read_csv(io.StringIO(response.text), header=1)
            df["AS"] = df["AS"].apply(lambda x: int(x.split("AS")[1]))
            df.fillna("", inplace=True)

            # Select and group columns
            df = df[
                [
                    "#Rank",
                    "AS",
                    "AS Name",
                    "CC",
                    "Users (est.)",
                    "% of Country",
                    "% of Internet",
                    "Samples",
                ]
            ]
            df = df.groupby(["AS", "AS Name"]).agg(lambda x: list(x)).reset_index()
            df = df.set_index("AS")

            return df

        except Exception as e:
            logger.error(f"Error fetching APNIC eyeballs data: {e}")
            raise

    def process_facilities_data(self, df_facilities: pd.DataFrame) -> dict:
        """Process facilities data.

        Args:
            df_facilities (pd.DataFrame): Facilities data

        Returns:
            Dict: Processed facilities data
        """
        df_facilities.fillna("", inplace=True)
        df_facilities["city_cc"] = df_facilities["city"] + "-" + df_facilities["country"]

        agg_df = (
            df_facilities.groupby(["local_asn"])
            .agg({"city_cc": lambda x: list(x), "name": lambda x: list(x)})
            .reset_index()
        )

        return agg_df.set_index("local_asn").to_dict("index")

    def process_as_type_data(self, df_as_type: pd.DataFrame) -> dict:
        """Process AS type data.

        Args:
            df_as_type (pd.DataFrame): AS type data

        Returns:
            Dict: Processed AS type data
        """
        df_as_type.fillna("", inplace=True)
        return df_as_type.set_index("asn").to_dict("index")

    def augment_caida_data(
        self,
        caida_file: str,
        output_file: str,
        facilities_dict: dict,
        as_type_dict: dict,
        apnic_data: pd.DataFrame,
        date: str,
    ) -> None:
        """Augment CAIDA ASN data with additional metadata.

        Args:
            caida_file (str): Path to CAIDA ASN file
            output_file (str): Path to output file
            facilities_dict (Dict): Processed facilities data
            as_type_dict (Dict): Processed AS type data
            apnic_data (pd.DataFrame): APNIC eyeballs data
            date (str): Date in YYYY-MM-DD format
        """
        with open(caida_file) as infile, open(output_file, "w") as outfile:
            for line in infile:
                data = json.loads(line.strip())
                asn = int(data["asn"])

                # Add user population data
                data["user_population"] = {}
                if asn in apnic_data.index:
                    data["user_population"] = {
                        "country": apnic_data.loc[asn]["CC"],
                        "samples": apnic_data.loc[asn]["Samples"],
                        "users": apnic_data.loc[asn]["Users (est.)"],
                        "% of country": apnic_data.loc[asn]["% of Country"],
                        "% of internet": apnic_data.loc[asn]["% of Internet"],
                    }

                # Add facilities data
                data["facilities"] = {}
                if asn in facilities_dict:
                    data["facilities"] = {
                        "city_cc": facilities_dict[asn]["city_cc"],
                        "name": facilities_dict[asn]["name"],
                    }

                # Add PeeringDB data
                if asn in as_type_dict:
                    data["PeeringDB_infotype"] = as_type_dict[asn]["info_type"]
                    data["PeeringDB_ratio"] = as_type_dict[asn]["info_ratio"]
                    data["PeeringDB_traffic"] = as_type_dict[asn]["info_traffic"]
                    data["PeeringDB_name"] = as_type_dict[asn]["name"]
                else:
                    data["PeeringDB_infotype"] = ""
                    data["PeeringDB_ratio"] = ""
                    data["PeeringDB_traffic"] = ""
                    data["PeeringDB_name"] = ""

                # Add date
                data["partition_date"] = date

                outfile.write(json.dumps(data) + "\n")

    def upload_to_bigquery(self, data_file: str) -> bool:
        """Upload augmented AS metadata to BigQuery.

        Args:
            data_file (str): Path to augmented data file

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Load data
            rows_to_insert = []
            with open(data_file) as f:
                for line in f:
                    rows_to_insert.append(json.loads(line.strip()))

            # Insert in batches
            batches = [
                rows_to_insert[i : i + self.batch_size]
                for i in range(0, len(rows_to_insert), self.batch_size)
            ]

            for batch in tqdm(batches, desc="Uploading to BigQuery"):
                errors = self.client.insert_rows_json(self.table_ref, batch)
                if errors:
                    logger.error(f"Error uploading batch: {errors}")
                    return False

            return True

        except Exception as e:
            logger.error(f"Error uploading to BigQuery: {e}")
            return False

    def update_as_metadata(
        self, date: str, caida_file: str, facilities_file: str, as_type_file: str
    ) -> bool:
        """Update AS metadata for a specific date.

        Args:
            date (str): Date in YYYY-MM-DD format
            caida_file (str): Path to CAIDA ASN file
            facilities_file (str): Path to facilities data file
            as_type_file (str): Path to AS type data file

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Convert date format for APNIC
            apnic_date = datetime.strptime(date, "%Y-%m-%d").strftime("%Y/%m/%d")

            # Load and process data
            df_facilities = pd.read_csv(facilities_file, index_col=0)
            df_as_type = pd.read_csv(as_type_file, index_col=0)

            facilities_dict = self.process_facilities_data(df_facilities)
            as_type_dict = self.process_as_type_data(df_as_type)
            apnic_data = self.fetch_apnic_eyeballs(apnic_date)

            # Create output filename
            output_file = caida_file.replace(".json", f"-augmented-{date}.json")

            # Augment and upload data
            self.augment_caida_data(
                caida_file, output_file, facilities_dict, as_type_dict, apnic_data, date
            )

            if not self.upload_to_bigquery(output_file):
                logger.error("Failed to upload data to BigQuery")
                return False

            logger.info(f"Successfully updated AS metadata for {date}")
            return True

        except Exception as e:
            logger.error(f"Error updating AS metadata: {e}")
            return False


def update_as_metadata(date: str) -> bool:
    """Update AS metadata as part of the HERMES enrichment pipeline.

    Args:
        date (str): Date in YYYY-MM-DD format

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        enricher = ASMetadataEnricher()

        # Define file paths
        base_dir = os.path.expanduser("~/Documents/GitHub/missing-peering-links")
        caida_file = os.path.join(
            base_dir, "data", "BGP_data", f"ASNS-{date.replace('-', '')}.json"
        )
        facilities_file = os.path.join(
            base_dir,
            "scripts",
            "data",
            "PeeringDB",
            f"AS_footprint_info_{date.split('-')[0]}-{date.split('-')[1]}.csv",
        )
        as_type_file = os.path.join(
            base_dir,
            "scripts",
            "data",
            "PeeringDB",
            f"AS_Type{date.split('-')[0]}-{date.split('-')[1]}.csv",
        )

        success = enricher.update_as_metadata(date, caida_file, facilities_file, as_type_file)

        if not success:
            logger.error("Failed to update AS metadata")
            return False

        logger.info("AS metadata update completed successfully")
        return True

    except Exception as e:
        logger.error(f"Error updating AS metadata: {str(e)}")
        return False


if __name__ == "__main__":
    # Example usage
    success = update_as_metadata("2025-06-10")

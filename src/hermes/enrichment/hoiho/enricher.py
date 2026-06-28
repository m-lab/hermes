import os
import pickle
import time
from typing import Any

import requests
from tqdm import tqdm

from hermes.enrichment.utils.common import BaseEnrichment, logger

HOIHO_URL = "https://api.hoiho.caida.org/lookups"


def load_pickle(file_path):
    """Load and return the object stored in a pickle file."""
    try:
        with open(file_path, "rb") as file:
            data = pickle.load(file)
        logger.info(f"Pickle file loaded successfully: {file_path}")
        return data
    except FileNotFoundError:
        logger.warning(f"Pickle file not found: {file_path}. Returning an empty dictionary.")
        return {}
    except pickle.UnpicklingError as e:
        logger.error(f"Invalid pickle file: {file_path}. Error: {e}")
        raise


def dump_pickle(obj, file_path):
    """Serialize a Python object and save it to a pickle file."""
    try:
        with open(file_path, "wb") as file:
            pickle.dump(obj, file)
        logger.info(f"Object successfully saved to: {file_path}")
    except Exception as e:
        logger.error(f"Error saving pickle file: {file_path}. Error: {e}")
        raise


class HOIHOEnricher(BaseEnrichment):
    def __init__(self, project_id: str = "mlab-collaboration"):
        """Initialize HOIHO enricher."""
        super().__init__(project_id)

        # API configuration
        self.hoiho_url = HOIHO_URL
        self.req_size = 10000  # Number of domains to query in one batch
        self.sleep_time = 1  # Sleep time between API calls

        self.hoiho_cache_path = os.path.join(self.cache_dir, "hoiho_output.pkl")
        # Load existing cache or initialize an empty cache
        if os.path.exists(self.hoiho_cache_path):
            logger.info(f"Loading HOIHO cache from: {self.hoiho_cache_path}")
            self.hoiho_cache = load_pickle(self.hoiho_cache_path)
        else:
            logger.warning(
                f"No existing cache found at: {self.hoiho_cache_path}. Initializing a new one."
            )
            self.hoiho_cache = {}
            dump_pickle(self.hoiho_cache, self.hoiho_cache_path)

    def query_hoiho(self, rdns_list: list[str]) -> dict[str, Any]:
        """Query HOIHO API in batches."""
        rdns_list = list(set(rdns_list))
        hoiho_responses = {}

        for i in tqdm(range((len(rdns_list) // self.req_size) + 1), desc="Querying HOIHO API"):
            batch = rdns_list[i * self.req_size : (i + 1) * self.req_size]
            if not batch:
                continue

            logger.info(f"Processing batch {i + 1}/{(len(rdns_list) // self.req_size) + 1}")
            try:
                response = requests.post(self.hoiho_url, json=batch)
                if response.status_code == 200 and "matches" in response.json():
                    matches = response.json().get("matches", [])
                    logger.info(f"Batch {i + 1}: Retrieved {len(matches)} matches.")
                    for match in matches:
                        hoiho_responses[match["hostname"].lower()] = match
                else:
                    logger.warning(
                        f"Batch {i + 1}: Failed to retrieve matches. Status: {response.status_code}"
                    )
            except requests.RequestException as e:
                logger.error(f"Batch {i + 1}: Request failed. Error: {e}")
            time.sleep(self.sleep_time)

        return hoiho_responses

    def enrich_hoiho_info(self, rdns_cache: dict[str, str]) -> dict[str, Any]:
        """Enrich HOIHO information for given IPs."""
        # Normalize input hostnames (remove trailing dots, lowercase)
        candidate_domains = {d.strip().lower().rstrip(".") for d in rdns_cache.values() if d}
        missing_domains = list(candidate_domains - self.hoiho_cache.keys())

        if missing_domains:
            logger.info(f"Querying HOIHO API for {len(missing_domains)} missing domains.")
            responses = self.query_hoiho(missing_domains)
            print(f"HOIHO responses received for {len(responses)} domains.")
            print(f"Total HOIHO cache size before update: {len(self.hoiho_cache)} domains.")

            # Normalize response keys before caching (remove trailing dots)
            normalized_responses = {k.lower().rstrip("."): v for k, v in responses.items()}
            self.hoiho_cache.update(normalized_responses)
            print(f"Total HOIHO cache size: {len(self.hoiho_cache)} domains.")
            dump_pickle(self.hoiho_cache, self.hoiho_cache_path)

        # Use normalized keys for lookup
        hoiho_info = {
            rdns: self.hoiho_cache[rdns.strip().lower().rstrip(".")]
            for rdns in rdns_cache.values()
            if rdns and rdns.strip().lower().rstrip(".") in self.hoiho_cache
        }
        print(hoiho_info)
        return hoiho_info


if __name__ == "__main__":
    cache_path = "../cache/hoiho_output.pkl"

    if not os.path.exists(cache_path):
        print(f"Cache file not found at: {cache_path}")
        print("Please check the path to your cache file.")
    else:
        # Load the cache
        with open(cache_path, "rb") as f:
            hoiho_cache = pickle.load(f)

        print(f"Total cache entries: {len(hoiho_cache)}")

        # Count entries with trailing dots
        entries_with_trailing_dot = [k for k in hoiho_cache.keys() if k.endswith(".")]
        entries_without_trailing_dot = [k for k in hoiho_cache.keys() if not k.endswith(".")]

        print(
            f"\nEntries WITH trailing dot: {len(entries_with_trailing_dot)} ({100 * len(entries_with_trailing_dot) / len(hoiho_cache):.2f}%)"
        )
        print(
            f"Entries WITHOUT trailing dot: {len(entries_without_trailing_dot)} ({100 * len(entries_without_trailing_dot) / len(hoiho_cache):.2f}%)"
        )

        # Show some examples
        if entries_with_trailing_dot:
            print("\nExamples of entries WITH trailing dot (first 10):")
            for key in list(entries_with_trailing_dot)[:10]:
                print(f"  - {key}")

        if entries_without_trailing_dot:
            print("\nExamples of entries WITHOUT trailing dot (first 10):")
            for key in list(entries_without_trailing_dot)[:10]:
                print(f"  - {key}")

        # Check for potential duplicates (same domain with and without trailing dot)
        normalized_keys = {}
        duplicates = []
        for key in hoiho_cache.keys():
            normalized = key.rstrip(".")
            if normalized in normalized_keys:
                duplicates.append((normalized_keys[normalized], key))
            else:
                normalized_keys[normalized] = key

        if duplicates:
            print(
                f"\n⚠️  Found {len(duplicates)} potential duplicate pairs (same domain with/without trailing dot):"
            )
            for orig, dup in duplicates[:10]:
                print(f"  - '{orig}' vs '{dup}'")
            if len(duplicates) > 10:
                print(f"  ... and {len(duplicates) - 10} more")

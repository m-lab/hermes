"""Generate the three files ``hermes.as_metadata`` is built from, in-process.

These used to come from two scripts in the private ``missing-peering-links``
repo, run as subprocesses under that repo's own conda environment:

* ``get_as_rank_data.py``   -> ``ASNS-<YYYYMMDD>.json``   (CAIDA AS Rank)
* ``PeeringDB_Crawler.py``  -> ``AS_Type<Y>-<M>.csv`` and
                               ``AS_footprint_info_<Y>-<M>.csv``

Neither can run on ``hermes-ec2``: the repo is not there. Worse, the guard in
``refresh_topology_tables.generate_as_metadata_inputs`` only checks that
``<repo>/scripts`` is a directory, and an empty ``scripts/`` tree exists on the
VM as a side effect of an old ``os.makedirs`` -- so the check PASSES and the run
then dies on the first missing script. ``hermes.as_metadata`` has sat at its
2026-07-01 snapshot ever since, which matters more than it sounds: it is the
source of ``cone.numberPrefixes`` (the prefix-overlap tiebreak in step 04) and
``organization.OrgName`` (every hop's ``associated_org``), so a stale table means
newly-allocated ASNs carry no organisation at all.

Both inputs are public and unauthenticated:

* **CAIDA AS Rank**, ``api.asrank.caida.org/v2/graphql``, paginated over ~121k
  ASNs. Queried with plain ``requests``; the upstream script used
  ``graphqlclient``, which is one dependency for a single POST.
* **PeeringDB**, from the same CAIDA archived daily dump the IXP snapshot
  already downloads -- so this adds no new source, and the AS-metadata inputs
  become dated and backfillable like everything else.

The geocoding in ``PeeringDB_Crawler`` (``generating_IXP_data``,
``match_org_asn_facilities_lat_lon``) is not on this path: ``netfac`` already
carries ``city`` and ``country`` per record, so no Nominatim lookups are needed.

Output formats are reproduced exactly, because ``ASMetadataEnricher`` reads them
with ``pd.read_csv(..., index_col=0)`` and line-delimited ``json.loads``:

* ASNS file is **JSONL** -- one AS Rank node per line, not a JSON array.
* Both CSVs are written **with** the DataFrame index, since the reader discards
  column 0 before using the named columns.
"""

import json
import logging
import os
import time
from pathlib import Path
from typing import Any

import pandas as pd
import requests

logger = logging.getLogger(__name__)

ASRANK_URL = "https://api.asrank.caida.org/v2/graphql"

#: AS Rank fields, matching the hermes.as_metadata schema one-for-one.
_ASNS_QUERY = """{
  asns(first:%d, offset:%d) {
    totalCount
    pageInfo { first hasNextPage }
    edges { node {
      asn asnName rank
      organization { orgId orgName }
      cliqueMember seen longitude latitude
      cone { numberAsns numberPrefixes numberAddresses }
      country { iso name }
      asnDegree { provider peer customer total transit sibling }
      announcing { numberPrefixes numberAddresses }
    } }
  }
}"""


def _post_page(
    http: requests.Session,
    url: str,
    query: str,
    timeout: float,
    attempts: int,
    offset: int,
) -> dict[str, Any]:
    """POST one page, retrying transient failures.

    Deep offsets are slow -- roughly 0.5s per page below offset 20,000 rising to
    ~10s past 60,000 -- so a page occasionally exceeds the read timeout. Without
    a retry a single slow page discards the whole crawl: one real run died at
    113,000 of 121,200 ASNs. Retries only transport-level failures and 5xx; a
    GraphQL error in the body is a real problem and is raised by the caller.
    """
    last: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            response = http.post(url, json={"query": query}, timeout=timeout)
            response.raise_for_status()
            return dict(response.json())
        except (requests.Timeout, requests.ConnectionError) as err:
            last = err
        except requests.HTTPError as err:
            status = err.response.status_code if err.response is not None else 0
            if status < 500:
                raise
            last = err
        if attempt < attempts:
            backoff = 5 * attempt
            logger.warning(
                "AS Rank offset %d failed (%s), attempt %d/%d, retrying in %ds",
                offset,
                type(last).__name__,
                attempt,
                attempts,
                backoff,
            )
            time.sleep(backoff)
    raise RuntimeError(f"AS Rank offset {offset} failed after {attempts} attempts: {last}")


def fetch_asrank_asns(
    out_path: str | Path,
    page_size: int = 1000,
    session: requests.Session | None = None,
    timeout: float = 300.0,
    url: str = ASRANK_URL,
    attempts: int = 4,
) -> int:
    """Download every AS Rank record to `out_path` as JSONL.

    Replaces ``get_as_rank_data.py``. Paginates on ``offset`` until
    ``hasNextPage`` is false, writing one node per line.

    Do not raise `page_size` looking for a speed-up -- it is measurably worse.
    With the full field set, ``first:10000`` took 47.3s for 10k rows against
    0.6s for 1k, so the server cost is superlinear in page size (a query asking
    only for ``asn`` shows the opposite, which is why that is a misleading thing
    to benchmark). What does cost time is offset depth: ~0.5s per page below
    offset 20,000, rising to ~10s beyond 60,000. A full ~121k-ASN crawl lands
    around 15-20 minutes, which is why this belongs on a monthly timer rather
    than inline anywhere.

    Returns:
        Number of ASNs written.

    Raises:
        RuntimeError: if the API returns an error or stops making progress --
            silently writing a short file would leave as_metadata missing ASNs
            with nothing to show for it.
    """
    http = session or requests.Session()
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    written = offset = 0
    total: int | None = None
    with out_path.open("w") as out:
        while True:
            payload = _post_page(
                http, url, _ASNS_QUERY % (page_size, offset), timeout, attempts, offset
            )
            if "errors" in payload:
                raise RuntimeError(f"AS Rank query failed at offset {offset}: {payload['errors']}")

            block = payload["data"]["asns"]
            total = block["totalCount"] if total is None else total
            edges = block["edges"]
            for edge in edges:
                out.write(json.dumps(edge["node"]) + "\n")
            written += len(edges)

            if not block["pageInfo"]["hasNextPage"]:
                break
            if not edges:
                raise RuntimeError(
                    f"AS Rank reported hasNextPage at offset {offset} but returned no rows"
                )
            offset += block["pageInfo"]["first"]

    logger.info("AS Rank: wrote %d of %s ASNs to %s", written, total, out_path)
    if total is not None and written < total:
        raise RuntimeError(f"AS Rank returned {written} of {total} ASNs")
    return written


def _dump_frame(dump: dict[str, Any], table: str) -> pd.DataFrame:
    rows = dump.get(table, {}).get("data") or []
    return pd.DataFrame(list(rows))


def peeringdb_as_type(dump: dict[str, Any]) -> pd.DataFrame:
    """PeeringDB network type/traffic per ASN, as ``AS_Type<Y>-<M>.csv``.

    Consumed via ``process_as_type_data``, which indexes on ``asn`` and reads
    ``info_type``, ``info_ratio``, ``info_traffic`` and ``name``.
    """
    frame = _dump_frame(dump, "net")
    columns = ["asn", "name", "org_id", "info_traffic", "info_ratio", "info_type"]
    missing = [c for c in columns if c not in frame.columns]
    if missing:
        raise RuntimeError(f"PeeringDB 'net' table missing columns: {missing}")
    return frame[columns]


def peeringdb_as_facilities(dump: dict[str, Any]) -> pd.DataFrame:
    """Per-ASN facility presence, as ``AS_footprint_info_<Y>-<M>.csv``.

    Consumed via ``process_facilities_data``, which groups on ``local_asn`` and
    builds ``city + "-" + country`` plus the facility ``name``. ``netfac``
    already carries all four, so no join against ``fac`` -- and no geocoding --
    is required.
    """
    frame = _dump_frame(dump, "netfac")
    columns = ["local_asn", "name", "city", "country"]
    missing = [c for c in columns if c not in frame.columns]
    if missing:
        raise RuntimeError(f"PeeringDB 'netfac' table missing columns: {missing}")
    return frame[columns]


def generate_as_metadata_inputs_native(
    date: str, paths: dict[str, Path], dump_path: str | Path
) -> None:
    """Write the three input files for `date` from public sources.

    Args:
        date: Target date, ``YYYY-MM-DD``.
        paths: ``{"caida": ..., "footprint": ..., "as_type": ...}`` -- the exact
            locations ``ASMetadataEnricher`` will read.
        dump_path: A CAIDA PeeringDB bulk dump, as fetched by
            ``peeringdb_ixp.snapshot.download_peeringdb_dump``.
    """
    with open(dump_path) as handle:
        dump = json.load(handle)

    for key in ("as_type", "footprint"):
        paths[key].parent.mkdir(parents=True, exist_ok=True)

    # index=True on purpose: the reader uses index_col=0 and discards column 0.
    as_type = peeringdb_as_type(dump)
    as_type.to_csv(paths["as_type"])
    facilities = peeringdb_as_facilities(dump)
    facilities.to_csv(paths["footprint"])
    logger.info(
        "[AS_METADATA] %d net rows -> %s, %d netfac rows -> %s",
        len(as_type),
        paths["as_type"].name,
        len(facilities),
        paths["footprint"].name,
    )

    fetch_asrank_asns(paths["caida"])

    missing = [str(p) for p in paths.values() if not os.path.exists(p)]
    if missing:
        raise RuntimeError("as_metadata input generation did not produce: " + ", ".join(missing))

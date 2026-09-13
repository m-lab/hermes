"""Build the IXP membership snapshot in-process, without the companion repo.

The IXP snapshot used to be produced by ``wrapper.py`` in the private
``missing-peering-links`` repo, which orchestrated six scripts through
subprocesses under its own conda environment and wrote a chain of intermediate
CSV and pipe-delimited files. Nothing about that chain could run on
``hermes-ec2``: the paths were hardcoded to one laptop, the repo was not there,
and neither was the interpreter. That is why ``ix_data.ixp_members`` went 96 days
without a refresh.

This module reproduces the same result natively. It keeps the upstream
semantics exactly -- the merge precedence, the IXP name normalisation, the
private-address filtering and the output ordering are ported rather than
reinvented, because a subtle difference here silently mis-attributes IXP hops
rather than failing loudly. What it drops is the incidental machinery: no
subprocesses, no conda environment, no CSV round-trip, no geocoding.

Two external inputs remain, both public and unauthenticated:

* **PeeringDB**, via CAIDA's archived daily bulk dump at
  ``publicdata.caida.org/datasets/peeringdb-v2/`` -- the same host the
  RouteViews enricher already uses, and dated, so IXP snapshots become
  backfillable the way BGP ones are.
* **PCH**, via ``pch.net/api/ixp``, which is live-only. It is polled one IXP at
  a time with a delay between calls, matching upstream; that politeness is what
  makes a full run take roughly an hour.

Upstream quirks preserved deliberately, each verified against the original:

* PeeringDB prefixes are grouped by IXP name keeping only the **first** IPv4 and
  IPv6 prefix per name. Lossy, but changing it would change which prefixes the
  name map is built from.
* Sources are merged **PCH first, then PeeringDB**, and the first source to
  claim an IP wins.
* Within one source, the **last** row for an IP wins.
* The combined output file is overwritten by the IPv4-only file upstream (both
  are written to the same path), so ``merged-members-gen-<date>.txt`` is IPv4
  and ``..._ipv6.txt`` is IPv6. Reproduced, because that is what
  ``_latest_merged_members_file`` and ``process_data_file`` expect.
"""

import gzip
import ipaddress
import json
import logging
import os
import shutil
import struct
import tempfile
import time
from collections import OrderedDict, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from functools import lru_cache
from pathlib import Path
from socket import inet_aton
from typing import Any

import certifi
import radix
import requests

logger = logging.getLogger(__name__)

CAIDA_PEERINGDB_BASE = "https://publicdata.caida.org/datasets/peeringdb-v2/"
PCH_BASE_URL = "https://www.pch.net/api/ixp"
#: EuroIX IXP Database. Note api.euro-ix.net does not exist; this is the host
#: the IXPDB API documentation actually points at.
IXPDB_PROVIDER_LIST = "https://api.ixpdb.net/v1/provider/list"

#: Seconds between PCH per-IXP calls. Upstream uses 3; keep it unless you have
#: cleared a faster rate with PCH -- there are ~1000 active IXPs behind this.
PCH_REQUEST_DELAY = float(os.environ.get("HERMES_IXP_PCH_DELAY", "3"))


# --------------------------------------------------------------------------
# PeeringDB: CAIDA archived bulk dump
# --------------------------------------------------------------------------


def download_peeringdb_dump(
    date: str, cache_dir: str, max_days_lookback: int = 7
) -> tuple[str, str] | None:
    """Download CAIDA's PeeringDB bulk dump for `date`, falling back nearby.

    Mirrors the RouteViews enricher: try the exact date, then -1..-N, then
    +1..+N, and report which date actually supplied the data so the caller can
    stamp real vintage rather than the date it happened to ask for.

    Args:
        date: Target date, ``YYYY-MM-DD`` or ``YYYYMMDD``.
        cache_dir: Directory to download into (created if absent).
        max_days_lookback: Days either side to search.

    Returns:
        ``(path, actual_date)`` with ``actual_date`` as ``YYYY-MM-DD``, or None.
    """
    os.makedirs(cache_dir, exist_ok=True)
    target = datetime.strptime(date, "%Y-%m-%d" if "-" in date else "%Y%m%d")

    offsets = (
        [0] + list(range(-1, -max_days_lookback - 1, -1)) + list(range(1, max_days_lookback + 1))
    )
    for offset in offsets:
        day = target + timedelta(days=offset)
        path = _try_peeringdb_dump(day, cache_dir)
        if path:
            actual = day.strftime("%Y-%m-%d")
            if offset:
                logger.warning("Using PeeringDB dump from %s (requested %s)", actual, target.date())
            return path, actual

    logger.error("No PeeringDB dump for %s or within %d days", target.date(), max_days_lookback)
    return None


def _try_peeringdb_dump(day: datetime, cache_dir: str) -> str | None:
    """Fetch one day's dump, using the cached copy when present."""
    name = f"peeringdb_2_dump_{day:%Y_%m_%d}.json"
    path = os.path.join(cache_dir, name)
    if os.path.isfile(path) and os.path.getsize(path) > 0:
        logger.info("PeeringDB dump %s already cached", path)
        return path

    url = f"{CAIDA_PEERINGDB_BASE}{day:%Y}/{day:%m}/{name}"
    try:
        head = requests.head(url, timeout=30)
        if head.status_code != 200:
            return None
        logger.info("Downloading %s", url)
        with requests.get(url, stream=True, timeout=600) as r:
            r.raise_for_status()
            tmp = path + ".part"
            with open(tmp, "wb") as f:
                shutil.copyfileobj(r.raw if not _is_gzip(r) else gzip.GzipFile(fileobj=r.raw), f)
            os.replace(tmp, path)
        return path
    except Exception as err:  # network/IO: try the next candidate date
        logger.warning("PeeringDB dump %s unavailable: %s", url, err)
        return None


def _is_gzip(response: requests.Response) -> bool:
    return response.headers.get("Content-Encoding") == "gzip" or response.url.endswith(".gz")


def _rows(dump: dict[str, Any], table: str) -> list[dict[str, Any]]:
    return dump.get(table, {}).get("data", []) or []


def peeringdb_records(
    dump_path: str,
) -> tuple[list[tuple[str, list[str], list[str]]], list[tuple[str, str, list[str], list[str]]]]:
    """Derive IXP prefixes and members from a PeeringDB bulk dump.

    Replaces ``PeeringDB_Crawler.generating_IXP_prefixes`` /
    ``generating_IXP_networks`` plus the two scripts that reformatted their CSVs.

    Returns:
        ``(prefix_records, member_records)`` where a prefix record is
        ``(ixp_name, v4_prefixes, v6_prefixes)`` and a member record is
        ``(ixp_name, asn, v4_ips, v6_ips)``.
    """
    with open(dump_path) as f:
        dump = json.load(f)

    ixlan_to_ix = {row["id"]: row.get("ix_id") for row in _rows(dump, "ixlan")}
    ix_name = {row["id"]: row.get("name") for row in _rows(dump, "ix")}

    # Prefixes: ixpfx -> ixlan -> ix, then ONE v4 and ONE v6 per IXP name.
    # "First wins" per name, matching the upstream groupby(...).agg('first').
    by_name: OrderedDict[str, dict[str, str]] = OrderedDict()
    for row in _rows(dump, "ixpfx"):
        name = ix_name.get(ixlan_to_ix.get(row.get("ixlan_id")))
        prefix = row.get("prefix")
        if not name or not prefix:
            continue
        slot = "prefix_v4" if row.get("protocol") == "IPv4" else "prefix_v6"
        entry = by_name.setdefault(name, {})
        entry.setdefault(slot, prefix)

    prefix_records = [
        (
            name,
            [entry["prefix_v4"]] if entry.get("prefix_v4") else [],
            [entry["prefix_v6"]] if entry.get("prefix_v6") else [],
        )
        for name, entry in by_name.items()
    ]

    # Members: netixlan already carries name, asn and both addresses.
    member_records = []
    for row in _rows(dump, "netixlan"):
        name = row.get("name")
        asn = row.get("asn")
        if name is None or asn is None:
            continue
        v4 = [row["ipaddr4"]] if row.get("ipaddr4") else []
        v6 = [row["ipaddr6"]] if row.get("ipaddr6") else []
        member_records.append((str(name), str(asn), v4, v6))

    logger.info(
        "PeeringDB: %d IXP prefix records, %d member records",
        len(prefix_records),
        len(member_records),
    )
    return prefix_records, member_records


# --------------------------------------------------------------------------
# PCH: live API
# --------------------------------------------------------------------------


def pch_records(
    delay: float = PCH_REQUEST_DELAY, session: requests.Session | None = None
) -> tuple[list[tuple[str, list[str], list[str]]], list[tuple[str, str, list[str], list[str]]]]:
    """Fetch IXP prefixes and members from pch.net.

    One request per active IXP with `delay` seconds between them, as upstream
    does. An IXP whose response is malformed is skipped rather than failing the
    run, again matching upstream.
    """
    http = session or requests.Session()
    directory = http.get(f"{PCH_BASE_URL}/directory/Active", timeout=120).json()

    prefix_records: list[tuple[str, list[str], list[str]]] = []
    member_records: list[tuple[str, str, list[str], list[str]]] = []

    for ixp in directory:
        if delay:
            time.sleep(delay)
        try:
            detail = http.get(f"{PCH_BASE_URL}/subnet_details/{ixp['id']}", timeout=120).json()
        except Exception as err:
            logger.warning("PCH subnet_details failed for %s: %s", ixp.get("name"), err)
            continue
        if not isinstance(detail, dict):
            logger.info("Unexpected PCH response for %s (id:%s)", ixp.get("name"), ixp.get("id"))
            continue

        v4_prefixes: set[str] = set()
        v6_prefixes: set[str] = set()
        v4_members: defaultdict[str, set[str]] = defaultdict(set)
        v6_members: defaultdict[str, set[str]] = defaultdict(set)

        for family, prefixes, members in (
            ("IPv6", v6_prefixes, v6_members),
            ("IPv4", v4_prefixes, v4_members),
        ):
            for subnet, entries in (detail.get(family) or {}).items():
                prefixes.add(subnet)
                for entry in entries.values():
                    members[str(entry["asn"])].add(entry["ip"])

        name = ixp["name"]
        for asn in v4_members.keys() | v6_members.keys():
            member_records.append((name, asn, sorted(v4_members[asn]), sorted(v6_members[asn])))
        prefix_records.append((name, sorted(v4_prefixes), sorted(v6_prefixes)))

    logger.info(
        "PCH: %d IXP prefix records, %d member records",
        len(prefix_records),
        len(member_records),
    )
    return prefix_records, member_records


# --------------------------------------------------------------------------
# EuroIX: IXPDB directory + per-IXP IX-F member exports
# --------------------------------------------------------------------------


def _ixf_cidrs(vlan: dict[str, Any], family: str) -> str | None:
    """``{"prefix": "185.1.210.0", "mask_length": 23}`` -> ``185.1.210.0/23``."""
    entry = vlan.get(family) or {}
    prefix, mask = entry.get("prefix"), entry.get("mask_length")
    return f"{prefix}/{mask}" if prefix and mask is not None else None


def _ixf_extract(
    doc: dict[str, Any], ixpdb_id: int, name: str
) -> tuple[tuple[str, list[str], list[str]] | None, list[tuple[str, str, list[str], list[str]]]]:
    """Pull one IXP's prefixes and members out of an IX-F export document.

    Scoping matters: a single export can serve many IXPs -- lg.megaport.com
    publishes 35 in one document -- so connections must be filtered to the
    ``ixp_list`` entry whose ``ixf_id`` is this IXP's IXPDB id. Counting the
    whole document against one IXP inflates its apparent contribution wildly
    (Megaport's Ashburn read as 3,408 interfaces instead of 124).
    """
    entries = [e for e in doc.get("ixp_list", []) if e.get("ixf_id") == ixpdb_id]
    if not entries:
        return None, []
    local_ids = {e.get("ixp_id") for e in entries}

    v4_pfx, v6_pfx = [], []
    for entry in entries:
        for vlan in entry.get("vlan") or []:
            if c := _ixf_cidrs(vlan, "ipv4"):
                v4_pfx.append(c)
            if c := _ixf_cidrs(vlan, "ipv6"):
                v6_pfx.append(c)

    members = []
    for member in doc.get("member_list", []):
        asn = member.get("asnum")
        v4, v6 = [], []
        for conn in member.get("connection_list") or []:
            if conn.get("ixp_id") not in local_ids:
                continue
            for vlan in conn.get("vlan_list") or []:
                if addr := (vlan.get("ipv4") or {}).get("address"):
                    v4.append(addr)
                if addr := (vlan.get("ipv6") or {}).get("address"):
                    v6.append(addr)
        if asn is not None and (v4 or v6):
            members.append((name, str(asn), v4, v6))

    return (name, v4_pfx, v6_pfx), members


#: Cross-signed ISRG Root YR, shipped beside this module. See the header inside
#: the file for provenance and for when to delete it.
_EXTRA_CA = Path(__file__).with_name("isrg-root-yr-cross-signed.pem")


@lru_cache(maxsize=1)
def _ca_bundle() -> str:
    """certifi plus the cross-signed ISRG Root YR, for EuroIX fetches only.

    AMS-IX's export is served under a Let's Encrypt chain terminating at ISRG
    Root YR, which certifi does not yet carry (verified against 2026.7.22, the
    latest release) -- so the fetch fails verification even though AMS-IX's
    configuration is correct. That single URL covers 14 AMS-IX exchanges and
    1,207 participants.

    The certificate added is the CROSS-SIGNED form, issued by ISRG Root X1,
    which certifi already trusts -- so it is verifiable rather than asserted.
    Scoped to this fetcher; nothing else in hermes uses this bundle, and
    verification is never disabled. Remove once certifi ships the root.
    """
    if not _EXTRA_CA.is_file():
        return certifi.where()
    fd, path = tempfile.mkstemp(prefix="hermes-ixp-ca-", suffix=".pem")
    with os.fdopen(fd, "w") as out:
        out.write(Path(certifi.where()).read_text())
        out.write("\n")
        out.write(_EXTRA_CA.read_text())
    logger.debug("EuroIX CA bundle: certifi + %s -> %s", _EXTRA_CA.name, path)
    return path


def _fetch_json(url: str, timeout: float) -> tuple[str, dict[str, Any] | None]:
    """Fetch one export, returning ``(url, doc_or_None)``. Never raises."""
    try:
        doc = requests.get(url, timeout=timeout, verify=_ca_bundle()).json()
    except Exception as err:
        logger.warning("EuroIX export %s unavailable: %s", url, type(err).__name__)
        return url, None
    return url, doc if isinstance(doc, dict) else None


def euroix_records(
    session: requests.Session | None = None,
    timeout: float = 15.0,
    max_workers: int = 16,
) -> tuple[list[tuple[str, list[str], list[str]]], list[tuple[str, str, list[str], list[str]]]]:
    """Fetch IXP prefixes and members from EuroIX's IXPDB and IX-F exports.

    IXPDB lists ~1130 IXPs; 391 publish an IX-F member export, and those sit
    behind only ~273 distinct URLs because one document often covers a whole
    operator's IXPs. Each URL is fetched once.

    Only ~5 of those IXPs are absent from PeeringDB, so this is not about new
    IXPs -- it is about interface depth at IXPs PeeringDB already lists, from
    exports the IXPs publish themselves. A 10-IXP sample found ~26% of IX-F
    interfaces missing from PeeringDB.

    Not every export carries addresses: IX.br publishes members with an empty
    ``connection_list`` (106 members, 106 connections, zero IPs anywhere in the
    document). That is their data, not a parse failure, so it is logged as
    "no interfaces" rather than passing silently as zero.
    """
    http = session or requests.Session()
    providers = http.get(IXPDB_PROVIDER_LIST, timeout=timeout).json()

    by_url: dict[str, list[dict[str, Any]]] = {}
    for prov in providers:
        url = (prov.get("apis") or {}).get("ixfexport")
        if url:
            by_url.setdefault(url, []).append(prov)

    prefix_records: list[tuple[str, list[str], list[str]]] = []
    member_records: list[tuple[str, str, list[str], list[str]]] = []
    failed = no_interfaces = unmatched = 0

    logger.info(
        "EuroIX: %d IXPs behind %d export URLs", sum(map(len, by_url.values())), len(by_url)
    )
    # Fetched concurrently, unlike PCH. PCH is ~1000 requests to ONE host, so it
    # needs a serial delay to be polite; these are ~273 requests to ~273
    # DIFFERENT operators, one each, so concurrency adds no per-host load. It
    # matters: serially with a generous timeout the dead endpoints dominate --
    # a measured ~29% of them time out, which at 45s each is about an hour of
    # doing nothing. 15s across 16 workers turns that into minutes.
    docs: dict[str, dict[str, Any] | None] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        for url, doc in pool.map(lambda u: _fetch_json(u, timeout), list(by_url)):
            docs[url] = doc

    for url, provs in by_url.items():
        doc = docs.get(url)
        if doc is None:
            failed += 1
            continue
        for prov in provs:
            ixpdb_id = prov.get("id")
            if not isinstance(ixpdb_id, int):
                unmatched += 1
                continue
            pfx, members = _ixf_extract(doc, ixpdb_id, str(prov.get("name") or ""))
            if pfx is None:
                unmatched += 1
                continue
            if not members:
                no_interfaces += 1
            prefix_records.append(pfx)
            member_records.extend(members)

    logger.info(
        "EuroIX: %d prefix records, %d member records "
        "(%d exports unreachable, %d IXPs publish no interfaces, %d unmatched)",
        len(prefix_records),
        len(member_records),
        failed,
        no_interfaces,
        unmatched,
    )
    return prefix_records, member_records


# --------------------------------------------------------------------------
# Merge (ported from merge-all-prefixes.py and merge-ixp-ips.py)
# --------------------------------------------------------------------------


def build_ixp_name_map(
    sources: list[tuple[str, list[tuple[str, list[str], list[str]]]]],
) -> dict[str, str]:
    """Map each source's IXP name onto a single canonical name.

    Ported from ``merge-all-prefixes.py`` + ``merge-ixp-ips.py::map_ixp_names``.
    Prefixes from every source go into one radix tree; a node reached by several
    sources ties their names together. PeeringDB's name wins when present,
    otherwise HE's, otherwise PCH's -- the same precedence as upstream, which got
    it from sorting the per-node keys case-insensitively (he, pch, pdb).

    Args:
        sources: ``(source_tag, prefix_records)`` pairs; tags are "pdb"/"pch"/"he".

    Returns:
        Mapping of underscore-normalised IXP name -> canonical name.
    """
    rtree = radix.Radix()
    for tag, records in sources:
        for ixp_name, v4, v6 in records:
            normalised = f"{tag}_{ixp_name.strip().replace(' ', '_')}"
            for prefix in (*v4, *v6):
                if not prefix:
                    continue
                try:
                    node = rtree.add(prefix)
                except (ValueError, TypeError) as err:
                    logger.debug("Skipping unparseable IXP prefix %r: %s", prefix, err)
                    continue
                for known in ("pdb", "pch", "he"):
                    node.data.setdefault(known, "n/a")
                node.data[tag] = normalised

    name_map: dict[str, str] = {}
    for node in rtree:
        ordered = OrderedDict(sorted(node.data.items(), key=lambda kv: kv[0].lower()))
        he, pch, pdb = (ordered.get(k, "n/a") for k in ("he", "pch", "pdb"))
        if pdb.startswith("pdb_"):
            canonical = pdb[4:]
            name_map[canonical] = canonical
            if he != "n/a":
                name_map[he[3:]] = canonical
            if pch != "n/a":
                name_map[pch[4:]] = canonical
        elif he != "n/a" and he[3:] not in name_map:
            name_map[he[3:]] = he[3:]
            if pch != "n/a":
                name_map[pch[4:]] = he[3:]
        elif pch != "n/a" and pch[4:] not in name_map:
            name_map[pch[4:]] = pch[4:]

    return name_map


def usable_asn(raw: str | int | None) -> str | None:
    """Return `raw` as a bare ASN string, or None if it is not usable.

    PCH returns an empty ASN for a substantial share of its interfaces. Upstream
    kept those rows, and because PCH wins the merge they overrode PeeringDB --
    then died at the far end, where ``process_data_file`` does ``int(asn)``,
    logs "Invalid row" and skips. Measured on the 2026-09 data: **37,863 of
    166,484 merged rows (22.7%) were discarded that way**, and PeeringDB held a
    valid ASN for 5,208 of them. Rejecting them here instead lets the merge fall
    through to a source that knows the ASN.
    """
    text = str(raw or "").strip().removeprefix("AS")
    return text if text.isdigit() and int(text) > 0 else None


def interfaces_from_records(
    records: list[tuple[str, str, list[str], list[str]]],
) -> dict[str, tuple[str, str]]:
    """Flatten member records to ``ip -> (asn, ixp_name)``, dropping private IPs.

    Ported from ``read_ixp_interfaces``, with one deliberate correction: records
    whose ASN is missing or non-numeric are skipped rather than carried forward
    as a blank (see `usable_asn`). Within one source the LAST record for an IP
    wins, matching the upstream dict assignment.
    """
    ip_data: dict[str, tuple[str, str]] = {}
    skipped = 0
    for ixp_name, asn, v4, v6 in records:
        clean_asn = usable_asn(asn)
        if clean_asn is None:
            skipped += 1
            continue
        for raw in (*v4, *v6):
            raw = (raw or "").strip()
            if not raw:
                continue
            try:
                parsed = ipaddress.ip_address(raw)
            except ValueError:
                logger.debug("Skipping invalid IXP interface IP: %r", raw)
                continue
            if parsed.is_private:
                continue
            ip_data[str(parsed)] = (clean_asn, ixp_name)
    if skipped:
        logger.info("Skipped %d member records with no usable ASN", skipped)
    return ip_data


def merge_interfaces(
    pdb_members: dict[str, tuple[str, str]],
    pch_members: dict[str, tuple[str, str]],
    name_map: dict[str, str],
    euroix_members: dict[str, tuple[str, str]] | None = None,
) -> dict[str, tuple[str, str]]:
    """Merge sources into ``ip -> (asn, canonical IXP name)``.

    Precedence, highest first: **EuroIX, then PCH, then PeeringDB**. The first
    source to claim an IP keeps it.

    EuroIX leads because IX-F exports are first-party -- published by the IXP
    about its own fabric -- where PeeringDB's netixlan entries are self-reported
    by members and PCH's are observed. PCH ahead of PeeringDB preserves the
    upstream ordering this was ported from.

    This is a deliberate data decision, not merely additive: EuroIX now
    overrides interfaces the other two also know, so ASNs and IXP labels can
    change for IPs that were previously attributed from PCH or PeeringDB.
    """
    merged: dict[str, tuple[str, str]] = {}
    for source in (euroix_members or {}, pch_members, pdb_members):
        for ip, (asn, raw_name) in source.items():
            if ip in merged:
                continue
            name = raw_name.strip().replace(" ", "_")
            merged[ip] = (asn, name_map.get(name, name))
    return merged


def _sorted_v4(ips: list[str]) -> list[str]:
    return sorted(ips, key=lambda ip: struct.unpack("!L", inet_aton(ip))[0])


def write_snapshot(merged: dict[str, tuple[str, str]], out_path: str) -> tuple[str, str]:
    """Write the IPv4 and IPv6 snapshot files.

    `out_path` receives the IPv4 rows and ``<out_path>_ipv6.txt`` the IPv6 rows,
    reproducing upstream -- which writes a combined file and then overwrites it
    with the IPv4-only one, because both resolve to the same path.

    Returns:
        ``(ipv4_path, ipv6_path)``.
    """
    v4: dict[str, tuple[str, str]] = {}
    v6: dict[str, tuple[str, str]] = {}
    for ip, value in merged.items():
        (v4 if ipaddress.ip_address(ip).version == 4 else v6)[ip] = value

    os.makedirs(os.path.dirname(os.path.abspath(out_path)) or ".", exist_ok=True)

    with open(out_path, "w") as f:
        f.write("# IPv4 IXP ASN members\n# Format: IP<tab>ASN<tab>IXP\n#\n")
        for ip in _sorted_v4(list(v4)):
            f.write(f"{ip}\t{v4[ip][0]}\t{v4[ip][1]}\n")

    v6_path = out_path.removesuffix(".txt") + "_ipv6.txt"
    with open(v6_path, "w") as f:
        f.write("# IPv6 IXP ASN members\n# Format: IP<tab>ASN<tab>IXP\n#\n")
        for ip in sorted(v6, key=ipaddress.IPv6Address):
            f.write(f"{ip}\t{v6[ip][0]}\t{v6[ip][1]}\n")

    logger.info("Wrote %d IPv4 rows to %s, %d IPv6 rows to %s", len(v4), out_path, len(v6), v6_path)
    return out_path, v6_path


def generate_snapshot(
    date: str,
    output_dir: str,
    cache_dir: str | None = None,
    pch_delay: float = PCH_REQUEST_DELAY,
    include_euroix: bool = True,
) -> tuple[str, str] | None:
    """Produce ``merged-members-gen-<YYYYMMDD>{,_ipv6}.txt`` for `date`.

    The in-process replacement for ``IXPCollector.run_wrapper_script``.

    Returns:
        ``(ipv4_path, ipv6_path)``, or None if the PeeringDB dump was unavailable.
    """
    cache_dir = cache_dir or os.path.join(output_dir, "peeringdb")
    downloaded = download_peeringdb_dump(date, cache_dir)
    if downloaded is None:
        return None
    dump_path, actual_date = downloaded

    pdb_prefixes, pdb_members = peeringdb_records(dump_path)
    pch_prefixes, pch_members = pch_records(delay=pch_delay)

    euroix_prefixes: list[tuple[str, list[str], list[str]]] = []
    euroix_members: list[tuple[str, str, list[str], list[str]]] = []
    if include_euroix:
        try:
            euroix_prefixes, euroix_members = euroix_records()
        except Exception as err:
            # Additive source: losing it degrades coverage but must not lose the
            # whole snapshot, which PeeringDB and PCH already carry.
            logger.warning("EuroIX source failed, continuing without it: %s", err)

    name_map = build_ixp_name_map(
        [("pdb", pdb_prefixes), ("pch", pch_prefixes), ("euroix", euroix_prefixes)]
    )
    merged = merge_interfaces(
        interfaces_from_records(pdb_members),
        interfaces_from_records(pch_members),
        name_map,
        euroix_members=interfaces_from_records(euroix_members),
    )

    stamp = actual_date.replace("-", "")
    return write_snapshot(merged, os.path.join(output_dir, f"merged-members-gen-{stamp}.txt"))

"""CAIDA HOIHO lookups: router hostname -> location, as ``hermes.geolocation`` rows.

HOIHO applies per-operator naming rules to a router's reverse-DNS hostname and
returns zero or more location guesses. This module only talks to the API and
shapes rows; reading candidates and writing tables is ``HermesEnrichment``'s job.

Three properties the previous client lacked, each of which matters for a lookup
that runs nightly over ~2.5M hostnames:

- every match is kept, not just the last one per hostname (step 04 chooses
  between several guesses deterministically);
- a hostname is reported as looked up whether or not it matched, so misses can
  be cached and are not re-queried every night;
- a batch that failed (HTTP error, timeout) is NOT reported as looked up, so a
  transient outage is retried instead of being cached as "no match".
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import Any

import requests

logger = logging.getLogger(__name__)

HOIHO_URL = "https://api.hoiho.caida.org/lookups"
DEFAULT_BATCH_SIZE = 10_000
REQUEST_TIMEOUT_SECONDS = 120

#: Column order of ``hermes.geolocation``.
GEOLOCATION_COLUMNS = (
    "hostname",
    "lat",
    "lon",
    "place",
    "cc",
    "state",
    "metro",
    "code",
    "locode",
    "domain",
    "match_strs",
    "match_meanings",
    "clli",
)


@dataclass
class LookupResult:
    """Outcome of one ``query_hoiho`` call."""

    rows: list[dict[str, Any]] = field(default_factory=list)
    looked_up: set[str] = field(default_factory=set)
    matched: set[str] = field(default_factory=set)
    failed_batches: int = 0
    ruleset_date: str | None = None


def normalize_hostname(hostname: str | None) -> str | None:
    """Lower-case, trim, drop the trailing root dot; ``None`` if nothing is left."""
    if hostname is None:
        return None
    value = hostname.strip().lower().rstrip(".").strip()
    return value or None


def _to_float(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except TypeError, ValueError:
        return None


def match_to_row(match: dict[str, Any]) -> dict[str, Any]:
    """Shape one HOIHO match as a ``hermes.geolocation`` row.

    Follows the conventions of the rows already in the table: lower-case hostname
    without a trailing dot, ``place`` suffixed with ``-CC``, ``metro`` left NULL for
    the metro pass to fill. ``cc`` is upper-cased: the API mixes ``uk`` and ``NL``.
    """
    cc = match.get("cc")
    cc = cc.upper() if isinstance(cc, str) and cc else None
    place = match.get("place") or None
    lat, lon = _to_float(match.get("lat")), _to_float(match.get("lng"))
    if lat is None or lon is None:
        lat = lon = None
    return {
        "hostname": normalize_hostname(match.get("hostname")),
        "lat": lat,
        "lon": lon,
        "place": f"{place}-{cc}" if place and cc else place,
        "cc": cc,
        "state": match.get("st"),
        "metro": None,
        "code": None,
        "locode": match.get("locode"),
        "domain": None,
        "match_strs": list(match.get("match_strs") or []),
        "match_meanings": list(match.get("match_meanings") or []),
        "clli": match.get("clli"),
    }


def query_hoiho(
    hostnames: Iterable[str | None],
    *,
    post: Callable[..., Any] = requests.post,
    batch_size: int = DEFAULT_BATCH_SIZE,
    sleep_seconds: float = 1.0,
    url: str = HOIHO_URL,
) -> LookupResult:
    """Look up ``hostnames`` in batches.

    Parameters
    ----------
    hostnames
        Raw hostnames; normalized and de-duplicated before sending.
    post
        ``requests.post``-compatible callable (injected in tests).
    batch_size
        Hostnames per request.
    sleep_seconds
        Pause between requests, to stay polite to CAIDA's public API.
    url
        Lookup endpoint.

    Returns
    -------
    LookupResult
        Rows for every match, plus which hostnames were successfully looked up.
    """
    unique = sorted({h for h in (normalize_hostname(x) for x in hostnames) if h})
    result = LookupResult()
    for start in range(0, len(unique), batch_size):
        batch = unique[start : start + batch_size]
        if start:
            time.sleep(sleep_seconds)
        try:
            response = post(url, json=batch, timeout=REQUEST_TIMEOUT_SECONDS)
            payload = response.json() if response.status_code == 200 else None
        except (requests.RequestException, ValueError) as exc:
            logger.warning("HOIHO batch %d failed: %s", start // batch_size + 1, exc)
            result.failed_batches += 1
            continue
        if not isinstance(payload, dict) or "matches" not in payload:
            logger.warning(
                "HOIHO batch %d failed: HTTP %s", start // batch_size + 1, response.status_code
            )
            result.failed_batches += 1
            continue
        result.ruleset_date = (payload.get("summary") or {}).get(
            "ruleset_date", result.ruleset_date
        )
        result.looked_up.update(batch)
        for match in payload["matches"]:
            row = match_to_row(match)
            if row["hostname"]:
                result.rows.append(row)
                result.matched.add(row["hostname"])
    return result

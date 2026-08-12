"""Country-code normalisation between Natural Earth and HERMES/IPInfo.

Two distinct code spaces meet here:

* Natural Earth ``ISO_A2`` / ``ISO_A2_EH``, which is ``-99`` for de-facto states
  and occasionally disagrees with ISO.
* the country codes that appear in HERMES geolocation
  (``unified_ip_to_geoloc.country`` / ``country_ip_info``), which follow IPInfo
  and therefore report dependent territories separately (PR, GL, AS, GU, VI,
  PF, NC, SJ, ...).

The geolocation space wins, per the plan: partitions must line up with the codes
the pipeline actually sees. Every deviation is explicit and auditable, never a
silent guess.
"""
from __future__ import annotations

import csv

import pandas as pd

from . import config as cfg

# Natural Earth ``-99`` de-facto states, mapped to the code geolocation uses.
# XK for Kosovo is the widely deployed user-assigned code (IPInfo emits XK).
NE_A3_TO_CC = {
    "KOS": "XK",   # Kosovo
    "CYN": "CY",   # Northern Cyprus -> reported as CY by IPInfo
    "SOL": "SO",   # Somaliland -> reported as SO
    "KAS": "IN",   # Siachen / Kashmir glacier polygon
    "SDS": "SS",   # South Sudan
    "PSX": "PS",   # Palestine
    "ATF": "TF",   # French Southern Territories
    "ATC": "AQ",   # Ashmore-adjacent Antarctic claim polygons
    "CNM": "CY",   # Cyprus UN buffer zone
    "USG": "CU",   # Guantanamo Bay
    "IOA": "IO",   # British Indian Ocean Territory
    "CLP": "PF",   # Clipperton Island (IPInfo groups with PF)
    "ESB": "CY",   # Dhekelia sovereign base area
    "WSB": "CY",   # Akrotiri sovereign base area
    "BJN": "ES",   # Bajo Nuevo
    "SER": "ES",   # Serranilla Bank
    "SCR": "CO",   # Scarborough / Serrana
    "IOU": "IO",
}

# Fallback when a feature has neither a usable ISO_A2 nor a mapped ADM0_A3.
NE_SOVEREIGN_TO_CC = {
    "FR1": "FR",
    "NL1": "NL",
    "DN1": "DK",
    "GB1": "GB",
    "US1": "US",
    "NZ1": "NZ",
    "AU1": "AU",
    "CH1": "CN",
    "KA1": "IN",
}


def load_country_code_overrides() -> dict[str, str]:
    """Read the operator-maintained override CSV (``from_code,to_code,reason``)."""
    path = cfg.COUNTRY_CODE_OVERRIDES_CSV
    if not path.exists():
        return {}
    lines = [ln for ln in path.read_text(encoding="utf-8").splitlines() if not ln.startswith(">")]
    out: dict[str, str] = {}
    for row in csv.DictReader(lines):
        key = (row.get("from_code") or "").strip()
        val = (row.get("to_code") or "").strip()
        if key and val:
            out[key] = val
    return out


def _first_usable(*values: object) -> str | None:
    for v in values:
        if v is None:
            continue
        s = str(v).strip()
        if s and s not in {"-99", "nan", "None"}:
            return s
    return None


def boundary_cc_from_row(row, overrides: dict[str, str] | None = None) -> str | None:
    """Resolve a Natural Earth boundary feature to a HERMES country code."""
    overrides = overrides or {}
    iso = _first_usable(row.get("ISO_A2_EH"), row.get("ISO_A2"))
    if iso is None:
        a3 = _first_usable(row.get("ADM0_A3"), row.get("SU_A3"))
        if a3 and a3 in NE_A3_TO_CC:
            iso = NE_A3_TO_CC[a3]
        else:
            sov = _first_usable(row.get("SOV_A3"))
            if sov and sov in NE_SOVEREIGN_TO_CC:
                iso = NE_SOVEREIGN_TO_CC[sov]
    if iso is None:
        return None
    a3 = _first_usable(row.get("ADM0_A3"))
    if a3 and a3 in NE_A3_TO_CC:
        iso = NE_A3_TO_CC[a3]
    return overrides.get(iso, iso)


def seed_cc_from_row(row, overrides: dict[str, str] | None = None) -> str | None:
    """Resolve a Natural Earth populated-place feature to a HERMES country code."""
    overrides = overrides or {}
    iso = _first_usable(row.get("ISO_A2"))
    if iso is None:
        a3 = _first_usable(row.get("ADM0_A3"))
        if a3 and a3 in NE_A3_TO_CC:
            iso = NE_A3_TO_CC[a3]
    if iso is None:
        sov = _first_usable(row.get("SOV_A3"))
        if sov and sov in NE_SOVEREIGN_TO_CC:
            iso = NE_SOVEREIGN_TO_CC[sov]
    if iso is None:
        return None
    return overrides.get(iso, iso)


def summarise(seed_ccs: set[str], boundary_ccs: set[str]) -> pd.DataFrame:
    """Side-by-side view of which codes exist on which side."""
    rows = []
    for cc in sorted(seed_ccs | boundary_ccs):
        rows.append(
            {
                "country_code": cc,
                "has_seeds": cc in seed_ccs,
                "has_boundary": cc in boundary_ccs,
            }
        )
    return pd.DataFrame(rows)

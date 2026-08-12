"""Stage 01 -- load, reconcile and clean the metro seed set.

Seed provenance. The existing
``mlab-collaboration.hermes.metro_polygons_with_population`` was generated from
**iGDB** (Anderson, Salamatian, Bischof, Dainotti, Barford, ACM IMC 2022) via
``hermes-code/data/city_polygons_with_population.csv``. iGDB's ``city_points`` /
``city_polygons`` layer is itself Natural Earth 10m populated places: same 7,342
rows, same names including NE's encoding corruption, and coordinates matching to
3 dp for 91.5% with 592 of the remaining 623 inside 1 km. So seeding from NE here
reproduces iGDB's seed set to sub-km accuracy, and NE is used because it also
supplies ADM1 boundaries and clean region names. Verified 2026-08-11: 7,342 NE places vs 7,301 polygon rows, with
``NAME`` -> ``city``, ``ADM1NAME`` -> ``state_resolved``, ``ISO_A2`` ->
``country_code`` matching exactly, including Antarctic research stations
(``Mirny Station``) and NE-specific ADM1 labels (``Tokelau aggregation``,
``South I. remainder``).

The old table carries no seed coordinates, so NE is how they are recovered. The
old table is still read, to (a) preserve the existing HERMES naming authority
including ``state_iso2``, and (b) prove the two sets line up.
"""
from __future__ import annotations

import csv
import hashlib
import math
import warnings
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd

from . import config as cfg
from .load_country_boundaries import download_natural_earth
from .normalize_country_codes import load_country_code_overrides, seed_cc_from_row

warnings.filterwarnings("ignore", category=FutureWarning)


def _clean_field(value) -> str | None:
    """Coerce pandas/NumPy missing values to None.

    Also collapses internal whitespace runs. Natural Earth ships names with double
    spaces (``Washington,  D.C.``, ``Ft.  Worth``), which would otherwise travel
    into the metro key and into every downstream group label.

    ``float('nan')`` is truthy, so ``state_resolved or state_iso2 or 'NA'`` would
    happily emit the literal string ``nan`` into a metro key. That is how
    ``Uummannaq-nan-GL`` appeared on the first run.
    """
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    text = " ".join(str(value).split())
    if text in ("", "nan", "NaN", "None", "<NA>", "-99"):
        return None
    return text


def metro_id_for(city: str, state_resolved: str | None, state_iso2: str | None, cc: str) -> str:
    """Stable id from the canonical metro identity, never from geometry.

    Derived from the same fields as the HERMES metro string so it survives a
    geometry rebuild. Hashed so it is safe as a join key regardless of the
    punctuation in city or region names.
    """
    region = _clean_field(state_resolved) or _clean_field(state_iso2) or "NA"
    key = f"{_clean_field(city)}\x1f{region}\x1f{_clean_field(cc)}"
    digest = hashlib.sha1(key.encode("utf-8")).hexdigest()[:12]
    return f"m_{digest}"


def metro_string(city: str, state_resolved: str | None, state_iso2: str | None, cc: str) -> str:
    """The single naming authority: ``City-COALESCE(state_resolved, state_iso2, 'NA')-CC``.

    Mirrors ``04_mapping_union.sql`` lines 111/752/1535, which is the form the
    rest of the pipeline joins on. The Phase-B enrichment SQL that used bare
    ``state_iso2`` is a regression, corrected as part of this build.
    """
    region = _clean_field(state_resolved) or _clean_field(state_iso2) or "NA"
    return f"{_clean_field(city)}-{region}-{_clean_field(cc)}"


def _load_ne_places() -> pd.DataFrame:
    paths = download_natural_earth()
    gdf = gpd.read_file(f"zip://{paths['populated_places']}")
    overrides = load_country_code_overrides()
    rows = []
    for _, r in gdf.iterrows():
        cc = seed_cc_from_row(r, overrides)
        adm1 = r.get("ADM1NAME")
        adm1 = None if adm1 is None or str(adm1) in ("nan", "None", "") else str(adm1)
        rows.append(
            {
                "city": str(r.get("NAME")),
                "state_resolved": adm1,
                "country_code": cc,
                "seed_lat": float(r.get("LATITUDE")),
                "seed_lon": float(r.get("LONGITUDE")),
                "ne_pop_max": int(r.get("POP_MAX") or 0),
                "ne_adm0_a3": str(r.get("ADM0_A3") or ""),
                "ne_featurecla": str(r.get("FEATURECLA") or ""),
                "source": "natural_earth_10m_populated_places",
            }
        )
    return pd.DataFrame(rows)


def _load_legacy_polygon_rows() -> pd.DataFrame:
    """Read the old table's attribute columns (no geometry -> a few MB scanned)."""
    from google.cloud import bigquery

    client = bigquery.Client(project=cfg.BQ_PROJECT)
    sql = f"""
    SELECT city, state_resolved, state_iso2, country_code,
           population_sum AS legacy_cell_population
    FROM `{cfg.OLD_POLYGON_TABLE}`
    """
    return client.query(sql).result().to_dataframe()


def _load_state_to_iso2() -> pd.DataFrame:
    """Authoritative ``state_resolved`` -> ``state_iso2`` map.

    Using ``hermes.state_to_iso2`` rather than the legacy polygon table means
    ``state_iso2`` survives Natural Earth city renames, which is what broke the
    naive legacy join for 235 of 7,301 metros.
    """
    from google.cloud import bigquery

    client = bigquery.Client(project=cfg.BQ_PROJECT)
    sql = f"""
    SELECT country_code, state, ANY_VALUE(subdivision_code) AS state_iso2
    FROM `{cfg.BQ_PROJECT}.{cfg.BQ_DATASET}.state_to_iso2`
    WHERE state IS NOT NULL AND subdivision_code IS NOT NULL
    GROUP BY country_code, state
    """
    df = client.query(sql).result().to_dataframe()
    return df.rename(columns={"state": "state_resolved"})


def natural_earth_vintage() -> dict[str, str]:
    """SHA-256 of each Natural Earth archive, so a build is reproducible.

    The legacy table was built from an older vintage: 7,066 of its 7,301 metros
    match this one exactly on (city, state_resolved, country_code); the rest are
    NE renames (``Chittagong`` -> ``Chattogram``) and ADM1 relabels.
    """
    out = {}
    for name in cfg.NE_FILES:
        path = cfg.DATA_RAW / f"ne_10m_{name}.zip"
        if path.exists():
            out[name] = hashlib.sha256(path.read_bytes()).hexdigest()[:16]
    return out


def _load_seed_additions() -> pd.DataFrame:
    """Operator-supplied seeds for populated places Natural Earth omits (§5)."""
    path: Path = cfg.SEED_ADDITIONS_CSV
    if not path.exists():
        return pd.DataFrame(
            columns=[
                "city", "state_resolved", "state_iso2", "country_code",
                "seed_lat", "seed_lon", "ne_pop_max", "source", "reason",
            ]
        )
    from .validate_seeds import read_commented_csv

    rows = []
    if True:
        for row in read_commented_csv(path):
            if not (row.get("city") or "").strip():
                continue
            rows.append(
                {
                    "city": row["city"].strip(),
                    "state_resolved": (row.get("state_resolved") or "").strip() or None,
                    "state_iso2": (row.get("state_iso2") or "").strip() or None,
                    "country_code": row["country_code"].strip(),
                    "seed_lat": float(row["seed_lat"]),
                    "seed_lon": float(row["seed_lon"]),
                    "ne_pop_max": int(row.get("population") or 0),
                    "source": (row.get("source") or "manual_addition").strip(),
                    "reason": (row.get("reason") or "").strip(),
                }
            )
    return pd.DataFrame(rows)


def build_seeds() -> tuple[pd.DataFrame, dict]:
    """Return the reconciled seed frame plus a provenance report."""
    ne = _load_ne_places()
    legacy = _load_legacy_polygon_rows()

    # Reconcile on the canonical identity triple so state_iso2 and the legacy
    # population figure carry over where the metro is unchanged.
    key = ["city", "state_resolved", "country_code"]
    for df in (ne, legacy):
        for k in key:
            df[k] = df[k].where(df[k].notna(), None)
    legacy_dedup = legacy.drop_duplicates(subset=key, keep="first")
    merged = ne.merge(legacy_dedup, on=key, how="left", indicator=True)

    matched = int((merged._merge == "both").sum())
    report = {
        "ne_places": int(len(ne)),
        "legacy_polygon_rows": int(len(legacy)),
        "legacy_rows_deduped": int(len(legacy_dedup)),
        "matched_to_legacy": matched,
        "ne_only": int((merged._merge == "left_only").sum()),
        "legacy_only": int(len(legacy_dedup) - matched),
    }
    merged = merged.drop(columns=["_merge"])

    # Legacy rows with no NE counterpart: surface them, never drop silently.
    lm = legacy_dedup.merge(ne[key], on=key, how="left", indicator=True)
    report["legacy_only_examples"] = (
        lm[lm._merge == "left_only"][key].head(30).to_dict("records")
    )

    additions = _load_seed_additions()
    if len(additions):
        additions["ne_adm0_a3"] = ""
        additions["ne_featurecla"] = "Manual addition"
        additions["legacy_cell_population"] = np.nan
        merged = pd.concat([merged, additions], ignore_index=True)
    report["manual_additions"] = int(len(additions))

    # state_iso2: prefer the authoritative lookup, fall back to the legacy value.
    s2i = _load_state_to_iso2()
    merged = merged.merge(
        s2i, on=["country_code", "state_resolved"], how="left", suffixes=("", "_auth")
    )
    legacy_s2 = merged.get("state_iso2")
    auth_s2 = merged.get("state_iso2_auth")
    if auth_s2 is not None:
        merged["state_iso2"] = auth_s2.where(auth_s2.notna(), legacy_s2)
        merged = merged.drop(columns=["state_iso2_auth"])
    report["state_iso2_resolved"] = int(merged["state_iso2"].notna().sum())
    report["natural_earth_vintage"] = natural_earth_vintage()

    for col in ("city", "state_resolved", "state_iso2", "country_code"):
        merged[col] = [_clean_field(v) for v in merged[col]]

    merged["metro"] = [
        metro_string(r.city, r.state_resolved, r.state_iso2, r.country_code)
        for r in merged.itertuples()
    ]
    merged["metro_id"] = [
        metro_id_for(r.city, r.state_resolved, r.state_iso2, r.country_code)
        for r in merged.itertuples()
    ]
    merged["seed_source"] = merged["source"]
    return merged, report


def save(df: pd.DataFrame) -> Path:
    df.to_parquet(cfg.S01_SEEDS, index=False)
    return cfg.S01_SEEDS


def load() -> pd.DataFrame:
    return pd.read_parquet(cfg.S01_SEEDS)


def main() -> None:
    df, report = build_seeds()
    save(df)
    print(f"seeds: {len(df)} -> {cfg.S01_SEEDS}")
    for k, v in report.items():
        if k != "legacy_only_examples":
            print(f"  {k}: {v}")
    if report["legacy_only_examples"]:
        print("  legacy-only (in old table, absent from Natural Earth):")
        for r in report["legacy_only_examples"][:10]:
            print(f"    {r}")


if __name__ == "__main__":
    main()

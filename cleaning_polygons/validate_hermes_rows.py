"""Stage 08 -- validate the new geometry against live HERMES data (plan section 19).

Runs old and new lookups side by side over ``unified_ip_to_geoloc`` and reports
the metrics the plan asks for, plus the Phase-2 disagreement breakdown.

Reference points from the pre-build audit of the *old* table:
  ~91.4M rows have coordinates
  146 fall outside every polygon
  1.084% receive a cross-country metro
  Pacific labels are wrong because overlapping polygons are resolved alphabetically

Targets for the new table: 0 cross-country assignments, 0 antimeridian overlap
defects, 0 high-Arctic misses where the country is known.

Cost: the comparison resolves *distinct coordinates*, not distinct IPs, so the
spatial work is over ~1M points rather than 99M rows. The scan is the
``unified_ip_to_geoloc`` columns only (~5 GiB, a few cents, billed to
mlab-collaboration). Every query is dry-run first.
"""
from __future__ import annotations

import json

import pandas as pd

from . import config as cfg

COORD = "COALESCE(lat_ip_info, lat)"
COORD_LON = "COALESCE(lon_ip_info, lon)"
COORD_CC = "COALESCE(country_ip_info, country)"


def _client():
    from google.cloud import bigquery

    return bigquery.Client(project=cfg.BQ_PROJECT)


def _run(sql: str, label: str) -> pd.DataFrame:
    from google.cloud import bigquery

    client = _client()
    dry = client.query(
        sql, job_config=bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    )
    gib = dry.total_bytes_processed / 2**30
    print(f"  [{label}] dry run {gib:.3f} GiB ~= ${gib / 1024 * 6.25:.4f}, billed to {cfg.BQ_PROJECT}")
    if gib > 1024:
        raise RuntimeError(f"{label}: refusing to scan {gib:.1f} GiB")
    job = client.query(sql, job_config=bigquery.QueryJobConfig(job_timeout_ms=1_800_000))
    df = job.result(timeout=1800).to_dataframe()
    print(f"  [{label}] billed {job.total_bytes_billed / 2**30:.3f} GiB")
    return df


# ---------------------------------------------------------------- resolution CTE
def _resolution_cte(table: str) -> str:
    """Shared CTE: distinct coordinates resolved against the new table."""
    return f"""
    coords AS (
      SELECT {COORD_LON} AS lon, {COORD} AS lat, {COORD_CC} AS cc, COUNT(*) AS n_rows
      FROM `{cfg.IP_GEOLOC_TABLE}`
      WHERE {COORD} IS NOT NULL AND {COORD_LON} IS NOT NULL
      GROUP BY 1, 2, 3
    ),
    covered AS (
      SELECT
        c.lon, c.lat, c.cc, c.n_rows,
        ARRAY_AGG(
          STRUCT(mp.metro_id, mp.metro, mp.city, mp.country_code AS metro_cc,
                 ST_DISTANCE(ST_GEOGPOINT(c.lon, c.lat),
                             ST_GEOGPOINT(mp.seed_lon, mp.seed_lat)) / 1000.0 AS km)
          ORDER BY ST_DISTANCE(ST_GEOGPOINT(c.lon, c.lat),
                               ST_GEOGPOINT(mp.seed_lon, mp.seed_lat)), mp.metro_id
          LIMIT 1
        )[OFFSET(0)] AS pick,
        COUNT(*) AS n_matches
      FROM coords c
      JOIN `{table}` mp
        ON mp.country_code = c.cc
       AND ST_COVERS(mp.polygon, ST_GEOGPOINT(c.lon, c.lat))
      GROUP BY 1, 2, 3, 4
    ),
    fallback AS (
      SELECT
        c.lon, c.lat, c.cc, c.n_rows,
        ARRAY_AGG(
          STRUCT(mp.metro_id, mp.metro, mp.city, mp.country_code AS metro_cc,
                 ST_DISTANCE(ST_GEOGPOINT(c.lon, c.lat),
                             ST_GEOGPOINT(mp.seed_lon, mp.seed_lat)) / 1000.0 AS km)
          ORDER BY ST_DISTANCE(ST_GEOGPOINT(c.lon, c.lat),
                               ST_GEOGPOINT(mp.seed_lon, mp.seed_lat)), mp.metro_id
          LIMIT 1
        )[OFFSET(0)] AS pick
      FROM coords c
      LEFT JOIN covered v USING (lon, lat, cc)
      JOIN `{table}` mp ON mp.country_code = c.cc
      WHERE v.lon IS NULL
      GROUP BY 1, 2, 3, 4
    ),
    resolved AS (
      SELECT lon, lat, cc, n_rows, pick, n_matches, 'polygon' AS method FROM covered
      UNION ALL
      SELECT lon, lat, cc, n_rows, pick, 0 AS n_matches,
             IF(pick.km <= {cfg.COASTAL_FALLBACK_KM},
                'country_nearest_fallback', 'country_nearest_fallback_far') AS method
      FROM fallback
    )
    """


def new_table_metrics() -> pd.DataFrame:
    sql = f"""
    WITH totals AS (
      SELECT
        COUNT(*) AS total_rows,
        COUNTIF({COORD} IS NOT NULL AND {COORD_LON} IS NOT NULL) AS rows_with_coords,
        COUNTIF({COORD_CC} IS NOT NULL) AS rows_with_country,
        COUNTIF({COORD} IS NOT NULL AND {COORD_LON} IS NOT NULL
                AND {COORD_CC} IS NULL) AS coords_without_country
      FROM `{cfg.IP_GEOLOC_TABLE}`
    ),
    {_resolution_cte(cfg.NEW_POLYGON_TABLE)}
    SELECT
      (SELECT total_rows FROM totals) AS total_rows,
      (SELECT rows_with_coords FROM totals) AS rows_with_coords,
      (SELECT rows_with_country FROM totals) AS rows_with_country,
      (SELECT coords_without_country FROM totals) AS coords_without_country,
      SUM(n_rows) AS rows_assigned_metro,
      SUM(IF(cc != pick.metro_cc, n_rows, 0)) AS rows_cross_country,
      SUM(IF(n_matches > 1, n_rows, 0)) AS rows_multi_polygon_match,
      SUM(IF(method = 'polygon', n_rows, 0)) AS rows_via_polygon,
      SUM(IF(method = 'country_nearest_fallback', n_rows, 0)) AS rows_via_fallback,
      SUM(IF(method = 'country_nearest_fallback_far', n_rows, 0)) AS rows_via_fallback_far,
      COUNT(DISTINCT pick.metro_id) AS distinct_metros_used,
      ROUND(APPROX_QUANTILES(pick.km, 100)[OFFSET(50)], 3) AS metro_distance_p50_km,
      ROUND(APPROX_QUANTILES(pick.km, 100)[OFFSET(95)], 3) AS metro_distance_p95_km,
      ROUND(APPROX_QUANTILES(pick.km, 100)[OFFSET(99)], 3) AS metro_distance_p99_km,
      ROUND(MAX(pick.km), 1) AS metro_distance_max_km
    FROM resolved
    """
    return _run(sql, "new-table metrics")


def old_table_metrics() -> pd.DataFrame:
    """The old table's figures, recomputed the same way for a fair comparison."""
    sql = f"""
    WITH coords AS (
      SELECT {COORD_LON} AS lon, {COORD} AS lat, {COORD_CC} AS cc, COUNT(*) AS n_rows
      FROM `{cfg.IP_GEOLOC_TABLE}`
      WHERE {COORD} IS NOT NULL AND {COORD_LON} IS NOT NULL
      GROUP BY 1, 2, 3
    ),
    -- The old table stores INVERTED polygons, so the non-containing row is the
    -- match, and it is not restricted by country at all.
    m AS (
      SELECT
        c.lon, c.lat, c.cc, c.n_rows,
        ARRAY_AGG(
          CONCAT(COALESCE(mp.city, 'Unknown'), '-',
                 COALESCE(mp.state_iso2, 'NA'), '-',
                 COALESCE(mp.country_code, 'Unknown'))
          ORDER BY CONCAT(COALESCE(mp.city, 'Unknown'), '-',
                          COALESCE(mp.state_iso2, 'NA'), '-',
                          COALESCE(mp.country_code, 'Unknown')) ASC
          LIMIT 1
        )[SAFE_OFFSET(0)] AS old_metro,
        COUNTIF(mp.city IS NOT NULL) AS n_matches,
        ANY_VALUE(mp.country_code) AS any_cc
      FROM coords c
      LEFT JOIN `{cfg.OLD_POLYGON_TABLE}` mp
        ON NOT ST_CONTAINS(mp.polygon, ST_GEOGPOINT(c.lon, c.lat))
      GROUP BY 1, 2, 3, 4
    )
    SELECT
      SUM(n_rows) AS rows_with_coords,
      SUM(IF(n_matches = 0, n_rows, 0)) AS rows_unassigned,
      SUM(IF(n_matches > 1, n_rows, 0)) AS rows_multi_polygon_match,
      SUM(IF(n_matches > 0
             AND cc IS NOT NULL
             AND SPLIT(old_metro, '-')[SAFE_OFFSET(ARRAY_LENGTH(SPLIT(old_metro,'-'))-1)] != cc,
             n_rows, 0)) AS rows_cross_country,
      COUNT(DISTINCT old_metro) AS distinct_metros_used
    FROM m
    """
    return _run(sql, "old-table metrics")


def disagreement_categories() -> pd.DataFrame:
    """Phase-2 comparison: where old and new disagree, and why (plan section 24)."""
    sql = f"""
    WITH {_resolution_cte(cfg.NEW_POLYGON_TABLE)},
    old AS (
      SELECT
        {COORD_LON} AS lon, {COORD} AS lat, {COORD_CC} AS cc,
        ANY_VALUE(metro) AS old_metro, COUNT(*) AS n_rows
      FROM `{cfg.IP_GEOLOC_TABLE}`
      WHERE {COORD} IS NOT NULL AND {COORD_LON} IS NOT NULL
      GROUP BY 1, 2, 3
    )
    SELECT
      CASE
        WHEN o.old_metro IS NULL THEN 'old_null'
        WHEN o.old_metro = 'Unknown-NA-Unknown' THEN 'old_unassigned_now_assigned'
        WHEN r.pick.metro = o.old_metro THEN 'identical'
        WHEN SPLIT(o.old_metro,'-')[SAFE_OFFSET(ARRAY_LENGTH(SPLIT(o.old_metro,'-'))-1)] != r.cc
             THEN 'old_was_cross_country'
        WHEN r.pick.city = SPLIT(o.old_metro, '-')[OFFSET(0)] THEN 'same_city_region_renamed'
        ELSE 'different_city'
      END AS category,
      SUM(r.n_rows) AS n_rows,
      COUNT(*) AS n_coords,
      ROUND(APPROX_QUANTILES(r.pick.km, 100)[OFFSET(50)], 2) AS p50_km_to_new_seed
    FROM resolved r
    LEFT JOIN old o ON o.lon = r.lon AND o.lat = r.lat AND o.cc = r.cc
    GROUP BY category
    ORDER BY n_rows DESC
    """
    return _run(sql, "disagreement categories")


def verify_polar_countries() -> pd.DataFrame:
    """The check deferred from validate_tessellation for polar territories.

    BigQuery's ST_COVERS is natively spherical, so it can evaluate a cell that
    touches a pole -- which planar shapely cannot. This is the authoritative
    verification for the countries in ``SHAPELY_UNVERIFIABLE_COUNTRIES``.
    """
    ccs = ", ".join(f"'{c}'" for c in sorted(cfg.SHAPELY_UNVERIFIABLE_COUNTRIES))
    sql = f"""
    WITH s AS (
      SELECT metro_id, metro, country_code, seed_lon, seed_lat
      FROM `{cfg.NEW_POLYGON_TABLE}`
      WHERE country_code IN ({ccs})
    ),
    r AS (
      SELECT
        s.country_code, s.metro_id AS want,
        ARRAY_AGG(mp.metro_id
                  ORDER BY ST_DISTANCE(ST_GEOGPOINT(s.seed_lon, s.seed_lat),
                                       ST_GEOGPOINT(mp.seed_lon, mp.seed_lat)),
                           mp.metro_id LIMIT 1)[SAFE_OFFSET(0)] AS got,
        COUNT(mp.metro_id) AS n_matches
      FROM s
      LEFT JOIN `{cfg.NEW_POLYGON_TABLE}` mp
        ON mp.country_code = s.country_code
       AND ST_COVERS(mp.polygon, ST_GEOGPOINT(s.seed_lon, s.seed_lat))
      GROUP BY 1, 2
    )
    SELECT country_code,
           COUNT(*) AS seeds,
           COUNTIF(got = want) AS resolve_to_self,
           COUNTIF(got IS NULL) AS no_match,
           COUNTIF(got IS NOT NULL AND got != want) AS resolve_to_other,
           COUNTIF(n_matches > 1) AS multi_match
    FROM r GROUP BY country_code
    """
    return _run(sql, "polar verification")


def named_case_check() -> pd.DataFrame:
    """The plan's section 18.8/18.9 coordinates, resolved through BigQuery."""
    from .validate_tessellation import ANTIMERIDIAN_CASES, ARCTIC_CASES

    rows = ", ".join(
        f"STRUCT('{label}' AS case_name, {lon} AS lon, {lat} AS lat, '{cc}' AS cc)"
        for label, lon, lat, cc in (ANTIMERIDIAN_CASES + ARCTIC_CASES)
    )
    sql = f"""
    WITH pts AS (SELECT * FROM UNNEST([{rows}])),
    cov AS (
      SELECT p.case_name, p.cc,
             ARRAY_AGG(STRUCT(mp.metro, mp.country_code AS metro_cc,
                       ST_DISTANCE(ST_GEOGPOINT(p.lon,p.lat),
                                   ST_GEOGPOINT(mp.seed_lon,mp.seed_lat))/1000.0 AS km)
                       ORDER BY ST_DISTANCE(ST_GEOGPOINT(p.lon,p.lat),
                                            ST_GEOGPOINT(mp.seed_lon,mp.seed_lat)),
                                mp.metro_id LIMIT 1)[OFFSET(0)] AS pick,
             COUNT(*) AS n_matches
      FROM pts p
      JOIN `{cfg.NEW_POLYGON_TABLE}` mp
        ON mp.country_code = p.cc
       AND ST_COVERS(mp.polygon, ST_GEOGPOINT(p.lon, p.lat))
      GROUP BY 1, 2
    ),
    fb AS (
      SELECT p.case_name, p.cc,
             ARRAY_AGG(STRUCT(mp.metro, mp.country_code AS metro_cc,
                       ST_DISTANCE(ST_GEOGPOINT(p.lon,p.lat),
                                   ST_GEOGPOINT(mp.seed_lon,mp.seed_lat))/1000.0 AS km)
                       ORDER BY ST_DISTANCE(ST_GEOGPOINT(p.lon,p.lat),
                                            ST_GEOGPOINT(mp.seed_lon,mp.seed_lat)),
                                mp.metro_id LIMIT 1)[OFFSET(0)] AS pick
      FROM pts p
      LEFT JOIN cov c USING (case_name)
      JOIN `{cfg.NEW_POLYGON_TABLE}` mp ON mp.country_code = p.cc
      WHERE c.case_name IS NULL
      GROUP BY 1, 2
    )
    SELECT case_name, cc AS expected_cc, pick.metro, pick.metro_cc,
           ROUND(pick.km, 1) AS km, n_matches, 'polygon' AS method,
           pick.metro_cc = cc AS ok
    FROM cov
    UNION ALL
    SELECT case_name, cc, pick.metro, pick.metro_cc, ROUND(pick.km, 1), 0,
           'country_nearest_fallback', pick.metro_cc = cc
    FROM fb
    ORDER BY case_name
    """
    return _run(sql, "named cases")


def main() -> None:
    out: dict = {}
    print("comparing old and new lookups over unified_ip_to_geoloc ...")
    for name, fn in (
        ("polar_verification", verify_polar_countries),
        ("named_cases", named_case_check),
        ("new_table", new_table_metrics),
        ("old_table", old_table_metrics),
        ("disagreement", disagreement_categories),
    ):
        df = fn()
        out[name] = df.to_dict("records")
        print(f"\n== {name}\n{df.to_string(index=False)}")
    cfg.S08_HERMES_VALIDATION.write_text(json.dumps(out, indent=2, default=str))
    print(f"\nwritten -> {cfg.S08_HERMES_VALIDATION}")


if __name__ == "__main__":
    main()

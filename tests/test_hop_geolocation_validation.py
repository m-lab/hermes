"""Step 04 router geolocation validation: refute locations a hop's RTT cannot reach.

Replaces the path-cumulative speed-of-light check, under which one mislocated hop
failed every later hop and steps 05/06/07 then dropped the whole measurement
(23.7% of measurements on 2026-10-14). See docs in 04_mapping_union.sql, 5b.
"""

import re

import pytest

from hermes.sql import loader


def _sql():
    return loader.load_query("04_mapping_union.sql", {"DAY": "2026-10-14"})


def _block(sql, start, end):
    i = sql.index(start)
    return sql[i : sql.index(end, i)]


def test_validation_pass_runs_before_the_main_statement():
    sql = _sql()
    assert sql.index("CREATE TEMP TABLE _hop_geo_validation") < sql.index(
        "CREATE TEMP TABLE _mapping_result"
    )
    v = _block(sql, "CREATE TEMP TABLE _hop_geo_validation", "CREATE TEMP TABLE _mapping_result")
    # per-hop bound: 200 km/ms (RTT * 100 km one way) with a fixed 100 km slack
    assert "q.rtt_q10 * 100" in v and "/ 1000 - 100" in v
    # robust evidence: exact 10th percentile over >= 3 traceroutes per (address, site)
    assert "HAVING COUNT(*) >= 3" in v
    assert "ORDER BY rtts)[OFFSET(CAST(FLOOR(0.1 * (COUNT(*) - 1)) AS INT64))]" in v
    assert "WHEN ns.s.rtt_q10 <= 1.5 THEN 'server_proximity'" in v
    assert "APPROX_" not in v  # deterministic


def test_both_paths_apply_the_decision_and_never_geolocate_unannounced_space():
    sql = _sql()
    assert sql.count("LEFT JOIN _hop_geo_validation hv") == 2
    assert sql.count("WHEN nwp.associated_asn IS NULL THEN 'unannounced'") == 2


def test_no_reply_hops_are_not_labelled_unannounced():
    # '*' hops also have NULL associated_asn; on 2026-10-14 all 23.6M of them were
    # labelled 'unannounced' before this guard existed.
    sql = _sql()
    guard = "WHEN nwp.addr IS NULL OR nwp.addr = '*' THEN NULL\n"
    label = "WHEN nwp.associated_asn IS NULL THEN 'unannounced'"
    assert len(re.findall(re.escape(guard) + r"\s*" + re.escape(label), sql)) == 2


def test_forward_check_is_per_hop_and_reverse_check_is_unchanged():
    sql = _sql()
    cte = sql[sql.index("\ndistance_rtt_checks AS (") :]
    fwd = _block(cte, "-- Per-hop bound", "END AS distance_rtt_check")
    assert "cumulative_distance_km" not in fwd
    assert re.search(r"WHEN rtts \* 100\s+< ST_DISTANCE\(.*\) / 1000 - 100", fwd, re.S)
    rev = _block(sql, "\nreverse_distance_rtt_checks AS (", "END AS distance_rtt_check")
    assert "(cumulative_distance_km / 200)" in rev


def test_observation_guard_removes_a_location_instead_of_the_measurement():
    norm = _block(_sql(), "\nnode_with_geo_info_normalized AS (", "\n),")
    assert "IF(n.loc_refuted, 'refuted_observation', n.geo_source) AS geo_source" in norm
    assert "IF(n.loc_refuted, NULL, n.latitude) AS latitude" in norm


# --- the decision logic on hand-built data (BigQuery, bills 0 bytes) ---------------
# One site at (0, 0). 1 degree of latitude ~ 111 km.
#   A: IPinfo ~2,000 km away, answers in ~1 ms   -> server_proximity (the site)
#   B: IPinfo ~2,000 km away, ~10 ms; RIPE ~300 km -> rtt_checked_ripe_ipmap
#   C: IPinfo ~2,000 km away, ~10 ms, no other     -> refuted (no location)
#   D: IPinfo ~50 km away, ~2 ms                   -> not refuted (no row)
#   E: IPinfo ~2,000 km away, only 2 traceroutes   -> too little evidence (no row)
#   F: answers within 1 ms of two sites ~2,200 km apart -> not one device (no row)
_OBS = [("A", r) for r in (1.0, 1.1, 1.2)] + [("B", r) for r in (10.0, 11.0, 12.0)]
_OBS += [("C", r) for r in (10.0, 10.5, 11.0)] + [("D", r) for r in (2.0, 2.1, 2.2)]
_OBS += [("E", r) for r in (1.0, 1.0)]
_OBS += [("F", r) for r in (1.0, 1.0, 1.0)]  # from site1 at (0, 0)
_OBS2 = [("F", r) for r in (1.0, 1.0, 1.0)]  # from site2 at (20, 0), ~2,200 km away
_GEO = {  # addr -> (ipinfo lat, ripe lat); all at longitude 0
    "A": (18.0, None),
    "B": (18.0, 2.7),
    "C": (18.0, None),
    "D": (0.45, None),
    "E": (18.0, None),
    "F": (18.0, None),
}


@pytest.mark.bq
def test_validation_decisions_on_a_hand_built_day():
    from google.cloud import bigquery

    sql = _sql()
    v = _block(sql, "CREATE TEMP TABLE _hop_geo_validation AS", ";\n")
    v = v.replace("CREATE TEMP TABLE _hop_geo_validation AS", "")
    obs = (
        " UNION ALL ".join(
            f"SELECT 'site1' AS dst_site, 0.0 AS dst_lat, 0.0 AS dst_lon, "
            f"STRUCT(2 AS ttl, '{a}' AS addr, CAST(NULL AS STRING) AS rdns_name, {r} AS rtts) AS n"
            for a, r in _OBS
        )
        + " UNION ALL "
        + " UNION ALL ".join(
            f"SELECT 'site2' AS dst_site, 20.0 AS dst_lat, 0.0 AS dst_lon, "
            f"STRUCT(2 AS ttl, '{a}' AS addr, CAST(NULL AS STRING) AS rdns_name, {r} AS rtts) AS n"
            for a, r in _OBS2
        )
    )
    geo = " UNION ALL ".join(
        f"SELECT '{a}' AS ip_address, {ip} AS lat_ip_info, 0.0 AS lon_ip_info, "
        f"{'NULL' if rp is None else rp} AS lat, {'NULL' if rp is None else 0.0} AS lon"
        for a, (ip, rp) in _GEO.items()
    )
    # swap the real inputs for the fixtures
    v = re.sub(
        r"FROM `mlab-collaboration\.hermes_union\.transient_events_union` t, "
        r"UNNEST\(t\.node_details\) AS n\s+WHERE t\.partition_date = '2026-10-14'",
        f"FROM (SELECT dst_site, dst_lat, dst_lon, n FROM ({obs})) t, "
        "UNNEST([t.n]) AS n WHERE TRUE",
        v,
    )
    v = v.replace(
        "LEFT JOIN _rdns_geo geo ON geo.rdns_name = a.rdns_name",
        "LEFT JOIN (SELECT CAST(NULL AS STRING) rdns_name, CAST(NULL AS FLOAT64) lat, "
        "CAST(NULL AS FLOAT64) lon) geo ON geo.rdns_name = a.rdns_name",
    )
    v = v.replace(
        "LEFT JOIN _closest_geo ip ON ip.ip_address = a.addr",
        f"LEFT JOIN ({geo}) ip ON ip.ip_address = a.addr",
    )
    assert "_rdns_geo" not in v and "_closest_geo" not in v and "transient_events_union" not in v
    rows = bigquery.Client(project="mlab-collaboration").query(v).result()
    got = {r.addr: (r.geo_source, None if r.lat is None else round(r.lat, 2)) for r in rows}
    assert got == {
        "A": ("server_proximity", 0.0),
        "B": ("rtt_checked_ripe_ipmap", 2.7),
        "C": ("refuted", None),
    }

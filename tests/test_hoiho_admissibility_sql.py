"""When step 04 may use a HOIHO location instead of IPinfo's.

From the 2026-11-21 HOIHO A/B (hermes_staging, 2026-11-24): HOIHO fails in two
ways -- ambiguous codes (``bgl`` -> Baglung NP, ``slc`` -> Shelter Cove, MTS
``nur`` -> Nuristan AF) and hostnames that name the place an address is SOLD in
rather than where the router sits (VSNL ``<ip>.static-mumbai``); IPinfo fails by
reporting the registrant for backbone ports (Level3 Milan -> Djibouti). So a
coordinate-bearing HOIHO row is used only when it is the only location, agrees
with IPinfo, or is corroborated by a PeeringDB facility of the hop's AS.

The UDF cases run on literals against BigQuery (0 bytes billed) and are gated
like test_golden_sql.py; the structural checks are offline.
"""

from __future__ import annotations

import os

import pytest

from hermes.sql import loader, paths

STEP04 = "04_mapping_union.sql"
PROJECT = os.environ.get("HERMES_TEST_PROJECT", "mlab-collaboration")


# ----------------------------------------------------------------- offline


def _step04() -> str:
    return loader.load_query(STEP04, {"DS": "hermes_union", "DAY": "2026-11-21"})


def test_step04_declares_the_admissibility_udfs():
    assert "hoiho_admissibility" in loader.required_udfs(paths.query_path(STEP04).read_text())


def test_hop_asn_is_resolved_once_for_both_directions():
    sql = _step04()
    assert "CREATE TEMP TABLE _addr_asn" in sql
    assert "reverse_masked_ip_addresses_v4" not in sql
    assert "masked_ip_addresses_v4" not in sql


def test_every_hoiho_join_goes_through_the_admissibility_table():
    sql = _step04()
    assert "CREATE TEMP TABLE _hoiho_by_addr" in sql
    # validation candidates + forward + reverse final selection
    assert sql.count("JOIN _hoiho_by_addr geo") == 3
    assert "JOIN _rdns_geo geo" not in sql


def test_geo_source_names_the_coordinate_provenance():
    sql = _step04()
    assert "THEN 'hoiho'" not in sql, "bare 'hoiho' label: use the admissibility basis"
    assert "geo.basis" in sql


# ----------------------------------------------------------------- BigQuery


def _creds_available() -> bool:
    return bool(
        os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") or os.environ.get("HERMES_BQ_TEST")
    )


@pytest.fixture(scope="module")
def ask():
    if not _creds_available():
        pytest.skip("BigQuery creds not configured (on-demand test)")
    from google.cloud import bigquery

    client = bigquery.Client(project=PROJECT)
    udfs = paths.udf_path("hoiho_admissibility").read_text()

    def run(expr: str):
        rows = list(client.query(f"{udfs}\nSELECT {expr} AS v").result())
        return rows[0]["v"]

    return run


POOL = [
    ("115.114.26.242.static-mumbai.vsnl.net.in", "115.114.26.242", True),
    ("203.197.164.138.ill-bgl.static.vsnl.net.in", "203.197.164.138", True),
    ("32-216-206-117.wlfr.ct.frontiernet.net", "32.216.206.117", True),
    ("117.206.216.32.in-addr.example.net", "32.216.206.117", True),
    ("host-1.dsl-pool.example.net", "198.51.100.7", True),
    ("be2015.rcr61.b074074-0.bue01.atlas.cogentco.com", "154.54.6.1", False),
    ("et-3-2-0-slc1-bcr2.bb.ebay.com", "66.135.0.1", False),
    ("ae12.cr1-ank2.ip4.gtt.net", "89.149.1.1", False),
]


@pytest.mark.bq
@pytest.mark.parametrize("hostname,addr,expected", POOL)
def test_pool_hostnames(ask, hostname, addr, expected):
    assert ask(f"hoiho_pool_hostname('{hostname}', '{addr}')") is expected


FACILITY = [
    ("Milan-IT", ["Milano-IT", "Frankfurt-DE"], True),
    ("Buenos Aires-AR", ["Buenos Aires-AR"], True),
    ("Frankfurt-DE", ["Frankfurt am Main-DE"], True),
    ("São Paulo-BR", ["Sao Paulo-BR"], True),
    ("Winston-Salem-US", ["Winston-Salem-US"], True),
    ("Nuristan-AF", ["Moscow-RU", "Novosibirsk-RU"], False),
    ("Shelter Cove-US", ["Salt Lake City-US", "San Jose-US"], False),
    ("Milan-IT", ["Milan-US"], False),
    ("Milan", ["Milan-IT"], False),
    ("Paris-FR", [], False),
]


@pytest.mark.bq
@pytest.mark.parametrize("place,facilities,expected", FACILITY)
def test_facility_city_match(ask, place, facilities, expected):
    arr = "ARRAY<STRING>[" + ",".join(f"'{f}'" for f in facilities) + "]"
    assert ask(f"hoiho_facility_match('{place}', {arr})") is expected


@pytest.mark.bq
def test_facility_match_tolerates_null_facilities(ask):
    assert ask("hoiho_facility_match('Paris-FR', CAST(NULL AS ARRAY<STRING>))") is False


BASIS = [
    # hoiho lat/lon, ipinfo lat/lon, pool, facility -> basis
    ("45.46, 9.19", "NULL, NULL", "FALSE", "FALSE", "hoiho_only"),
    ("47.61, -122.33", "47.45, -122.30", "FALSE", "FALSE", "hoiho_agree"),
    ("45.46, 9.19", "11.59, 43.15", "FALSE", "TRUE", "hoiho_facility"),
    ("45.46, 9.19", "11.59, 43.15", "FALSE", "FALSE", None),
    ("45.46, 9.19", "NULL, NULL", "TRUE", "TRUE", None),
    ("NULL, NULL", "11.59, 43.15", "FALSE", "TRUE", None),
]


@pytest.mark.bq
@pytest.mark.parametrize("hoiho,ipinfo,pool,facility,expected", BASIS)
def test_basis(ask, hoiho, ipinfo, pool, facility, expected):
    got = ask(
        "hoiho_basis(CAST({} AS FLOAT64), CAST({} AS FLOAT64), CAST({} AS FLOAT64), "
        "CAST({} AS FLOAT64), {}, {})".format(
            *hoiho.split(", "), *ipinfo.split(", "), pool, facility
        )
    )
    assert got == expected

"""Reference-snapshot selection in step 04 (04_mapping_union.sql).

Two kinds of reference table, two rules:

* FULL snapshots (as_metadata, ixp_members, unified_ip_to_as[_ipv6] per source):
  one snapshot date = the latest COMPLETE snapshot <= DAY, else the earliest
  (flagged); only keys present in it are candidates; undated rows are unused.
* INCREMENTAL caches (unified_ip_to_geoloc*, unified_ip_to_rdns*): per key, the
  latest row <= DAY; a future row only when the key has nothing else.

The string tests run offline. The ``bq`` tests execute the real statements from
the rendered script on hand-built literal data (0 bytes billed): every reference
table is textually replaced with an inline literal before the query is sent.
"""

from __future__ import annotations

import os
import re

import pytest

from hermes.sql import loader, paths

DAY = "2026-09-01"
SQL_FILE = "04_mapping_union.sql"


def _rendered(day: str = DAY) -> str:
    return loader.load_query(
        SQL_FILE,
        {
            "DAY": day,
            "ONE_WEEK_EARLIER": "2026-08-25",
            "DETECTION_GRANULARITY": "metro",
            "DS": "hermes_staging",
        },
    )


def _raw() -> str:
    return paths.query_path(SQL_FILE).read_text(encoding="utf-8")


def _statement(sql: str, name: str) -> str:
    """Body of ``CREATE TEMP TABLE <name> AS <body>;`` (without the semicolon)."""
    m = re.search(rf"CREATE TEMP TABLE {name} AS\n", sql)
    assert m, name
    out = []
    for line in sql[m.end() :].split("\n"):
        out.append(line)
        if line.rstrip().endswith(";") and not line.lstrip().startswith("--"):
            break
    return "\n".join(out).rstrip().rstrip(";")


def _udf() -> str:
    return paths.udf_path("snapshot_is_complete").read_text(encoding="utf-8")


# --- string-level (offline) -------------------------------------------------


def test_step04_inlines_the_completeness_udf():
    assert "snapshot_is_complete" in loader.required_udfs(_raw())
    assert "CREATE TEMP FUNCTION snapshot_is_complete" in _rendered()


def test_no_lookup_uses_closest_date_as_primary_key():
    """The old rule ordered by |partition_date - DAY| first, letting future rows win."""
    for name in ("_closest_as_metadata", "_closest_rdns", "_closest_geo", "_extracted_prefixes"):
        body = _statement(_raw(), name)
        assert "DENSE_RANK" not in body, name
        for m in re.finditer(r"ORDER BY\s*\n?\s*(.+)", body):
            assert not m.group(1).lstrip().startswith("ABS(DATE_DIFF"), (name, m.group(1))


def test_per_key_caches_rank_future_rows_last():
    for name in ("_closest_rdns", "_closest_geo"):
        body = _statement(_raw(), name)
        order = body[body.index("ORDER BY") :]
        first_key = order.split("\n")[1].strip()
        assert first_key.startswith("(CASE WHEN partition_date > DATE '${DAY}' THEN 1"), name


def test_prefix_snapshot_is_chosen_per_source_and_recorded():
    body = _statement(_raw(), "_extracted_prefixes")
    assert "as_chosen" in body and "GROUP BY ip_version, source" in body
    # undated rows never enter the snapshot catalogue
    assert body.count("WHERE partition_date IS NOT NULL") >= 3
    assert "OR partition_date IS NULL" not in body
    assert "as_partition_date" in body


def test_main_mapping_statement_is_untouched():
    """It sits near BigQuery's planning limit; this change must not add CTEs to it."""
    sql = _raw()
    a = sql.index("CREATE TEMP TABLE _mapping_result AS")
    b = sql.index("CREATE TEMP TABLE _src_metro", a)
    body = "\n".join(ln for ln in sql[a:b].splitlines() if not ln.lstrip().startswith("--"))
    assert len(re.findall(r"^([a-z_0-9]+) AS \(", body, re.M)) == 52


# --- BigQuery, literal data only --------------------------------------------


def _bq_creds() -> bool:
    return bool(
        os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") or os.environ.get("HERMES_BQ_TEST")
    )


def _run(sql: str) -> list[dict]:
    if not _bq_creds():
        pytest.skip("BigQuery creds not configured (on-demand test)")
    from google.cloud import bigquery

    c = bigquery.Client(project="mlab-collaboration")
    job = c.query(sql, job_config=bigquery.QueryJobConfig(maximum_bytes_billed=10 * 2**20))
    rows = [dict(r) for r in job.result()]
    assert (job.total_bytes_billed or 0) <= 10 * 2**20
    return rows


_AS_TYPE = "STRUCT<ip_prefix STRING, asn INT64, source STRING, ixp STRING, partition_date DATE>"
_HOP_EMPTY = (
    "(SELECT * FROM UNNEST(ARRAY<STRUCT<id STRING, date DATE, raw STRUCT<Annotations "
    "STRUCT<Network STRUCT<CIDR STRING, ASNumber INT64>>>>>[]))"
)
_IXP_TYPE = "STRUCT<ipv4 STRING, asn INT64, name STRING, partition_date DATE>"


def _literal(type_: str, rows: list[str]) -> str:
    return f"(SELECT * FROM UNNEST(ARRAY<{type_}>[{', '.join(rows)}]))"


def _prefix_sql(day: str, v4_rows: list[str], ixp_rows: list[str] | None = None) -> str:
    body = _statement(_rendered(day), "_extracted_prefixes")
    ixp_rows = ixp_rows or ["('80.81.192.1', 1, 'IX', DATE '2020-01-01')"]
    body = (
        body.replace("`mlab-collaboration.hermes.unified_ip_to_as`", _literal(_AS_TYPE, v4_rows))
        .replace(
            "`mlab-collaboration.hermes.unified_ip_to_as_ipv6`",
            _literal(_AS_TYPE, ["('2001:db8::/32', 1, 'RouteViews', NULL, DATE '2020-01-01')"]),
        )
        .replace("`measurement-lab.ndt_raw.hopannotation2`", _HOP_EMPTY)
        .replace("`mlab-collaboration.ix_data.ixp_members`", _literal(_IXP_TYPE, ixp_rows))
    )
    assert "mlab-collaboration." not in body and "measurement-lab." not in body
    return body


def _rv(prefix: str, asn: int, d: str | None) -> str:
    date = f"DATE '{d}'" if d else "NULL"
    return f"('{prefix}', {asn}, 'RouteViews', NULL, {date})"


def _filler(d: str, n: int = 3) -> list[str]:
    """Unrelated prefixes so a snapshot has a realistic size for the completeness guard."""
    return [_rv(f"203.0.{i}.0/24", 64500 + i, d) for i in range(n)]


def _lpm(prefix_sql: str, ip: str) -> str:
    return f"""
    {_udf()}
    WITH ep AS ({prefix_sql})
    SELECT ip_prefix, asn, mask, as_partition_date
    FROM ep
    WHERE ip_version = 'v4'
      AND NET.IP_FROM_STRING('{ip}') & NET.IP_NET_MASK(4, mask) = network_bin
    ORDER BY mask DESC, asn
    """


@pytest.mark.bq
def test_withdrawn_more_specific_does_not_beat_current_covering_prefix():
    # /24 only in the old snapshot (withdrawn since); /16 in the current one.
    rows = (
        [_rv("10.1.2.0/24", 111, "2026-06-01"), _rv("10.1.0.0/16", 222, "2026-06-01")]
        + _filler("2026-06-01")
        + [_rv("10.1.0.0/16", 222, "2026-08-20")]
        + _filler("2026-08-20")
        # a FUTURE snapshot re-adds the /24 under another origin: must be ignored
        + [_rv("10.1.2.0/24", 333, "2026-10-01"), _rv("10.1.0.0/16", 222, "2026-10-01")]
        + _filler("2026-10-01")
    )
    got = _run(_lpm(_prefix_sql(DAY, rows), "10.1.2.3"))
    assert got[0]["ip_prefix"] == "10.1.0.0/16" and got[0]["asn"] == 222
    assert {r["ip_prefix"] for r in got} == {"10.1.0.0/16"}
    assert str(got[0]["as_partition_date"]) == "2026-08-20"


@pytest.mark.bq
def test_partial_snapshot_is_skipped():
    # 2026-08-25 is a truncated load (1 row vs 4): DAY falls after it, but the
    # complete 2026-08-20 snapshot must be used.
    rows = (
        [_rv("10.1.0.0/16", 222, "2026-08-20")]
        + _filler("2026-08-20")
        + [_rv("10.9.0.0/16", 999, "2026-08-25")]
        + [_rv("10.1.0.0/16", 222, "2026-09-10")]
        + _filler("2026-09-10")
    )
    got = _run(_lpm(_prefix_sql(DAY, rows), "10.1.2.3"))
    assert [str(r["as_partition_date"]) for r in got] == ["2026-08-20"]
    assert _run(_lpm(_prefix_sql(DAY, rows), "10.9.0.1")) == []


@pytest.mark.bq
def test_backfill_before_first_snapshot_uses_earliest_and_flags_it():
    rows = (
        [_rv("10.1.0.0/16", 222, "2025-05-28")]
        + _filler("2025-05-28")
        + [_rv("10.1.0.0/16", 333, "2025-06-09")]
        + _filler("2025-06-09")
        # undated rows have no vintage and are never used
        + [_rv("10.1.2.0/24", 444, None)]
    )
    got = _run(_lpm(_prefix_sql("2025-03-01", rows), "10.1.2.3"))
    assert len(got) == 1
    assert got[0]["asn"] == 222
    assert str(got[0]["as_partition_date"]) == "2025-05-28"  # > DAY: flagged


@pytest.mark.bq
def test_ixp_membership_uses_one_snapshot():
    body = _prefix_sql(
        DAY,
        [_rv("10.1.0.0/16", 222, "2026-08-20")] + _filler("2026-08-20"),
        ixp_rows=[
            "('80.81.192.1', 1, 'OLD-IX', DATE '2026-06-01')",  # left the IXP since
            "('80.81.192.2', 2, 'IX', DATE '2026-06-01')",
            "('80.81.192.2', 2, 'IX', DATE '2026-08-01')",
            "('80.81.192.3', 3, 'IX', DATE '2026-08-01')",
            "('80.81.192.9', 9, 'NEW-IX', DATE '2026-10-01')",  # future member
            "('80.81.192.2', 2, 'IX', DATE '2026-10-01')",
            "('80.81.192.3', 3, 'IX', DATE '2026-10-01')",
        ],
    )
    tail = "SELECT * FROM extracted_prefixes_v4\nUNION ALL\nSELECT * FROM extracted_prefixes_v6"
    assert tail in body
    body = body.replace(
        tail, "SELECT ipv4, partition_date FROM ixp_ranked WHERE rank = 1 ORDER BY ipv4"
    )
    got = _run(_udf() + "\n" + body)
    assert [(r["ipv4"], str(r["partition_date"])) for r in got] == [
        ("80.81.192.2", "2026-08-01"),
        ("80.81.192.3", "2026-08-01"),
    ]


_GEO_TYPE = (
    "STRUCT<ip_address STRING, city STRING, country STRING, lat FLOAT64, lon FLOAT64, "
    "score FLOAT64, city_ip_info STRING, country_ip_info STRING, lat_ip_info FLOAT64, "
    "lon_ip_info FLOAT64, rank INT64, region_ip_info STRING, metro STRING, polygon GEOGRAPHY, "
    "partition_date DATE, geoloc_snapshot STRING, geoloc_snapshot_date DATE>"
)


def _geo(ip: str, metro: str | None, d: str) -> str:
    m = f"'{metro}'" if metro else "NULL"
    return (
        f"('{ip}', NULL, NULL, NULL, NULL, NULL, {m}, NULL, 1.0, 2.0, NULL, NULL, {m}, "
        f"NULL, DATE '{d}', NULL, DATE '{d}')"
    )


@pytest.mark.bq
def test_geo_cache_is_as_of_day_per_ip():
    rows = [
        # A: latest past wins over an older past and a closer future row
        _geo("192.0.2.1", "old", "2026-06-01"),
        _geo("192.0.2.1", "past", "2026-08-30"),
        _geo("192.0.2.1", "future", "2026-09-02"),
        # B: only future rows -> earliest future, flagged by its partition_date
        _geo("192.0.2.2", "f1", "2026-09-10"),
        _geo("192.0.2.2", "f2", "2026-10-10"),
        # C: an empty recent past row does not shadow an older located one,
        #    and a located future row does not beat them
        _geo("192.0.2.3", "located", "2026-05-01"),
        _geo("192.0.2.3", None, "2026-08-31"),
        _geo("192.0.2.3", "future", "2026-09-05"),
    ]
    body = (
        _statement(_rendered(), "_closest_geo")
        .replace("`mlab-collaboration.hermes.unified_ip_to_geoloc`", _literal(_GEO_TYPE, rows))
        .replace("`mlab-collaboration.hermes.unified_ip_to_geoloc_ipv6`", _literal(_GEO_TYPE, []))
    )
    got = {
        r["ip_address"]: (r["metro"], str(r["partition_date"]))
        for r in _run(f"SELECT ip_address, metro, partition_date FROM ({body})")
    }
    assert got == {
        "192.0.2.1": ("past", "2026-08-30"),
        "192.0.2.2": ("f1", "2026-09-10"),
        "192.0.2.3": ("located", "2026-05-01"),
    }


_RDNS_TYPE = "STRUCT<ip_address STRING, hostname STRING, timestamp TIMESTAMP, partition_date DATE>"


@pytest.mark.bq
def test_rdns_cache_is_as_of_day_per_ip():
    rows = [
        "('192.0.2.1', 'past.example', NULL, DATE '2026-08-01')",
        "('192.0.2.1', 'future.example', NULL, DATE '2026-09-02')",
        "('192.0.2.2', 'undated.example', NULL, NULL)",
        "('192.0.2.2', 'future.example', NULL, DATE '2026-09-02')",
        "('192.0.2.3', 'f1.example', NULL, DATE '2026-09-03')",
        "('192.0.2.3', 'f2.example', NULL, DATE '2026-09-30')",
    ]
    body = (
        _statement(_rendered(), "_closest_rdns")
        .replace("`mlab-collaboration.hermes.unified_ip_to_rdns`", _literal(_RDNS_TYPE, rows))
        .replace("`mlab-collaboration.hermes.unified_ip_to_rdns_ipv6`", _literal(_RDNS_TYPE, []))
    )
    got = {
        r["ip_address"]: r["hostname"] for r in _run(f"SELECT ip_address, hostname FROM ({body})")
    }
    assert got == {
        "192.0.2.1": "past.example",
        "192.0.2.2": "undated.example",
        "192.0.2.3": "f1.example",
    }


_ASM_TYPE = (
    "STRUCT<asn STRING, asnName STRING, rank INT64, "
    "cone STRUCT<numberAsns INT64, numberPrefixes INT64, numberAddresses INT64>, "
    "partition_date DATE>"
)


@pytest.mark.bq
def test_as_metadata_uses_one_snapshot():
    def r(asn: str, name: str, d: str) -> str:
        return f"('{asn}', '{name}', 1, STRUCT(1, 1, 1), DATE '{d}')"

    rows = [
        r("1", "one-2025", "2025-06-10"),
        r("2", "two-2025", "2025-06-10"),
        r("1", "one-2026", "2026-06-01"),
        r("2", "two-2026", "2026-06-01"),
        r("3", "three-2026", "2026-06-01"),  # new ASN, not in the chosen snapshot
    ]
    body = _statement(_rendered("2026-01-01"), "_closest_as_metadata").replace(
        "`mlab-collaboration.hermes.as_metadata`", _literal(_ASM_TYPE, rows)
    )
    got = {
        x["asn"]: x["asnName"]
        for x in _run(_udf() + f"\nSELECT asn, asnName FROM ({body}) ORDER BY asn")
    }
    assert got == {"1": "one-2025", "2": "two-2025"}

"""refresh_hoiho_geolocation: candidates -> HOIHO -> geolocation + lookup cache.

Defects this replaces (verified on hermes-ec2, 2026-11-23): the lookup call was
commented out and the pickle caches were empty, so the step was a silent no-op
from 2026-06 onward; the IPv6 pass retargeted its metro rewrite to a
``geolocation_ipv6`` table that does not exist; rows were streamed and then the
same table was immediately replaced by CREATE OR REPLACE.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest import mock

from google.cloud import bigquery

from hermes.enrichment.hoiho import lookup, refresh

GEO = "p.hermes_staging.geolocation"
LOOKUPS = "p.hermes_staging.hoiho_lookups"
RDNS = "p.hermes.unified_ip_to_rdns"
SCHEMAS = Path(__file__).resolve().parents[1] / "scripts" / "schemas"


def _client(candidates: list[str]) -> mock.MagicMock:
    client = mock.MagicMock()
    client.query.return_value.result.return_value = [{"hostname": h} for h in candidates]
    return client


def _fake_query(rows=(), looked_up=(), failed=0):
    def query(hostnames, **_):
        return lookup.LookupResult(
            rows=[dict(r) for r in rows],
            looked_up=set(looked_up),
            matched={r["hostname"] for r in rows},
            failed_batches=failed,
            ruleset_date="2024-08",
        )

    return query


def _run(client, query):
    return refresh.refresh_hoiho_geolocation(
        client,
        "2026-11-01",
        rdns_table=RDNS,
        geolocation_table=GEO,
        lookups_table=LOOKUPS,
        lookback_days=30,
        query=query,
    )


def _row(hostname: str) -> dict:
    return lookup.match_to_row({"hostname": hostname, "place": "Paris", "cc": "FR"})


def test_candidates_exclude_hostnames_already_looked_up_or_stored():
    client = _client([])
    _run(client, _fake_query())
    sql = client.query.call_args_list[0].args[0]
    assert f"`{RDNS}`" in sql
    assert f"`{GEO}`" in sql and f"`{LOOKUPS}`" in sql
    assert "'2026-10-02'" in sql and "'2026-11-01'" in sql


def test_no_candidates_means_no_api_call_and_no_writes():
    client = _client([])
    query = mock.Mock(side_effect=AssertionError("must not call HOIHO"))
    _run(client, query)
    client.load_table_from_json.assert_not_called()


def test_matches_and_lookups_are_loaded_not_streamed():
    client = _client(["a.fr", "b.fr"])
    _run(client, _fake_query(rows=[_row("a.fr")], looked_up=["a.fr", "b.fr"]))
    client.insert_rows_json.assert_not_called()
    targets = [c.args[1] for c in client.load_table_from_json.call_args_list]
    assert targets == [GEO, LOOKUPS], "geolocation before the cache, so a crash re-queries"


def test_lookup_rows_record_hits_and_misses():
    client = _client(["a.fr", "b.fr"])
    _run(client, _fake_query(rows=[_row("a.fr")], looked_up=["a.fr", "b.fr"]))
    rows = client.load_table_from_json.call_args_list[1].args[0]
    assert {(r["hostname"], r["matched"]) for r in rows} == {("a.fr", True), ("b.fr", False)}
    assert {r["ruleset_date"] for r in rows} == {"2024-08"}


def test_loads_append():
    client = _client(["a.fr"])
    _run(client, _fake_query(rows=[_row("a.fr")], looked_up=["a.fr"]))
    for call in client.load_table_from_json.call_args_list:
        assert call.kwargs["job_config"].write_disposition == bigquery.WriteDisposition.WRITE_APPEND


def test_metro_pass_targets_the_given_table_after_new_matches():
    client = _client(["a.fr"])
    _run(client, _fake_query(rows=[_row("a.fr")], looked_up=["a.fr"]))
    metro_sql = client.query.call_args_list[-1].args[0]
    assert metro_sql.startswith(f"CREATE OR REPLACE TABLE `{GEO}`") or (
        f"CREATE OR REPLACE TABLE `{GEO}`" in metro_sql
    )
    assert "geolocation_ipv6" not in metro_sql


def test_no_metro_pass_when_nothing_matched():
    client = _client(["b.fr"])
    _run(client, _fake_query(looked_up=["b.fr"]))
    assert all("CREATE OR REPLACE" not in c.args[0] for c in client.query.call_args_list)


def test_lookups_table_is_created_if_missing():
    client = _client([])
    _run(client, _fake_query())
    table = client.create_table.call_args.args[0]
    assert table.reference.table_id == "hoiho_lookups"
    assert client.create_table.call_args.kwargs["exists_ok"] is True


def test_lookups_schema_matches_the_init_project_schema_file():
    on_disk = json.loads((SCHEMAS / "hermes.hoiho_lookups.json").read_text())
    in_code = [
        {"name": f.name, "type": f.field_type, "mode": f.mode} for f in refresh.LOOKUPS_SCHEMA
    ]
    assert [(f["name"], f["type"], f.get("mode", "NULLABLE")) for f in on_disk] == [
        (f["name"], f["type"], f["mode"]) for f in in_code
    ]


def test_summary_reports_failed_batches():
    client = _client(["a.fr"])
    summary = _run(client, _fake_query(looked_up=[], failed=1))
    assert summary.failed_batches == 1
    assert summary.candidates == 1
    client.load_table_from_json.assert_not_called()


def test_geolocation_load_schema_matches_production():
    on_disk = json.loads((SCHEMAS / "hermes.geolocation.json").read_text())
    assert [(f["name"], f["type"], f.get("mode", "NULLABLE")) for f in on_disk] == [
        (f.name, f.field_type, f.mode) for f in refresh.GEOLOCATION_SCHEMA
    ]

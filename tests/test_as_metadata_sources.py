"""Tests for building the hermes.as_metadata inputs in-process.

These replace two scripts from the private missing-peering-links repo. The
formats are load-bearing: ASMetadataEnricher reads the CSVs with
``index_col=0`` and the AS Rank file line by line, so a plausible-looking but
differently-shaped file would fail late, during upload, rather than here.
"""

from __future__ import annotations

import json

import pandas as pd
import pytest
import requests

from hermes.enrichment.as_metadata.paths import (
    DEFAULT_AS_METADATA_DIR,
    as_metadata_input_paths,
    resolve_as_metadata_dir,
)
from hermes.enrichment.as_metadata.sources import (
    fetch_asrank_asns,
    peeringdb_as_facilities,
    peeringdb_as_type,
)

# --- paths ---------------------------------------------------------------------


def test_default_matches_the_historical_layout(monkeypatch):
    monkeypatch.delenv("HERMES_AS_METADATA_DIR", raising=False)
    paths = as_metadata_input_paths("2026-09-01")
    assert str(paths["caida"]).startswith(DEFAULT_AS_METADATA_DIR)
    assert paths["caida"].name == "ASNS-20260901.json"
    assert paths["footprint"].name == "AS_footprint_info_2026-09.csv"
    assert paths["as_type"].name == "AS_Type2026-09.csv"


def test_env_var_moves_the_base(monkeypatch):
    monkeypatch.setenv("HERMES_AS_METADATA_DIR", "/srv/asmeta")
    assert resolve_as_metadata_dir() == __import__("pathlib").Path("/srv/asmeta")
    assert str(as_metadata_input_paths("2026-09-01")["caida"]).startswith("/srv/asmeta")


def test_explicit_base_beats_the_environment(monkeypatch):
    monkeypatch.setenv("HERMES_AS_METADATA_DIR", "/srv/env")
    assert str(as_metadata_input_paths("2026-09-01", "/srv/arg")["caida"]).startswith("/srv/arg")


def test_peeringdb_inputs_are_keyed_by_month_not_day():
    """Why as_metadata belongs on a monthly cadence, not the biweekly one.

    The CAIDA file is per-day but both PeeringDB CSVs are per-month, so running
    twice in one month regenerates identical inputs and writes a duplicate
    partition -- as_metadata has no re-run guard.
    """
    first = as_metadata_input_paths("2026-09-01")
    fifteenth = as_metadata_input_paths("2026-09-15")
    assert first["as_type"] == fifteenth["as_type"]
    assert first["footprint"] == fifteenth["footprint"]
    assert first["caida"] != fifteenth["caida"]


# --- PeeringDB-derived CSVs -----------------------------------------------------

DUMP = {
    "net": {
        "data": [
            {
                "asn": 15169,
                "name": "Google",
                "org_id": 1,
                "info_traffic": "100+ Tbps",
                "info_ratio": "Mostly Outbound",
                "info_type": "Content",
                "irrelevant": "dropped",
            }
        ]
    },
    "netfac": {
        "data": [
            {
                "local_asn": 15169,
                "name": "Equinix LD5",
                "city": "Slough",
                "country": "GB",
                "fac_id": 7,
            }
        ]
    },
}


def test_as_type_keeps_exactly_the_columns_the_reader_uses():
    frame = peeringdb_as_type(DUMP)
    assert list(frame.columns) == [
        "asn",
        "name",
        "org_id",
        "info_traffic",
        "info_ratio",
        "info_type",
    ]


def test_facilities_take_city_and_country_from_netfac_directly():
    # netfac already carries city/country, so no join against fac and no geocoding.
    frame = peeringdb_as_facilities(DUMP)
    assert list(frame.columns) == ["local_asn", "name", "city", "country"]
    assert frame.iloc[0]["city"] == "Slough"


def test_missing_columns_fail_loudly():
    for table, fn in (("net", peeringdb_as_type), ("netfac", peeringdb_as_facilities)):
        with pytest.raises(RuntimeError, match=f"'{table}' table missing columns"):
            fn({table: {"data": [{"unexpected": 1}]}})


def test_empty_tables_fail_rather_than_writing_an_empty_file():
    # An empty as_metadata input would upload a partition with no ASNs.
    with pytest.raises(RuntimeError):
        peeringdb_as_type({})


def test_csvs_round_trip_through_the_enricher_processors(tmp_path):
    """The real contract: written with an index, read back with index_col=0."""
    from hermes.enrichment.as_metadata.enricher import ASMetadataEnricher

    as_type_path = tmp_path / "AS_Type2026-09.csv"
    fac_path = tmp_path / "AS_footprint_info_2026-09.csv"
    peeringdb_as_type(DUMP).to_csv(as_type_path)
    peeringdb_as_facilities(DUMP).to_csv(fac_path)

    enricher = object.__new__(ASMetadataEnricher)
    as_type = ASMetadataEnricher.process_as_type_data(
        enricher, pd.read_csv(as_type_path, index_col=0)
    )
    facilities = ASMetadataEnricher.process_facilities_data(
        enricher, pd.read_csv(fac_path, index_col=0)
    )
    assert as_type[15169]["info_type"] == "Content"
    assert as_type[15169]["name"] == "Google"
    assert facilities[15169]["city_cc"] == ["Slough-GB"]
    assert facilities[15169]["name"] == ["Equinix LD5"]


# --- AS Rank pagination ---------------------------------------------------------


class _Page:
    def __init__(self, nodes, has_next, total, first):
        self._payload = {
            "data": {
                "asns": {
                    "totalCount": total,
                    "pageInfo": {"first": first, "hasNextPage": has_next},
                    "edges": [{"node": n} for n in nodes],
                }
            }
        }

    def raise_for_status(self):
        pass

    def json(self):
        return self._payload


def _session(pages):
    class _S:
        def __init__(self):
            self.calls = 0

        def post(self, url, json=None, timeout=None):
            page = pages[self.calls]
            self.calls += 1
            return page

    return _S()


def test_pagination_walks_every_page(tmp_path):
    out = tmp_path / "ASNS.json"
    pages = [
        _Page([{"asn": "1"}, {"asn": "2"}], True, 3, 2),
        _Page([{"asn": "3"}], False, 3, 2),
    ]
    assert fetch_asrank_asns(out, page_size=2, session=_session(pages)) == 3
    lines = out.read_text().strip().split("\n")
    assert [json.loads(x)["asn"] for x in lines] == ["1", "2", "3"]


def test_short_result_is_an_error_not_a_silent_truncation(tmp_path):
    # Writing fewer ASNs than totalCount would leave as_metadata quietly missing
    # networks, which is exactly the failure mode this whole area suffered from.
    pages = [_Page([{"asn": "1"}], False, 5000, 1)]
    with pytest.raises(RuntimeError, match="returned 1 of 5000"):
        fetch_asrank_asns(tmp_path / "ASNS.json", session=_session(pages))


def test_graphql_errors_are_raised(tmp_path):
    class _Err:
        def raise_for_status(self):
            pass

        def json(self):
            return {"errors": [{"message": "boom"}]}

    class _S:
        def post(self, url, json=None, timeout=None):
            return _Err()

    with pytest.raises(RuntimeError, match="AS Rank query failed"):
        fetch_asrank_asns(tmp_path / "ASNS.json", session=_S())


def test_has_next_page_with_no_rows_does_not_loop_forever(tmp_path):
    pages = [_Page([], True, 10, 0)]
    with pytest.raises(RuntimeError, match="returned no rows"):
        fetch_asrank_asns(tmp_path / "ASNS.json", session=_session(pages))


# --- retries -------------------------------------------------------------------


class _Boom:
    """A response that raises `exc` on send, or an HTTP error with `status`."""

    def __init__(self, exc=None, status=None):
        self.exc = exc
        self.status = status

    def raise_for_status(self):
        if self.status:
            err = requests.HTTPError(f"{self.status}")
            err.response = type("R", (), {"status_code": self.status})()
            raise err

    def json(self):
        return {}


def _flaky_session(sequence):
    class _S:
        def __init__(self):
            self.calls = 0

        def post(self, url, json=None, timeout=None):
            item = sequence[self.calls]
            self.calls += 1
            if isinstance(item, _Boom) and item.exc:
                raise item.exc
            return item

    return _S()


def test_a_timed_out_page_is_retried_not_fatal(tmp_path, monkeypatch):
    """One slow page must not discard the whole crawl.

    A real run died at 113,000 of 121,200 ASNs on a ReadTimeout at a deep
    offset; deep offsets take ~10s against ~0.5s shallow, so the occasional
    overrun is expected rather than exceptional.
    """
    monkeypatch.setattr("time.sleep", lambda _s: None)
    sequence = [
        _Boom(exc=requests.Timeout("slow")),
        _Page([{"asn": "1"}], False, 1, 1),
    ]
    out = tmp_path / "ASNS.json"
    assert fetch_asrank_asns(out, session=_flaky_session(sequence), attempts=3) == 1
    assert json.loads(out.read_text().strip())["asn"] == "1"


def test_retries_are_bounded(tmp_path, monkeypatch):
    monkeypatch.setattr("time.sleep", lambda _s: None)
    sequence = [_Boom(exc=requests.Timeout("slow"))] * 3
    with pytest.raises(RuntimeError, match="failed after 3 attempts"):
        fetch_asrank_asns(tmp_path / "ASNS.json", session=_flaky_session(sequence), attempts=3)


def test_server_errors_retry_but_client_errors_do_not(tmp_path, monkeypatch):
    monkeypatch.setattr("time.sleep", lambda _s: None)
    # 503 is transient -> retried, then succeeds.
    ok = fetch_asrank_asns(
        tmp_path / "a.json",
        session=_flaky_session([_Boom(status=503), _Page([{"asn": "1"}], False, 1, 1)]),
        attempts=3,
    )
    assert ok == 1

    # 400 means the query is wrong; retrying just repeats it.
    with pytest.raises(requests.HTTPError):
        fetch_asrank_asns(
            tmp_path / "b.json", session=_flaky_session([_Boom(status=400)] * 3), attempts=3
        )

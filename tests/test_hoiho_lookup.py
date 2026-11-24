"""HOIHO API client and row normalization (no network, no BigQuery).

Re-enabling HOIHO after it sat disabled from 2026-06 to 2026-11. The old client
keyed responses by hostname -- keeping only the LAST of several matches -- and
cached only matches, so every non-matching hostname was re-queried every night.
A batch that failed was silently indistinguishable from "no match".
"""

from __future__ import annotations

import math

import requests

from hermes.enrichment.hoiho import lookup


class _Resp:
    def __init__(self, status: int, payload: object):
        self.status_code = status
        self._payload = payload

    def json(self) -> object:
        return self._payload


def _post_returning(*responses):
    calls: list[list[str]] = []
    queue = list(responses)

    def post(url, json, timeout):
        calls.append(list(json))
        item = queue.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    return post, calls


# --------------------------------------------------------------------- hostnames


def test_normalize_hostname_lowercases_and_strips_trailing_dot():
    assert lookup.normalize_hostname("  AE-1.R20.Londen12.UK.bb.gin.ntt.NET. ") == (
        "ae-1.r20.londen12.uk.bb.gin.ntt.net"
    )


def test_normalize_hostname_rejects_empty():
    assert lookup.normalize_hostname(None) is None
    assert lookup.normalize_hostname("  .  ") is None


# --------------------------------------------------------------------- rows


def test_match_to_row_matches_the_existing_table_conventions():
    row = lookup.match_to_row(
        {
            "hostname": "Be-12.ccr41.AMS03.atlas.cogentco.com",
            "match_strs": ["ams"],
            "match_meanings": ["iata"],
            "cc": "NL",
            "place": "Amsterdam",
            "st": "NH",
            "lat": "52.308601",
            "lng": "4.763890",
            "iata": "ams",
        }
    )
    assert row["hostname"] == "be-12.ccr41.ams03.atlas.cogentco.com"
    assert math.isclose(row["lat"], 52.308601) and math.isclose(row["lon"], 4.76389)
    assert row["place"] == "Amsterdam-NL"
    assert row["cc"] == "NL"
    assert row["state"] == "NH"
    assert row["metro"] is None
    assert row["match_strs"] == ["ams"] and row["match_meanings"] == ["iata"]
    assert set(row) == set(lookup.GEOLOCATION_COLUMNS)


def test_match_to_row_uppercases_country_code():
    # The API returns "uk" for CLLI matches and "NL" for IATA ones; 6,333 stored rows
    # carry a lower-case cc, which then never equals an upper-case one downstream.
    row = lookup.match_to_row({"hostname": "x.uk", "cc": "uk", "clli": "londen"})
    assert row["cc"] == "UK"
    assert row["clli"] == "londen"


def test_match_to_row_tolerates_missing_or_bad_coordinates():
    row = lookup.match_to_row({"hostname": "a.b", "lat": "n/a", "match_strs": None})
    assert row["lat"] is None and row["lon"] is None
    assert row["place"] is None
    assert row["match_strs"] == [] and row["match_meanings"] == []


# --------------------------------------------------------------------- API client


def test_query_keeps_every_match_for_a_hostname():
    post, _ = _post_returning(
        _Resp(
            200,
            {
                "summary": {"ruleset_date": "2024-08"},
                "matches": [
                    {"hostname": "a.winchester.tn", "place": "Winchester", "cc": "US", "st": "TN"},
                    {"hostname": "a.winchester.tn", "place": "Winchester", "cc": "US", "st": "VA"},
                ],
            },
        )
    )
    result = lookup.query_hoiho(["a.winchester.tn"], post=post, sleep_seconds=0)
    assert [r["state"] for r in result.rows] == ["TN", "VA"]
    assert result.ruleset_date == "2024-08"


def test_query_records_misses_as_looked_up():
    post, _ = _post_returning(_Resp(200, {"matches": [{"hostname": "hit.example"}]}))
    result = lookup.query_hoiho(["hit.example", "miss.example"], post=post, sleep_seconds=0)
    assert result.looked_up == {"hit.example", "miss.example"}
    assert result.matched == {"hit.example"}


def test_failed_batch_is_not_recorded_as_a_miss():
    post, calls = _post_returning(
        _Resp(200, {"matches": []}),
        _Resp(503, {"error": "unavailable"}),
        requests.ConnectionError("reset"),
    )
    result = lookup.query_hoiho(["a.x", "b.x", "c.x"], post=post, batch_size=1, sleep_seconds=0)
    assert len(calls) == 3
    assert result.looked_up == {calls[0][0]}
    assert result.failed_batches == 2


def test_query_normalizes_and_dedups_input():
    post, calls = _post_returning(_Resp(200, {"matches": []}))
    lookup.query_hoiho(["A.X.", "a.x", None, ""], post=post, sleep_seconds=0)
    assert calls == [["a.x"]]


def test_query_batches_by_batch_size():
    post, calls = _post_returning(*[_Resp(200, {"matches": []})] * 3)
    lookup.query_hoiho([f"h{i}.x" for i in range(5)], post=post, batch_size=2, sleep_seconds=0)
    assert [len(c) for c in calls] == [2, 2, 1]


def test_empty_input_makes_no_request():
    post, calls = _post_returning()
    result = lookup.query_hoiho([], post=post, sleep_seconds=0)
    assert calls == [] and result.looked_up == set() and result.rows == []

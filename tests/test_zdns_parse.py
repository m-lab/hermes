"""Tests for zDNS output parsing (current nested schema + legacy fallback)."""

from __future__ import annotations

from hermes.enrichment.zdns.enricher import parse_zdns_result

# Current zdns schema: status/data/timestamp nested under results.PTR
NEW_FORMAT = {
    "name": "8.8.8.8",
    "results": {
        "PTR": {
            "status": "NOERROR",
            "timestamp": "2026-06-12T13:24:52Z",
            "data": {"answers": [{"type": "PTR", "answer": "dns.google."}]},
        }
    },
}

# Legacy schema: keys at top level
LEGACY_FORMAT = {
    "name": "1.1.1.1",
    "status": "NOERROR",
    "timestamp": "2026-01-01T00:00:00Z",
    "data": {"answers": [{"type": "PTR", "answer": "one.one.one.one."}]},
}

NXDOMAIN = {
    "name": "10.0.0.1",
    "results": {"PTR": {"status": "NXDOMAIN", "timestamp": "2026-06-12T13:24:52Z", "data": {}}},
}


def test_new_format_extracts_ptr_and_timestamp():
    rows = parse_zdns_result(NEW_FORMAT, "2026-06-10")
    assert rows == [
        {
            "ip_address": "8.8.8.8",
            "hostname": "dns.google.",
            "timestamp": "2026-06-12T13:24:52Z",
            "partition_date": "2026-06-10",
        }
    ]


def test_legacy_format_still_supported():
    rows = parse_zdns_result(LEGACY_FORMAT, "2026-06-10")
    assert rows[0]["hostname"] == "one.one.one.one."
    assert rows[0]["timestamp"] == "2026-01-01T00:00:00Z"


def test_nxdomain_yields_none_hostname():
    rows = parse_zdns_result(NXDOMAIN, "2026-06-10")
    assert rows == [
        {
            "ip_address": "10.0.0.1",
            "hostname": None,
            "timestamp": "2026-06-12T13:24:52Z",
            "partition_date": "2026-06-10",
        }
    ]


def test_non_ptr_answer_yields_none_hostname():
    rec = {
        "name": "9.9.9.9",
        "results": {
            "PTR": {
                "status": "NOERROR",
                "timestamp": "2026-06-12T13:24:52Z",
                "data": {"answers": [{"type": "CNAME", "answer": "ignored"}]},
            }
        },
    }
    assert parse_zdns_result(rec, "2026-06-10")[0]["hostname"] is None

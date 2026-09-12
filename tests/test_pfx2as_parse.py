"""Tests for CAIDA pfx2as origin-AS parsing (MOAS, AS_SET, private, malformed)."""

from __future__ import annotations

from hermes.enrichment.routeviews.pfx2as import parse_origin_asns


def test_single_origin():
    assert parse_origin_asns("15169") == [15169]


def test_moas_keeps_every_origin_in_file_order():
    # The real 2026-07-13 rv6 line for 2402:8100::/32. Keeping only the first
    # origin attributed a Vodafone Idea prefix to Google, because CAIDA sorts
    # MOAS origins ascending and Google's AS36040 is the smallest here.
    assert parse_origin_asns("36040,38266,45271") == [36040, 38266, 45271]


def test_as_set_is_split_not_concatenated():
    # int("15169_19527") == 151919527 -- Python reads "_" as a digit separator,
    # so the unsplit field used to yield an ASN that does not exist.
    assert parse_origin_asns("15169_19527") == [15169, 19527]
    assert 151919527 not in parse_origin_asns("15169_19527")


def test_mixed_separators():
    # Both separators in one field, e.g. rv6 2607:f8f0:600::/40.
    assert parse_origin_asns("271_4476,11105,36391") == [271, 4476, 11105, 36391]


def test_private_asns_are_dropped():
    assert parse_origin_asns("64512") == []
    assert parse_origin_asns("65534") == []
    assert parse_origin_asns("4200000000") == []
    assert parse_origin_asns("4294967295") == []
    # The real rv6 line for 2404:c900::/48. Both 65001 and 65261 are private,
    # so only the public origin survives.
    assert parse_origin_asns("58682_65001,65261") == [58682]


def test_public_asns_adjacent_to_the_private_range_are_kept():
    assert parse_origin_asns("64511") == [64511]
    assert parse_origin_asns("65535") == [65535]
    assert parse_origin_asns("4199999999") == [4199999999]


def test_duplicates_are_collapsed_preserving_first_position():
    assert parse_origin_asns("3356,15169,3356") == [3356, 15169]


def test_malformed_tokens_are_skipped():
    assert parse_origin_asns("") == []
    assert parse_origin_asns("   ") == []
    assert parse_origin_asns("abc") == []
    assert parse_origin_asns("AS15169") == []
    assert parse_origin_asns(",,") == []
    # A good origin next to junk is still returned.
    assert parse_origin_asns("abc,15169") == [15169]


def test_non_ascii_digits_are_rejected():
    # str.isdigit() accepts these and int() parses them; the regex must not.
    assert parse_origin_asns("١٥١٦٩") == []


def test_surrounding_whitespace_and_newline_are_tolerated():
    assert parse_origin_asns(" 36040,38266\n") == [36040, 38266]

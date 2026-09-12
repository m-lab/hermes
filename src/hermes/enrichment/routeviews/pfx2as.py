"""Parsing for the origin-AS field of a CAIDA pfx2as line.

The third tab-separated column of a pfx2as record is not always a single ASN::

    12345           one origin
    36040,38266     MOAS -- several ASes originate the same prefix
    11164_11537     an AS_SET origin
    271_4476,11105  both separators in one field

Two defects in the previous inline parsing motivated this module (both verified
against production data on 2026-09-12):

* ``parts[2].split(",")[0]`` kept only the first origin.  CAIDA emits MOAS
  origins sorted ascending, so "first" systematically meant "numerically
  smallest ASN".  That is why ``2402:8100::/32`` -- ``36040,38266,45271``, a
  Vodafone Idea (VIL) prefix per APNIC -- was stored as Google's AS36040 and
  surfaced as ``associated_org = 'Google LLC'`` on every hop inside it.

* ``int()`` treats ``_`` as a digit separator, so an AS_SET that was never split
  parsed silently into a fabricated ASN: ``int("15169_19527") == 151919527``.
  The private-ASN guard does not catch those, because the concatenations land
  below the 4200000000 floor.  Production held 22,814 such rows (4,297 distinct
  invented ASNs) in ``unified_ip_to_as_ipv6`` and 61,094 rows (2,170 distinct)
  in ``unified_ip_to_as``.

Returning *every* origin, rather than choosing one here, is what the downstream
SQL already expects: ``04_mapping_union.sql`` ranks equally-specific prefix
matches by customer-cone size, so handing it the real candidate set lets that
tiebreak do its job instead of this parser silently pre-empting it.
"""

import re

# One origin token must be ASCII digits and nothing else.  str.isdigit() is not
# enough: it accepts Unicode digits such as "١٢", which int() then
# parses happily into a number that was never in the file.
_ASN_TOKEN = re.compile(r"[0-9]+\Z")

# CAIDA separates MOAS origins with "," and AS_SET members with "_".
_ORIGIN_SEPARATORS = re.compile(r"[,_]")


def parse_origin_asns(field: str) -> list[int]:
    """Return every usable origin ASN in a pfx2as origin field, in file order.

    Private-use ASNs are dropped (they can never attribute a hop to a real
    network), as are malformed tokens.  The result is de-duplicated but keeps
    the order CAIDA listed, and is empty when nothing usable remains -- callers
    should treat that as "skip this prefix".

    Args:
        field: The raw third column of a pfx2as line, e.g. ``"36040,38266"``.

    Returns:
        The distinct public origin ASNs, in the order they appeared.
    """
    origins: list[int] = []
    seen: set[int] = set()

    for token in _ORIGIN_SEPARATORS.split(field.strip()):
        if not _ASN_TOKEN.match(token):
            continue

        asn = int(token)

        # Private-use ranges (RFC 6996 / RFC 7300).  Unchanged from the previous
        # inline check, deliberately: this commit fixes how origins are split,
        # not which ASNs count as routable.
        if (64512 <= asn <= 65534) or (4200000000 <= asn):
            continue

        if asn not in seen:
            seen.add(asn)
            origins.append(asn)

    return origins

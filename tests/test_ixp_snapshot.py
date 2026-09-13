"""Tests for the in-process IXP snapshot builder.

These lock the semantics ported from the companion repo's merge scripts. A
silent difference here mis-attributes IXP hops rather than failing, so each test
names the upstream behaviour it pins.
"""

from __future__ import annotations

import ipaddress
import json

from hermes.enrichment.peeringdb_ixp.snapshot import (
    build_ixp_name_map,
    interfaces_from_records,
    merge_interfaces,
    peeringdb_records,
    usable_asn,
    write_snapshot,
)

# --- name mapping (merge-all-prefixes.py + map_ixp_names) ----------------------


def test_peeringdb_name_wins_and_pch_maps_onto_it():
    # Same prefix seen by both sources -> PeeringDB's name is canonical.
    name_map = build_ixp_name_map(
        [
            ("pdb", [("DE-CIX Frankfurt", ["80.81.192.0/21"], [])]),
            ("pch", [("DE-CIX FRA", ["80.81.192.0/21"], [])]),
        ]
    )
    assert name_map["DE-CIX_Frankfurt"] == "DE-CIX_Frankfurt"
    assert name_map["DE-CIX_FRA"] == "DE-CIX_Frankfurt"


def test_pch_only_prefix_keeps_its_own_name():
    name_map = build_ixp_name_map([("pdb", []), ("pch", [("Some IX", ["192.0.2.0/24"], [])])])
    assert name_map["Some_IX"] == "Some_IX"


def test_disjoint_prefixes_do_not_merge_names():
    name_map = build_ixp_name_map(
        [
            ("pdb", [("IX One", ["192.0.2.0/24"], [])]),
            ("pch", [("IX Two", ["198.51.100.0/24"], [])]),
        ]
    )
    assert name_map["IX_One"] == "IX_One"
    assert name_map["IX_Two"] == "IX_Two"


def test_unparseable_prefixes_are_skipped_not_fatal():
    name_map = build_ixp_name_map([("pdb", [("Broken", ["not-a-prefix"], ["192.0.2.0/24"])])])
    assert name_map == {"Broken": "Broken"}


# --- interface flattening (read_ixp_interfaces) --------------------------------


def test_private_and_invalid_addresses_are_dropped():
    got = interfaces_from_records(
        [("IX", "64500", ["80.81.192.1", "10.0.0.1", "not-an-ip"], ["2001:7f8::1", "2001:db8::1"])]
    )
    assert "80.81.192.1" in got
    assert "2001:7f8::1" in got
    assert "10.0.0.1" not in got, "private IPv4 must be dropped"
    assert "not-an-ip" not in got
    # 2001:db8::/32 is documentation space, which ipaddress reports as private --
    # as are 192.0.2.0/24 and 198.51.100.0/24, so do not use those as "public"
    # fixtures here.
    assert "2001:db8::1" not in got


def test_as_prefix_is_stripped_from_the_asn():
    assert interfaces_from_records([("IX", "AS15169", ["80.81.192.1"], [])])["80.81.192.1"] == (
        "15169",
        "IX",
    )


def test_within_one_source_the_last_record_wins():
    # Upstream assigns into a dict while streaming the file, so later lines win.
    got = interfaces_from_records(
        [("IX A", "1", ["80.81.192.1"], []), ("IX B", "2", ["80.81.192.1"], [])]
    )
    assert got["80.81.192.1"] == ("2", "IX B")


def test_addresses_are_normalised():
    got = interfaces_from_records([("IX", "1", [], ["2001:0DB9:0000::1"])])
    assert "2001:db9::1" in got


# --- merge precedence (merge_interfaces) ---------------------------------------


def test_pch_wins_over_peeringdb_for_a_shared_ip():
    # Upstream processes PCH first and skips IPs already claimed.
    merged = merge_interfaces(
        pdb_members={"192.0.2.1": ("111", "PDB IX")},
        pch_members={"192.0.2.1": ("222", "PCH IX")},
        name_map={},
    )
    assert merged["192.0.2.1"] == ("222", "PCH_IX")


def test_peeringdb_supplies_ips_pch_does_not_have():
    merged = merge_interfaces(
        pdb_members={"198.51.100.1": ("111", "PDB IX")},
        pch_members={"192.0.2.1": ("222", "PCH IX")},
        name_map={},
    )
    assert merged["198.51.100.1"] == ("111", "PDB_IX")


def test_names_are_underscored_then_mapped():
    merged = merge_interfaces(
        pdb_members={},
        pch_members={"192.0.2.1": ("1", "DE-CIX FRA")},
        name_map={"DE-CIX_FRA": "DE-CIX_Frankfurt"},
    )
    assert merged["192.0.2.1"] == ("1", "DE-CIX_Frankfurt")


# --- output (write_snapshot) ---------------------------------------------------


def test_v4_and_v6_split_into_separate_files_in_order(tmp_path):
    merged = {
        "198.51.100.5": ("3", "C"),
        "192.0.2.1": ("1", "A"),
        "2001:db9::5": ("4", "D"),
        "2001:db9::1": ("2", "B"),
    }
    v4_path, v6_path = write_snapshot(merged, str(tmp_path / "merged-members-gen-20260911.txt"))
    assert v6_path.endswith("_ipv6.txt")

    def rows(path):
        return [
            line.rstrip("\n").split("\t")
            for line in open(path)
            if line.strip() and not line.startswith("#")
        ]

    # IPv4 sorted numerically, IPv6 sorted as addresses -- upstream ordering.
    assert [r[0] for r in rows(v4_path)] == ["192.0.2.1", "198.51.100.5"]
    assert [r[0] for r in rows(v6_path)] == ["2001:db9::1", "2001:db9::5"]
    # Exactly the three columns process_data_file parses.
    assert all(len(r) == 3 for r in rows(v4_path) + rows(v6_path))


def test_ipv4_file_contains_no_ipv6_rows(tmp_path):
    # The upstream IPv4 write overwrites the combined file at the same path, so
    # merged-members-gen-<date>.txt must be IPv4-only.
    v4_path, _ = write_snapshot(
        {"192.0.2.1": ("1", "A"), "2001:db9::1": ("2", "B")},
        str(tmp_path / "merged-members-gen-20260911.txt"),
    )
    for line in open(v4_path):
        if line.startswith("#") or not line.strip():
            continue
        assert ipaddress.ip_address(line.split("\t")[0]).version == 4


# --- PeeringDB derivation ------------------------------------------------------


def _dump(tmp_path, **tables):
    path = tmp_path / "dump.json"
    path.write_text(json.dumps({k: {"data": v} for k, v in tables.items()}))
    return str(path)


def test_ixp_names_containing_commas_survive(tmp_path):
    """A comma in an IXP name must not corrupt the record.

    The companion pipeline round-trips through CSV and then splits on "|", so
    'CNIX (Qianhai, Shenzhen)' came out as name '"CNIX (Qianhai' with the prefix
    shifted into the IPv6 field. Two real IXPs are affected in the 2026-09 dump.
    """
    path = _dump(
        tmp_path,
        ix=[{"id": 1, "name": "CNIX (Qianhai, Shenzhen)"}],
        ixlan=[{"id": 10, "ix_id": 1}],
        ixpfx=[{"ixlan_id": 10, "prefix": "103.168.98.0/23", "protocol": "IPv4"}],
        netixlan=[],
    )
    prefixes, _ = peeringdb_records(path)
    assert prefixes == [("CNIX (Qianhai, Shenzhen)", ["103.168.98.0/23"], [])]


def test_only_the_first_prefix_per_family_is_kept(tmp_path):
    # Matches upstream groupby(name).agg('first'); lossy, but load-bearing.
    path = _dump(
        tmp_path,
        ix=[{"id": 1, "name": "IX"}],
        ixlan=[{"id": 10, "ix_id": 1}],
        ixpfx=[
            {"ixlan_id": 10, "prefix": "192.0.2.0/24", "protocol": "IPv4"},
            {"ixlan_id": 10, "prefix": "198.51.100.0/24", "protocol": "IPv4"},
            {"ixlan_id": 10, "prefix": "2001:db9::/64", "protocol": "IPv6"},
        ],
        netixlan=[],
    )
    prefixes, _ = peeringdb_records(path)
    assert prefixes == [("IX", ["192.0.2.0/24"], ["2001:db9::/64"])]


def test_members_come_straight_from_netixlan(tmp_path):
    path = _dump(
        tmp_path,
        ix=[],
        ixlan=[],
        ixpfx=[],
        netixlan=[
            {"name": "IX", "asn": 15169, "ipaddr4": "192.0.2.1", "ipaddr6": "2001:db9::1"},
            {"name": "IX", "asn": 3356, "ipaddr4": None, "ipaddr6": None},
        ],
    )
    _, members = peeringdb_records(path)
    assert members == [("IX", "15169", ["192.0.2.1"], ["2001:db9::1"]), ("IX", "3356", [], [])]


def test_missing_tables_do_not_crash(tmp_path):
    path = tmp_path / "empty.json"
    path.write_text(json.dumps({}))
    assert peeringdb_records(str(path)) == ([], [])


def test_comma_in_an_ixp_name_does_not_break_the_name_map():
    """A comma inside an IXP name must not stop the sources being tied together.

    Upstream serialises the per-prefix names into one comma-joined field and then
    re-splits on "," -- so a name containing a comma produces extra fields and
    the ``pdb_`` entry moves off index 2 and is never found:

        n/a,pch_DE-CIX_ASEAN_(Singapore,_Malaysia,_Brunei),pdb_DE-CIX_ASEAN
        -> ['n/a', 'pch_DE-CIX_ASEAN_(Singapore', '_Malaysia', '_Brunei)',
            'pdb_DE-CIX_ASEAN']

    The map then never learns that PCH's name and PeeringDB's are the same IXP,
    and 456 rows keep the un-normalised PCH variant. Real case, verified against
    the 2026-09 dump.
    """
    name_map = build_ixp_name_map(
        [
            ("pdb", [("DE-CIX ASEAN", ["103.162.254.0/24"], ["2001:df6:480::/64"])]),
            (
                "pch",
                [
                    (
                        "DE-CIX ASEAN (Singapore, Malaysia, Brunei)",
                        ["103.162.254.0/24"],
                        ["2001:DF6:480::/64"],
                    )
                ],
            ),
        ]
    )
    assert name_map["DE-CIX_ASEAN"] == "DE-CIX_ASEAN"
    assert name_map["DE-CIX_ASEAN_(Singapore,_Malaysia,_Brunei)"] == "DE-CIX_ASEAN"


# --- unusable ASNs (PCH blanks) ------------------------------------------------


def test_usable_asn_accepts_plain_and_as_prefixed():
    assert usable_asn("15169") == "15169"
    assert usable_asn("AS15169") == "15169"
    assert usable_asn(15169) == "15169"
    assert usable_asn(" 15169 ") == "15169"


def test_usable_asn_rejects_blanks_and_junk():
    for raw in ("", " ", None, "AS", "n/a", "abc", "12.5", "1_2", "0", "-1"):
        assert usable_asn(raw) is None, raw


def test_blank_asn_records_are_skipped_entirely():
    got = interfaces_from_records([("IX", "", ["80.81.192.1"], [])])
    assert got == {}, "a record with no ASN is unusable, not a blank-ASN row"


def test_peeringdb_fills_in_where_pch_has_no_asn():
    """The 22.7% data loss this fixes.

    PCH returns an empty ASN for many interfaces. Upstream kept those rows, and
    since PCH wins the merge they overrode PeeringDB -- then died at
    ``process_data_file``, which does int(asn), logs "Invalid row" and skips.
    Measured on 2026-09 data: 37,863 of 166,484 merged rows (22.7%) discarded,
    PeeringDB holding a valid ASN for 5,208 of them. Dropping the unusable record
    lets the merge fall through, recovering +5,341 ingestable interfaces.
    """
    pdb = interfaces_from_records([("PDB IX", "8728", ["37.9.49.10"], [])])
    pch = interfaces_from_records([("PCH IX", "", ["37.9.49.10"], [])])
    assert pch == {}
    merged = merge_interfaces(pdb, pch, name_map={})
    assert merged["37.9.49.10"] == ("8728", "PDB_IX")


def test_pch_still_wins_when_it_has_a_real_asn():
    # The fix must not change precedence where PCH actually knows the ASN.
    pdb = interfaces_from_records([("PDB IX", "111", ["80.81.192.1"], [])])
    pch = interfaces_from_records([("PCH IX", "222", ["80.81.192.1"], [])])
    assert merge_interfaces(pdb, pch, name_map={})["80.81.192.1"] == ("222", "PCH_IX")


# --- EuroIX / IX-F exports -----------------------------------------------------

from hermes.enrichment.peeringdb_ixp.snapshot import _ixf_extract  # noqa: E402

MEGAPORT_STYLE = {
    # One document serving two IXPs, as lg.megaport.com does for 35 of them.
    "ixp_list": [
        {
            "ixp_id": 14,
            "ixf_id": 608,
            "shortname": "IX-ASH",
            "vlan": [{"ipv4": {"prefix": "206.53.172.0", "mask_length": 24}}],
        },
        {
            "ixp_id": 1,
            "ixf_id": 570,
            "shortname": "IX-SYD",
            "vlan": [{"ipv6": {"prefix": "2001:dead::", "mask_length": 64}}],
        },
    ],
    "member_list": [
        {
            "asnum": 15169,
            "connection_list": [
                {"ixp_id": 14, "vlan_list": [{"ipv4": {"address": "206.53.172.1"}}]},
                {"ixp_id": 1, "vlan_list": [{"ipv6": {"address": "2001:dead::1"}}]},
            ],
        },
        {
            "asnum": 3356,
            "connection_list": [
                {"ixp_id": 1, "vlan_list": [{"ipv6": {"address": "2001:dead::2"}}]}
            ],
        },
    ],
}


def test_multi_ixp_export_is_scoped_to_the_requested_ixp():
    """The trap that made one IXP look 27x bigger than it is.

    lg.megaport.com publishes 35 IXPs in one document. Counting all of it against
    a single IXP read Megaport Ashburn as 3,408 interfaces instead of 124, and
    made two unrelated IXPs report identical totals.
    """
    pfx, members = _ixf_extract(MEGAPORT_STYLE, 608, "IX-ASH")
    assert pfx == ("IX-ASH", ["206.53.172.0/24"], [])
    assert members == [("IX-ASH", "15169", ["206.53.172.1"], [])]

    pfx, members = _ixf_extract(MEGAPORT_STYLE, 570, "IX-SYD")
    assert pfx == ("IX-SYD", [], ["2001:dead::/64"])
    assert {m[1] for m in members} == {"15169", "3356"}


def test_unmatched_ixf_id_returns_nothing():
    assert _ixf_extract(MEGAPORT_STYLE, 99999, "Nope") == (None, [])


def test_export_with_members_but_no_addresses_yields_no_interfaces():
    """IX.br's shape: members present, connection_list carries only ixp_id.

    Verified live -- 106 members, 106 connections, zero IP addresses anywhere in
    the document. Their data, not a parse failure, so it must come back empty
    rather than raising.
    """
    doc = {
        "ixp_list": [{"ixp_id": 18, "ixf_id": 160, "shortname": "IX.br Salvador"}],
        "member_list": [{"asnum": 263009, "connection_list": [{"ixp_id": 18}]}],
    }
    pfx, members = _ixf_extract(doc, 160, "IX.br Salvador")
    assert pfx == ("IX.br Salvador", [], [])
    assert members == []


def test_prefix_requires_both_prefix_and_mask():
    doc = {
        "ixp_list": [
            {
                "ixp_id": 1,
                "ixf_id": 7,
                "vlan": [
                    {"ipv4": {"prefix": "185.1.210.0"}},  # no mask_length
                    {"ipv4": {"prefix": "185.1.211.0", "mask_length": 23}},
                ],
            }
        ],
        "member_list": [],
    }
    pfx, _ = _ixf_extract(doc, 7, "IX")
    assert pfx == ("IX", ["185.1.211.0/23"], [])


def test_euroix_is_additive_and_cannot_override(monkeypatch):
    # EuroIX runs last, so an IP another source already claimed keeps its owner.
    pdb = interfaces_from_records([("PDB IX", "111", ["80.81.192.1"], [])])
    eu = interfaces_from_records([("EU IX", "999", ["80.81.192.1"], ["2001:7f8::9"])])
    merged = merge_interfaces(pdb, {}, name_map={}, euroix_members=eu)
    assert merged["80.81.192.1"] == ("111", "PDB_IX"), "EuroIX must not override"
    assert merged["2001:7f8::9"] == ("999", "EU_IX"), "but must fill gaps"

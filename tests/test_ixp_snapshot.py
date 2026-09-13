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

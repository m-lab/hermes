"""Reverse (revtr) hop RTTs: 0 means the server's ping to the hop got no reply.

Verified 2026-10-28 against measurement-lab.revtr_raw.ping1: a hop's rtt is the RTT of a
separate ping from the M-Lab server to that hop, and 20-42% of hops per type carry 0
with no ping behind it. Read as 0 ms, those hops passed as 'Within baseline', failed the
reverse distance check, and fed 0 into the fishy type-4 rule and step 05 hop RTTs.
"""

from hermes.sql import loader


def test_reverse_rtt_zero_is_read_as_missing():
    sql = loader.load_query("04_mapping_union.sql", {"DAY": "2026-10-14"})
    assert "NULLIF(reverse_node.rtt, 0) AS rtts" in sql
    assert "reverse_node.rtt AS rtts" not in sql

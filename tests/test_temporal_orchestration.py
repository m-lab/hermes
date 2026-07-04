import pandas as pd

from hermes.pipeline import temporal_verdict as tv


def _prev_df():
    # group G1: forward path diverts b->x (reroute); group G2: stable path (congestion)
    rows = [
        # G1 forward: usual a-b,b-c ; day-of a-b,b-x
        dict(
            src_dst_pair="G1",
            ip_version="v4",
            direction="forward",
            edge="a-b",
            prev_u=1.0,
            prev_d=1.0,
            base_hop_rtt=10,
            day_hop_rtt=10,
            healthy_n=20,
            dayof_n=20,
        ),
        dict(
            src_dst_pair="G1",
            ip_version="v4",
            direction="forward",
            edge="b-c",
            prev_u=1.0,
            prev_d=0.0,
            base_hop_rtt=12,
            day_hop_rtt=12,
            healthy_n=20,
            dayof_n=20,
        ),
        dict(
            src_dst_pair="G1",
            ip_version="v4",
            direction="forward",
            edge="b-x",
            prev_u=0.0,
            prev_d=1.0,
            base_hop_rtt=None,
            day_hop_rtt=90,
            healthy_n=20,
            dayof_n=20,
        ),
        # G2 forward: same path, dst hop RTT jumps on m-n (congestion)
        dict(
            src_dst_pair="G2",
            ip_version="v4",
            direction="forward",
            edge="k-m",
            prev_u=1.0,
            prev_d=1.0,
            base_hop_rtt=10,
            day_hop_rtt=11,
            healthy_n=20,
            dayof_n=20,
        ),
        dict(
            src_dst_pair="G2",
            ip_version="v4",
            direction="forward",
            edge="m-n",
            prev_u=1.0,
            prev_d=1.0,
            base_hop_rtt=15,
            day_hop_rtt=85,
            healthy_n=20,
            dayof_n=20,
        ),
    ]
    return pd.DataFrame(rows)


def test_compute_verdicts(monkeypatch):
    monkeypatch.setattr(tv, "_read_prevalences", lambda c, d: _prev_df())
    # culprit set: pretend G1's diverted edge b-x is a known culprit
    monkeypatch.setattr(tv, "_culprit_edges", lambda c, d: {"G1": {"b-x"}})
    out = {r["src_dst_pair"]: r for r in tv.compute_temporal_verdicts("client", "2026-06-19")}
    assert out["G1"]["verdict"] == "reroute"
    assert out["G1"]["changed_segment"] == "b-x"
    assert out["G1"]["agrees_with_culprit"] is True
    assert out["G2"]["verdict"] == "congestion_in_place"
    assert out["G2"]["congested_segment"] == "m-n"  # biggest day-vs-base hop RTT jump
    assert out["G2"]["agrees_with_culprit"] is False

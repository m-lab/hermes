"""Temporal verdict support is per direction and counts only observed routes (fix #10).

- 05 counted healthy_n / dayof_n over every measurement, including server-only and
  all-'*' paths that observe no edge; such paths diluted prev_u to 0 and supplied
  the support for a "reroute".
- temporal_verdict took healthy_n / dayof_n as max() across directions, so a
  direction with 1 healthy measurement decided on the other direction's 20.
"""

from __future__ import annotations

import os

import pandas as pd
import pytest

from hermes.pipeline import temporal_verdict as tv
from hermes.pipeline.temporal_verdict import classify_directions
from hermes.sql import loader

PREV = "05_temporal_edge_prevalences_union.sql"


# ------------------------------------------------------------------ classify


def test_direction_without_own_support_never_decides():
    div = {"forward": 0.1, "reverse": 2.0}
    support = {"forward": (20, 20), "reverse": (1, 20)}
    assert classify_directions(div, support, tau=1.0) == ("congestion_in_place", "forward")


def test_single_qualifying_direction_is_used_alone():
    div = {"forward": 0.0, "reverse": 1.5}
    support = {"forward": (0, 0), "reverse": (8, 6)}
    assert classify_directions(div, support, tau=1.0) == ("reroute", "reverse")


def test_no_qualifying_direction_is_indeterminate():
    div = {"forward": 3.0, "reverse": 3.0}
    support = {"forward": (20, 4), "reverse": (4, 20)}
    assert classify_directions(div, support, tau=1.0) == ("indeterminate", None)


def test_both_directions_qualify_keeps_old_rule():
    support = {"forward": (9, 9), "reverse": (9, 9)}
    assert classify_directions({"forward": 1.2, "reverse": 1.2}, support, 1.0) == (
        "reroute",
        "forward",
    )
    assert classify_directions({"forward": 0.2, "reverse": 0.4}, support, 1.0) == (
        "congestion_in_place",
        "reverse",
    )


# ------------------------------------------------------- compute_temporal_verdicts


def _row(direction, edge, prev_u, prev_d, healthy_n, dayof_n):
    return dict(
        src_dst_pair="G - lbl - site - v4",
        ip_version="v4",
        direction=direction,
        edge=edge,
        prev_u=prev_u,
        prev_d=prev_d,
        base_hop_rtt=10.0,
        day_hop_rtt=12.0,
        healthy_n=healthy_n,
        dayof_n=dayof_n,
    )


def _verdict(monkeypatch, rows):
    monkeypatch.setattr(tv, "_read_prevalences", lambda c, d, **kw: pd.DataFrame(rows))
    monkeypatch.setattr(tv, "_culprit_edges", lambda c, d, **kw: {})
    (row,) = tv.compute_temporal_verdicts("client", "2026-10-14")
    return row


def test_borrowed_support_cannot_make_a_reroute(monkeypatch):
    rows = [
        # forward: 20 healthy / 20 day-of, same route
        _row("forward", "a-b", 1.0, 1.0, 20, 20),
        _row("forward", "b-c", 1.0, 1.0, 20, 20),
        # reverse: one healthy measurement, day-of route entirely different
        _row("reverse", "c-b", 1.0, 0.0, 1, 20),
        _row("reverse", "c-x", 0.0, 1.0, 1, 20),
        _row("reverse", "x-a", 0.0, 1.0, 1, 20),
    ]
    row = _verdict(monkeypatch, rows)
    assert row["verdict"] == "congestion_in_place" and row["change_dir"] == "forward"
    assert row["div_reverse"] == 2.0  # still reported, just not decisive


def test_reverse_alone_without_forward_rows(monkeypatch):
    rows = [_row("reverse", "c-x", 0.0, 1.0, 10, 10), _row("reverse", "c-b", 1.0, 0.0, 10, 10)]
    row = _verdict(monkeypatch, rows)
    assert row["verdict"] == "reroute" and row["change_dir"] == "reverse"
    assert row["changed_segment"] == "c-x"


# ------------------------------------------------- 05 on literal hops (BigQuery)


def _prevalence_query(hops: list[tuple]) -> str:
    """05's real ordered -> edges -> totals -> agg -> SELECT on literal hop rows."""
    sql = loader.load_query(PREV, {"DAY": "2026-10-14"})
    body = sql[sql.index("\nordered AS (") + 1 :]
    lit = " UNION ALL ".join(
        f"SELECT '{i}' AS id, 'G' AS src_dst_pair, 'v4' AS ip_version, {day} AS is_day, "
        f"{anom} AS is_anomaly, 'forward' AS direction, {ttl} AS ttl, '{node}' AS node, "
        f"{rtt} AS rtts"
        for i, day, anom, ttl, node, rtt in hops
    )
    return f"WITH hops AS ({lit}),\n{body}"


def _server_only_vs_responding() -> list[tuple]:
    hops = []
    for k in range(10):  # healthy, server row only: no edge observed
        hops.append((f"h{k}", "FALSE", "FALSE", 1, "100-Server-X-US", 0.0))
    for k in range(10):  # anomalous day-of, a responding 3-hop route
        for ttl, node in enumerate(["100-Server-X-US", "200-R-Y-US", "300-C-Z-US"], start=1):
            hops.append((f"a{k}", "TRUE", "TRUE", ttl, node, 10.0 * ttl))
    return hops


@pytest.mark.bq
def test_server_only_healthy_paths_are_not_support_for_a_reroute(monkeypatch):
    if not (os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") or os.environ.get("HERMES_BQ_TEST")):
        pytest.skip("BigQuery creds not configured (on-demand test)")
    from google.cloud import bigquery

    q = _prevalence_query(_server_only_vs_responding())
    df = (
        bigquery.Client(project="mlab-collaboration")
        .query(q)
        .to_dataframe(create_bqstorage_client=False)
    )
    assert set(df["healthy_n"]) == {0} and set(df["dayof_n"]) == {10}
    row = _verdict(monkeypatch, df.to_dict("records"))
    assert row["verdict"] == "indeterminate", row


def test_emitted_rows_are_still_every_direction_with_a_day_of_anomaly():
    """Counting support over edges must not drop a pair's verdict row: the emitted
    group/directions stay those with a day-of anomalous measurement (routed or not)."""
    sql = loader.load_query(PREV, {"DAY": "2026-10-14"})
    assert "FROM ordered WHERE is_anomaly AND is_day" in sql
    assert "JOIN anomalous_dirs USING (src_dst_pair, ip_version, direction)" in sql
    assert "WHERE t.dayof_n > 0" not in sql


def _unrouted_day_of() -> list[tuple]:
    hops = []
    for k in range(10):  # healthy, routed
        for ttl, node in enumerate(["100-S-X-US", "200-R-Y-US"], start=1):
            hops.append((f"h{k}", "FALSE", "FALSE", ttl, node, 1.0 * ttl))
    for k in range(10):  # anomalous day-of, server row only
        hops.append((f"a{k}", "TRUE", "TRUE", 1, "100-S-X-US", 0.0))
    return hops


@pytest.mark.bq
def test_pair_with_unrouted_day_of_paths_is_indeterminate_not_dropped(monkeypatch):
    if not (os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") or os.environ.get("HERMES_BQ_TEST")):
        pytest.skip("BigQuery creds not configured (on-demand test)")
    from google.cloud import bigquery

    q = _prevalence_query(_unrouted_day_of())
    df = (
        bigquery.Client(project="mlab-collaboration")
        .query(q)
        .to_dataframe(create_bqstorage_client=False)
    )
    assert len(df) == 1 and int(df["healthy_n"].iloc[0]) == 10 and int(df["dayof_n"].iloc[0]) == 0
    row = _verdict(monkeypatch, df.to_dict("records"))
    assert row["verdict"] == "indeterminate" and row["change_dir"] is None

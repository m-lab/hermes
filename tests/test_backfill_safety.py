"""Backfill-safety guards in the union orchestrator (all BigQuery calls mocked).

Covers: Phase C failures gating Phase D/E and the exit code; Phase 4's own
resume check; --delete-first failing loudly (incl. streaming buffers); and the
giga_meter_measurements ordering rule (ascending, never before a later partition).
"""

from __future__ import annotations

import sys
from datetime import date, timedelta
from types import SimpleNamespace

import pandas as pd
import pytest
from google.api_core.exceptions import NotFound

from hermes.pipeline import correlation_tomography as ct
from hermes.pipeline import union

D = date(2025, 6, 10)


def _d(n: int) -> date:
    return D + timedelta(days=n)


# ── giga ordering rule (pure) ────────────────────────────────────────────


def test_giga_conflict_when_later_partition_exists_and_04_will_run():
    # Descending backfill: D+1..D+3 already processed, D is new.
    conflicts = union.giga_order_conflicts(
        [D], giga_present={_d(1), _d(3)}, mapping_present={_d(1), _d(3)}
    )
    assert conflicts == {D: [_d(1), _d(3)]}


def test_giga_no_conflict_beyond_lookback_or_for_earlier_partitions():
    conflicts = union.giga_order_conflicts([D], giga_present={_d(-3), _d(8)}, mapping_present=set())
    assert conflicts == {}


def test_giga_no_conflict_when_04_is_skipped_by_resume_guard():
    # D already mapped -> 04 is skipped -> no giga write (nightly resume case).
    conflicts = union.giga_order_conflicts([D], giga_present={_d(1)}, mapping_present={D, _d(1)})
    assert conflicts == {}


def test_giga_metro_delete_of_whole_window_is_safe():
    batch = [D, _d(1), _d(2)]
    conflicts = union.giga_order_conflicts(
        batch,
        giga_present=set(batch),
        mapping_present=set(batch),
        cleared=set(batch),
        giga_cleared=True,
    )
    assert conflicts == {}


def test_giga_delete_that_keeps_giga_still_conflicts():
    # City-mode --delete-first clears the mapping but not giga: D+1's copy stays.
    batch = [D, _d(1)]
    conflicts = union.giga_order_conflicts(
        batch,
        giga_present=set(batch),
        mapping_present=set(batch),
        cleared=set(batch),
        giga_cleared=False,
    )
    assert conflicts == {D: [_d(1)]}


def test_rerun_of_older_date_conflicts_with_newer_neighbours_outside_batch():
    conflicts = union.giga_order_conflicts(
        [D],
        giga_present={D, _d(2)},
        mapping_present={D, _d(2)},
        cleared={D},
        giga_cleared=True,
    )
    assert conflicts == {D: [_d(2)]}


def _stub_populated(monkeypatch, giga, mapping):
    def fake(project_id, table):
        return set(giga) if table.endswith("giga_meter_measurements") else set(mapping)

    monkeypatch.setattr(union, "get_populated_dates", fake)


def test_check_giga_order_raises_by_default_and_warns_with_flag(monkeypatch, caplog):
    _stub_populated(monkeypatch, giga={_d(1)}, mapping={_d(1)})
    with pytest.raises(union.GigaOrderError, match="allow-giga-out-of-order"):
        union.check_giga_order("p", [D])
    with caplog.at_level("ERROR"):
        assert union.check_giga_order("p", [D], allow=True) == {D: [_d(1)]}
    assert any("PROCEEDING" in m for m in caplog.messages)


# ── main(): guard before delete, exit codes ──────────────────────────────


def _main(monkeypatch, argv):
    monkeypatch.setattr(sys, "argv", ["hermes-pipeline", *argv])
    monkeypatch.setattr(union, "print_active_credentials", lambda: None)


def test_refused_giga_run_deletes_nothing_and_exits_2(monkeypatch):
    _main(monkeypatch, ["--rerun-dates", D.isoformat(), "--delete-first"])
    _stub_populated(monkeypatch, giga={D, _d(2)}, mapping={D, _d(2)})
    deleted, ran = [], []
    monkeypatch.setattr(union, "delete_dates", lambda *a, **k: deleted.append(a))
    monkeypatch.setattr(union, "run_dates", lambda *a, **k: ran.append(a) or [])
    with pytest.raises(SystemExit) as exc:
        union.main()
    assert exc.value.code == 2
    assert deleted == [] and ran == []


def test_main_exits_nonzero_when_any_date_incomplete(monkeypatch):
    _main(monkeypatch, ["--rerun-dates", D.isoformat()])
    _stub_populated(monkeypatch, giga=set(), mapping=set())
    monkeypatch.setattr(union, "run_dates", lambda *a, **k: [D])
    with pytest.raises(SystemExit) as exc:
        union.main()
    assert exc.value.code == 1


def test_rerun_dates_are_processed_ascending(monkeypatch):
    _main(monkeypatch, ["--rerun-dates", _d(2).isoformat(), D.isoformat()])
    _stub_populated(monkeypatch, giga=set(), mapping=set())
    seen = {}
    monkeypatch.setattr(union, "run_dates", lambda dates, *a, **k: seen.update(d=dates) or [])
    union.main()
    assert seen["d"] == [D, _d(2)]


def test_dry_run_never_deletes(monkeypatch):
    _main(monkeypatch, ["--rerun-dates", D.isoformat(), "--delete-first", "--dry-run"])
    _stub_populated(monkeypatch, giga=set(), mapping=set())
    monkeypatch.setattr(
        union, "delete_dates", lambda *a, **k: pytest.fail("dry run must not delete")
    )
    monkeypatch.setattr(union, "run_dates", lambda *a, **k: [])
    union.main()


# ── delete_dates: loud failure + streaming buffer ────────────────────────


class FakeDeleteClient:
    def __init__(self, buffers=None, fail_on=(), missing=()):
        self.buffers = buffers or {}  # table -> list of successive buffer states
        self.fail_on = fail_on
        self.missing = missing
        self.queries: list[str] = []

    def get_table(self, table):
        if any(m in table for m in self.missing):
            raise NotFound(table)
        seq = self.buffers.get(table.rsplit(".", 1)[1], [None])
        state = seq.pop(0) if len(seq) > 1 else seq[0]
        return SimpleNamespace(streaming_buffer=state)

    def query(self, q):
        self.queries.append(q)

        def result():
            if any(f in q for f in self.fail_on):
                raise RuntimeError("would affect rows in the streaming buffer")

        return SimpleNamespace(result=result)


def _patch_client(monkeypatch, client):
    monkeypatch.setattr(union.bigquery, "Client", lambda **kw: client)
    monkeypatch.setattr(union.time, "sleep", lambda s: None)


def test_delete_failure_raises_after_attempting_every_table(monkeypatch):
    client = FakeDeleteClient(fail_on=("temporal_path_verdicts",))
    _patch_client(monkeypatch, client)
    with pytest.raises(union.DeleteFailedError, match="temporal_path_verdicts"):
        union.delete_dates("p", [D])
    assert len(client.queries) == len(union.delete_tables())


def test_streaming_buffer_is_waited_out_then_deleted(monkeypatch):
    buf = SimpleNamespace(estimated_rows=10, oldest_entry_time="t")
    client = FakeDeleteClient(buffers={"correlation_hyperedges_tomography_v2": [buf, buf, None]})
    _patch_client(monkeypatch, client)
    union.delete_dates("p", [D], streaming_buffer_timeout_s=3600, streaming_buffer_poll_s=1)
    assert any("correlation_hyperedges_tomography_v2" in q for q in client.queries)


def test_streaming_buffer_timeout_raises_before_any_delete(monkeypatch):
    buf = SimpleNamespace(estimated_rows=10, oldest_entry_time="t")
    client = FakeDeleteClient(buffers={"temporal_path_verdicts": [buf]})
    _patch_client(monkeypatch, client)
    with pytest.raises(union.DeleteFailedError, match="streaming buffer"):
        union.delete_dates("p", [D], streaming_buffer_timeout_s=0)
    assert client.queries == []


def test_missing_table_is_skipped_not_fatal(monkeypatch):
    client = FakeDeleteClient(missing=("correlation_entity_stats_multigranularity",))
    _patch_client(monkeypatch, client)
    union.delete_dates("p", [D])
    assert not any("entity_stats" in q for q in client.queries)
    assert len(client.queries) == len(union.delete_tables()) - 1


# ── run_dates gating ─────────────────────────────────────────────────────


@pytest.fixture
def pipeline(monkeypatch):
    """Mock every BigQuery-touching call of run_dates; record what ran."""
    calls = {"04": [], "05": [], "D": [], "E": []}
    state = {"fail_04": set(), "failed_fill": []}

    monkeypatch.setattr(union, "ensure_baseline", lambda *a, **k: list(state["failed_fill"]))
    monkeypatch.setattr(union, "run_enrichment", lambda *a, **k: None)
    monkeypatch.setattr(union, "find_stale_merged_partitions", lambda *a, **k: [])
    monkeypatch.setattr(union, "find_unattributed_partitions", lambda *a, **k: [])

    def fake_parallel(dates, project_id, sql_files, *a, **k):
        if sql_files is union.SQL_FILES_PUBLIC:
            calls["E"].extend(dates)
        elif any(f.startswith("05_") for f in sql_files):
            calls["05"].extend(dates)
        return [f"Success: {d:%Y-%m-%d}" for d in dates]

    def fake_step04(args):
        day = args[0]
        calls["04"].append(day)
        if day in state["fail_04"]:
            return f"Error: {day:%Y-%m-%d} - boom"
        return f"Success: {day:%Y-%m-%d}"

    def fake_tomo(args):
        calls["D"].append(args[0])
        return f"Success: {args[0]:%Y-%m-%d}"

    def fake_tomo_many(worker_args, n):
        return [fake_tomo(wa) for wa in worker_args]

    monkeypatch.setattr(union, "_run_parallel_sql", fake_parallel)
    monkeypatch.setattr(union, "_run_sql_steps_worker", fake_step04)
    monkeypatch.setattr(union, "_run_tomography_worker", fake_tomo)
    monkeypatch.setattr(union, "_run_phase_d_tomography", fake_tomo_many)
    return calls, state


def _run(dates, **kw):
    return union.run_dates(dates, "p", max_workers=1, skip_data_check=True, dry_run=False, **kw)


def test_all_success_returns_no_incomplete_and_04_runs_ascending(pipeline):
    calls, _ = pipeline
    assert _run([_d(2), D, _d(1)]) == []
    assert calls["04"] == [D, _d(1), _d(2)]
    assert calls["E"] == [D, _d(1), _d(2)]


def test_phase_c_failure_gates_d_and_e_and_withholds_next_week(pipeline):
    calls, state = pipeline
    state["fail_04"] = {D}
    incomplete = _run([D, _d(1), _d(9)])
    # D failed 04; D+1 is within 7 days so its 04 is withheld; D+9 is independent.
    assert calls["04"] == [D, _d(9)]
    assert calls["D"] == [_d(9)]
    assert calls["E"] == [_d(9)]
    assert incomplete == [D, _d(1)]


def test_allow_flag_lets_later_04_run_after_failure(pipeline):
    calls, state = pipeline
    state["fail_04"] = {D}
    incomplete = _run([D, _d(1)], allow_giga_out_of_order=True)
    assert calls["04"] == [D, _d(1)]
    assert calls["D"] == [_d(1)]
    assert incomplete == [D]


def test_failed_baseline_day_withholds_dependent_target(pipeline):
    calls, state = pipeline
    state["failed_fill"] = [D - timedelta(days=3)]
    incomplete = _run([D, _d(10)])
    assert incomplete == [D]
    assert calls["E"] == [_d(10)]


def test_dates_with_failed_baseline_ignores_in_batch_and_out_of_window():
    blocked = union.dates_with_failed_baseline(
        [D, _d(1)], failed_fill=[D, D - timedelta(days=8), D - timedelta(days=1)]
    )
    # D itself is in the batch (A1 retries it); D-8 is outside D's window.
    assert blocked == {D: [D - timedelta(days=1)], _d(1): [D - timedelta(days=1)]}


# ── correlation tomography: Phase 4 resume decision ──────────────────────


@pytest.fixture
def tomo(monkeypatch):
    calls: list[str] = []
    present: set[str] = set()

    monkeypatch.setattr(
        ct, "step_already_done", lambda client, table, day: table.rsplit(".", 1)[1] in present
    )
    edges = pd.DataFrame({"path_type": ["anomalous"], "src_dst_pair": ["a"]})
    monkeypatch.setattr(ct, "download_edges", lambda *a, **k: calls.append("P1") or edges)
    monkeypatch.setattr(ct, "_load_dataset_query", lambda *a, **k: "SQL")
    monkeypatch.setattr(ct, "_read_df", lambda job: pd.DataFrame())

    class Client:
        def query(self, sql):
            return None

    monkeypatch.setattr(ct.bigquery, "Client", lambda **kw: Client())
    monkeypatch.setattr(ct, "run_greedy_set_cover", lambda *a, **k: calls.append("P2") or ([], 1))
    monkeypatch.setattr(ct, "attribute_unexplained", lambda *a, **k: [])
    monkeypatch.setattr(ct, "compute_hyperedges", lambda *a, **k: calls.append("P3"))
    monkeypatch.setattr(
        ct, "run_mixed_granularity_cover", lambda *a, **k: calls.append("P4") or ([], [])
    )
    monkeypatch.setattr(ct, "upload_entity_stats", lambda *a, **k: calls.append("P4-es"))
    monkeypatch.setattr(ct, "upload_multigranularity", lambda *a, **k: calls.append("P4-mg"))
    return calls, present


def _tomo(**kw):
    ct.run_correlation_tomography(D, project_id="p", write_multigranularity=True, **kw)


def test_phase4_reruns_when_hyperedges_exist_but_multigran_missing(tomo):
    calls, present = tomo
    present.add("correlation_hyperedges_tomography_v2")
    _tomo()
    assert "P2" not in calls and "P3" not in calls
    assert calls[-3:] == ["P4", "P4-es", "P4-mg"]


def test_full_skip_only_when_both_outputs_present(tomo):
    calls, present = tomo
    present.update(
        {"correlation_hyperedges_tomography_v2", "correlation_culprits_multigranularity"}
    )
    _tomo()
    assert calls == []


def test_hyperedges_only_rerun_when_multigran_present(tomo):
    calls, present = tomo
    present.add("correlation_culprits_multigranularity")
    _tomo()
    assert "P2" in calls and "P3" in calls and "P4" not in calls


def test_hyperedge_check_alone_when_multigran_not_requested(tomo):
    calls, present = tomo
    present.add("correlation_hyperedges_tomography_v2")
    ct.run_correlation_tomography(D, project_id="p", write_multigranularity=False)
    assert calls == []

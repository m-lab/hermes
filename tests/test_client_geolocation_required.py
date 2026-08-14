"""Client geolocation must fail loudly, never silently produce nothing.

Steps 02/03 take client geography *only* from `unified_src_ip_to_geoloc`. If the
IPInfo reader is unavailable, the table is written with NULL geography, every
measurement groups under NULL, and the run completes reporting no anomalies. An
empty answer that looks like a successful one is the worst available outcome, so
both the enrichment entrypoint and Phase A0 must raise instead.
"""

from __future__ import annotations

import inspect
from unittest import mock

import pytest

from hermes.enrichment.main import HermesEnrichment
from hermes.pipeline import union


def test_client_geolocation_refuses_to_run_without_ipinfo_reader():
    enrichment = HermesEnrichment.__new__(HermesEnrichment)
    enrichment.ipv6 = False
    enrichment.ipinfo = mock.Mock(reader=None, ipinfo_db_path="/app/cache/absent.snapshot")

    with pytest.raises(RuntimeError, match="IPInfo reader unavailable"):
        enrichment.process_geolocation("2026-08-07", lookback_days=0, source="clients")


def test_topology_geolocation_still_degrades_rather_than_raising():
    """The guard must be scoped to clients: hops keep other enrichers' geolocation."""
    enrichment = HermesEnrichment.__new__(HermesEnrichment)
    enrichment.ipv6 = False
    enrichment.ipinfo = mock.Mock(reader=None, ipinfo_db_path="/app/cache/absent.snapshot")

    # Fails later (no BigQuery client on this bare instance), but must not fail on
    # the reader guard — that is the distinction being asserted.
    with pytest.raises(Exception) as excinfo:
        enrichment.process_geolocation("2026-08-07", lookback_days=0, source="topology")
    assert "IPInfo reader unavailable" not in str(excinfo.value)


def test_phase_a0_failure_is_fatal():
    """A0 must not be wrapped in a try/except that lets detection proceed.

    Asserted against the source because the alternative is running the whole
    pipeline. The specific hazard is a resurrected `except ... : logger.error(...)`
    around `source="clients"`, which was correct only while 02/03 read MaxMind.
    """
    src = inspect.getsource(union.run_dates)
    marker = 'source="clients",'
    assert marker in src, "Phase A0 client enrichment call not found"

    before = src[: src.index(marker)]
    phase_a0 = before[before.rindex("Phase A0") :]
    assert "try:" not in phase_a0, (
        "Phase A0 is wrapped in try/except again — a swallowed failure yields a "
        "batch grouped entirely under NULL, which reads as 'no anomalies'."
    )

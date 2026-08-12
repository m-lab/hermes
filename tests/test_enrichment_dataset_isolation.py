"""Enrichment must respect --target, and must not do topology work for clients.

Two defects found by the first staging run of Phase A0:

1. The client-candidate query hardcoded ``hermes_union.merged_download_upload``, so a
   ``--target staging`` run collected its client IPs from *production*. Invisible in
   the output, because staging holds a copy of the same partitions — which is exactly
   why it needs a test rather than an eyeball.

2. ``run_enrichment`` ran rDNS and HOIHO regardless of ``source``. Both infer
   geolocation from router hostnames, which residential CPE do not carry, and both
   write to the *topology* tables — so Phase A0 was duplicating Phase B's work and
   mutating topology state.
"""

from __future__ import annotations

import inspect
from unittest import mock

from hermes.enrichment.main import HermesEnrichment
from hermes.pipeline import union


def _captured_client_query(dataset: str) -> str:
    enrichment = HermesEnrichment.__new__(HermesEnrichment)
    enrichment.ipv6 = False
    enrichment.ipinfo = mock.Mock(reader=object())
    enrichment.tables = {
        "src_ip_to_geoloc": "mlab-collaboration.hermes.unified_src_ip_to_geoloc",
        "ip_to_geoloc": "mlab-collaboration.hermes.unified_ip_to_geoloc",
    }
    enrichment.client = mock.MagicMock()
    # Return no candidate IPs so the method stops after building the query.
    enrichment.client.query.return_value.result.return_value = []

    enrichment.process_geolocation("2026-08-07", lookback_days=0, source="clients", dataset=dataset)
    return enrichment.client.query.call_args[0][0]


def test_client_candidates_come_from_the_target_dataset():
    sql = _captured_client_query("hermes_staging")
    assert "`mlab-collaboration.hermes_staging.merged_download_upload`" in sql
    assert "hermes_union.merged_download_upload" not in sql


def test_client_candidates_default_to_production():
    sql = _captured_client_query("hermes_union")
    assert "`mlab-collaboration.hermes_union.merged_download_upload`" in sql


def test_run_enrichment_threads_dataset_into_table_names():
    """The transient_events override must follow the target dataset too."""
    src = inspect.getsource(union.run_enrichment)
    assert 'f"mlab-collaboration.{dataset}.transient_events_union"' in src
    assert '"mlab-collaboration.hermes_union.transient_events_union"' not in src


def test_run_enrichment_skips_rdns_and_hoiho_for_clients():
    src = inspect.getsource(union.run_enrichment)
    marker = 'if source == "clients":'
    assert marker in src, "no client branch in run_enrichment"

    after = src[src.index(marker) :]
    guard = after[: after.index("continue")]
    assert "process_rdns" not in guard and "process_hoiho" not in guard, (
        "client branch must reach `continue` without running rDNS/HOIHO"
    )
    # And the calls must still exist for topology.
    assert "process_rdns" in src and "process_hoiho_geolocation" in src

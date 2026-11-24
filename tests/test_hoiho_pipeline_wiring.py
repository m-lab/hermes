"""run_enrichment calls HOIHO once per topology batch, IPv4 only, per target.

Before: HOIHO ran for IPv6 too (under a log line saying it was skipped), and a
staging run skipped it entirely, so a HOIHO change could not be tested there.
"""

from __future__ import annotations

from datetime import date, timedelta
from unittest import mock

import pytest

from hermes.pipeline import union

TODAY = date.today().isoformat()


@pytest.fixture
def calls(monkeypatch):
    recorded: list[dict] = []

    def fake_refresh(client, day, **kwargs):
        recorded.append({"day": day, **kwargs})

    monkeypatch.setattr(union, "refresh_hoiho_geolocation", fake_refresh)
    monkeypatch.setattr(union, "HermesEnrichment", mock.MagicMock())
    monkeypatch.setattr(union.bigquery, "Client", mock.MagicMock())
    return recorded


def test_production_topology_refreshes_hoiho_once_into_production(calls):
    union.run_enrichment(TODAY, "p", lookback_days=0, source="topology")
    assert len(calls) == 1, "IPv4 only: the IPv6 pass must not call HOIHO"
    (call,) = calls
    assert call["day"] == TODAY
    assert call["geolocation_table"] == f"{union.PROJECT}.hermes.geolocation"
    assert call["lookups_table"] == f"{union.PROJECT}.hermes.hoiho_lookups"
    assert call["rdns_table"] == f"{union.PROJECT}.hermes.unified_ip_to_rdns"
    assert call["lookback_days"] == union.HOIHO_LOOKBACK_DAYS


def test_staging_topology_refreshes_hoiho_into_staging_only(calls):
    union.run_enrichment(TODAY, "p", source="topology", dataset=union.STAGING_DATASET)
    (call,) = calls
    assert call["geolocation_table"] == f"{union.PROJECT}.hermes_staging.geolocation"
    assert call["lookups_table"] == f"{union.PROJECT}.hermes_staging.hoiho_lookups"
    union.HermesEnrichment.assert_not_called()


@pytest.mark.parametrize("dataset", [union.DEFAULT_DATASET, union.STAGING_DATASET])
def test_client_enrichment_never_calls_hoiho(calls, dataset):
    union.run_enrichment(TODAY, "p", source="clients", dataset=dataset)
    assert calls == []


def test_old_dates_skip_hoiho(calls):
    old = (date.today() - timedelta(days=120)).isoformat()
    union.run_enrichment(old, "p", source="topology")
    assert calls == []

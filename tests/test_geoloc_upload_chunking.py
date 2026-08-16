"""Enrichment must upload in chunks, so memory tracks the chunk not the run.

Resolving everything before uploading kept four full-size copies alive at once --
the candidate list, one Future per IP, ``new_geo``, and the ``rows`` copy the upload
builds. That OOM-killed a 7,069,402 IP run at a 28 GB cap and a 5,937,414 IP run at
4 GB, which is what forced backfills through one container per day.
"""

from __future__ import annotations

from unittest import mock

import pytest

from hermes.enrichment.main import GEOLOC_UPLOAD_CHUNK, HermesEnrichment


def test_chunk_size_is_sane():
    assert 10_000 <= GEOLOC_UPLOAD_CHUNK <= 1_000_000


def _enrichment(n_candidates: int):
    e = HermesEnrichment.__new__(HermesEnrichment)
    e.ipv6 = False
    e.ipinfo = mock.Mock(reader=object(), snapshot_name="ipinfo_2025-08-07.snapshot")
    e.ripe_ipmap = mock.Mock()
    e.client = mock.MagicMock()
    e.tables = {
        "src_ip_to_geoloc": "mlab-collaboration.hermes.unified_src_ip_to_geoloc",
        "ip_to_geoloc": "mlab-collaboration.hermes.unified_ip_to_geoloc",
    }
    ips = [f"10.0.{i // 256}.{i % 256}" for i in range(n_candidates)]
    e.client.query.return_value.result.return_value = [mock.Mock(ip_address=ip) for ip in ips]
    return e


#: The real cap is 250k; the loop is exercised with a tiny one so the test does not
#: have to materialise a million mock IPs to prove it chunks.
TEST_CHUNK = 4


@pytest.mark.parametrize(
    ("candidates", "expected_uploads"),
    [(1, 1), (TEST_CHUNK, 1), (TEST_CHUNK + 1, 2), (TEST_CHUNK * 3, 3), (TEST_CHUNK * 3 + 2, 4)],
)
def test_uploads_once_per_chunk(candidates, expected_uploads):
    e = _enrichment(candidates)
    with (
        mock.patch("hermes.enrichment.main.GEOLOC_UPLOAD_CHUNK", TEST_CHUNK),
        mock.patch.object(HermesEnrichment, "_get_geolocation_data", return_value={"x": 1}),
        mock.patch.object(HermesEnrichment, "_upload_geolocation_data") as upload,
        mock.patch.object(HermesEnrichment, "_update_metro_for_geolocation_table") as metro,
    ):
        e.process_geolocation("2025-07-31", lookback_days=0, source="clients")

    assert upload.call_count == expected_uploads
    # No chunk may exceed the cap -- that is the whole point of the loop.
    for call in upload.call_args_list:
        assert len(call[0][0]) <= TEST_CHUNK
    # Total uploaded must equal the candidate set: chunking must not drop IPs.
    assert sum(len(c[0][0]) for c in upload.call_args_list) == candidates
    # The metro MERGE is partition-scoped; running it per chunk re-scans for nothing.
    assert metro.call_count == 1


def test_no_upload_and_no_metro_when_nothing_resolves():
    e = _enrichment(5)
    with (
        mock.patch.object(HermesEnrichment, "_get_geolocation_data", return_value=None),
        mock.patch.object(HermesEnrichment, "_upload_geolocation_data") as upload,
        mock.patch.object(HermesEnrichment, "_update_metro_for_geolocation_table") as metro,
    ):
        e.process_geolocation("2025-07-31", lookback_days=0, source="clients")

    upload.assert_not_called()
    metro.assert_not_called()


def test_ungeolocated_rows_do_not_satisfy_the_staleness_join():
    """A chunk that dies leaves rows with no location; they must be retried.

    Without this the poisoning mode from 2026-08-12 returns: rows exist, so the
    staleness join treats the IP as covered and it is never enriched again.
    """
    e = _enrichment(1)
    with (
        mock.patch.object(HermesEnrichment, "_get_geolocation_data", return_value={"x": 1}),
        mock.patch.object(HermesEnrichment, "_upload_geolocation_data"),
        mock.patch.object(HermesEnrichment, "_update_metro_for_geolocation_table"),
    ):
        e.process_geolocation("2025-07-31", lookback_days=0, source="clients")

    sql = e.client.query.call_args[0][0]
    assert "COALESCE(lat_ip_info, lat) IS NOT NULL" in sql

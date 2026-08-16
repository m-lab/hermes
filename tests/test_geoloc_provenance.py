"""Every geolocation row must record which IPInfo dump produced it.

Without this column a backfilled partition is indistinguishable from a
contemporaneous one. The July 2025 roll-back is geolocated with a 2026-06-12 dump
because no 2025 dump exists — an ~11 month gap that must travel with the data rather
than living in a log line or someone's memory.
"""

from __future__ import annotations

import inspect
from unittest import mock

from hermes.enrichment.main import HermesEnrichment
from hermes.pipeline import union


def test_uploaded_rows_carry_the_snapshot_name():
    enrichment = HermesEnrichment.__new__(HermesEnrichment)
    enrichment.ipv6 = False
    enrichment.ipinfo = mock.Mock(snapshot_name="ipinfo_2026-06-12.snapshot")
    enrichment.client = mock.MagicMock()
    enrichment.tables = {
        "src_ip_to_geoloc": "mlab-collaboration.hermes.unified_src_ip_to_geoloc",
        "ip_to_geoloc": "mlab-collaboration.hermes.unified_ip_to_geoloc",
    }

    geo = {
        "1.2.3.4": dict.fromkeys(
            [
                "city",
                "country",
                "lat",
                "lon",
                "score",
                "city_ip_info",
                "country_ip_info",
                "region_ip_info",
                "lat_ip_info",
                "lon_ip_info",
                "metro",
                "polygon",
                "rank",
            ]
        )
    }
    enrichment._upload_geolocation_data(geo, "2025-07-31", source="clients")

    rows = enrichment.client.load_table_from_json.call_args[0][0]
    assert rows[0]["geoloc_snapshot"] == "ipinfo_2026-06-12.snapshot"

    schema = enrichment.client.load_table_from_json.call_args[1]["job_config"].schema
    assert "geoloc_snapshot" in {f.name for f in schema}, (
        "the load schema must declare the column or the value is dropped silently"
    )


def test_run_enrichment_pins_the_snapshot_to_the_processing_date():
    src = inspect.getsource(union.run_enrichment)
    assert "snapshot_date=" in src, (
        "run_enrichment must pass the processing date; without it every backfill "
        "silently geolocates with today's dump"
    )
    assert "HermesEnrichment(" in src

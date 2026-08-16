"""Every geolocation row must record which IPInfo dump produced it.

Without this column a backfilled partition is indistinguishable from a
contemporaneous one. The July 2025 roll-back is geolocated with a 2026-06-12 dump
because no 2025 dump exists — an ~11 month gap that must travel with the data rather
than living in a log line or someone's memory.
"""

from __future__ import annotations

import inspect
from unittest import mock

import pytest

from hermes.enrichment.main import HermesEnrichment
from hermes.pipeline import union
from hermes.sql.loader import load_query


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


def test_upload_writes_the_dump_date_as_a_date_column():
    """The DATE form is the join key; the filename string alone is not usable."""
    enrichment = HermesEnrichment.__new__(HermesEnrichment)
    enrichment.ipv6 = False
    enrichment.ipinfo = mock.Mock(snapshot_name="ipinfo_2025-08-07.snapshot")
    enrichment.client = mock.MagicMock()
    enrichment.tables = {
        "src_ip_to_geoloc": "mlab-collaboration.hermes.unified_src_ip_to_geoloc",
        "ip_to_geoloc": "mlab-collaboration.hermes.unified_ip_to_geoloc",
    }
    fields = [
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
    enrichment._upload_geolocation_data({"1.2.3.4": dict.fromkeys(fields)}, "2025-07-31", "clients")

    rows = enrichment.client.load_table_from_json.call_args[0][0]
    assert rows[0]["geoloc_snapshot_date"] == "2025-08-07"
    schema = enrichment.client.load_table_from_json.call_args[1]["job_config"].schema
    by_name = {f.name: f.field_type for f in schema}
    assert by_name.get("geoloc_snapshot_date") == "DATE"


@pytest.mark.parametrize(
    "step", ["02_detect_anomalies_union.sql", "03_build_transient_events_union.sql"]
)
def test_detection_matches_on_dump_date_not_partition_date(step):
    """partition_date is the run's target date, so it is not a usable time key.

    A batched run stamps every IP in its lookback with one value -- 3,592,678 rows
    landed on 2025-07-31 carrying traffic from 07-11 onward -- so ordering by it
    selects rows according to how runs were batched.
    """
    sql = load_query(step, {"DAY": "2025-07-31", "ONE_WEEK_EARLIER": "2025-07-24"})
    assert "COALESCE(geoloc_snapshot_date, partition_date)" in sql, (
        f"{step} must order by the dump date, falling back to partition_date only "
        "for rows written before the column existed"
    )
    # and the column must survive the IPv4/IPv6 union: one SELECT per family plus
    # the two COALESCE references in the ORDER BY
    assert sql.count("geoloc_snapshot_date") >= 4, (
        "the column must appear in both union SELECT lists, or the ORDER BY cannot resolve it"
    )

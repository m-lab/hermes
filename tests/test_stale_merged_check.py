"""Tests for the stale ``merged_download_upload`` detector.

Step 01 can write a partition that is present and non-empty — so invisible to
``step_already_done()`` — while holding a fraction of the available
measurements. Three such dates were found by hand in Aug 2026; nothing in the
pipeline detected them. The thresholds below are pinned to those real cases.
"""

from datetime import date
from unittest.mock import MagicMock, patch

from hermes.pipeline import union

# Real pre-repair figures (rows, neighbour median), measured 2026-08-07.
KNOWN_STALE = {
    date(2026, 5, 27): (650_293, 4_524_178),
    date(2026, 7, 13): (385_526, 4_578_093),
    date(2026, 7, 17): (1_177_571, 4_886_559),
}


def _row(day, rows, median):
    r = MagicMock()
    r.d, r.total_rows, r.local_median = day, rows, median
    r.ratio = rows / median
    return r


def _client_returning(rows):
    client = MagicMock()
    client.query.return_value.result.return_value = rows
    return client


def test_known_stale_dates_fall_below_threshold():
    """Each real case must sit under the default 0.5 cutoff, with margin."""
    for day, (rows, median) in KNOWN_STALE.items():
        ratio = rows / median
        assert ratio < 0.5, f"{day} at {ratio:.3f} would not be flagged"
        # the worst real case was 0.084; anything near the cutoff means the
        # default is tuned too tightly to be trustworthy
        assert ratio < 0.25, f"{day} at {ratio:.3f} is uncomfortably near the cutoff"


def test_healthy_partition_is_not_flagged():
    """A normal date sits near 1.0 and must stay well clear of the cutoff."""
    assert 4_690_291 / 4_524_178 > 0.5


def test_returns_tuples_for_flagged_partitions():
    rows = [_row(day, r, m) for day, (r, m) in sorted(KNOWN_STALE.items())]
    with patch.object(union.bigquery, "Client", return_value=_client_returning(rows)):
        out = union.find_stale_merged_partitions("proj")

    assert [d for d, _, _, _ in out] == sorted(KNOWN_STALE)
    assert all(ratio < 0.5 for *_, ratio in out)


def test_empty_when_nothing_is_stale():
    with patch.object(union.bigquery, "Client", return_value=_client_returning([])):
        assert union.find_stale_merged_partitions("proj") == []


def test_query_failure_returns_empty_and_does_not_raise():
    """An audit that fails must not mask the pipeline's own result."""
    client = MagicMock()
    client.query.side_effect = RuntimeError("permission denied")
    with patch.object(union.bigquery, "Client", return_value=client):
        assert union.find_stale_merged_partitions("proj") == []


def test_reads_only_partition_metadata():
    """The check must stay free — it runs on every invocation."""
    client = _client_returning([])
    with patch.object(union.bigquery, "Client", return_value=client):
        union.find_stale_merged_partitions("proj")

    sql = client.query.call_args[0][0]
    assert "INFORMATION_SCHEMA.PARTITIONS" in sql
    # scanning the table itself would defeat the point
    assert "FROM `mlab-collaboration.hermes_union.merged_download_upload`" not in sql


def test_excludes_the_day_itself_from_its_own_median():
    """Otherwise a stale date drags down the baseline it is measured against."""
    client = _client_returning([])
    with patch.object(union.bigquery, "Client", return_value=client):
        union.find_stale_merged_partitions("proj")

    assert "b.d != a.d" in client.query.call_args[0][0]


def test_targets_the_step_01_output_table():
    """Pinned to the resume map so a table rename cannot silently orphan this."""
    assert (
        union.SQL_FILE_TO_OUTPUT_TABLE["01_merge_upload_download_union.sql"]
        == "mlab-collaboration.hermes_union.merged_download_upload"
    )

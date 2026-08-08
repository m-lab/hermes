"""Guards on the tomography DataFrame download and its REST fallback.

The fallback in ``_read_df`` used to call a bare ``query_job.to_dataframe()``.
That argument defaults to ``create_bqstorage_client=True``, so once
``google-cloud-bigquery-storage`` was installed the fallback silently rebuilt a
Storage client and re-raised the same PermissionDenied it was catching. Every
Phase D failed with a 403 *after* logging "using REST download".
"""

from unittest.mock import MagicMock, patch

from hermes.pipeline import correlation_tomography as ct


def _job():
    job = MagicMock()
    job.to_dataframe.return_value = "DF"
    return job


def test_uses_storage_client_when_available():
    """The fast Arrow path is preferred when the storage client can be built."""
    job = _job()
    fake_storage = MagicMock()
    with patch.dict("sys.modules", {"google.cloud.bigquery_storage": fake_storage}):
        with patch("google.cloud.bigquery_storage.BigQueryReadClient", create=True) as client:
            assert ct._read_df(job) == "DF"

    kwargs = job.to_dataframe.call_args.kwargs
    assert "bqstorage_client" in kwargs
    assert client.called


def test_fallback_disables_bqstorage_client_creation():
    """The regression: the fallback must not re-create a Storage client.

    Without create_bqstorage_client=False the retry hits the identical 403 and
    the whole phase dies despite the "using REST download" log line.
    """
    job = MagicMock()
    job.to_dataframe.side_effect = [PermissionError("403 readsessions.create"), "DF"]

    assert ct._read_df(job) == "DF"

    assert job.to_dataframe.call_count == 2
    fallback_kwargs = job.to_dataframe.call_args_list[1].kwargs
    assert fallback_kwargs.get("create_bqstorage_client") is False, (
        "fallback would rebuild the Storage client and re-raise the same error"
    )


def test_fallback_does_not_pass_a_storage_client():
    """Belt and braces: the retry must carry no storage client at all."""
    job = MagicMock()
    job.to_dataframe.side_effect = [RuntimeError("storage unavailable"), "DF"]

    ct._read_df(job)

    assert "bqstorage_client" not in job.to_dataframe.call_args_list[1].kwargs


def test_fallback_logs_before_retrying(caplog):
    """Operators need the reason in the log, not a silent slow path."""
    job = MagicMock()
    job.to_dataframe.side_effect = [PermissionError("403 readsessions.create"), "DF"]

    with caplog.at_level("WARNING"):
        ct._read_df(job)

    assert any("Storage Read API unavailable" in r.message for r in caplog.records)

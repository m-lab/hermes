"""Guards on the tomography DataFrame download and its REST fallback.

The fallback in ``_read_df`` used to call a bare ``query_job.to_dataframe()``.
That argument defaults to ``create_bqstorage_client=True``, so once
``google-cloud-bigquery-storage`` was installed the fallback silently rebuilt a
Storage client and re-raised the same PermissionDenied it was catching. Every
Phase D failed with a 403 *after* logging "using REST download".

These tests drive both branches explicitly. They must not depend on whether the
machine running them happens to have application-default credentials: with ADC
the Storage client constructs and the Arrow call is attempted, without it
construction raises and the Arrow call never happens. An earlier version of this
file leaked that difference into its ``side_effect`` sequencing and passed
locally while failing in CI.
"""

import sys
import types
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import google.cloud

from hermes.pipeline import correlation_tomography as ct


@contextmanager
def fake_storage(construct_error: Exception | None = None):
    """Make ``from google.cloud import bigquery_storage`` deterministic.

    ``construct_error`` makes ``BigQueryReadClient()`` raise, standing in for the
    real-world cases: missing ADC, or the 403 when the identity lacks
    ``bigquery.readsessions.create``.
    """
    module = types.ModuleType("google.cloud.bigquery_storage")
    module.BigQueryReadClient = MagicMock(side_effect=construct_error, return_value=MagicMock())
    with (
        patch.object(google.cloud, "bigquery_storage", module, create=True),
        patch.dict(sys.modules, {"google.cloud.bigquery_storage": module}),
    ):
        yield module


def test_uses_storage_client_when_available():
    """The fast Arrow path is preferred when the storage client can be built."""
    job = MagicMock()
    job.to_dataframe.return_value = "DF"

    with fake_storage() as module:
        assert ct._read_df(job) == "DF"

    assert module.BigQueryReadClient.called
    job.to_dataframe.assert_called_once()
    assert "bqstorage_client" in job.to_dataframe.call_args.kwargs


def test_fallback_disables_bqstorage_client_creation_when_client_cannot_be_built():
    """Construction fails (no ADC): the single REST call must disable auto-create.

    This is the regression. Without ``create_bqstorage_client=False`` the retry
    builds a Storage client anyway and re-raises the error being handled.
    """
    job = MagicMock()
    job.to_dataframe.return_value = "DF"

    with fake_storage(construct_error=PermissionError("403 readsessions.create")):
        assert ct._read_df(job) == "DF"

    job.to_dataframe.assert_called_once()
    assert job.to_dataframe.call_args.kwargs.get("create_bqstorage_client") is False
    assert "bqstorage_client" not in job.to_dataframe.call_args.kwargs


def test_fallback_disables_bqstorage_client_creation_when_arrow_read_fails():
    """Construction succeeds but the Arrow read 403s: the retry must still disable it."""
    job = MagicMock()
    job.to_dataframe.side_effect = [PermissionError("403 readsessions.create"), "DF"]

    with fake_storage():
        assert ct._read_df(job) == "DF"

    assert job.to_dataframe.call_count == 2
    first, second = job.to_dataframe.call_args_list
    assert "bqstorage_client" in first.kwargs
    assert second.kwargs.get("create_bqstorage_client") is False
    assert "bqstorage_client" not in second.kwargs


def test_fallback_logs_before_retrying(caplog):
    """Operators need the reason in the log, not a silent slow path."""
    job = MagicMock()
    job.to_dataframe.return_value = "DF"

    with caplog.at_level("WARNING"), fake_storage(construct_error=RuntimeError("nope")):
        ct._read_df(job)

    assert any("Storage Read API unavailable" in r.message for r in caplog.records)

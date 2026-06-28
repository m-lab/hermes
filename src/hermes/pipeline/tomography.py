"""Correlation-tomography backend dispatch.

Two interchangeable implementations localize anomaly root causes:

- ``python``   : hybrid greedy set-cover in
  :mod:`hermes.pipeline.correlation_tomography` (cheaper).
- ``bigquery`` : the in-BigQuery SQL set-cover loop
  (``06_correlation_tomography_bigquery_union.sql``).

The pipeline default is ``python`` (current production behavior).
"""

from __future__ import annotations

import datetime as dt
from typing import Any

DEFAULT_BACKEND = "python"
BACKENDS = ("python", "bigquery")


def _run_python(date: dt.date, *, project_id: str, **kwargs: Any) -> None:
    """Run the Python hybrid greedy set-cover tomography.

    Parameters
    ----------
    date
        The day to process.
    project_id
        BigQuery project to run against.
    **kwargs
        Forwarded to :func:`hermes.pipeline.correlation_tomography.run_correlation_tomography`.
    """
    from hermes.pipeline.correlation_tomography import run_correlation_tomography

    run_correlation_tomography(date, project_id=project_id, **kwargs)


def _run_bigquery(date: dt.date, *, project_id: str, **kwargs: Any) -> None:
    """Run the in-BigQuery SQL set-cover loop tomography.

    Submits ``06_correlation_tomography_bigquery_union.sql`` via the
    SQL loader, substituting ``${DAY}`` with the ISO-formatted date.

    Parameters
    ----------
    date
        The day to process.
    project_id
        BigQuery project to run against.
    **kwargs
        Currently unused; reserved for future BigQuery job options.
    """
    from google.cloud import bigquery

    from hermes.sql import loader

    sql = loader.load_query(
        "06_correlation_tomography_bigquery_union.sql",
        {"DAY": date.strftime("%Y-%m-%d")},  # query uses ${DAY}
    )
    bigquery.Client(project=project_id).query(sql).result()


def run_tomography(
    date: dt.date,
    *,
    backend: str = DEFAULT_BACKEND,
    project_id: str,
    **kwargs: Any,
) -> None:
    """Run correlation tomography for ``date`` using the chosen backend.

    Parameters
    ----------
    date
        The day to process.
    backend
        ``"python"`` (default, hybrid set-cover) or ``"bigquery"`` (SQL loop).
    project_id
        BigQuery project to run against.
    **kwargs
        Forwarded to the selected backend.

    Raises
    ------
    ValueError
        If ``backend`` is not one of :data:`BACKENDS`.
    """
    if backend == "python":
        _run_python(date, project_id=project_id, **kwargs)
    elif backend == "bigquery":
        _run_bigquery(date, project_id=project_id, **kwargs)
    else:
        raise ValueError(f"Unknown tomography backend {backend!r}; expected one of {BACKENDS}")

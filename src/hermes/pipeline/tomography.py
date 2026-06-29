"""Correlation-tomography backend dispatch.

The pipeline runs the v2 hybrid greedy set-cover implemented in
:mod:`hermes.pipeline.correlation_tomography` (``python`` backend only).
"""

from __future__ import annotations

import datetime as dt
from typing import Any

DEFAULT_BACKEND = "python"
BACKENDS = ("python",)


def run_tomography(
    date: dt.date,
    *,
    backend: str = DEFAULT_BACKEND,
    project_id: str,
    **kwargs: Any,
) -> None:
    """Run v2 correlation tomography for ``date`` (python backend only).

    Parameters
    ----------
    date
        The day to process.
    backend
        Must be ``"python"`` (the only supported backend).
    project_id
        BigQuery project to run against.
    **kwargs
        Forwarded to :func:`hermes.pipeline.correlation_tomography.run_correlation_tomography`.

    Raises
    ------
    ValueError
        If ``backend`` is not one of :data:`BACKENDS`.
    """
    if backend != "python":
        raise ValueError(f"Unknown tomography backend {backend!r}; expected one of {BACKENDS}")
    from hermes.pipeline import correlation_tomography

    return correlation_tomography.run_correlation_tomography(date, project_id=project_id, **kwargs)

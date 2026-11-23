"""Deployment settings shared by the pipeline, enrichment and SQL loader.

``PROJECT`` is the Google Cloud project that HERMES bills its BigQuery jobs to
and that holds its datasets (``hermes_union``, ``hermes_staging``, ``hermes``,
``ix_data``). It defaults to the M-Lab deployment, ``mlab-collaboration``.
Override it with the ``HERMES_PROJECT`` environment variable to run the pipeline
in your own project, e.g. ``docker run -e HERMES_PROJECT=my-project ...``.

The value is resolved once, at import, because table names built from it are
module-level constants. Set the variable before starting the process; changing
``os.environ`` afterwards has no effect.

The M-Lab measurement sources (``measurement-lab.ndt.*``, ``measurement-lab.ndt_raw.*``,
``measurement-lab.revtr_raw.*``) are owned by M-Lab and are never retargeted.
"""

from __future__ import annotations

import os
import re

DEFAULT_PROJECT = "mlab-collaboration"

# Project ids are lowercase letters, digits and hyphens; legacy domain-scoped ids
# add "domain:" and dots. Anything else (a backtick, whitespace) would be
# interpolated into SQL, so it is rejected outright.
_PROJECT_ID = re.compile(r"(?:[a-z0-9.-]+:)?[a-z][a-z0-9-]{4,28}[a-z0-9]")


def resolve_project(environ: dict[str, str] | None = None) -> str:
    """Return the configured project, validating any override.

    Parameters
    ----------
    environ
        Mapping to read ``HERMES_PROJECT`` from (defaults to ``os.environ``).

    Returns
    -------
    str
        ``HERMES_PROJECT`` if set and non-empty, else ``DEFAULT_PROJECT``.

    Raises
    ------
    ValueError
        If ``HERMES_PROJECT`` is not a well-formed Google Cloud project id.
    """
    value = (os.environ if environ is None else environ).get("HERMES_PROJECT", "").strip()
    if not value:
        return DEFAULT_PROJECT
    if not _PROJECT_ID.fullmatch(value):
        raise ValueError(f"HERMES_PROJECT is not a valid Google Cloud project id: {value!r}")
    return value


PROJECT = resolve_project()

"""Locate the packaged SQL assets.

Resolution order: the ``HERMES_SQL_DIR`` environment variable if set,
otherwise the ``sql`` directory shipped inside the ``hermes.sql`` package.

Examples
--------
>>> from hermes.sql import paths
>>> paths.query_path("01_merge_upload_download_union.sql").is_file()
True
"""

from __future__ import annotations

import os
from importlib import resources
from pathlib import Path


def sql_root() -> Path:
    """Return the root directory holding ``queries/`` and ``udfs/``.

    Returns
    -------
    pathlib.Path
        ``$HERMES_SQL_DIR`` if set, else the packaged ``hermes/sql`` directory.
    """
    override = os.environ.get("HERMES_SQL_DIR")
    if override:
        return Path(override)
    pkg_path = resources.files("hermes.sql")
    # resources.files() returns Traversable; cast to str for Path compatibility.
    return Path(str(pkg_path))


def query_path(name: str) -> Path:
    """Return the path to a query file under ``queries/``.

    Parameters
    ----------
    name
        Query filename (e.g. ``"01_merge_upload_download_union.sql"``).

    Returns
    -------
    pathlib.Path
        Path to the query file under the resolved ``queries/`` directory.
    """
    return sql_root() / "queries" / name


def udf_path(name: str) -> Path:
    """Return the path to a UDF file under ``udfs/``.

    Parameters
    ----------
    name
        UDF filename or bare name; a ``.sql`` suffix is appended if absent.

    Returns
    -------
    pathlib.Path
        Path to the UDF file under the resolved ``udfs/`` directory.
    """
    fname = name if name.endswith(".sql") else f"{name}.sql"
    return sql_root() / "udfs" / fname

"""Assemble self-contained BigQuery SQL from query + UDF files.

A query may declare the UDFs it needs with directive comments::

    -- @requires-udf: has_loop

``load_query`` reads the query, prepends each required UDF (from
``sql/udfs/<name>.sql``) so the submitted statement is self-contained, and
substitutes ``${...}`` parameters.
"""

from __future__ import annotations

import re
from string import Template

from hermes.sql import paths

_REQUIRES = re.compile(r"^\s*--\s*@requires-udf:\s*(?P<name>[\w./-]+)\s*$", re.MULTILINE)


def required_udfs(query_text: str) -> list[str]:
    """Return UDF names declared via ``-- @requires-udf:`` directives.

    Parameters
    ----------
    query_text
        Full text of a SQL query.

    Returns
    -------
    list of str
        UDF names in declaration order (empty if none declared).
    """
    return _REQUIRES.findall(query_text)


def load_query(name: str, params: dict[str, object] | None = None) -> str:
    """Load a query, prepend its required UDFs, and substitute parameters.

    Parameters
    ----------
    name
        Query filename under ``sql/queries/``.
    params
        Mapping for ``${...}`` placeholders (``Template.safe_substitute``).

    Returns
    -------
    str
        Assembled, self-contained SQL text.

    Raises
    ------
    FileNotFoundError
        If the query or any declared UDF file is missing.
    """
    query_text = paths.query_path(name).read_text(encoding="utf-8")
    udf_blocks = []
    for udf_name in required_udfs(query_text):
        udf_blocks.append(paths.udf_path(udf_name).read_text(encoding="utf-8").rstrip())
    assembled = "\n\n".join([*udf_blocks, query_text]) if udf_blocks else query_text
    return Template(assembled).safe_substitute(params or {})

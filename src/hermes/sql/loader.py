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

#: Parameters every query gets unless the caller overrides them.
#:
#: ``METRO_POLYGONS`` is the single place the metro polygon table is named. It
#: used to be hard-coded at six sites, which is how the ``state_iso2`` regression
#: managed to live in two of them while the other four were correct.
#:
#: NOTE: the value here must be a table of the ``metro_polygons_v2`` shape --
#: ordinary positive geometry matched with ``ST_COVERS``. It is NOT
#: interchangeable with the old ``metro_polygons_with_population``, whose
#: geometry reads back inverted and requires ``NOT ST_CONTAINS``. Pointing this
#: at the old table would silently match every polygon except the correct one.
#: ``DS`` is the pipeline (operational) dataset every step reads and writes.
#: Reference datasets -- ``hermes``, ``measurement-lab`` -- are deliberately NOT
#: parameterised: they are shared read-only inputs, so a staging run uses the same
#: geolocation, AS metadata and IXP data as production. This is the same isolation
#: rule ``correlation_tomography._load_dataset_query`` already applies to Phase D.
DEFAULT_PARAMS: dict[str, object] = {
    "DS": "hermes_union",
    "METRO_POLYGONS": "mlab-collaboration.hermes.metro_polygons_v2",
}


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
    # DEFAULT_PARAMS first so an omitted ${METRO_POLYGONS} can never survive
    # safe_substitute into the submitted SQL as a literal.
    merged = {**DEFAULT_PARAMS, **(params or {})}
    return Template(assembled).safe_substitute(merged)

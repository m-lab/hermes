"""Parsing of step-06 node and edge strings — the one place that knows their format.

A node is ``<ASN>-<location>``, where the location is a metro / place label such as
``Miami-Florida-US``; an unresolved hop is the bare node ``*``. An edge joins two
nodes with ``" - "``: ``"7552-Hue-Thừa Thiên - Huế-VN - 7552-Can Tho-Can Tho-VN"``.

The delimiter is not unique. Region and city names may themselves contain
``" - "`` (``Bà Rịa - Vũng Tàu``, ``Rabat - Salé - Zemmour - Zaer``,
``Happy Valley - Goose Bay``), so splitting on the first ``" - "`` cut such edges
inside a place name and produced ASNs like ``'Vũng Tàu'``. The node boundary is
the ``" - "`` that is followed by the next node's ASN token (digits or ``*``) and
its ``-``. Measured over all 201,513 distinct edge culprits in
``hermes_staging.correlation_entity_stats_multigranularity`` (2026-10-31): every
edge has exactly one such boundary, while 232 contain more than one ``" - "``.
"""

from __future__ import annotations

import re

# " - " followed by an ASN token (digits, or '*' for an unresolved hop) and then the
# node's own '-' or the end of the edge (a bare '*' / bare-ASN node).
_NODE_BOUNDARY = re.compile(r" - (?=(?:\d+|\*)(?:-|$))")


def split_edge(edge: object) -> tuple[str, str] | None:
    """Split an edge string into its two node strings, or ``None`` if it is not one."""
    if not isinstance(edge, str):
        return None
    m = _NODE_BOUNDARY.search(edge)
    if m is None:
        return None
    return edge[: m.start()].strip(), edge[m.end() :].strip()


def node_asn(node: str) -> str:
    """The ASN token of a node (``'*'`` for an unresolved hop)."""
    return node.split("-", 1)[0].strip()


def node_location(node: str) -> str | None:
    """Everything after the ASN token, or ``None`` for a node without one."""
    return node.split("-", 1)[1] if "-" in node else None

"""One-time idempotent bootstrap of pipeline output tables that are written via
DELETE+INSERT or streaming (so they must pre-exist). DDLs bill 0 bytes."""

from __future__ import annotations

import logging

from hermes.sql import loader

logger = logging.getLogger(__name__)

DDL_FILES = [
    "create_correlation_hyperedges_tomography_v2.sql",
    "create_temporal_path_verdicts.sql",
    "create_events_explained_daily.sql",
    "create_place_canonical_metro.sql",
]


def bootstrap(client) -> None:
    """Run each CREATE TABLE IF NOT EXISTS DDL once."""
    for name in DDL_FILES:
        logger.info("Bootstrapping via %s", name)
        client.query(loader.load_query(name, {})).result()


if __name__ == "__main__":
    from google.cloud import bigquery

    logging.basicConfig(level=logging.INFO)
    bootstrap(bigquery.Client(project="mlab-collaboration"))

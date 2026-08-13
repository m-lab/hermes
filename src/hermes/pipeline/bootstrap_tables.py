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
    # Phase-4 multi-granularity outputs. Production writes these on every run
    # (write_multigranularity=True in _run_tomography_worker), so their DDL must
    # be bootstrappable here — previously the tables existed only because they
    # had been created by hand on the VM.
    "create_correlation_culprits_multigranularity.sql",
    "create_correlation_entity_stats_multigranularity.sql",
    # Must precede the view and all Step0-aware writers.
    "add_client_geo_source_columns.sql",
    # Stable nested compatibility interface over the legacy physical table.
    "create_events_enriched.sql",
]

DEFAULT_SOURCE_DATASET = "hermes_union"
DEFAULT_PUBLISHED_DATASET = "hermes"


def _ddl_params(name: str, source_dataset: str, published_dataset: str) -> dict[str, object]:
    """Return the substitutions required by a bootstrap DDL."""
    params: dict[str, object] = {"DS": source_dataset}
    if name == "create_events_enriched.sql":
        params["PUBLISHED_DS"] = published_dataset
    return params


def bootstrap(
    client,
    *,
    source_dataset: str = DEFAULT_SOURCE_DATASET,
    published_dataset: str = DEFAULT_PUBLISHED_DATASET,
) -> None:
    """Create or refresh each bootstrapped table/view definition."""
    for name in DDL_FILES:
        logger.info("Bootstrapping via %s", name)
        params = _ddl_params(name, source_dataset, published_dataset)
        client.query(loader.load_query(name, params)).result()


if __name__ == "__main__":
    from google.cloud import bigquery

    logging.basicConfig(level=logging.INFO)
    bootstrap(bigquery.Client(project="mlab-collaboration"))

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
    # Must precede step 07, which names n_baseline in its INSERT column list.
    # create_events_explained_daily.sql above is CREATE TABLE IF NOT EXISTS, so on the
    # already-created production table it is a no-op and cannot add the column.
    "add_n_baseline_column.sql",
    # Upload-throughput public metrics and explicit signal identity. This must
    # precede step 07 and uses the same append order as the fresh-table DDL.
    "add_upload_anomaly_columns.sql",
    # Step 01 appends upload_id positionally, so the column must exist first.
    "add_upload_id_column.sql",
    # Stable nested compatibility interface over the legacy physical table.
    # Published into ``source_dataset`` -- see create_events_enriched.sql.
    "create_events_enriched.sql",
    # LAST, and it must stay last: it ALTERs every partitioned table in the
    # dataset, so everything above has to exist before it runs. Requiring the
    # partition filter is also the loudest failure mode in this list -- an
    # unfiltered reader stops working the moment it lands -- so it is the step
    # a partial bootstrap should reach last, not first.
    "require_partition_filter.sql",
]

DEFAULT_SOURCE_DATASET = "hermes_union"


def bootstrap(client, *, source_dataset: str = DEFAULT_SOURCE_DATASET) -> None:
    """Create or refresh each bootstrapped table/view definition.

    Every DDL is parameterised by ``DS`` alone, so a staging bootstrap
    (``source_dataset="hermes_staging"``) reads, writes and publishes entirely
    within staging. There is deliberately no second "published" dataset knob:
    it let the view's dataset drift from its source, and a staging render left
    ``${PUBLISHED_DS}`` unsubstituted in the submitted SQL.
    """
    for name in DDL_FILES:
        logger.info("Bootstrapping via %s", name)
        client.query(loader.load_query(name, {"DS": source_dataset})).result()


if __name__ == "__main__":
    from google.cloud import bigquery

    logging.basicConfig(level=logging.INFO)
    bootstrap(bigquery.Client(project="mlab-collaboration"))

"""Step 04 writes its two outputs atomically and dedups GIGA rows per trace.

External review, 2026-10-31:
* the mapping INSERT committed before the GIGA INSERT, and the resume guard looks only
  at events_with_as_and_geoloc, so a failure in between left a date "done" with no
  GIGA rows;
* the GIGA NOT EXISTS keyed on src_group_label, so the same trace written again on a
  later date under a NULL or different label became a second row (2% in October).
"""

from __future__ import annotations

import os
import re

import pytest

from hermes.sql import loader


def _sql() -> str:
    return loader.load_query(
        "04_mapping_union.sql",
        {"DAY": "2026-10-14", "DS": "hermes_union", "ONE_WEEK_EARLIER": "2026-10-07"},
    )


def test_both_inserts_are_inside_one_transaction():
    sql = _sql()
    begin = sql.index("BEGIN TRANSACTION;")
    commit = sql.index("COMMIT TRANSACTION;")
    events = sql.index("INSERT INTO `mlab-collaboration.hermes_union.events_with_as_and_geoloc`")
    giga = sql.index("INSERT INTO `mlab-collaboration.hermes_union.giga_meter_measurements`")
    assert begin < events < giga < commit
    assert sql.count("BEGIN TRANSACTION;") == 1 and sql.count("COMMIT TRANSACTION;") == 1
    # nothing that writes may follow the commit
    assert not re.search(r"\b(INSERT|DELETE|MERGE|UPDATE)\b", sql[commit:])


def _dedup_predicate() -> str:
    sql = _sql()
    giga = sql[sql.index("INSERT INTO `mlab-collaboration.hermes_union.giga_meter_measurements`") :]
    start = giga.index("AND NOT EXISTS (")
    end = giga.index(");", start)
    pred = giga[start : end + 1]
    return pred.replace("`mlab-collaboration.hermes_union.giga_meter_measurements`", "giga_lit")


@pytest.mark.bq
def test_label_change_or_null_does_not_write_a_second_copy():
    if not (os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") or os.environ.get("HERMES_BQ_TEST")):
        pytest.skip("BigQuery creds not configured (on-demand test)")
    from google.cloud import bigquery

    query = f"""
      WITH giga_lit AS (
        SELECT DATE '2026-10-10' AS partition_date, 't1' AS id, 100 AS src_asn,
               'Ulaanbaatar-Ulaanbaatar-MN' AS src_group_label, 'site1' AS dst_site
      ),
      _mapping_result AS (
        SELECT 't1' AS id, 100 AS src_asn, CAST(NULL AS STRING) AS src_group_label,
               'site1' AS dst_site, 'null label, same trace' AS case_
        UNION ALL SELECT 't1', 100, 'Dalandzadgad-Omnogovi-MN', 'site1', 'other label, same trace'
        UNION ALL SELECT 't1', 100, 'Ulaanbaatar-Ulaanbaatar-MN', 'site2', 'other site'
        UNION ALL SELECT 't2', 100, CAST(NULL AS STRING), 'site1', 'new trace'
      )
      SELECT case_ FROM _mapping_result WHERE TRUE {_dedup_predicate()} ORDER BY case_"""
    rows = [r.case_ for r in bigquery.Client(project="mlab-collaboration").query(query).result()]
    assert rows == ["new trace", "other site"]

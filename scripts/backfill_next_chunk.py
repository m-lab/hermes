#!/usr/bin/env python3
"""Pick the next backfill work item for the backward walk, from live coverage.

Why coverage-driven rather than a hardcoded chunk list
------------------------------------------------------
``fill_to_june.sh`` carried its chunks as a literal array. That is fine when a
human launches each run and reads the log, but this walk is ~20 unattended days
long. A hardcoded list has no memory of what actually landed: a chunk that
half-failed is stepped over and never noticed. Asking BigQuery which partitions
exist makes a failed chunk simply come back tomorrow, and makes the driver stop
on its own when the window is full.

The partition metadata query is free (``INFORMATION_SCHEMA.PARTITIONS``), so this
costs nothing to run every day.

Output (one line on stdout, for the shell driver to read):

    PHASE_E <d1> <d2> ...     dates that have upstream but no events_explained_daily
    WALK <start> <end>        a full-pipeline chunk, inclusive
    COMPLETE                  nothing left to do

Work is emitted in cost order, cheapest first:

1. ``PHASE_E`` -- upstream is intact and only the public table is missing. ~4 GiB
   per date instead of ~1.4 TiB, so clearing these first is nearly free.
2. ``WALK`` over a hole *inside* existing coverage. Holes are repaired before the
   walk extends the record backwards, on the principle that a gap in the middle of
   published history is worse than a shorter history.
3. ``WALK`` backwards from ``--ceil`` toward ``--floor``.
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, timedelta

from google.cloud import bigquery

DATASET = "mlab-collaboration.hermes_union"

#: Written by Phase C/D. A date having these but not ``events_explained_daily``
#: needs only Phase E, not a full re-run.
UPSTREAM = ("events_with_as_and_geoloc", "correlation_hyperedges_tomography_v2")
FINAL = "events_explained_daily"


def coverage(client: bigquery.Client) -> dict[str, set[date]]:
    """Dates with at least one row, per table.

    ``total_rows > 0`` matters: a DELETE leaves the partition metadata behind, so
    presence in PARTITIONS is not presence of data.
    """
    sql = f"""
        SELECT table_name, PARSE_DATE('%Y%m%d', partition_id) AS d
        FROM `{DATASET}.INFORMATION_SCHEMA.PARTITIONS`
        WHERE partition_id NOT IN ('__NULL__', '__UNPARTITIONED__')
          AND total_rows > 0
    """
    out: dict[str, set[date]] = {}
    for row in client.query(sql).result():
        out.setdefault(row.table_name, set()).add(row.d)
    return out


def _days(lo: date, hi: date) -> list[date]:
    return [lo + timedelta(i) for i in range((hi - lo).days + 1)]


def read_skips(path: str | None) -> set[date]:
    """Dates the driver has given up on, so the walk does not retry forever.

    A date with genuinely no NDT input would otherwise be picked every day and
    burn a chunk's quota producing nothing.
    """
    if not path:
        return set()
    try:
        with open(path) as fh:
            return {date.fromisoformat(ln.strip()) for ln in fh if ln.strip()}
    except FileNotFoundError:
        return set()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--floor", type=date.fromisoformat, required=True)
    ap.add_argument("--ceil", type=date.fromisoformat, required=True)
    ap.add_argument("--chunk", type=int, default=7)
    ap.add_argument("--skip-file")
    ap.add_argument(
        "--repair-from",
        type=date.fromisoformat,
        default=date(2025, 6, 13),
        help="Lower bound of the repair scan; defaults to the start of existing "
        "contiguous coverage, so holes anywhere in published history are found.",
    )
    ap.add_argument(
        "--repair-to",
        type=date.fromisoformat,
        default=date.today() - timedelta(days=4),
        help="Upper bound of the repair scan. Defaults to four days ago: the "
        "nightly only reaches today-2, so a tighter bound would report dates that "
        "are merely not due yet as holes and spend a chunk re-running them.",
    )
    ap.add_argument("--project", default="mlab-collaboration")
    args = ap.parse_args()

    cov = coverage(bigquery.Client(project=args.project))
    final = cov.get(FINAL, set())
    skips = read_skips(args.skip_file)

    # 1) Phase-E-only repairs anywhere in existing history.
    upstream_ok = set.intersection(*(cov.get(t, set()) for t in UPSTREAM))
    phase_e = sorted(
        d
        for d in upstream_ok - final - skips
        if args.repair_from <= d <= args.repair_to
    )
    if phase_e:
        print("PHASE_E " + " ".join(d.isoformat() for d in phase_e))
        return 0

    # 2) Holes inside existing coverage, where upstream is missing too and the
    #    whole pipeline has to run. Emitted as the newest contiguous run, capped
    #    at --chunk, so a long hole is worked through a chunk per day.
    holes = sorted(
        (
            d
            for d in _days(args.repair_from, args.repair_to)
            if d not in final and d not in skips
        ),
        reverse=True,
    )
    if holes:
        end = holes[0]
        run_start = end
        for d in holes:
            if (run_start - d).days <= 1 and (end - d).days < args.chunk:
                run_start = d
            else:
                break
        print(f"WALK {run_start.isoformat()} {end.isoformat()}")
        return 0

    # 3) Backward walk. Newest missing date wins: --start-date auto-builds step 01
    #    for the preceding week, which is the *next* chunk's input, so walking
    #    backwards pays step 01 once per chunk instead of twice.
    missing = [
        d
        for d in sorted(_days(args.floor, args.ceil), reverse=True)
        if d not in final and d not in skips
    ]
    if not missing:
        print("COMPLETE")
        return 0

    end = missing[0]
    start = max(args.floor, end - timedelta(days=args.chunk - 1))
    print(f"WALK {start.isoformat()} {end.isoformat()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

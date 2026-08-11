"""Assemble the pipeline SQL against a REHEARSAL dataset, with a hard prod gate.

Rewrites every `mlab-collaboration.hermes_union.` reference to the target dataset
and refuses to emit anything if a single prod reference survives. All writes in
01/02/03/04/05/07 target hermes_union, so a gate on that string covers every
write path -- reads to measurement-lab.*, hermes.* and ix_data.* are untouched
and stay pointed at prod.

    python scripts/build_staging_sql.py --day 2026-08-07 --out /tmp/staging_sql

See docs/proposals/2026-08-group-granularity.md.
"""

import argparse
import sys
from datetime import date, timedelta
from pathlib import Path

from hermes.sql import loader

STEPS = [
    "01_merge_upload_download_union.sql",
    "02_detect_anomalies_union.sql",
    "03_build_transient_events_union.sql",
    "04_mapping_union.sql",
    "05_temporal_tomography_union.sql",
    "07_translating_to_public_format_union.sql",
]

PROD = "mlab-collaboration.hermes_union."
SHARED_READ_TABLES = (
    "mlab-collaboration.hermes_union.place_canonical_metro",
)


def build(
    day: str,
    dataset: str,
    out: Path,
    detection_granularity: str = "maxmind_city",
) -> int:
    if dataset == "hermes_union":
        print("REFUSED: this script is for rehearsal datasets, not production.")
        return 2

    target = f"mlab-collaboration.{dataset}."
    one_week = (date.fromisoformat(day) - timedelta(days=7)).isoformat()
    out.mkdir(parents=True, exist_ok=True)

    written, failures = [], []
    for step in STEPS:
        sql = loader.load_query(
            step,
            {
                "DAY": day,
                "ONE_WEEK_EARLIER": one_week,
                "DETECTION_GRANULARITY": detection_granularity,
            },
        )
        protected = sql
        placeholders = {}
        for index, table in enumerate(SHARED_READ_TABLES):
            placeholder = f"__HERMES_SHARED_READ_{index}__"
            protected = protected.replace(table, placeholder)
            placeholders[placeholder] = table
        rewritten = protected.replace(PROD, target)
        for placeholder, table in placeholders.items():
            rewritten = rewritten.replace(placeholder, table)

        # ---- GATE -------------------------------------------------------
        # Anything left after removing explicitly shared, read-only references
        # means an operational read/write could still reach production.
        gated = rewritten
        for table in SHARED_READ_TABLES:
            gated = gated.replace(table, "")
        if PROD in gated or "hermes_union" in gated:
            bad = [
                f"    line {i}: {ln.strip()[:100]}"
                for i, ln in enumerate(gated.splitlines(), 1)
                if "hermes_union" in ln or PROD in ln
            ]
            failures.append(f"  {step}\n" + "\n".join(bad))
            continue

        path = out / step
        path.write_text(rewritten, encoding="utf-8")
        written.append((step, len(rewritten), rewritten.count(target)))

    if failures:
        print("GATE FAILED -- production references survived the rewrite:")
        print("\n".join(failures))
        print("\nNothing written. Fix the rewrite before running anything.")
        return 1

    print(f"GATE PASSED -- 0 unsafe references to `{PROD}` in {len(written)} file(s)")
    for step, size, refs in written:
        print(f"  {step:<48} {size:>7,} chars  {refs:>3} -> {dataset}")
    print(f"\nwrote to {out}")
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--day", required=True)
    ap.add_argument("--dataset", default="hermes_staging")
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument(
        "--detection-granularity",
        choices=("maxmind_city", "metro"),
        default="maxmind_city",
    )
    a = ap.parse_args()
    sys.exit(build(a.day, a.dataset, a.out, a.detection_granularity))

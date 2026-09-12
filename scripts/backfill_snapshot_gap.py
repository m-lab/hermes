#!/usr/bin/env python3
"""Days between a date and the nearest IPInfo dump actually on disk.

This exists because the failure it guards against is silent. ``closest_snapshot``
picks the nearest surviving dump and logs nothing about how far away it is, so a
backfill whose era's dumps were never restored from Deep Archive geolocates
against a database months off and produces a normal-looking partition.

Mirrors ``hermes.enrichment.ipinfo.enricher.closest_snapshot``: nearest by absolute
day distance, ties to the earlier dump.

Prints ``<gap_days> <snapshot_basename>``; exits 1 if the cache holds no dumps.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from datetime import date

PATTERN = re.compile(r"^ipinfo_(\d{4}-\d{2}-\d{2})\.snapshot$")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("target", type=date.fromisoformat)
    ap.add_argument("--cache", default=os.path.expanduser("~/hermes-docker-cache"))
    args = ap.parse_args()

    snaps = []
    for name in os.listdir(args.cache):
        m = PATTERN.match(name)
        if m:
            snaps.append((date.fromisoformat(m.group(1)), name))
    if not snaps:
        print("no IPInfo snapshots in cache", file=sys.stderr)
        return 1

    d, name = min(snaps, key=lambda s: (abs((s[0] - args.target).days), s[0]))
    print(f"{abs((d - args.target).days)} {name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

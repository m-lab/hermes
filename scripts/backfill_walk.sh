#!/bin/bash
# Spend the day's leftover BigQuery budget on backfill, walking hermes_union
# backwards to a floor date.
#
# Runs after the day run (union-pipeline.timer, 15:00 UTC), on the same PT quota
# day, and sizes its chunk from whatever that run left behind. Deliberately a
# SEPARATE unit from the nightly rather than an extra phase inside it: the day run
# is production and must not acquire a new way to fail. If this script dies, the
# published record is still current.
#
# Everything up to the docker run is a refusal. Chunk selection lives in
# backfill_next_chunk.py, which reads live coverage so a failed chunk retries
# instead of being silently stepped over.
#
# Sizing is measured, not estimated (2025-06-13..19, seven dates, 2026-08-25):
# 9.64 TiB in 4h47m, i.e. ~1.38 TiB/date including the auto-baseline step 01 for
# the next chunk. A chunk must not START unless a whole chunk's worth of budget
# remains -- an earlier version checked a threshold mid-chunk, ran through the cap
# and failed 17 steps. That is why the size is computed up front and never revised
# while running.
set -uo pipefail

REPO="${REPO:-$HOME/hermes-build-gate}"
VENV_PY="${VENV_PY:-$HOME/hermes-code-union/venv/bin/python}"
IMAGE="${IMAGE:-hermes-pipeline:latest}"
CACHE="${CACHE:-$HOME/hermes-docker-cache}"

FLOOR="${FLOOR:-2025-01-25}"   # oldest IPInfo dump in existence; see below
CEIL="${CEIL:-2025-06-12}"     # day before current contiguous coverage starts

# --- the budget ----------------------------------------------------------------
# TARGET_TIB is the ceiling this whole box may bill in one PT day, NOT the quota.
# The admin quota is ~15 TiB/user/PT-day and blowing it 403s *every* subsequent
# query that day, including the next nightly and every dashboard. 12 leaves ~3 TiB
# of headroom for ad-hoc work and for the nightly overrunning its usual ~1.45 TiB.
TARGET_TIB="${TARGET_TIB:-12.0}"
TIB_PER_DATE="${TIB_PER_DATE:-1.38}"
# One source of truth: passed to the picker AND to the pipeline, so the dates it
# selects can never be written at a granularity the run then refuses.
GRANULARITY="${GRANULARITY:-metro}"
MIN_CHUNK="${MIN_CHUNK:-2}"    # below this, wait for tomorrow rather than dribble
MAX_CHUNK="${MAX_CHUNK:-10}"   # ~7 h at the measured rate; keeps us clear of 15:00
# DRY_RUN=1 exercises every refusal, the budget arithmetic, the picker and the
# geolocation guard for real, then hands --dry-run to the pipeline so nothing is
# written. This is how the driver gets verified on the box before it is enabled.
DRY_RUN="${DRY_RUN:-0}"
NIGHTLY_GUARD_MIN="${NIGHTLY_GUARD_MIN:-240}"  # refuse to start within this of 15:00 UTC
# Calibrated, not guessed. With 2025-01-25 / 02-19 / 03-23 restored alongside the
# 2025-05-13-and-later dumps already cached, the widest legitimate gap any chunk
# midpoint in [2025-01-25, 2025-06-12] sees is ~26 d (mid-April, between the 03-23
# and 05-13 dumps). Without those three restored, a mid-March chunk sits 64 d from
# 2025-05-13 -- so anything above ~45 means the era's dumps are not on disk.
MAX_SNAPSHOT_GAP_DAYS="${MAX_SNAPSHOT_GAP_DAYS:-45}"

STATE="${STATE:-$HOME/.hermes-backfill}"
# Per-granularity: a date retired for a metro run may be perfectly fillable at
# maxmind_city, so one shared list would leak a refusal across regimes.
SKIPS="$STATE/skip-dates-$GRANULARITY.txt"
LOG="${LOG:-$HOME/logs/backfill_walk.log}"
STATUS="${STATUS:-$HOME/logs/backfill_status.txt}"

mkdir -p "$STATE" "$(dirname "$LOG")"; touch "$SKIPS"
log() { printf '%s %s\n' "$(date -u +%FT%TZ)" "$*" | tee -a "$LOG"; }
say() { printf '%s %s\n' "$(date -u +%FT%TZ)" "$*" > "$STATUS"; log "$*"; }

# --- refusal 1: the VM hosts one 28g container at a time -----------------------
# A second meets the OOM killer, not a queue. The nightly owns 15:00-~16:00 UTC.
if docker ps --format '{{.Names}}' | grep -q .; then
  say "SKIP: a container is already running; not starting"
  exit 0
fi

# --- refusal 2: do not start where the nightly would land on top of us ---------
H=$(date -u +%H); M=$(date -u +%M)
MINS=$(( (15 * 60) - (10#$H * 60 + 10#$M) ))
if [ $MINS -gt 0 ] && [ $MINS -lt $NIGHTLY_GUARD_MIN ]; then
  say "SKIP: nightly in ${MINS}m"
  exit 0
fi

# --- refusal 3: quota, treated as untrusted ------------------------------------
# quota_used.py ends in `except Exception: print(0.0)`, so it reports 0.0 on ANY
# failure -- including the ADC invalid_grant that happens every ~2 weeks. A 0.0
# would read as "nothing used, go ahead" and wave every chunk through. The driver
# only ever runs after the nightly, which always bills something, so a genuine 0.0
# does not occur; aborting on a legitimately idle day is the deliberate trade.
Q=$($VENV_PY "$HOME/quota_used.py" 2>/dev/null | tail -1)
case "$Q" in
  ''|*[!0-9.]*) say "ABORT: quota reading '$Q' is not numeric -- refusing to run blind"; exit 1 ;;
esac
if awk "BEGIN{exit !($Q <= 0)}"; then
  say "ABORT: quota reads 0.0, which quota_used.py also prints on failure (check ADC)"
  exit 1
fi
# --- size the chunk to what the day run left ------------------------------------
# Integer date count, floored: a partial date is not a thing the pipeline can run,
# and rounding up is how you land past the cap.
CHUNK=$(awk "BEGIN{n=int(($TARGET_TIB - $Q) / $TIB_PER_DATE); print (n<0)?0:n}")
if [ "$CHUNK" -gt "$MAX_CHUNK" ]; then CHUNK=$MAX_CHUNK; fi
REMAIN=$(awk "BEGIN{printf \"%.2f\", $TARGET_TIB - $Q}")
if [ "$CHUNK" -lt "$MIN_CHUNK" ]; then
  say "SKIP: ${REMAIN} TiB left of the ${TARGET_TIB} TiB target fits only ${CHUNK} date(s); minimum is ${MIN_CHUNK}"
  exit 0
fi
log "budget: ${Q} TiB used, ${REMAIN} TiB left of ${TARGET_TIB} -> ${CHUNK} date(s) at ${TIB_PER_DATE} TiB each"

# --- pick the work -------------------------------------------------------------
ITEM=$($VENV_PY "$REPO/scripts/backfill_next_chunk.py" \
        --floor "$FLOOR" --ceil "$CEIL" --chunk "$CHUNK" --skip-file "$SKIPS" \
        --granularity "$GRANULARITY" 2>>"$LOG")
if [ -z "$ITEM" ]; then say "ABORT: chunk picker produced nothing (see $LOG)"; exit 1; fi
set -- $ITEM
MODE=$1; shift

if [ "$MODE" = "COMPLETE" ]; then
  say "COMPLETE: ${FLOOR}..${CEIL} fully covered; disable with: systemctl --user disable hermes-backfill.timer"
  exit 0
fi

if [ "$MODE" = "PHASE_E" ]; then
  # Upstream exists; only the public table is missing. The dates need not be
  # contiguous, so the span is handed to --fill-missing, which processes only the
  # absent partitions and leaves the rest alone. step_already_done (union.py:740)
  # then skips 01-05 per date because their output tables already hold it, and
  # Phase D skips on the same basis, so this really is Phase E only -- ~11 GB/date
  # (measured: step 07 billed 10.95 GB in the 2026-09-03 nightly). 07 self-deletes
  # its partition, so it is idempotent.
  # NB a --dry-run of this prints steps 01-07 for every date and is NOT evidence
  # to the contrary: union.py:1229 is a separate early-return path that enumerates
  # SQL_FILES unconditionally and never consults the resume guard.
  # --skip-data-check because these dates demonstrably have input: their upstream
  # partitions are populated. It saves ~34 GiB per date of NDT availability scan.
  S=$1; for D in "$@"; do E=$D; done
  EXTRA="--fill-missing --skip-data-check"
  say "PHASE_E repair for $# date(s): $*"
else
  S=$1; E=$2
  # --fill-missing so a chunk that partially landed earlier retries only its holes.
  # Coverage is read from INFORMATION_SCHEMA, so it costs nothing.
  EXTRA="--fill-missing"
  say "WALK chunk ${S}..${E} (quota ${Q} TiB)"

  # --- refusal 4: geolocation provenance --------------------------------------
  # Only meaningful for a walk chunk: Phase E does no enrichment, so no dump is
  # consulted. The dump nearest the chunk must actually be on disk. Never
  # restoring one raises nothing at run time -- closest_snapshot silently falls
  # back to the nearest survivor -- so without this check a chunk in spring 2025
  # would geolocate against the 2025-05-13 dump and look entirely normal.
  # Nothing older than 2025-01-25 exists anywhere, which is why FLOOR is that date.
  HALF_SPAN=$(( ( $(date -u -d "$E" +%s) - $(date -u -d "$S" +%s) ) / 172800 ))
  MID=$(date -u -d "$S + ${HALF_SPAN} days" +%F)
  GAP=$($VENV_PY "$REPO/scripts/backfill_snapshot_gap.py" "$MID" --cache "$CACHE" 2>>"$LOG")
  if [ -z "$GAP" ]; then say "ABORT: could not read IPInfo cache"; exit 1; fi
  set -- $GAP
  if [ "$1" -gt "$MAX_SNAPSHOT_GAP_DAYS" ]; then
    say "ABORT: nearest IPInfo dump to ${MID} is $2 ($1 d away, limit ${MAX_SNAPSHOT_GAP_DAYS}). Restore that era's dumps from Deep Archive first."
    exit 1
  fi
  log "geolocation: ${MID} -> $2 ($1 d)"
fi

# --- run ------------------------------------------------------------------------
BEFORE=$Q
if [ "$DRY_RUN" = "1" ]; then
  EXTRA="$EXTRA --dry-run"
  log "DRY_RUN=1: pipeline invoked with --dry-run; no partition will be written"
fi
docker run --rm --name hermes-backfill --memory=28g --memory-swap=28g \
  --env-file /etc/hermes/hermes.env \
  -e GOOGLE_APPLICATION_CREDENTIALS=/app/credentials.json \
  -v "$CACHE":/app/cache \
  -v "$HOME/.config/gcloud/application_default_credentials.json":/app/credentials.json:ro \
  "$IMAGE" \
  --target prod --detection-granularity "$GRANULARITY" \
  --start-date "$S" --end-date "$E" --tomography-workers 1 $EXTRA >>"$LOG" 2>&1
RC=$?

AFTER=$($VENV_PY "$HOME/quota_used.py" 2>/dev/null | tail -1)

# --- converge: a chunk that lands nothing twice is retired ----------------------
# Without this, a date with no NDT input is picked every single day and burns a
# chunk's quota producing nothing, and the walk never reaches the floor.
NEW=$($VENV_PY - "$S" "$E" <<'PY' 2>>"$LOG"
import sys, warnings; warnings.filterwarnings("ignore")
from datetime import date, timedelta
from google.cloud import bigquery
s, e = date.fromisoformat(sys.argv[1]), date.fromisoformat(sys.argv[2])
c = bigquery.Client(project="mlab-collaboration")
q = """SELECT PARSE_DATE('%Y%m%d', partition_id) d
       FROM `mlab-collaboration.hermes_union.INFORMATION_SCHEMA.PARTITIONS`
       WHERE table_name='events_explained_daily' AND total_rows>0
         AND partition_id NOT IN ('__NULL__','__UNPARTITIONED__')"""
have = {r.d for r in c.query(q).result()}
want = {s + timedelta(i) for i in range((e - s).days + 1)}
print(" ".join(d.isoformat() for d in sorted(want - have)))
PY
)

# A dry run writes nothing, so every date is still "missing". Retiring dates on
# that basis would poison the skip file and permanently exclude good dates.
if [ -n "$NEW" ] && [ "$DRY_RUN" != "1" ]; then
  for D in $NEW; do
    A="$STATE/attempts-$D"
    N=$(( $(cat "$A" 2>/dev/null || echo 0) + 1 ))
    echo "$N" > "$A"
    if [ "$N" -ge 2 ]; then
      echo "$D" >> "$SKIPS"
      log "retiring $D after $N attempts with no partition written"
    fi
  done
fi

say "DONE rc=${RC} ${MODE} ${S}..${E} cost $(awk "BEGIN{printf \"%.2f\", $AFTER-$BEFORE}") TiB, still missing: ${NEW:-none}"
exit 0

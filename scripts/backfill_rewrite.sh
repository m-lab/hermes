#!/bin/bash
# Rewrite already-published hermes_union dates with the current code, oldest first,
# one budget-sized chunk per day.
#
# backfill_walk.sh fills HOLES (--fill-missing) walking backwards. This driver
# re-processes dates that already exist, which a correctness fix requires, and it
# must go FORWARD: step 04's giga_meter_measurements dedup only looks back 7 days,
# so a date has to be rewritten after the dates before it.
#
# Same refusals as backfill_walk.sh (one container on the host, keep clear of the
# 15:00 UTC day run, never run blind on the quota). Progress lives in a cursor file
# holding the next date to rewrite. The cursor only advances over dates whose
# outputs were verifiably written by THIS run; a failed date is retried from where
# it failed, and a date that fails twice stops the rewrite for a human to look at
# rather than being skipped -- a rewrite with a silent hole in it is worse than a
# paused one.
#
# --allow-giga-out-of-order: check_giga_order refuses step 04 for a date while a
# LATER giga partition exists, because 04's dedup cannot see forward. During this
# rewrite every later partition is either an old one that a later chunk will delete
# and regenerate (--delete-first), or one the day run writes while the rewrite is in
# progress, which the final chunks also rewrite (the rewrite only finishes when the
# cursor passes the newest published date). Any duplicate the forward-blind dedup
# lets through therefore lands in a partition that is regenerated afterwards.
set -uo pipefail

REPO="${REPO:-$HOME/hermes-build-gate}"
VENV_PY="${VENV_PY:-$HOME/hermes-code-union/venv/bin/python}"
IMAGE="${IMAGE:-hermes-pipeline:latest}"
CACHE="${CACHE:-$HOME/hermes-docker-cache}"

FLOOR="${FLOOR:?set FLOOR to the first date to rewrite, e.g. 2026-08-01}"
TARGET_TIB="${TARGET_TIB:-12.0}"
TIB_PER_DATE="${TIB_PER_DATE:-1.45}"
GRANULARITY="${GRANULARITY:-metro}"
MIN_CHUNK="${MIN_CHUNK:-2}"
MAX_CHUNK="${MAX_CHUNK:-10}"
DRY_RUN="${DRY_RUN:-0}"
NIGHTLY_GUARD_MIN="${NIGHTLY_GUARD_MIN:-240}"
MAX_ATTEMPTS="${MAX_ATTEMPTS:-2}"

STATE="${STATE:-$HOME/.hermes-rewrite}"
CURSOR="$STATE/cursor-$GRANULARITY"
LOG="${LOG:-$HOME/logs/backfill_rewrite.log}"
STATUS="${STATUS:-$HOME/logs/backfill_rewrite_status.txt}"

mkdir -p "$STATE" "$(dirname "$LOG")"
log() { printf '%s %s\n' "$(date -u +%FT%TZ)" "$*" | tee -a "$LOG"; }
say() { printf '%s %s\n' "$(date -u +%FT%TZ)" "$*" > "$STATUS"; log "$*"; }

[ -f "$CURSOR" ] || echo "$FLOOR" > "$CURSOR"
NEXT=$(cat "$CURSOR")

# --- refusal 1: one 28g container at a time ------------------------------------
if docker ps --format '{{.Names}}' | grep -q .; then
  say "SKIP: a container is already running; not starting"
  exit 0
fi

# --- refusal 2: keep clear of the 15:00 UTC day run ----------------------------
H=$(date -u +%H); M=$(date -u +%M)
MINS=$(( (15 * 60) - (10#$H * 60 + 10#$M) ))
if [ $MINS -gt 0 ] && [ $MINS -lt $NIGHTLY_GUARD_MIN ]; then
  say "SKIP: day run in ${MINS}m"
  exit 0
fi

# --- refusal 3: quota, treated as untrusted (see backfill_walk.sh) -------------
Q=$($VENV_PY "$HOME/quota_used.py" 2>/dev/null | tail -1)
case "$Q" in
  ''|*[!0-9.]*) say "ABORT: quota reading '$Q' is not numeric -- refusing to run blind"; exit 1 ;;
esac
if awk "BEGIN{exit !($Q <= 0)}"; then
  say "ABORT: quota reads 0.0, which quota_used.py also prints on failure (check ADC)"
  exit 1
fi

# --- how far to go: the newest date the day run has published ------------------
LATEST=$($VENV_PY - <<'PY' 2>>"$LOG"
import warnings; warnings.filterwarnings("ignore")
from google.cloud import bigquery
c = bigquery.Client(project="mlab-collaboration")
q = """SELECT MAX(PARSE_DATE('%Y%m%d', partition_id)) d
       FROM `mlab-collaboration.hermes_union.INFORMATION_SCHEMA.PARTITIONS`
       WHERE table_name = 'events_explained_daily' AND total_rows > 0
         AND partition_id NOT IN ('__NULL__', '__UNPARTITIONED__')"""
print(list(c.query(q).result())[0].d)
PY
)
case "$LATEST" in
  [0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]) ;;
  *) say "ABORT: could not read the newest published date ('$LATEST')"; exit 1 ;;
esac
if [[ "$NEXT" > "$LATEST" ]]; then
  say "COMPLETE: rewrote ${FLOOR}..${LATEST}; disable the timer (the day run keeps it current)"
  exit 0
fi

# --- size the chunk ------------------------------------------------------------
CHUNK=$(awk "BEGIN{n=int(($TARGET_TIB - $Q) / $TIB_PER_DATE); print (n<0)?0:n}")
if [ "$CHUNK" -gt "$MAX_CHUNK" ]; then CHUNK=$MAX_CHUNK; fi
REMAIN=$(awk "BEGIN{printf \"%.2f\", $TARGET_TIB - $Q}")
if [ "$CHUNK" -lt "$MIN_CHUNK" ]; then
  say "SKIP: ${REMAIN} TiB left of ${TARGET_TIB} fits only ${CHUNK} date(s); minimum is ${MIN_CHUNK}"
  exit 0
fi
S="$NEXT"
E=$(date -u -d "$S + $((CHUNK - 1)) days" +%F)
if [[ "$E" > "$LATEST" ]]; then E="$LATEST"; fi
log "budget: ${Q} TiB used, ${REMAIN} TiB left -> rewriting ${S}..${E}"

# --- refusal 4: a date that already failed MAX_ATTEMPTS times ------------------
A="$STATE/attempts-$S"
N=$(cat "$A" 2>/dev/null || echo 0)
if [ "$N" -ge "$MAX_ATTEMPTS" ]; then
  say "STOPPED: ${S} failed ${N} times; inspect $LOG, then delete $A to retry"
  exit 1
fi

# --- run ------------------------------------------------------------------------
START_TS=$(date -u +%FT%TZ)
EXTRA=""
if [ "$DRY_RUN" = "1" ]; then
  EXTRA="--dry-run"
  log "DRY_RUN=1: pipeline invoked with --dry-run; nothing is deleted or written"
fi
say "REWRITE ${S}..${E} (quota ${Q} TiB)"
docker run --rm --name hermes-rewrite --memory=28g --memory-swap=28g \
  --env-file /etc/hermes/hermes.env \
  -e GOOGLE_APPLICATION_CREDENTIALS=/app/credentials.json \
  -v "$CACHE":/app/cache \
  -v "$HOME/.config/gcloud/application_default_credentials.json":/app/credentials.json:ro \
  "$IMAGE" \
  --target prod --detection-granularity "$GRANULARITY" \
  --start-date "$S" --end-date "$E" --force-rerun --delete-first \
  --allow-giga-out-of-order --tomography-workers 1 $EXTRA >>"$LOG" 2>&1
RC=$?
AFTER=$($VENV_PY "$HOME/quota_used.py" 2>/dev/null | tail -1)

if [ "$DRY_RUN" = "1" ]; then
  say "DRY_RUN done rc=${RC} for ${S}..${E}; cursor unchanged at ${S}"
  exit 0
fi

# --- verify: advance only over dates rewritten by this run ----------------------
# A date counts when both the mapping table and the public table have a non-empty
# partition modified after this run started. Streaming tables are not checked here
# (INFORMATION_SCHEMA lags them); the pipeline's own Phase-E integrity check fails
# the run if correlation output is missing.
DONE_UNTIL=$($VENV_PY - "$S" "$E" "$START_TS" <<'PY' 2>>"$LOG"
import sys, warnings; warnings.filterwarnings("ignore")
from datetime import date, datetime, timedelta, timezone
from google.cloud import bigquery
s, e = date.fromisoformat(sys.argv[1]), date.fromisoformat(sys.argv[2])
t0 = datetime.strptime(sys.argv[3], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
c = bigquery.Client(project="mlab-collaboration")
q = """SELECT table_name, PARSE_DATE('%Y%m%d', partition_id) d, total_rows, last_modified_time
       FROM `mlab-collaboration.hermes_union.INFORMATION_SCHEMA.PARTITIONS`
       WHERE table_name IN ('events_with_as_and_geoloc', 'events_explained_daily')
         AND partition_id BETWEEN @s AND @e"""
cfg = bigquery.QueryJobConfig(query_parameters=[
    bigquery.ScalarQueryParameter("s", "STRING", s.strftime("%Y%m%d")),
    bigquery.ScalarQueryParameter("e", "STRING", e.strftime("%Y%m%d"))])
fresh = {}
for r in c.query(q, job_config=cfg).result():
    if r.total_rows > 0 and r.last_modified_time >= t0:
        fresh.setdefault(r.d, set()).add(r.table_name)
d, last_ok = s, None
while d <= e and fresh.get(d) == {"events_with_as_and_geoloc", "events_explained_daily"}:
    last_ok, d = d, d + timedelta(days=1)
print(last_ok.isoformat() if last_ok else "NONE")
PY
)
COST=$(awk "BEGIN{printf \"%.2f\", $AFTER-$Q}")
if [ "$DONE_UNTIL" = "NONE" ] || [ -z "$DONE_UNTIL" ]; then
  echo $((N + 1)) > "$A"
  say "FAILED rc=${RC}: nothing rewritten in ${S}..${E} (attempt $((N + 1))/${MAX_ATTEMPTS}), cost ${COST} TiB; cursor stays at ${S}"
  exit 1
fi
NEW_NEXT=$(date -u -d "$DONE_UNTIL + 1 day" +%F)
echo "$NEW_NEXT" > "$CURSOR"
rm -f "$A"
if [ "$DONE_UNTIL" != "$E" ]; then
  say "PARTIAL rc=${RC}: rewrote ${S}..${DONE_UNTIL} of ${S}..${E}, cost ${COST} TiB; next ${NEW_NEXT}"
  exit 1
fi
say "DONE rc=${RC}: rewrote ${S}..${E}, cost ${COST} TiB; next ${NEW_NEXT}"
exit 0

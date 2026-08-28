#!/bin/bash
# Restore archived IPInfo snapshots from Deep Archive onto hermes-ec2 and keep
# them in the pipeline cache for a bounded window (default 7 days).
#
# TWO SEPARATE CLOCKS -- do not conflate them:
#   * restore-object --restore-request Days=N  -> how long the temporary copy
#     stays readable IN S3. Only needs to cover the download; N=7 also buys a
#     free re-fetch during the week without another 48 h Bulk wait.
#   * the ledger expiry below                 -> how long the file stays ON THE
#     VM. S3 has no say in this; nothing deletes a local file but us.
#
#   ./restore_from_glacier.sh request 2026-03-18 2026-03-19   # kick off (Bulk ~48 h)
#   ./restore_from_glacier.sh status  2026-03-18 2026-03-19   # poll
#   ./restore_from_glacier.sh fetch   2026-03-18 2026-03-19   # download + start the 7-day clock
#   ./restore_from_glacier.sh ledger                          # what is on loan, and until when
#   ./restore_from_glacier.sh reap                            # delete expired loans (idempotent)
#   ./restore_from_glacier.sh keep    2026-03-18              # cancel the clock, keep permanently
set -euo pipefail

BUCKET="${BUCKET:?set BUCKET}"
REGION="${REGION:-us-east-2}"
PREFIX="${PREFIX:-ipinfo}"
CACHE="${CACHE:-$HOME/hermes-docker-cache}"
LEDGER="${LEDGER:-$HOME/.hermes-ipinfo-loans.tsv}"   # name<TAB>expiry_epoch<TAB>size<TAB>sha256
LOG="${LOG:-$HOME/logs/ipinfo_restore.log}"
S3_DAYS="${S3_DAYS:-7}"     # S3-side availability window
VM_DAYS="${VM_DAYS:-7}"     # VM-side retention
TIER="${TIER:-Bulk}"        # Bulk ~48 h $0.0025/GB | Standard ~12 h $0.02/GB. Deep Archive has no Expedited.

mkdir -p "$(dirname "$LOG")"; touch "$LEDGER"
log() { printf '%s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" | tee -a "$LOG"; }

pipeline_running() {
    sudo docker ps --format '{{.Image}}' 2>/dev/null | grep -q 'hermes-pipeline'
}

usage() { echo "usage: $0 request|status|fetch|ledger|reap|keep [dates...]" >&2; exit 2; }
[ $# -ge 1 ] || usage
cmd="$1"; shift

case "$cmd" in

request)
  for d in "$@"; do
    aws s3api restore-object --bucket "$BUCKET" --key "$PREFIX/ipinfo_$d.snapshot" --region "$REGION" \
      --restore-request "{\"Days\":$S3_DAYS,\"GlacierJobParameters\":{\"Tier\":\"$TIER\"}}"
    log "requested $d (S3 Days=$S3_DAYS Tier=$TIER)"
  done
  echo "Bulk restores take up to 48 h. Poll with: $0 status $*"
  ;;

status)
  for d in "$@"; do
    r=$(aws s3api head-object --bucket "$BUCKET" --key "$PREFIX/ipinfo_$d.snapshot" \
          --region "$REGION" --query Restore --output text 2>/dev/null || echo NOT-REQUESTED)
    printf '%s  %s\n' "$d" "$r"
  done
  ;;

fetch)
  for d in "$@"; do
    key="$PREFIX/ipinfo_$d.snapshot"; dest="$CACHE/ipinfo_$d.snapshot"
    r=$(aws s3api head-object --bucket "$BUCKET" --key "$key" --region "$REGION" \
          --query Restore --output text 2>/dev/null || echo "")
    case "$r" in
      *'ongoing-request="false"'*) ;;
      *) log "SKIP $d: not restored yet (${r:-not requested})"; continue ;;
    esac
    aws s3 cp "s3://$BUCKET/$key" "$dest" --region "$REGION"
    want=$(aws s3api head-object --bucket "$BUCKET" --key "$key" --region "$REGION" \
             --query 'Metadata.sha256' --output text)
    got=$(sha256sum "$dest" | cut -d' ' -f1)
    if [ "$want" != "$got" ] && [ "$want" != "None" ]; then
      rm -f "$dest"; log "FAIL $d checksum mismatch want=$want got=$got -- removed partial file"; exit 1
    fi
    exp=$(( $(date -u +%s) + VM_DAYS*86400 ))
    grep -v "^ipinfo_$d.snapshot	" "$LEDGER" > "$LEDGER.tmp" 2>/dev/null || true
    printf 'ipinfo_%s.snapshot\t%s\t%s\t%s\n' "$d" "$exp" "$(stat -c %s "$dest")" "$got" >> "$LEDGER.tmp"
    mv "$LEDGER.tmp" "$LEDGER"
    log "fetched $d -> $dest (on loan until $(date -u -d @$exp +%Y-%m-%dT%H:%MZ))"
  done
  ;;

ledger)
  now=$(date -u +%s)
  printf '%-32s %-22s %s\n' SNAPSHOT 'EXPIRES (UTC)' STATE
  while IFS=$'\t' read -r n exp sz sha; do
    [ -n "${n:-}" ] || continue
    if   [ ! -f "$CACHE/$n" ];      then st="GONE"
    elif [ "$now" -ge "$exp" ];     then st="EXPIRED (reapable)"
    else st="$(( (exp-now)/86400 ))d $(( (exp-now)%86400/3600 ))h left"; fi
    printf '%-32s %-22s %s\n' "$n" "$(date -u -d @"$exp" +%Y-%m-%dT%H:%MZ)" "$st"
  done < "$LEDGER"
  ;;

reap)
  if pipeline_running; then log "reap deferred: a hermes-pipeline container is running"; exit 0; fi
  now=$(date -u +%s); kept=""
  while IFS=$'\t' read -r n exp sz sha; do
    [ -n "${n:-}" ] || continue
    f="$CACHE/$n"
    if [ "$now" -lt "$exp" ]; then kept+="$n	$exp	$sz	$sha"$'\n'; continue; fi
    if [ ! -f "$f" ]; then log "reap: $n already gone"; continue; fi
    # Never delete a file that is no longer the one we restored -- a nightly
    # download or a manual replacement takes precedence over the loan.
    if [ "$(stat -c %s "$f")" != "$sz" ]; then
      log "reap: $n changed on disk (size $(stat -c %s "$f") != $sz) -- leaving it, dropping the loan"; continue
    fi
    rm -f "$f"; log "reap: deleted expired loan $n (re-restore from s3://$BUCKET/$PREFIX/$n)"
  done < "$LEDGER"
  printf '%s' "$kept" > "$LEDGER"
  ;;

keep)
  for d in "$@"; do
    grep -v "^ipinfo_$d.snapshot	" "$LEDGER" > "$LEDGER.tmp" 2>/dev/null || true
    mv "$LEDGER.tmp" "$LEDGER"; log "keep: $d removed from the loan ledger, will not be reaped"
  done
  ;;

*) echo "unknown subcommand: $cmd" >&2; usage ;;
esac

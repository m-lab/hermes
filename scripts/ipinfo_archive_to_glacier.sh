#!/bin/bash
# Upload the IPInfo snapshots that exist ONLY on this laptop to S3 Glacier Deep
# Archive. One object per snapshot -- deliberately NOT tarred, so a backfill can
# restore just the dates it needs instead of the whole 34 GB.
#
# Bucket must be in us-east-2 (hermes-ec2's region) so S3->EC2 restore traffic
# is free. Prereq: `aws login` (the default profile's session expires).
set -euo pipefail

BUCKET="${BUCKET:-hermes-ipinfo-archive-627275104670}"
REGION=us-east-2
PREFIX=ipinfo
MANIFEST="${MANIFEST:-}"
if [ -z "$MANIFEST" ] || [ ! -f "$MANIFEST" ]; then
  echo "set MANIFEST=<file> -- one absolute path per line, the snapshots to archive." >&2
  echo "Build one with: find <cache dirs> -name 'ipinfo_*.snapshot' > /tmp/manifest.txt" >&2
  exit 2
fi

aws sts get-caller-identity >/dev/null   # fail fast on an expired session

while read -r f; do
  [ -f "$f" ] || { echo "MISSING, skipping: $f" >&2; continue; }
  key="$PREFIX/$(basename "$f")"
  if aws s3api head-object --bucket "$BUCKET" --key "$key" --region "$REGION" >/dev/null 2>&1; then
    echo "already archived: $key"; continue
  fi
  echo "uploading $key ($(du -h "$f" | cut -f1))"
  # NB: do NOT write this as `aws s3 cp ... && echo ok`. Under `set -e` a failure
  # on the LEFT of && does not exit the script, so a dead credential would grind
  # through the whole manifest failing 44 times instead of stopping at the first.
  # Run this with a STATIC IAM key (AWS_PROFILE=ipinfo-writer), never with
  # `aws login` credentials: long uploads fail with "CreateOAuth2Token ... grant is
  # invalid, expired, revoked, or malformed" even though short calls keep working.
  # Seen with both `s3 cp` and `put-object`, so it is not multipart threading; root
  # cause unconfirmed. See docs/reference/ipinfo-archive-s3.md.
  # put-object is used because every snapshot is under the 5 GB single-PUT ceiling.
  if aws s3api put-object \
      --bucket "$BUCKET" --key "$key" --body "$f" \
      --region "$REGION" \
      --storage-class DEEP_ARCHIVE \
      --checksum-algorithm SHA256 \
      --metadata "sha256=$(shasum -a 256 "$f" | cut -d' ' -f1)" \
      --output text --query 'ETag' >/dev/null; then
    echo "  ok $key"
  else
    echo "  FAILED $key -- stopping. Re-run after fixing; already-uploaded objects are skipped." >&2
    exit 1
  fi
done < "$MANIFEST"

echo "done. verify with: aws s3 ls s3://$BUCKET/$PREFIX/ --region $REGION --human-readable --summarize"

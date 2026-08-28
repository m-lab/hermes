# Archiving IPInfo snapshots to S3 Glacier Deep Archive

## Why this exists

IPInfo serves only the **current** `standard_location.mmdb`. There is no historical
endpoint. Any dump we no longer hold is gone permanently, and with it the ability to
reproduce the geolocation of any partition date that dump was pinned to (see
[geolocation-provenance.md](geolocation-provenance.md) and
`hermes.enrichment.ipinfo.enricher.closest_snapshot`).

As of 2026-08-28 the VM cache `hermes-ec2:~/hermes-docker-cache` covered
2025-05-13 → 2025-12-01, then **jumped to 2026-06-03** before becoming daily. The
2026-01-05 → 2026-06-11 dumps existed only on one laptop. This archive closes that
single-point-of-failure.

Deleting a snapshot does not raise an error at run time. `closest_snapshot` silently
picks the nearest surviving dump, so a backfill of a gap date would geolocate against
a database up to six months off and log nothing. That silence is the reason the
archive exists and the reason the reaper below is conservative.

## The bucket

| | |
|---|---|
| Bucket | `hermes-ipinfo-archive-627275104670` |
| Region | `us-east-2` — deliberately the same region as `hermes-ec2`, so restores download free |
| Account | `627275104670` (the `ls3748@columbia.edu` identity, via `aws login`) |
| Storage class | `DEEP_ARCHIVE` on every object |
| Key layout | `ipinfo/ipinfo_YYYY-MM-DD.snapshot`, one object per dump |
| Public access | fully blocked |

One object per dump, **not** a tarball: a backfill needs two or three dumps, never all
44, so per-object granularity makes restore latency and cost negligible.

Each object carries the source file's SHA-256 in user metadata (`Metadata.sha256`).
`ipinfo_restore_from_glacier.sh fetch` verifies against it and refuses a mismatch —
the same failure class as the 11 MB truncated `wget` stub that
`MIN_SNAPSHOT_BYTES` exists to reject.

## Cross-account gotcha

**`hermes-ec2` is in AWS account `662148474519`. The bucket is in `627275104670`.**
The instance ID resolves from the VM's own metadata service but returns
`InvalidInstanceID.NotFound` from the bucket's account, so an instance profile
**cannot** be attached from there. Same region, so transfer is still free.

Current auth: IAM user `hermes-ec2-ipinfo-reader` in `627275104670`, its access key
installed at `hermes-ec2:~/.aws/credentials` (mode 600). Its inline policy
`ipinfo-archive-read` allows only `s3:GetObject`, `s3:RestoreObject`,
`s3:ListBucket`, `s3:GetBucketLocation` on this one bucket. Verified on the VM:
`PutObject`, `DeleteObject` and `ListAllMyBuckets` are all denied.

Rotate or revoke with `aws iam delete-access-key --user-name hermes-ec2-ipinfo-reader`.
The better long-term design is a role in `662148474519` attached to the instance plus
a bucket policy trusting it — no long-lived key on the host. That needs credentials in
the M-Lab account, which the Columbia identity does not have.

## Two independent clocks — do not conflate them

| Knob | Controls | Default |
|---|---|---|
| `S3_DAYS` → `restore-object --restore-request Days=N` | how long the temporary copy is readable **in S3** | 7 |
| `VM_DAYS` → the loan ledger | how long the file stays **on the VM** | 7 |

S3 has no say over the VM's filesystem. Once `aws s3 cp` lands a file in the cache,
nothing removes it but the reaper. `S3_DAYS=7` is not required for the download — it
buys a free re-fetch during the week instead of another 48 h Bulk wait.

## Costs (33.69 GB, 44 objects, us-east-2)

| | |
|---|---|
| Storage | **$0.033/month** ($0.40/year) |
| Restore all 44, Bulk (~48 h) | $0.09 |
| Restore all 44, Standard (~12 h) | $0.68 |
| Temporary restored copy, 7 days | $0.18 |
| Download to `hermes-ec2` (same region) | free |

Deep Archive has **no Expedited tier** — 12 h Standard or 48 h Bulk only, so a restore
is always a plan-ahead operation. Minimum billable storage duration is **180 days**.

If same-day access is ever needed, Glacier Instant Retrieval holds the same data for
~$0.13/month with millisecond access.

## Runbook

Archive new snapshots (from the machine holding them):

```bash
AWS_PROFILE=ipinfo-writer \
MANIFEST=/tmp/manifest.txt \
BUCKET=hermes-ipinfo-archive-627275104670 \
  scripts/ipinfo_archive_to_glacier.sh
```

`AWS_PROFILE=ipinfo-writer` is required, not optional — see the traps section below.

Restore onto the VM for a week:

```bash
export BUCKET=hermes-ipinfo-archive-627275104670
scripts/ipinfo_restore_from_glacier.sh request 2026-03-18 2026-03-19   # Bulk, ~48 h
scripts/ipinfo_restore_from_glacier.sh status  2026-03-18 2026-03-19   # poll
scripts/ipinfo_restore_from_glacier.sh fetch   2026-03-18 2026-03-19   # download, start the clock
scripts/ipinfo_restore_from_glacier.sh ledger                          # what is on loan
scripts/ipinfo_restore_from_glacier.sh keep    2026-03-18              # cancel the clock, keep it
```

The reaper (`scripts/systemd/ipinfo-loan-reaper.{service,timer}`, 13:00 UTC daily —
after `hermes-wrapper` at 11:30 and before `union-pipeline` at 15:00) deletes expired
loans. Three guards:

1. **Only ledgered files are ever deleted.** The nightly cache is invisible to it.
2. **A file whose size no longer matches the ledger is left alone** and its loan
   dropped, so a nightly download or manual replacement wins over the loan.
3. **It self-defers while any `hermes-pipeline` container is running**, so a restored
   snapshot cannot vanish mid-backfill.

## Two traps this tooling was built around

**`aws login` credentials cannot carry a bulk upload; use a static IAM key.**
Long-running uploads fail with `CreateOAuth2Token ... The provided authorization
grant is invalid, expired, revoked, or malformed`, while short calls
(`sts get-caller-identity`, `head-object`) keep succeeding seconds later — so it
presents as an expired session and is not one.

Reproduced 2026-08-28 with **both** `aws s3 cp` (multi-threaded multipart) and
`aws s3api put-object` (a single request), which rules out multipart threading as the
cause. **The root cause is not confirmed.** What was observed: the cached token type
is `access_token_sigv4` (DPoP-bound, short-lived), `~/.aws/login/cache` held two
session files — one refreshed on demand, one six weeks stale — and the failure
correlates with request *duration*, not with elapsed session age.

The workaround, which works reliably: IAM user `hermes-ipinfo-writer` in account
627275104670 with a static access key in the local `ipinfo-writer` profile, scoped to
`PutObject`/`GetObject` on this bucket prefix. Static keys have no refresh path to
fail. Run uploads with `AWS_PROFILE=ipinfo-writer`.

The script keeps `s3api put-object` (every snapshot is under the 5 GB single-PUT
ceiling) because it is proven working; with static credentials `s3 cp` would also
work and would be faster on large files.

**`cmd && echo ok` silently disables `set -e`.** Under `set -e` a failure on the
**left** of `&&` does not exit the script; only the command after the final `&&`
does. An early version wrote the upload that way and ground through 16 objects
failing on a dead credential instead of stopping at the first. The script now uses
`if aws s3api put-object ...; then ... else exit 1; fi`.

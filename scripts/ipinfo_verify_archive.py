#!/usr/bin/env python3
"""Verify archived IPInfo snapshots against their local originals.

Runs entirely on object metadata -- it never restores anything, because a Deep
Archive read costs 12-48 h and money.

Per object:
  * ETag vs a locally recomputed one. For a single-request PUT the ETag is the
    plain MD5 of the bytes S3 stored, so this is genuine end-to-end integrity
    verification. For a multipart upload the ETag is md5(concat(part md5s))
    with a "-N" part-count suffix, which this script reconstructs -- comparing a
    whole-file MD5 against a multipart ETag reports a false mismatch.
  * Metadata.sha256 vs a freshly computed local SHA-256.
  * ContentLength vs local size, and StorageClass == DEEP_ARCHIVE.

Usage:  AWS_PROFILE=ipinfo-writer ./ipinfo_verify_archive.py <manifest>
Exits non-zero if anything fails, so it can gate a decision about the originals.
"""
import hashlib
import json
import os
import subprocess
import sys

BUCKET = os.environ.get("BUCKET", "hermes-ipinfo-archive-627275104670")
REGION = os.environ.get("REGION", "us-east-2")
PREFIX = os.environ.get("PREFIX", "ipinfo")
#: AWS CLI default multipart chunk size. Only used to reproduce a multipart ETag.
CHUNK = 8 * 1024 * 1024


def hashes(path, parts):
    """(md5_or_multipart_etag, sha256) for ``path``, in a single read."""
    whole, sha, digests = hashlib.md5(), hashlib.sha256(), []
    with open(path, "rb") as fh:
        while True:
            block = fh.read(CHUNK)
            if not block:
                break
            whole.update(block)
            sha.update(block)
            digests.append(hashlib.md5(block).digest())
    if parts:
        etag = hashlib.md5(b"".join(digests)).hexdigest() + f"-{len(digests)}"
    else:
        etag = whole.hexdigest()
    return etag, sha.hexdigest()


def head(key):
    out = subprocess.run(
        ["aws", "s3api", "head-object", "--bucket", BUCKET, "--key", key,
         "--region", REGION, "--output", "json"],
        capture_output=True, text=True)
    return json.loads(out.stdout) if out.returncode == 0 else None


def main():
    if len(sys.argv) != 2:
        sys.exit("usage: ipinfo_verify_archive.py <manifest>")
    paths = [l.strip() for l in open(sys.argv[1]) if l.strip()]
    ok = bad = 0
    for path in paths:
        key = f"{PREFIX}/{os.path.basename(path)}"
        meta = head(key)
        if meta is None:
            print(f"MISSING  {key}"); bad += 1; continue
        etag = meta["ETag"].strip('"')
        # A "-N" suffix means the object was uploaded multipart.
        local_etag, local_sha = hashes(path, parts="-" in etag)
        errs = []
        if local_etag != etag:
            errs.append(f"ETAG({local_etag} vs {etag})")
        if meta.get("Metadata", {}).get("sha256") not in (local_sha, None):
            errs.append("SHA256")
        if meta["ContentLength"] != os.path.getsize(path):
            errs.append(f"SIZE({os.path.getsize(path)} vs {meta['ContentLength']})")
        if meta.get("StorageClass") != "DEEP_ARCHIVE":
            errs.append(f"CLASS({meta.get('StorageClass')})")
        if errs:
            print(f"MISMATCH {key} -- {' '.join(errs)}"); bad += 1
        else:
            print(f"OK       {key}"); ok += 1
    print(f"---\nverified {ok}, problems {bad}")
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()

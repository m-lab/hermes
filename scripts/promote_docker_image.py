#!/usr/bin/env python3
"""Promote one validated Docker image to sandbox and production tags."""

from __future__ import annotations

import argparse
import subprocess
from collections.abc import Sequence


def _run(args: Sequence[str]) -> str:
    result = subprocess.run(args, check=True, capture_output=True, text=True)
    return result.stdout.strip()


def image_id(tag: str) -> str:
    """Resolve a Docker tag to its immutable local image ID."""
    return _run(("docker", "image", "inspect", "--format={{.Id}}", tag))


def promote(
    source_tag: str,
    *,
    sandbox_tag: str = "hermes-pipeline:sandbox",
    production_tag: str = "hermes-pipeline:latest",
) -> str:
    """Point sandbox first, then production, at exactly ``source_tag``'s image."""
    if sandbox_tag == production_tag:
        raise ValueError("sandbox and production tags must be different")

    source_id = image_id(source_tag)
    if not source_id:
        raise RuntimeError(f"Docker returned no image ID for {source_tag!r}")

    # Sandbox always advances first. If production tagging fails, production
    # remains on its prior image rather than getting ahead of sandbox.
    _run(("docker", "image", "tag", source_tag, sandbox_tag))
    if image_id(sandbox_tag) != source_id:
        raise RuntimeError("sandbox tag did not resolve to the validated source image")

    _run(("docker", "image", "tag", source_tag, production_tag))
    sandbox_id = image_id(sandbox_tag)
    production_id = image_id(production_tag)
    if sandbox_id != source_id or production_id != source_id:
        raise RuntimeError("promotion verification failed: sandbox and production differ")
    return source_id


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source_tag", help="validated candidate image tag")
    parser.add_argument("--sandbox-tag", default="hermes-pipeline:sandbox")
    parser.add_argument("--production-tag", default="hermes-pipeline:latest")
    args = parser.parse_args(argv)

    promoted_id = promote(
        args.source_tag,
        sandbox_tag=args.sandbox_tag,
        production_tag=args.production_tag,
    )
    print(
        f"Promoted {promoted_id}: {args.sandbox_tag} first, "
        f"then {args.production_tag}; both verified."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

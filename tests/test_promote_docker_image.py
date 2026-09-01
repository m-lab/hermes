"""Tests for sandbox-first Docker image promotion."""

from __future__ import annotations

import subprocess

import pytest
from scripts import promote_docker_image


def test_promotion_tags_sandbox_before_production_and_verifies(monkeypatch):
    calls: list[tuple[str, ...]] = []

    def fake_run(args, *, check, capture_output, text):
        command = tuple(args)
        calls.append(command)
        output = "sha256:candidate\n" if "inspect" in command else ""
        return subprocess.CompletedProcess(args, 0, stdout=output)

    monkeypatch.setattr(promote_docker_image.subprocess, "run", fake_run)

    result = promote_docker_image.promote("hermes-pipeline:candidate")

    assert result == "sha256:candidate"
    sandbox_tag = ("docker", "image", "tag", "hermes-pipeline:candidate", "hermes-pipeline:sandbox")
    production_tag = (
        "docker",
        "image",
        "tag",
        "hermes-pipeline:candidate",
        "hermes-pipeline:latest",
    )
    assert calls.index(sandbox_tag) < calls.index(production_tag)
    assert calls[-2:] == [
        ("docker", "image", "inspect", "--format={{.Id}}", "hermes-pipeline:sandbox"),
        ("docker", "image", "inspect", "--format={{.Id}}", "hermes-pipeline:latest"),
    ]


def test_promotion_rejects_one_tag_for_both_environments():
    with pytest.raises(ValueError, match="must be different"):
        promote_docker_image.promote(
            "hermes-pipeline:candidate",
            sandbox_tag="hermes-pipeline:latest",
            production_tag="hermes-pipeline:latest",
        )

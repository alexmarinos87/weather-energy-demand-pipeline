"""Exercise issue #99 through the real scenario CLI, not a mock evaluator."""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from interval_policy_evidence_fixtures import three_scenario_evidence
from forecasting import interval_policy_retained_compatibility_manifest as manifests
from forecasting import run_interval_policy_retained_compatibility as command
from forecasting._interval_policy_retained_compatibility_common import (
    IntervalPolicyRetainedCompatibilityError,
)

RUN_ID = "ipca-" + "9" * 24
ROLES = ("slices", "summary", "report", "manifest")


def output_paths(root: Path, output_format: str = "csv") -> dict[str, Path]:
    return {
        role: root / (
            f"interval_policy_retained_compatibility_{role}_{RUN_ID}."
            + ({"report": "md", "manifest": "json"}.get(role, output_format))
        )
        for role in ROLES
    }


@pytest.fixture
def invocation(tmp_path):
    _, checks, trends = three_scenario_evidence()
    checks_path, trends_path = tmp_path / "checks.csv", tmp_path / "trends.csv"
    checks.to_csv(checks_path, index=False)
    trends.to_csv(trends_path, index=False)
    output = tmp_path / "output"
    args = [
        "--slice-trends", str(trends_path), "--health-checks", str(checks_path),
        "--output-dir", str(output), "--compatibility-run-id", RUN_ID,
        "--compatibility-run-timestamp", "2026-01-20T01:00:00Z",
    ]
    return args, output, (checks_path, trends_path)


def intercept_publication(monkeypatch, observer):
    """Inject at the actual final-name operation on both old and new writers."""
    original_replace, original_link = Path.replace, Path.hardlink_to

    def replace(source, destination):
        observer(Path(destination))
        return original_replace(source, destination)

    def link(destination, source):
        observer(Path(destination))
        return original_link(destination, source)

    monkeypatch.setattr(Path, "replace", replace)
    monkeypatch.setattr(Path, "hardlink_to", link)


def assert_only(root: Path, expected: set[Path]) -> None:
    assert set(root.iterdir()) == expected


@pytest.mark.parametrize("output_format", ["csv", "parquet"])
def test_success_publishes_four_verifiable_files_without_mutating_inputs(
    invocation, output_format, capsys
):
    args, output, sources = invocation
    original = {path: path.read_bytes() for path in sources}
    assert command.main(args + ["--output-format", output_format]) == 0
    paths = output_paths(output, output_format)
    assert_only(output, set(paths.values()))
    reader = pd.read_csv if output_format == "csv" else pd.read_parquet
    summary = reader(paths["summary"])
    document = json.loads(paths["manifest"].read_text(encoding="utf-8"))
    manifests.verify_compatibility_manifest(document, summary, artifact_directory=output)
    tightened = summary.set_index("scenario").loc["tightened"]
    assert tightened["previous_policy_status"] == "healthy"
    assert tightened["current_policy_status"] == "failed"
    assert capsys.readouterr().out.count("Wrote ") == 4
    assert {path: path.read_bytes() for path in sources} == original


@pytest.mark.parametrize("output_format", ["csv", "parquet"])
@pytest.mark.parametrize("role", ROLES)
def test_collision_on_any_output_leaves_no_new_artifacts(
    invocation, output_format, role, capsys
):
    args, output, _ = invocation
    output.mkdir()
    existing = output_paths(output, output_format)[role]
    existing.write_bytes(b"retained-evidence")
    unrelated = output / "keep.txt"
    unrelated.write_bytes(b"unrelated")
    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        command.main(args + ["--output-format", output_format])
    assert existing.read_bytes() == b"retained-evidence"
    assert unrelated.read_bytes() == b"unrelated"
    assert_only(output, {existing, unrelated})
    assert "Wrote " not in capsys.readouterr().out


@pytest.mark.parametrize("role", ROLES)
def test_dangling_output_symlink_is_never_replaced(invocation, role):
    args, output, _ = invocation
    output.mkdir()
    existing = output_paths(output)[role]
    target = output.parent / "must-not-be-created"
    existing.symlink_to(target)
    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        command.main(args)
    assert existing.is_symlink()
    assert not target.exists()
    assert_only(output, {existing})


@pytest.mark.parametrize("output_format", ["csv", "parquet"])
def test_serialization_failure_publishes_nothing(invocation, output_format, monkeypatch):
    args, output, _ = invocation
    method = "to_csv" if output_format == "csv" else "to_parquet"
    original = getattr(pd.DataFrame, method)

    def fail_summary(frame, *args, **kwargs):
        if "previous_policy_status" in frame.columns:
            raise OSError("injected summary serialization failure")
        return original(frame, *args, **kwargs)

    monkeypatch.setattr(pd.DataFrame, method, fail_summary)
    with pytest.raises(OSError, match="injected summary"):
        command.main(args + ["--output-format", output_format])
    assert_only(output, set())


def test_manifest_validation_failure_publishes_nothing(invocation, monkeypatch):
    args, output, _ = invocation

    def reject(*args, **kwargs):
        raise IntervalPolicyRetainedCompatibilityError("injected manifest rejection")

    monkeypatch.setattr(manifests, "verify_compatibility_manifest", reject)
    with pytest.raises(IntervalPolicyRetainedCompatibilityError, match="injected manifest"):
        command.main(args)
    assert_only(output, set())


@pytest.mark.parametrize("role", ROLES)
def test_competing_publication_is_not_replaced_or_deleted(invocation, role, monkeypatch):
    args, output, _ = invocation
    collision = output_paths(output)[role]

    def compete(destination):
        if destination == collision:
            collision.write_bytes(b"competing-evidence")

    intercept_publication(monkeypatch, compete)
    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        command.main(args)
    assert collision.read_bytes() == b"competing-evidence"
    assert_only(output, {collision})


@pytest.mark.parametrize("failure", [OSError, KeyboardInterrupt])
def test_publication_failure_rolls_back_own_files(invocation, failure, monkeypatch, capsys):
    args, output, _ = invocation
    report = output_paths(output)["report"]

    def interrupt(destination):
        if destination == report:
            raise failure("injected publication interruption")

    intercept_publication(monkeypatch, interrupt)
    with pytest.raises(failure, match="injected publication"):
        command.main(args)
    assert_only(output, set())
    assert "Wrote " not in capsys.readouterr().out


def test_rollback_does_not_delete_a_replacement_file(invocation, monkeypatch):
    args, output, _ = invocation
    paths = output_paths(output)

    def replace_before_failure(destination):
        if destination == paths["report"]:
            paths["slices"].unlink()
            paths["slices"].write_bytes(b"replacement-owned-by-someone-else")
            raise OSError("injected failure after replacement")

    intercept_publication(monkeypatch, replace_before_failure)
    with pytest.raises(OSError, match="injected failure"):
        command.main(args)
    assert paths["slices"].read_bytes() == b"replacement-owned-by-someone-else"
    assert_only(output, {paths["slices"]})


def test_bundle_is_verified_before_publication_and_manifest_is_last(invocation, monkeypatch):
    args, output, _ = invocation
    paths = output_paths(output)
    original = manifests.verify_compatibility_manifest
    verified = []
    published = []

    def verify_staging(*args, **kwargs):
        assert not any(path.exists() for path in paths.values())
        result = original(*args, **kwargs)
        verified.append(True)
        return result

    def observe(destination):
        if destination in paths.values():
            assert verified, "Final files must not be exposed before verification"
            published.append(destination)
            if destination == paths["manifest"]:
                assert all(paths[role].is_file() for role in ROLES if role != "manifest")

    monkeypatch.setattr(manifests, "verify_compatibility_manifest", verify_staging)
    intercept_publication(monkeypatch, observe)
    assert command.main(args) == 0
    assert published == list(paths.values())

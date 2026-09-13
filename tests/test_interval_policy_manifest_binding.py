"""Regression evidence for #98; hashes alone do not establish valid bindings."""
from __future__ import annotations

from copy import deepcopy
import hashlib
from pathlib import Path

import pandas as pd
import pytest

from forecasting._interval_policy_candidate_revision_common import digest
from forecasting._interval_policy_retained_compatibility_common import (
    IntervalPolicyRetainedCompatibilityError,
    compatibility_summary_sha256,
)
from forecasting.interval_policy_retained_compatibility_manifest import (
    build_compatibility_manifest,
    verify_compatibility_manifest,
)
from forecasting.interval_policy_retained_compatibility_review import (
    IntervalPolicyRetainedCompatibilityReviewError,
    create_retained_compatibility_review,
)
from test_interval_policy_retained_compatibility import run


def reseal(document):
    document["manifest_sha256"] = digest({
        key: value for key, value in document.items() if key != "manifest_sha256"
    })
    return document


def make_bundle(directory: Path, output_format: str = "csv"):
    slices, summary, report = run()
    directory.mkdir(parents=True, exist_ok=True)
    paths = {
        "slices": directory / f"slices.{output_format}",
        "summary": directory / f"summary.{output_format}",
        "report": directory / "report.md",
    }
    for role, frame in (("slices", slices), ("summary", summary)):
        if output_format == "csv":
            frame.to_csv(paths[role], index=False)
        else:
            frame.to_parquet(paths[role], index=False)
    paths["report"].write_text(report, encoding="utf-8")
    return summary, paths, build_compatibility_manifest(summary, artifacts=paths)


@pytest.fixture
def bundle(tmp_path):
    return make_bundle(tmp_path)


def verify(bundle, document=None):
    summary, paths, original = bundle
    verify_compatibility_manifest(
        original if document is None else document,
        summary,
        artifact_directory=paths["summary"].parent,
    )


@pytest.mark.parametrize("output_format", ["csv", "parquet"])
def test_valid_bundle_round_trip_is_read_only(tmp_path, output_format):
    summary, paths, document = make_bundle(tmp_path, output_format)
    original_summary, original_document = summary.copy(deep=True), deepcopy(document)
    original_bytes = {role: path.read_bytes() for role, path in paths.items()}
    reader = pd.read_csv if output_format == "csv" else pd.read_parquet
    reopened = reader(paths["summary"]).iloc[::-1]
    verify_compatibility_manifest(document, reopened, artifact_directory=tmp_path)
    pd.testing.assert_frame_equal(summary, original_summary)
    assert document == original_document
    assert {role: path.read_bytes() for role, path in paths.items()} == original_bytes


@pytest.mark.parametrize("policy", ["previous_policy", "current_policy"])
@pytest.mark.parametrize("change", ["threshold", "missing", "extra", "version"])
def test_rehashed_policy_snapshot_tampering_is_rejected(bundle, policy, change):
    document = deepcopy(bundle[2])
    snapshot = document[policy]
    if change == "threshold":
        snapshot["max_evaluation_age_minutes"] = 999999
    elif change == "missing":
        del snapshot["min_calibration_observation_count"]
    elif change == "extra":
        snapshot["allow_automatic_promotion"] = True
    else:
        snapshot["candidate_version"] = "unreviewed-policy-version"
    with pytest.raises(IntervalPolicyRetainedCompatibilityError):
        verify(bundle, reseal(document))


@pytest.mark.parametrize("change", ["different", "missing", "naive"])
def test_manifest_timestamp_is_bound_to_summary(bundle, change):
    document = deepcopy(bundle[2])
    if change == "missing":
        del document["compatibility_run_timestamp_utc"]
    else:
        document["compatibility_run_timestamp_utc"] = (
            "2026-01-21T01:00:00Z" if change == "different" else "2026-01-20T01:00:00"
        )
    with pytest.raises(IntervalPolicyRetainedCompatibilityError):
        verify(bundle, reseal(document))


@pytest.mark.parametrize("location", ["document", "artifact"])
def test_runtime_schema_rejects_unknown_fields(bundle, location):
    document = deepcopy(bundle[2])
    target = document if location == "document" else document["artifacts"][0]
    target["unreviewed_field"] = "not in the manifest contract"
    with pytest.raises(IntervalPolicyRetainedCompatibilityError):
        verify(bundle, reseal(document))


def test_duplicate_artifact_role_is_rejected_even_with_valid_hashes(bundle):
    document = deepcopy(bundle[2])
    document["artifacts"].append(deepcopy(document["artifacts"][0]))
    with pytest.raises(IntervalPolicyRetainedCompatibilityError):
        verify(bundle, reseal(document))


def test_distinct_roles_cannot_point_to_one_file(bundle):
    document = deepcopy(bundle[2])
    source = document["artifacts"][0]
    document["artifacts"][2].update(filename=source["filename"], sha256=source["sha256"])
    with pytest.raises(IntervalPolicyRetainedCompatibilityError):
        verify(bundle, reseal(document))


def test_swapped_summary_and_slices_are_not_a_valid_bundle(bundle):
    document = deepcopy(bundle[2])
    for entry in document["artifacts"]:
        if entry["role"] in {"summary", "slices"}:
            entry["role"] = "slices" if entry["role"] == "summary" else "summary"
    with pytest.raises(IntervalPolicyRetainedCompatibilityError):
        verify(bundle, reseal(document))


@pytest.mark.parametrize("filename", [" report.md", "report.md ", "nested\\report.md"])
def test_artifact_names_are_not_normalized_or_platform_ambiguous(bundle, filename):
    document = deepcopy(bundle[2])
    summary, paths, _ = bundle
    if "\\" in filename:
        (paths["report"].parent / filename).write_bytes(paths["report"].read_bytes())
    document["artifacts"][2]["filename"] = filename
    with pytest.raises(IntervalPolicyRetainedCompatibilityError):
        verify(bundle, reseal(document))


@pytest.mark.parametrize("outside", [False, True])
def test_symlink_artifacts_are_rejected_before_reading(bundle, outside):
    _, paths, _ = bundle
    report = paths["report"]
    target = (report.parent.parent / f"{report.parent.name}-external.md"
              if outside else report.with_name("actual-report.md"))
    target.write_bytes(report.read_bytes())
    report.unlink()
    report.symlink_to(target)
    with pytest.raises(IntervalPolicyRetainedCompatibilityError):
        verify(bundle)


@pytest.mark.parametrize("output_format", ["csv", "parquet"])
def test_builder_rejects_saved_summary_that_differs_from_supplied_summary(tmp_path, output_format):
    summary, paths, _ = make_bundle(tmp_path, output_format)
    saved = summary.copy()
    saved["source_health_checks_sha256"] = "f" * 64
    if output_format == "csv":
        saved.to_csv(paths["summary"], index=False)
    else:
        saved.to_parquet(paths["summary"], index=False)
    with pytest.raises(IntervalPolicyRetainedCompatibilityError, match="summary"):
        build_compatibility_manifest(summary, artifacts=paths)


def test_rehashed_saved_summary_is_still_bound_to_supplied_summary(bundle):
    summary, paths, document = bundle
    saved = summary.copy()
    saved["source_health_checks_sha256"] = "f" * 64
    saved.to_csv(paths["summary"], index=False)
    document = deepcopy(document)
    next(item for item in document["artifacts"] if item["role"] == "summary")["sha256"] = (
        hashlib.sha256(paths["summary"].read_bytes()).hexdigest()
    )
    with pytest.raises(IntervalPolicyRetainedCompatibilityError, match="summary"):
        verify(bundle, reseal(document))


def test_rehashed_in_memory_summary_does_not_replace_saved_evidence(bundle):
    summary, paths, document = bundle
    changed = summary.copy()
    changed["source_health_checks_sha256"] = "f" * 64
    document = deepcopy(document)
    document["compatibility_summary_sha256"] = compatibility_summary_sha256(changed)
    with pytest.raises(IntervalPolicyRetainedCompatibilityError, match="summary"):
        verify_compatibility_manifest(reseal(document), changed, artifact_directory=paths["summary"].parent)


def test_builder_rejects_scattered_artifact_directories(bundle, tmp_path):
    summary, paths, _ = bundle
    other = tmp_path / "other"
    other.mkdir()
    moved = other / "report.md"
    moved.write_bytes(paths["report"].read_bytes())
    with pytest.raises(IntervalPolicyRetainedCompatibilityError):
        build_compatibility_manifest(summary, artifacts={**paths, "report": moved})


def test_builder_rejects_symlink_artifact(bundle):
    summary, paths, _ = bundle
    report = paths["report"]
    target = report.with_name("actual.md")
    report.rename(target)
    report.symlink_to(target)
    with pytest.raises(IntervalPolicyRetainedCompatibilityError):
        build_compatibility_manifest(summary, artifacts=paths)


def test_builder_rejects_empty_artifact(bundle):
    summary, paths, _ = bundle
    paths["report"].write_bytes(b"")
    with pytest.raises(IntervalPolicyRetainedCompatibilityError):
        build_compatibility_manifest(summary, artifacts=paths)


def test_downstream_review_cannot_accept_rehashed_policy_mismatch(bundle):
    summary, paths, document = bundle
    document = deepcopy(document)
    document["current_policy"]["max_evaluation_age_minutes"] = 999999
    with pytest.raises(IntervalPolicyRetainedCompatibilityReviewError):
        create_retained_compatibility_review(
            summary, reseal(document), artifact_directory=paths["summary"].parent,
            decision="accept_non_retroactive_transition",
            reviewer_name="Regression Test Reviewer", reviewer_role="Test fixture",
            review_ticket="fixture-only-manifest-binding",
            rationale="Reject this inconsistent bundle without recording a review.",
            reviewed_at_utc="2026-01-20T02:00:00Z",
        )

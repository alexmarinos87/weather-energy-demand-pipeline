from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
from jsonschema import Draft202012Validator, FormatChecker

import forecasting.interval_policy_code_change_request_review as module
from forecasting._interval_policy_candidate_revision_common import digest
from forecasting.interval_policy_code_change_request_review import (
    CODE_CHANGE_REQUEST_REVIEW_SAFETY_FIELDS,
    IntervalPolicyCodeChangeRequestReviewError,
    create_interval_policy_code_change_request_review,
    verify_interval_policy_code_change_request_review,
    write_interval_policy_code_change_request_review,
)
from forecasting.run_interval_policy_code_change_request_review import main


ROOT = Path(__file__).resolve().parents[1]
COMMIT = "1" * 40
TREE = "2" * 40
SOURCE = "policy source"


def request() -> dict:
    return {
        "code_change_request_id": "ipccr-" + "3" * 24,
        "code_change_request_sha256": "4" * 64,
        "source_implementation_dry_run_review_id": "ipidr-" + "5" * 24,
        "source_implementation_dry_run_review_sha256": "5" * 64,
        "source_implementation_dry_run_id": "ipid-" + "6" * 24,
        "source_implementation_dry_run_sha256": "6" * 64,
        "current_repository_commit": COMMIT,
        "current_repository_tree": TREE,
        "policy_source_path": "forecasting/interval_monitoring.py",
        "current_policy_source_sha256": "7" * 64,
        "current_policy_source_git_blob_sha1": "8" * 40,
        "active_policy_sha256": "9" * 64,
        "proposed_policy_sha256": "a" * 64,
        "revised_candidate_id": "reviewed-candidate-r4",
        "revised_candidate_version": "interval-monitoring-candidate-v5",
        "changed_threshold_fields": [
            "max_recent_coverage_shortfall_pct_points"
        ],
        "changed_threshold_count": 1,
        "changed_threshold_digest": "b" * 64,
        "reviewed_patch_sha256": "c" * 64,
        "intended_paths": [
            "forecasting/interval_monitoring.py",
            "tests/test_interval_monitoring.py",
        ],
        "validation_commands": [
            "python -m compileall -q forecasting tests",
            "python -m pytest -q",
        ],
        "requested_branch_name": "agent/apply-reviewed-interval-policy",
        "requested_pr_title": "Apply reviewed interval monitoring defaults",
        "requested_at_utc": "2026-01-20T08:00:00Z",
        "next_action": "named_code_change_request_review_required",
    }


@pytest.fixture(autouse=True)
def upstream_verification(monkeypatch):
    monkeypatch.setattr(
        module,
        "verify_interval_policy_code_change_request",
        lambda *args, **kwargs: None,
    )


def create(decision="accept_for_separate_policy_defaults_pr", updates=()):
    return create_interval_policy_code_change_request_review(
        request(),
        {},
        {},
        {},
        pd.DataFrame(),
        {},
        {},
        {},
        pd.DataFrame(),
        current_repository_commit=COMMIT,
        current_repository_tree=TREE,
        policy_source_text=SOURCE,
        review_decision=decision,
        reviewer_name="Alex Reviewer",
        reviewer_role="Data Platform Owner",
        review_ticket="GOV-136",
        rationale=(
            "The exact repository-bound request was reviewed without creating "
            "or authorising a policy-defaults pull request."
        ),
        requested_updates=updates,
        reviewed_at_utc="2026-01-20T09:00:00Z",
    )


def verify(document):
    verify_interval_policy_code_change_request_review(
        document,
        request(),
        {},
        {},
        {},
        pd.DataFrame(),
        {},
        {},
        {},
        pd.DataFrame(),
        current_repository_commit=COMMIT,
        current_repository_tree=TREE,
        policy_source_text=SOURCE,
    )


def test_acceptance_is_named_hashed_and_non_authorising():
    review = create()
    verify(review)
    assert review["review_effect"] == "separate_policy_defaults_pr_required"
    assert review["follow_up_human_action_required"] is True
    assert all(
        review[field] is False
        for field in CODE_CHANGE_REQUEST_REVIEW_SAFETY_FIELDS
    )


def test_reject_and_revision_semantics_are_distinct():
    rejected = create("reject_code_change_request")
    assert rejected["next_action"] == "no_further_action_recorded"
    assert rejected["follow_up_human_action_required"] is False
    with pytest.raises(
        IntervalPolicyCodeChangeRequestReviewError,
        match="at least one",
    ):
        create("request_code_change_request_revision")
    revised = create(
        "request_code_change_request_revision",
        ["Rebind the request to the latest repository base before review."],
    )
    assert revised["requested_updates"]


def test_tampering_and_early_review_are_rejected():
    review = create()
    review["changed_threshold_count"] = 2
    with pytest.raises(
        IntervalPolicyCodeChangeRequestReviewError,
        match="hash",
    ):
        verify(review)
    with pytest.raises(
        IntervalPolicyCodeChangeRequestReviewError,
        match="cannot predate",
    ):
        create_interval_policy_code_change_request_review(
            request(),
            {},
            {},
            {},
            pd.DataFrame(),
            {},
            {},
            {},
            pd.DataFrame(),
            current_repository_commit=COMMIT,
            current_repository_tree=TREE,
            policy_source_text=SOURCE,
            review_decision="reject_code_change_request",
            reviewer_name="Alex",
            reviewer_role="Owner",
            review_ticket="GOV-136",
            rationale="A sufficiently detailed rationale for rejecting the request.",
            reviewed_at_utc="2026-01-20T07:00:00Z",
        )


def test_review_satisfies_versioned_schema():
    review = create()
    schema = json.loads(
        (
            ROOT
            / "data-contracts"
            / "interval_policy_code_change_request_review_schema.json"
        ).read_text(encoding="utf-8")
    )
    errors = list(
        Draft202012Validator(
            schema,
            format_checker=FormatChecker(),
        ).iter_errors(review)
    )
    assert errors == []


def test_immutable_writer_and_cli(tmp_path):
    review = create()
    direct = tmp_path / "direct"
    write_interval_policy_code_change_request_review(
        direct,
        review,
        request(),
        {},
        {},
        {},
        pd.DataFrame(),
        {},
        {},
        {},
        pd.DataFrame(),
        current_repository_commit=COMMIT,
        current_repository_tree=TREE,
        policy_source_text=SOURCE,
    )
    assert len(list(direct.iterdir())) == 2
    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        write_interval_policy_code_change_request_review(
            direct,
            review,
            request(),
            {},
            {},
            {},
            pd.DataFrame(),
            {},
            {},
            {},
            pd.DataFrame(),
            current_repository_commit=COMMIT,
            current_repository_tree=TREE,
            policy_source_text=SOURCE,
        )

    inputs = tmp_path / "inputs"
    inputs.mkdir()
    for name, value in (
        ("request.json", request()),
        ("dry-run-review.json", {}),
        ("proposal.json", {}),
        ("disposition.json", {}),
        ("manifest.json", {}),
        ("package.json", {}),
        ("decision.json", {}),
    ):
        (inputs / name).write_text(json.dumps(value), encoding="utf-8")
    pd.DataFrame({"placeholder": [1]}).to_csv(
        inputs / "revision.csv",
        index=False,
    )
    pd.DataFrame({"placeholder": [1]}).to_csv(
        inputs / "source.csv",
        index=False,
    )
    (inputs / "policy.py").write_text(SOURCE, encoding="utf-8")
    output = tmp_path / "cli"
    args = [
        "--code-change-request", str(inputs / "request.json"),
        "--implementation-dry-run-review", str(inputs / "dry-run-review.json"),
        "--implementation-dry-run", str(inputs / "proposal.json"),
        "--disposition", str(inputs / "disposition.json"),
        "--revision-sensitivity-summary", str(inputs / "revision.csv"),
        "--revision-sensitivity-manifest", str(inputs / "manifest.json"),
        "--revision-package", str(inputs / "package.json"),
        "--source-decision", str(inputs / "decision.json"),
        "--source-sensitivity-summary", str(inputs / "source.csv"),
        "--policy-source", str(inputs / "policy.py"),
        "--current-repository-commit", COMMIT,
        "--current-repository-tree", TREE,
        "--review-decision", "accept_for_separate_policy_defaults_pr",
        "--reviewer-name", "Alex Reviewer",
        "--reviewer-role", "Data Platform Owner",
        "--review-ticket", "GOV-136",
        "--rationale", "The request was accepted for a separate reviewed defaults PR.",
        "--reviewed-at-utc", "2026-01-20T09:00:00Z",
        "--output-dir", str(output),
    ]
    assert main(args) == 0
    assert len(list(output.iterdir())) == 2

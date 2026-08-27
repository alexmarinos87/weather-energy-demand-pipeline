from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
from jsonschema import Draft202012Validator, FormatChecker

import forecasting.interval_policy_implementation_dry_run_review as module
from forecasting._interval_policy_candidate_revision_common import digest
from forecasting.interval_policy_implementation_dry_run_review import (
    DRY_RUN_REVIEW_SAFETY_FIELDS,
    IntervalPolicyImplementationDryRunReviewError,
    create_interval_policy_implementation_dry_run_review,
    verify_interval_policy_implementation_dry_run_review,
    write_interval_policy_implementation_dry_run_review,
)
from forecasting.run_interval_policy_implementation_dry_run_review import main


ROOT = Path(__file__).resolve().parents[1]
BASE_COMMIT = "1" * 40
BASE_TREE = "2" * 40


def proposal() -> dict:
    core = {
        "implementation_dry_run_id": "ipid-" + "3" * 24,
        "source_disposition_id": "iprd-" + "4" * 24,
        "repository_base_commit": BASE_COMMIT,
        "repository_base_tree": BASE_TREE,
        "policy_source_path": "forecasting/interval_monitoring.py",
        "policy_source_sha256": "5" * 64,
        "policy_source_git_blob_sha1": "6" * 40,
        "active_policy_sha256": "7" * 64,
        "proposed_policy_sha256": "8" * 64,
        "revised_candidate_id": "revised-candidate-r3",
        "revised_candidate_version": "interval-monitoring-review-candidate-v4",
        "active_to_proposed_changes": [
            {
                "field": "max_recent_coverage_shortfall_pct_points",
                "current_value": 5.0,
                "proposed_value": 3.0,
            }
        ],
        "intended_paths": [
            "forecasting/interval_monitoring.py",
            "tests/test_interval_monitoring.py",
        ],
        "validation_commands": [
            "python -m compileall -q forecasting tests",
            "python -m pytest -q",
        ],
        "prepared_at_utc": "2026-01-20T06:00:00Z",
        "next_review_action": "named_implementation_dry_run_review_required",
    }
    return {
        **core,
        "implementation_dry_run_sha256": digest(core),
    }


def disposition() -> dict:
    return {
        "disposition_id": "iprd-" + "4" * 24,
        "disposition_sha256": "4" * 64,
    }


@pytest.fixture(autouse=True)
def upstream_verification(monkeypatch):
    monkeypatch.setattr(
        module,
        "verify_interval_policy_implementation_dry_run",
        lambda *args, **kwargs: None,
    )


def create(decision_name="accept_for_separate_code_change_pr", updates=()):
    return create_interval_policy_implementation_dry_run_review(
        proposal(),
        disposition(),
        pd.DataFrame(),
        {},
        {},
        {},
        pd.DataFrame(),
        repository_base_commit=BASE_COMMIT,
        repository_base_tree=BASE_TREE,
        policy_source_text="source",
        review_decision=decision_name,
        reviewer_name="Alex Reviewer",
        reviewer_role="Data Platform Owner",
        review_ticket="GOV-134",
        rationale=(
            "The exact repository-base-bound dry-run patch and validation plan "
            "were reviewed without authorising implementation."
        ),
        requested_updates=updates,
        reviewed_at_utc="2026-01-20T07:00:00Z",
    )


def test_acceptance_is_named_hashed_and_non_authorising():
    review = create()
    verify_interval_policy_implementation_dry_run_review(
        review,
        proposal(),
        disposition(),
        pd.DataFrame(),
        {},
        {},
        {},
        pd.DataFrame(),
        repository_base_commit=BASE_COMMIT,
        repository_base_tree=BASE_TREE,
        policy_source_text="source",
    )
    assert review["review_effect"] == "separate_code_change_pr_required"
    assert review["follow_up_human_action_required"] is True
    assert all(review[field] is False for field in DRY_RUN_REVIEW_SAFETY_FIELDS)


def test_reject_and_revision_semantics_are_distinct():
    rejected = create("reject_implementation_dry_run")
    assert rejected["next_action"] == "no_further_action_recorded"
    assert rejected["follow_up_human_action_required"] is False
    with pytest.raises(
        IntervalPolicyImplementationDryRunReviewError, match="at least one"
    ):
        create("request_dry_run_revision")
    revised = create(
        "request_dry_run_revision",
        ["Rebind the proposal to the latest repository base before review."],
    )
    assert revised["requested_updates"]


def test_tampering_and_early_review_are_rejected():
    with pytest.raises(
        IntervalPolicyImplementationDryRunReviewError, match="cannot precede"
    ):
        create_interval_policy_implementation_dry_run_review(
            proposal(),
            disposition(),
            pd.DataFrame(),
            {},
            {},
            {},
            pd.DataFrame(),
            repository_base_commit=BASE_COMMIT,
            repository_base_tree=BASE_TREE,
            policy_source_text="source",
            review_decision="reject_implementation_dry_run",
            reviewer_name="Alex",
            reviewer_role="Owner",
            review_ticket="GOV-134",
            rationale="A sufficiently detailed rationale for rejecting the dry run.",
            reviewed_at_utc="2026-01-20T05:00:00Z",
        )
    review = create()
    review["changed_threshold_count"] = 2
    with pytest.raises(
        IntervalPolicyImplementationDryRunReviewError, match="hash"
    ):
        verify_interval_policy_implementation_dry_run_review(
            review,
            proposal(),
            disposition(),
            pd.DataFrame(),
            {},
            {},
            {},
            pd.DataFrame(),
            repository_base_commit=BASE_COMMIT,
            repository_base_tree=BASE_TREE,
            policy_source_text="source",
        )


def test_review_satisfies_versioned_schema():
    review = create()
    schema = json.loads(
        (
            ROOT
            / "data-contracts"
            / "interval_policy_implementation_dry_run_review_schema.json"
        ).read_text(encoding="utf-8")
    )
    errors = list(
        Draft202012Validator(
            schema, format_checker=FormatChecker()
        ).iter_errors(review)
    )
    assert errors == []


def test_immutable_writer_and_cli(tmp_path):
    review = create()
    direct = tmp_path / "direct"
    write_interval_policy_implementation_dry_run_review(
        direct,
        review,
        proposal(),
        disposition(),
        pd.DataFrame(),
        {},
        {},
        {},
        pd.DataFrame(),
        repository_base_commit=BASE_COMMIT,
        repository_base_tree=BASE_TREE,
        policy_source_text="source",
    )
    assert len(list(direct.iterdir())) == 2
    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        write_interval_policy_implementation_dry_run_review(
            direct,
            review,
            proposal(),
            disposition(),
            pd.DataFrame(),
            {},
            {},
            {},
            pd.DataFrame(),
            repository_base_commit=BASE_COMMIT,
            repository_base_tree=BASE_TREE,
            policy_source_text="source",
        )

    inputs = tmp_path / "inputs"
    inputs.mkdir()
    for name, value in (
        ("proposal.json", proposal()),
        ("disposition.json", disposition()),
        ("manifest.json", {}),
        ("package.json", {}),
        ("decision.json", {}),
    ):
        (inputs / name).write_text(json.dumps(value), encoding="utf-8")
    pd.DataFrame({"placeholder": [1]}).to_csv(
        inputs / "revision.csv", index=False
    )
    pd.DataFrame({"placeholder": [1]}).to_csv(
        inputs / "source.csv", index=False
    )
    (inputs / "policy.py").write_text("source", encoding="utf-8")
    output = tmp_path / "cli"
    args = [
        "--implementation-dry-run", str(inputs / "proposal.json"),
        "--disposition", str(inputs / "disposition.json"),
        "--revision-sensitivity-summary", str(inputs / "revision.csv"),
        "--revision-sensitivity-manifest", str(inputs / "manifest.json"),
        "--revision-package", str(inputs / "package.json"),
        "--source-decision", str(inputs / "decision.json"),
        "--source-sensitivity-summary", str(inputs / "source.csv"),
        "--policy-source", str(inputs / "policy.py"),
        "--repository-base-commit", BASE_COMMIT,
        "--repository-base-tree", BASE_TREE,
        "--review-decision", "accept_for_separate_code_change_pr",
        "--reviewer-name", "Alex Reviewer",
        "--reviewer-role", "Data Platform Owner",
        "--review-ticket", "GOV-134",
        "--rationale", "The exact dry-run patch was accepted for a separate code PR.",
        "--reviewed-at-utc", "2026-01-20T07:00:00Z",
        "--output-dir", str(output),
    ]
    assert main(args) == 0
    assert len(list(output.iterdir())) == 2

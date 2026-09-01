from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
from jsonschema import Draft202012Validator, FormatChecker

import forecasting.interval_policy_code_change_request as module
from forecasting._interval_policy_candidate_revision_common import digest
from forecasting.interval_policy_code_change_request import (
    CODE_CHANGE_REQUEST_SAFETY_FIELDS,
    IntervalPolicyCodeChangeRequestError,
    create_interval_policy_code_change_request,
    source_git_blob_sha1,
    source_sha256,
    verify_interval_policy_code_change_request,
    write_interval_policy_code_change_request,
)
from forecasting.run_interval_policy_code_change_request import main


ROOT = Path(__file__).resolve().parents[1]
CURRENT_COMMIT = "9" * 40
CURRENT_TREE = "a" * 40
SOURCE = "source"


def proposal() -> dict:
    core = {
        "implementation_dry_run_id": "ipid-" + "3" * 24,
        "implementation_dry_run_sha256": "3" * 64,
        "repository_base_commit": "1" * 40,
        "repository_base_tree": "2" * 40,
        "policy_source_path": "forecasting/interval_monitoring.py",
        "policy_source_sha256": source_sha256(SOURCE),
        "policy_source_git_blob_sha1": source_git_blob_sha1(SOURCE),
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
        "dry_run_patch": "--- a/policy\n+++ b/policy\n",
        "intended_paths": [
            "forecasting/interval_monitoring.py",
            "tests/test_interval_monitoring.py",
        ],
        "validation_commands": [
            "python -m compileall -q forecasting tests",
            "python -m pytest -q",
        ],
    }
    return core


def review() -> dict:
    return {
        "implementation_dry_run_review_id": "ipidr-" + "4" * 24,
        "implementation_dry_run_review_sha256": "4" * 64,
        "review_decision": "accept_for_separate_code_change_pr",
        "review_effect": "separate_code_change_pr_required",
        "follow_up_human_action_required": True,
        "reviewed_at_utc": "2026-01-20T07:00:00Z",
    }


def disposition() -> dict:
    return {
        "disposition_id": "iprd-" + "5" * 24,
        "disposition_sha256": "5" * 64,
    }


@pytest.fixture(autouse=True)
def upstream_verification(monkeypatch):
    monkeypatch.setattr(
        module,
        "verify_interval_policy_implementation_dry_run_review",
        lambda *args, **kwargs: None,
    )


def create():
    return create_interval_policy_code_change_request(
        review(),
        proposal(),
        disposition(),
        pd.DataFrame(),
        {},
        {},
        {},
        pd.DataFrame(),
        current_repository_commit=CURRENT_COMMIT,
        current_repository_tree=CURRENT_TREE,
        policy_source_text=SOURCE,
        requested_branch_name="agent/apply-reviewed-interval-policy",
        requested_pr_title="Apply reviewed interval monitoring defaults",
        requested_by="Alex Requester",
        requester_role="Data Platform Owner",
        request_ticket="GOV-135",
        rationale=(
            "Prepare a separately reviewed implementation branch and pull request "
            "from the retained, base-validated policy evidence."
        ),
        requested_at_utc="2026-01-20T08:00:00Z",
    )


def verify(document):
    verify_interval_policy_code_change_request(
        document,
        review(),
        proposal(),
        disposition(),
        pd.DataFrame(),
        {},
        {},
        {},
        pd.DataFrame(),
        current_repository_commit=CURRENT_COMMIT,
        current_repository_tree=CURRENT_TREE,
        policy_source_text=SOURCE,
    )


def test_request_is_named_hashed_base_bound_and_non_applying():
    document = create()
    verify(document)
    assert document["request_status"] == (
        "ready_for_named_code_change_request_review"
    )
    assert document["requested_branch_name"].startswith("agent/")
    assert document["changed_threshold_count"] == 1
    assert all(
        document[field] is False for field in CODE_CHANGE_REQUEST_SAFETY_FIELDS
    )


def test_nonaccepted_review_stale_source_and_main_branch_are_rejected():
    changed_review = review()
    changed_review["review_decision"] = "reject_implementation_dry_run"
    with pytest.raises(
        IntervalPolicyCodeChangeRequestError, match="requires an accept"
    ):
        create_interval_policy_code_change_request(
            changed_review,
            proposal(),
            disposition(),
            pd.DataFrame(),
            {},
            {},
            {},
            pd.DataFrame(),
            current_repository_commit=CURRENT_COMMIT,
            current_repository_tree=CURRENT_TREE,
            policy_source_text=SOURCE,
            requested_branch_name="agent/apply-reviewed-interval-policy",
            requested_pr_title="Apply reviewed interval monitoring defaults",
            requested_by="Alex",
            requester_role="Owner",
            request_ticket="GOV-135",
            rationale="A sufficiently detailed request rationale for this test.",
            requested_at_utc="2026-01-20T08:00:00Z",
        )
    with pytest.raises(
        IntervalPolicyCodeChangeRequestError, match="no longer matches"
    ):
        create_interval_policy_code_change_request(
            review(),
            proposal(),
            disposition(),
            pd.DataFrame(),
            {},
            {},
            {},
            pd.DataFrame(),
            current_repository_commit=CURRENT_COMMIT,
            current_repository_tree=CURRENT_TREE,
            policy_source_text="changed source",
            requested_branch_name="agent/apply-reviewed-interval-policy",
            requested_pr_title="Apply reviewed interval monitoring defaults",
            requested_by="Alex",
            requester_role="Owner",
            request_ticket="GOV-135",
            rationale="A sufficiently detailed request rationale for this test.",
            requested_at_utc="2026-01-20T08:00:00Z",
        )
    with pytest.raises(
        IntervalPolicyCodeChangeRequestError, match="separate feature branch"
    ):
        create_interval_policy_code_change_request(
            review(),
            proposal(),
            disposition(),
            pd.DataFrame(),
            {},
            {},
            {},
            pd.DataFrame(),
            current_repository_commit=CURRENT_COMMIT,
            current_repository_tree=CURRENT_TREE,
            policy_source_text=SOURCE,
            requested_branch_name="main",
            requested_pr_title="Apply reviewed interval monitoring defaults",
            requested_by="Alex",
            requester_role="Owner",
            request_ticket="GOV-135",
            rationale="A sufficiently detailed request rationale for this test.",
            requested_at_utc="2026-01-20T08:00:00Z",
        )


def test_tampering_and_early_request_are_rejected():
    document = create()
    document["changed_threshold_count"] = 2
    with pytest.raises(IntervalPolicyCodeChangeRequestError, match="hash"):
        verify(document)
    with pytest.raises(
        IntervalPolicyCodeChangeRequestError, match="cannot predate"
    ):
        create_interval_policy_code_change_request(
            review(),
            proposal(),
            disposition(),
            pd.DataFrame(),
            {},
            {},
            {},
            pd.DataFrame(),
            current_repository_commit=CURRENT_COMMIT,
            current_repository_tree=CURRENT_TREE,
            policy_source_text=SOURCE,
            requested_branch_name="agent/apply-reviewed-interval-policy",
            requested_pr_title="Apply reviewed interval monitoring defaults",
            requested_by="Alex",
            requester_role="Owner",
            request_ticket="GOV-135",
            rationale="A sufficiently detailed request rationale for this test.",
            requested_at_utc="2026-01-20T06:00:00Z",
        )


def test_request_satisfies_versioned_schema():
    document = create()
    schema = json.loads(
        (
            ROOT
            / "data-contracts"
            / "interval_policy_code_change_request_schema.json"
        ).read_text(encoding="utf-8")
    )
    errors = list(
        Draft202012Validator(
            schema, format_checker=FormatChecker()
        ).iter_errors(document)
    )
    assert errors == []


def test_immutable_writer_and_cli(tmp_path):
    document = create()
    direct = tmp_path / "direct"
    write_interval_policy_code_change_request(
        direct,
        document,
        review(),
        proposal(),
        disposition(),
        pd.DataFrame(),
        {},
        {},
        {},
        pd.DataFrame(),
        current_repository_commit=CURRENT_COMMIT,
        current_repository_tree=CURRENT_TREE,
        policy_source_text=SOURCE,
    )
    assert len(list(direct.iterdir())) == 2
    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        write_interval_policy_code_change_request(
            direct,
            document,
            review(),
            proposal(),
            disposition(),
            pd.DataFrame(),
            {},
            {},
            {},
            pd.DataFrame(),
            current_repository_commit=CURRENT_COMMIT,
            current_repository_tree=CURRENT_TREE,
            policy_source_text=SOURCE,
        )

    inputs = tmp_path / "inputs"
    inputs.mkdir()
    for name, value in (
        ("review.json", review()),
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
    (inputs / "policy.py").write_text(SOURCE, encoding="utf-8")
    output = tmp_path / "cli"
    args = [
        "--implementation-dry-run-review", str(inputs / "review.json"),
        "--implementation-dry-run", str(inputs / "proposal.json"),
        "--disposition", str(inputs / "disposition.json"),
        "--revision-sensitivity-summary", str(inputs / "revision.csv"),
        "--revision-sensitivity-manifest", str(inputs / "manifest.json"),
        "--revision-package", str(inputs / "package.json"),
        "--source-decision", str(inputs / "decision.json"),
        "--source-sensitivity-summary", str(inputs / "source.csv"),
        "--policy-source", str(inputs / "policy.py"),
        "--current-repository-commit", CURRENT_COMMIT,
        "--current-repository-tree", CURRENT_TREE,
        "--requested-branch-name", "agent/apply-reviewed-interval-policy",
        "--requested-pr-title", "Apply reviewed interval monitoring defaults",
        "--requested-by", "Alex Requester",
        "--requester-role", "Data Platform Owner",
        "--request-ticket", "GOV-135",
        "--rationale",
        "Prepare a separately reviewed implementation branch and pull request.",
        "--requested-at-utc", "2026-01-20T08:00:00Z",
        "--output-dir", str(output),
    ]
    assert main(args) == 0
    assert len(list(output.iterdir())) == 2

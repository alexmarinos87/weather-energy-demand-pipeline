from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
from jsonschema import Draft202012Validator, FormatChecker

import forecasting.interval_policy_historical_annotation_proposal_review as module
from forecasting.interval_policy_historical_annotation_proposal import (
    PROPOSAL_CONTRACT_VERSION,
    PROPOSAL_SAFETY_FIELDS,
)
from forecasting.interval_policy_historical_annotation_proposal_review import (
    REVIEW_SAFETY_FIELDS,
    IntervalPolicyHistoricalAnnotationProposalReviewError,
    create_historical_annotation_proposal_review,
    verify_historical_annotation_proposal_review,
    write_historical_annotation_proposal_review,
)
from forecasting.run_interval_policy_historical_annotation_proposal_review import (
    main,
)


ROOT = Path(__file__).resolve().parents[1]
REVIEW_ID = "iphapr-" + "a" * 24


@pytest.fixture(autouse=True)
def source_proposal_verification(monkeypatch):
    monkeypatch.setattr(
        module,
        "verify_historical_annotation_proposal",
        lambda *args, **kwargs: None,
    )


def source_review() -> dict:
    return {
        "review_id": "ipcr-" + "1" * 24,
        "review_sha256": "2" * 64,
    }


def proposal() -> dict:
    return {
        "proposal_id": "iphap-" + "3" * 24,
        "proposal_revision": 1,
        "proposal_sha256": "4" * 64,
        "source_review_id": source_review()["review_id"],
        "source_review_sha256": source_review()["review_sha256"],
        "compatibility_run_id": "ipca-" + "5" * 24,
        "trend_run_id": "iht-" + "6" * 24,
        "compatibility_summary_sha256": "7" * 64,
        "compatibility_manifest_sha256": "8" * 64,
        "requested_actions": [
            "Prepare explanatory annotation evidence for the transition."
        ],
        "requested_action_responses": [
            {
                "requested_action": (
                    "Prepare explanatory annotation evidence for the transition."
                ),
                "response": "The proposed annotation supplies the requested context.",
                "annotation_ids": ["transition_context"],
            }
        ],
        "annotations": [
            {
                "annotation_id": "transition_context",
                "scope": "compatibility_run",
                "scenario": None,
                "annotation_text": (
                    "This assessment predates the reviewed policy transition."
                ),
                "justification": (
                    "The annotation adds context without changing retained status."
                ),
            }
        ],
        "annotation_count": 1,
        "proposed_at_utc": "2026-01-20T03:00:00Z",
        "follow_up_human_review_required": True,
        "next_action": "named_historical_annotation_proposal_review_required",
        **{field: False for field in PROPOSAL_SAFETY_FIELDS},
        "proposal_contract_version": PROPOSAL_CONTRACT_VERSION,
    }


def create(**overrides):
    values = {
        "artifact_directory": Path("/tmp/source-artifacts"),
        "decision": "accept_for_separate_annotation_storage_change",
        "reviewer_name": "Alex Reviewer",
        "reviewer_role": "Data Platform Owner",
        "review_ticket": "GOV-143",
        "rationale": (
            "The proposal is suitable for a separately reviewed annotation "
            "storage change request."
        ),
        "reviewed_at_utc": "2026-01-20T04:00:00Z",
        "review_id": REVIEW_ID,
    }
    values.update(overrides)
    return create_historical_annotation_proposal_review(
        proposal(),
        source_review(),
        pd.DataFrame({"placeholder": [1]}),
        {},
        **values,
    )


def test_acceptance_is_named_bound_and_non_applying():
    review = create()
    verify_historical_annotation_proposal_review(
        review,
        proposal(),
        source_review(),
        pd.DataFrame({"placeholder": [1]}),
        {},
        artifact_directory=Path("/tmp/source-artifacts"),
    )
    assert review["decision_effect"] == (
        "separate_annotation_storage_change_request_required"
    )
    assert review["follow_up_human_action_required"] is True
    assert review["requested_updates"] == []
    assert review["annotation_count"] == 1
    assert all(review[field] is False for field in REVIEW_SAFETY_FIELDS)


def test_revision_requires_updates_and_other_decisions_reject_them():
    with pytest.raises(
        IntervalPolicyHistoricalAnnotationProposalReviewError,
        match="at least one",
    ):
        create(decision="request_historical_annotation_proposal_revision")
    revision = create(
        decision="request_historical_annotation_proposal_revision",
        requested_updates=[
            "Clarify the historical policy date represented by the annotation."
        ],
    )
    assert revision["next_action"] == (
        "revised_historical_annotation_proposal_required"
    )
    with pytest.raises(
        IntervalPolicyHistoricalAnnotationProposalReviewError,
        match="Only a revision-request",
    ):
        create(
            decision="reject_historical_annotation_proposal",
            requested_updates=["This update is not permitted on a rejection."],
        )


def test_review_rejects_early_time_source_tampering_and_authority_escalation():
    with pytest.raises(
        IntervalPolicyHistoricalAnnotationProposalReviewError,
        match="cannot precede",
    ):
        create(reviewed_at_utc="2026-01-20T02:00:00Z")
    review = create()
    changed_proposal = proposal()
    changed_proposal["annotation_count"] = 2
    with pytest.raises(
        IntervalPolicyHistoricalAnnotationProposalReviewError,
        match="annotation_count binding",
    ):
        verify_historical_annotation_proposal_review(
            review,
            changed_proposal,
            source_review(),
            pd.DataFrame({"placeholder": [1]}),
            {},
            artifact_directory=Path("/tmp/source-artifacts"),
        )
    review = create()
    review["annotation_storage_change_authorized"] = True
    review["review_sha256"] = module.digest(
        {key: value for key, value in review.items() if key != "review_sha256"}
    )
    with pytest.raises(
        IntervalPolicyHistoricalAnnotationProposalReviewError,
        match="must be false",
    ):
        verify_historical_annotation_proposal_review(
            review,
            proposal(),
            source_review(),
            pd.DataFrame({"placeholder": [1]}),
            {},
            artifact_directory=Path("/tmp/source-artifacts"),
        )


def test_review_satisfies_versioned_schema():
    review = create()
    schema = json.loads(
        (
            ROOT
            / "data-contracts"
            / "interval_policy_historical_annotation_proposal_review_schema.json"
        ).read_text(encoding="utf-8")
    )
    errors = list(
        Draft202012Validator(
            schema, format_checker=FormatChecker()
        ).iter_errors(review)
    )
    assert errors == []


def test_writer_and_cli_are_immutable(tmp_path):
    review = create()
    summary = pd.DataFrame({"placeholder": [1]})
    direct = tmp_path / "direct"
    outputs = write_historical_annotation_proposal_review(
        direct,
        review,
        proposal(),
        source_review(),
        summary,
        {},
        artifact_directory=tmp_path,
    )
    assert set(outputs) == {"json", "markdown"}
    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        write_historical_annotation_proposal_review(
            direct,
            review,
            proposal(),
            source_review(),
            summary,
            {},
            artifact_directory=tmp_path,
        )

    inputs = tmp_path / "inputs"
    inputs.mkdir()
    (inputs / "proposal.json").write_text(
        json.dumps(proposal()), encoding="utf-8"
    )
    (inputs / "source-review.json").write_text(
        json.dumps(source_review()), encoding="utf-8"
    )
    summary.to_csv(inputs / "summary.csv", index=False)
    (inputs / "manifest.json").write_text("{}", encoding="utf-8")
    output = tmp_path / "cli"
    args = [
        "--annotation-proposal",
        str(inputs / "proposal.json"),
        "--compatibility-review",
        str(inputs / "source-review.json"),
        "--compatibility-summary",
        str(inputs / "summary.csv"),
        "--compatibility-manifest",
        str(inputs / "manifest.json"),
        "--artifact-directory",
        str(inputs),
        "--decision",
        "accept_for_separate_annotation_storage_change",
        "--reviewer-name",
        "Alex Reviewer",
        "--reviewer-role",
        "Data Platform Owner",
        "--review-ticket",
        "GOV-143",
        "--rationale",
        "Accept the proposal only for a separate storage-change request.",
        "--reviewed-at-utc",
        "2026-01-20T04:00:00Z",
        "--review-id",
        REVIEW_ID,
        "--output-dir",
        str(output),
    ]
    assert main(args) == 0
    assert len(list(output.iterdir())) == 2
    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        main(args)

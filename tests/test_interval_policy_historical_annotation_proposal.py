from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
from jsonschema import Draft202012Validator, FormatChecker

import forecasting.interval_policy_historical_annotation_proposal as module
from forecasting.interval_policy_historical_annotation_proposal import (
    PROPOSAL_SAFETY_FIELDS,
    IntervalPolicyHistoricalAnnotationProposalError,
    create_historical_annotation_proposal,
    verify_historical_annotation_proposal,
    write_historical_annotation_proposal,
)
from forecasting.interval_policy_retained_compatibility_review import (
    REVIEW_CONTRACT_VERSION,
    REVIEW_SAFETY_FIELDS,
)
from forecasting.run_interval_policy_historical_annotation_proposal import main


ROOT = Path(__file__).resolve().parents[1]
PROPOSAL_ID = "iphap-" + "a" * 24
REQUESTED_ACTION = (
    "Prepare explanatory annotation evidence for the historical transition."
)


@pytest.fixture(autouse=True)
def source_review_verification(monkeypatch):
    monkeypatch.setattr(
        module,
        "verify_retained_compatibility_review",
        lambda *args, **kwargs: None,
    )


def review(*, decision: str = "request_historical_annotation") -> dict:
    requested = [REQUESTED_ACTION] if decision == "request_historical_annotation" else []
    effect = {
        "request_historical_annotation": (
            "separate_historical_annotation_proposal_required"
        ),
        "accept_non_retroactive_transition": (
            "non_retroactive_transition_accepted"
        ),
    }[decision]
    return {
        "review_id": "ipcr-" + "1" * 24,
        "review_sha256": "2" * 64,
        "review_revision": 1,
        "compatibility_run_id": "ipca-" + "3" * 24,
        "compatibility_run_timestamp_utc": "2026-01-20T01:00:00Z",
        "trend_run_id": "iht-" + "4" * 24,
        "compatibility_summary_sha256": "5" * 64,
        "compatibility_manifest_sha256": "6" * 64,
        "previous_policy_id": "previous-five-point",
        "previous_shortfall_threshold_pct_points": 5.0,
        "current_policy_id": "reviewed-three-point",
        "current_shortfall_threshold_pct_points": 3.0,
        "decision": decision,
        "decision_effect": effect,
        "reviewer_name": "Alex Reviewer",
        "reviewer_role": "Data Platform Owner",
        "review_ticket": "GOV-141",
        "rationale": "A sufficiently detailed source review rationale.",
        "requested_actions": requested,
        "reviewed_at_utc": "2026-01-20T02:00:00Z",
        "scenario_evidence": [
            {
                "scenario": "stable",
                "retained_monitor_status": "healthy",
                "previous_policy_status": "healthy",
                "current_policy_status": "healthy",
                "compatibility_classification": "fully_compatible",
                "retained_status_compatibility": "matches_both_policies",
                "changed_slice_count": 0,
                "newly_failed_slice_count": 0,
                "human_review_required": False,
            },
            {
                "scenario": "tightened",
                "retained_monitor_status": "healthy",
                "previous_policy_status": "healthy",
                "current_policy_status": "failed",
                "compatibility_classification": "scenario_status_escalation",
                "retained_status_compatibility": "matches_previous_policy_only",
                "changed_slice_count": 1,
                "newly_failed_slice_count": 1,
                "human_review_required": True,
            },
        ],
        "named_human_review_confirmed": True,
        "follow_up_human_action_required": bool(requested),
        **{field: False for field in REVIEW_SAFETY_FIELDS},
        "review_contract_version": REVIEW_CONTRACT_VERSION,
    }


def annotations() -> list[dict]:
    return [
        {
            "annotation_id": "policy_transition_context",
            "scope": "compatibility_run",
            "scenario": None,
            "annotation_text": (
                "This retained assessment predates the reviewed three-point "
                "coverage-shortfall policy transition."
            ),
            "justification": (
                "The text adds policy context without modifying the historical "
                "monitor status or source evidence."
            ),
        },
        {
            "annotation_id": "tightened_scenario_context",
            "scope": "scenario",
            "scenario": "tightened",
            "annotation_text": (
                "The tightened scenario becomes failed under the reviewed policy "
                "while its retained status remains healthy."
            ),
            "justification": (
                "The scenario annotation makes the counterfactual escalation "
                "visible without rewriting the retained result."
            ),
        },
    ]


def responses() -> list[dict]:
    return [
        {
            "requested_action": REQUESTED_ACTION,
            "response": (
                "The run-level and scenario-level annotations provide the "
                "requested explanatory context."
            ),
            "annotation_ids": [
                "policy_transition_context",
                "tightened_scenario_context",
            ],
        }
    ]


def create(**overrides):
    values = {
        "artifact_directory": Path("/tmp/source-artifacts"),
        "annotations": annotations(),
        "requested_action_responses": responses(),
        "proposed_by": "Alex Proposer",
        "proposer_role": "Data Platform Owner",
        "proposal_ticket": "GOV-142",
        "rationale": (
            "Prepare a bounded explanatory annotation proposal for a separate "
            "named human review."
        ),
        "proposed_at_utc": "2026-01-20T03:00:00Z",
        "proposal_id": PROPOSAL_ID,
    }
    values.update(overrides)
    return create_historical_annotation_proposal(
        review(),
        pd.DataFrame({"placeholder": [1]}),
        {},
        **values,
    )


def test_proposal_binds_requested_review_and_keeps_all_authority_false():
    proposal = create()
    verify_historical_annotation_proposal(
        proposal,
        review(),
        pd.DataFrame({"placeholder": [1]}),
        {},
        artifact_directory=Path("/tmp/source-artifacts"),
    )
    assert proposal["source_review_id"] == review()["review_id"]
    assert proposal["annotation_count"] == 2
    assert proposal["next_action"] == (
        "named_historical_annotation_proposal_review_required"
    )
    assert proposal["follow_up_human_review_required"] is True
    assert all(proposal[field] is False for field in PROPOSAL_SAFETY_FIELDS)


def test_wrong_source_decision_unknown_scenario_and_early_time_are_rejected():
    with pytest.raises(
        IntervalPolicyHistoricalAnnotationProposalError,
        match="requires a request_historical_annotation",
    ):
        create_historical_annotation_proposal(
            review(decision="accept_non_retroactive_transition"),
            pd.DataFrame({"placeholder": [1]}),
            {},
            artifact_directory=Path("/tmp/source-artifacts"),
            annotations=annotations(),
            requested_action_responses=responses(),
            proposed_by="Alex",
            proposer_role="Owner",
            proposal_ticket="GOV-142",
            rationale="A sufficiently detailed rejected proposal rationale.",
        )
    changed_annotations = annotations()
    changed_annotations[1]["scenario"] = "unknown"
    with pytest.raises(
        IntervalPolicyHistoricalAnnotationProposalError,
        match="Unknown compatibility scenario",
    ):
        create(annotations=changed_annotations)
    with pytest.raises(
        IntervalPolicyHistoricalAnnotationProposalError,
        match="cannot precede",
    ):
        create(proposed_at_utc="2026-01-20T01:00:00Z")


def test_requested_actions_and_annotations_must_be_covered_exactly():
    changed = responses()
    changed[0]["requested_action"] = (
        "A different action that was not requested by the reviewer."
    )
    with pytest.raises(
        IntervalPolicyHistoricalAnnotationProposalError,
        match="exactly and in order",
    ):
        create(requested_action_responses=changed)
    changed = responses()
    changed[0]["annotation_ids"] = ["policy_transition_context"]
    with pytest.raises(
        IntervalPolicyHistoricalAnnotationProposalError,
        match="Every proposed annotation",
    ):
        create(requested_action_responses=changed)


def test_proposal_rejects_self_tampering_and_authority_escalation():
    proposal = create()
    proposal["rationale"] += " changed"
    with pytest.raises(
        IntervalPolicyHistoricalAnnotationProposalError,
        match="proposal hash",
    ):
        verify_historical_annotation_proposal(
            proposal,
            review(),
            pd.DataFrame({"placeholder": [1]}),
            {},
            artifact_directory=Path("/tmp/source-artifacts"),
        )
    proposal = create()
    proposal["historical_annotation_applied"] = True
    proposal["proposal_sha256"] = module.digest(
        {
            key: value
            for key, value in proposal.items()
            if key != "proposal_sha256"
        }
    )
    with pytest.raises(
        IntervalPolicyHistoricalAnnotationProposalError,
        match="must be false",
    ):
        verify_historical_annotation_proposal(
            proposal,
            review(),
            pd.DataFrame({"placeholder": [1]}),
            {},
            artifact_directory=Path("/tmp/source-artifacts"),
        )


def test_proposal_satisfies_versioned_schema():
    proposal = create()
    schema = json.loads(
        (
            ROOT
            / "data-contracts"
            / "interval_policy_historical_annotation_proposal_schema.json"
        ).read_text(encoding="utf-8")
    )
    errors = list(
        Draft202012Validator(
            schema, format_checker=FormatChecker()
        ).iter_errors(proposal)
    )
    assert errors == []


def test_writer_and_cli_are_immutable(tmp_path):
    proposal = create()
    summary = pd.DataFrame({"placeholder": [1]})
    direct = tmp_path / "direct"
    outputs = write_historical_annotation_proposal(
        direct,
        proposal,
        review(),
        summary,
        {},
        artifact_directory=tmp_path,
    )
    assert set(outputs) == {"json", "markdown"}
    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        write_historical_annotation_proposal(
            direct,
            proposal,
            review(),
            summary,
            {},
            artifact_directory=tmp_path,
        )

    inputs = tmp_path / "inputs"
    inputs.mkdir()
    (inputs / "review.json").write_text(
        json.dumps(review()), encoding="utf-8"
    )
    summary.to_csv(inputs / "summary.csv", index=False)
    (inputs / "manifest.json").write_text("{}", encoding="utf-8")
    (inputs / "proposal-input.json").write_text(
        json.dumps(
            {
                "annotations": annotations(),
                "requested_action_responses": responses(),
            }
        ),
        encoding="utf-8",
    )
    output = tmp_path / "cli"
    args = [
        "--compatibility-review",
        str(inputs / "review.json"),
        "--compatibility-summary",
        str(inputs / "summary.csv"),
        "--compatibility-manifest",
        str(inputs / "manifest.json"),
        "--artifact-directory",
        str(inputs),
        "--proposal-input",
        str(inputs / "proposal-input.json"),
        "--proposed-by",
        "Alex Proposer",
        "--proposer-role",
        "Data Platform Owner",
        "--proposal-ticket",
        "GOV-142",
        "--rationale",
        "Prepare bounded explanatory annotations for a separate named review.",
        "--proposed-at-utc",
        "2026-01-20T03:00:00Z",
        "--proposal-id",
        PROPOSAL_ID,
        "--output-dir",
        str(output),
    ]
    assert main(args) == 0
    assert len(list(output.iterdir())) == 2
    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        main(args)

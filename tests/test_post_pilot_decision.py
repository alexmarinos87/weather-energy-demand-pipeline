from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path

import pandas as pd
import pytest
from jsonschema import Draft202012Validator, FormatChecker

from forecasting.fabric_pilot_receipt import (
    ASSESSMENT_CONTRACT_VERSION,
    RECEIPT_CONTRACT_VERSION,
    _digest as pilot_digest,
)
from forecasting.model_registry import (
    register_candidate,
    transition_candidate,
    write_candidate_revision,
)
from forecasting.post_pilot_decision import (
    DECISION_CONTRACT_VERSION,
    PostPilotDecisionError,
    create_post_pilot_decision,
    verify_post_pilot_decision,
    write_post_pilot_decision,
)
from forecasting.run_post_pilot_decision import main


BASE = pd.Timestamp("2026-01-01T00:00:00Z")


def _candidate_history(*, approved: bool = True):
    promotion = pd.DataFrame(
        [
            {
                "assessment_id": "assessment-evidence-1",
                "assessment_timestamp_utc": BASE,
                "comparison_run_id": "comparison-1",
                "reconciliation_run_id": "reconciliation-1",
                "baseline_model": "ridge_weather_lag",
                "candidate_model": "ridge_target_weather",
                "assessment_status": "eligible_for_human_review",
                "automatic_promotion_allowed": False,
                "failed_check_count": 0,
                "policy_version": "policy-v1",
                "assessment_contract_version": "assessment-v1",
            }
        ]
    )
    health = pd.DataFrame(
        [
            {
                "monitor_run_id": "monitor-1",
                "monitor_timestamp_utc": BASE,
                "monitor_status": "healthy",
                "automatic_remediation_allowed": False,
                "failed_error_check_count": 0,
                "failed_warning_check_count": 0,
                "policy_version": "health-policy-v1",
                "monitoring_contract_version": "health-v1",
            }
        ]
    )
    manifest_1, event_1 = register_candidate(
        promotion,
        health,
        repository="alexmarinos87/weather-energy-demand-pipeline",
        code_commit_sha="1" * 40,
        code_tree_sha="2" * 40,
        candidate_version="0.1.0",
        training_data_boundary_utc=BASE,
        feature_contract_versions=("time-horizon-v1",),
        forecast_weather_contract_version="target-weather-v1",
        actor="registrar",
        reason="Register candidate evidence",
        created_at_utc=BASE + pd.Timedelta(minutes=10),
    )
    if not approved:
        return manifest_1, [(manifest_1, event_1)]
    manifest_2, event_2 = transition_candidate(
        manifest_1,
        action="review_requested",
        actor="owner",
        reason="Request human review",
        event_timestamp_utc=BASE + pd.Timedelta(minutes=20),
    )
    manifest_3, event_3 = transition_candidate(
        manifest_2,
        action="approved",
        actor="reviewer",
        reason="Approved for controlled manual pilot",
        review_ticket="REVIEW-001",
        event_timestamp_utc=BASE + pd.Timedelta(minutes=30),
    )
    return manifest_3, [
        (manifest_1, event_1),
        (manifest_2, event_2),
        (manifest_3, event_3),
    ]


def _receipt(candidate_id: str, *, run_status: str = "completed") -> dict:
    receipt = {
        "receipt_id": "fpr-" + "1" * 24,
        "receipt_revision": 1,
        "receipt_state": "recorded",
        "pilot_id": "fpl-" + "2" * 24,
        "candidate_id": candidate_id,
        "external_run_id": "fabric-run-001",
        "run_status": run_status,
        "started_at_utc": (BASE + pd.Timedelta(hours=1)).isoformat(),
        "ended_at_utc": (BASE + pd.Timedelta(hours=2)).isoformat(),
        "recorded_at_utc": (BASE + pd.Timedelta(hours=2, minutes=10)).isoformat(),
        "evidence_files": [
            {
                "evidence_sequence": 1,
                "evidence_role": "run_log",
                "relative_path": "logs/pilot.log",
                "size_bytes": 12,
                "sha256": "a" * 64,
            }
        ],
        "credential_references_used": ["OPENWEATHER_API_KEY"],
        "active_model_before": "ridge_weather_lag",
        "active_model_after": "ridge_weather_lag",
        "single_use_authorization": True,
        "authorization_consumed": True,
        "execution_authorized": True,
        "execution_performed": True,
        "manual_execution_confirmed": True,
        "automatic_execution_used": False,
        "schedule_activation_allowed": False,
        "deployment_authorized": False,
        "model_activation_authorized": False,
        "credential_values_recorded": False,
        "source_evidence_mutated": False,
        "receipt_contract_version": RECEIPT_CONTRACT_VERSION,
    }
    receipt["receipt_hash"] = pilot_digest(receipt, exclude=("receipt_hash",))
    return receipt


def _assessment(receipt: dict, *, eligible: bool = True) -> dict:
    checks = [
        {
            "check_sequence": 1,
            "check_scope": "safety",
            "check_name": "pilot_within_bounds",
            "passed": eligible,
            "observed_value": str(eligible),
            "expected_value": "True",
            "details": "Synthetic post-pilot assessment fixture.",
        }
    ]
    failed = 0 if eligible else 1
    assessment = {
        "assessment_id": "fra-" + "3" * 24,
        "assessment_revision": 1,
        "pilot_id": receipt["pilot_id"],
        "receipt_id": receipt["receipt_id"],
        "receipt_hash": receipt["receipt_hash"],
        "authorization_id": "fpa-" + "4" * 24,
        "external_run_id": receipt["external_run_id"],
        "assessed_at_utc": (BASE + pd.Timedelta(hours=3)).isoformat(),
        "checks": checks,
        "assessment_outcome": (
            "eligible_for_post_pilot_review" if eligible else "pilot_failed"
        ),
        "run_status": receipt["run_status"],
        "rollback_required": not eligible,
        "rollback_performed": not eligible,
        "check_count": 1,
        "passed_check_count": 1 - failed,
        "failed_check_count": failed,
        "post_pilot_human_decision_required": True,
        "automatic_model_activation_allowed": False,
        "deployment_authorized": False,
        "active_model_unchanged": True,
        "source_evidence_mutated": False,
        "assessment_contract_version": ASSESSMENT_CONTRACT_VERSION,
    }
    assessment["assessment_hash"] = pilot_digest(
        assessment, exclude=("assessment_hash",)
    )
    return assessment


def _decision(**overrides):
    candidate, _ = _candidate_history()
    receipt = _receipt(candidate["candidate_id"])
    assessment = _assessment(receipt)
    kwargs = {
        "decision": "continue_evidence_collection",
        "decided_by": "alexmarinos87",
        "decision_role": "model-owner",
        "review_ticket": "PILOT-REVIEW-001",
        "reason": "Pilot remained within reviewed bounds.",
        "follow_up_actions": ["Collect a second independent pilot window"],
        "decided_at_utc": BASE + pd.Timedelta(hours=4),
    }
    kwargs.update(overrides)
    return create_post_pilot_decision(candidate, receipt, assessment, **kwargs)


def test_eligible_pilot_allows_continue_but_authorizes_nothing():
    decision = _decision()
    assert decision["decision"] == "continue_evidence_collection"
    assert decision["decision_effect"] == "new_pilot_plan_required"
    assert decision["human_decision_confirmed"] is True
    assert decision["registry_mutation_performed"] is False
    assert decision["new_pilot_authorized"] is False
    assert decision["model_activation_authorized"] is False
    assert decision["deployment_authorized"] is False
    assert decision["active_model_unchanged"] is True


def test_failed_pilot_cannot_continue_evidence_collection():
    candidate, _ = _candidate_history()
    receipt = _receipt(candidate["candidate_id"])
    assessment = _assessment(receipt, eligible=False)
    with pytest.raises(PostPilotDecisionError, match="fully eligible"):
        create_post_pilot_decision(
            candidate,
            receipt,
            assessment,
            decision="continue_evidence_collection",
            decided_by="owner",
            decision_role="model-owner",
            review_ticket="PILOT-REVIEW-002",
            reason="Continue despite failure",
            follow_up_actions=["Retry"],
            decided_at_utc=BASE + pd.Timedelta(hours=4),
        )


def test_failed_pilot_can_require_a_new_candidate_revision():
    candidate, _ = _candidate_history()
    receipt = _receipt(candidate["candidate_id"])
    assessment = _assessment(receipt, eligible=False)
    decision = create_post_pilot_decision(
        candidate,
        receipt,
        assessment,
        decision="revise_candidate",
        decided_by="owner",
        decision_role="model-owner",
        review_ticket="PILOT-REVIEW-003",
        reason="Pilot exposed a protected-slice regression.",
        follow_up_actions=["Register a new candidate version with fresh evidence"],
        revision_requirements=["Reduce 60-minute humidity sensitivity"],
        decided_at_utc=BASE + pd.Timedelta(hours=4),
    )
    assert decision["decision_effect"] == "new_candidate_required"
    assert decision["revision_requirements"] == [
        "Reduce 60-minute humidity sensitivity"
    ]
    assert decision["registry_mutation_performed"] is False


def test_retirement_is_a_recommendation_not_a_registry_mutation():
    decision = _decision(
        decision="retire_candidate",
        reason="Candidate did not improve protected slices.",
        follow_up_actions=["Record a separate immutable registry retirement event"],
        retirement_reason="Candidate did not improve protected test slices",
    )
    assert decision["decision_effect"] == "registry_retirement_required"
    assert decision["retirement_reason"]
    assert decision["registry_mutation_performed"] is False


def test_revision_requires_explicit_requirements():
    with pytest.raises(PostPilotDecisionError, match="at least one revision"):
        _decision(
            decision="revise_candidate",
            reason="Revise candidate",
            follow_up_actions=["Register a revised candidate"],
        )


def test_retirement_requires_a_reason():
    with pytest.raises(PostPilotDecisionError, match="retirement reason"):
        _decision(
            decision="retire_candidate",
            reason="Retire candidate",
            follow_up_actions=["Record registry retirement"],
        )


def test_candidate_must_be_in_approved_state():
    candidate, _ = _candidate_history(approved=False)
    receipt = _receipt(candidate["candidate_id"])
    assessment = _assessment(receipt)
    with pytest.raises(PostPilotDecisionError, match="approved candidate"):
        create_post_pilot_decision(
            candidate,
            receipt,
            assessment,
            decision="continue_evidence_collection",
            decided_by="owner",
            decision_role="model-owner",
            review_ticket="PILOT-REVIEW-004",
            reason="Continue evidence",
            follow_up_actions=["Plan another pilot"],
            decided_at_utc=BASE + pd.Timedelta(hours=4),
        )


def test_receipt_candidate_and_assessment_chain_must_match():
    candidate, _ = _candidate_history()
    receipt = _receipt("twc-" + "9" * 24)
    assessment = _assessment(receipt)
    with pytest.raises(PostPilotDecisionError, match="candidate_id"):
        create_post_pilot_decision(
            candidate,
            receipt,
            assessment,
            decision="retire_candidate",
            decided_by="owner",
            decision_role="model-owner",
            review_ticket="PILOT-REVIEW-005",
            reason="Retire mismatched candidate",
            follow_up_actions=["Record retirement"],
            retirement_reason="Mismatched evidence",
            decided_at_utc=BASE + pd.Timedelta(hours=4),
        )

    receipt = _receipt(candidate["candidate_id"])
    assessment = _assessment(receipt)
    assessment["receipt_id"] = "fpr-" + "8" * 24
    assessment["assessment_hash"] = pilot_digest(
        assessment, exclude=("assessment_hash",)
    )
    with pytest.raises(PostPilotDecisionError, match="receipt_id"):
        create_post_pilot_decision(
            candidate,
            receipt,
            assessment,
            decision="retire_candidate",
            decided_by="owner",
            decision_role="model-owner",
            review_ticket="PILOT-REVIEW-006",
            reason="Retire mismatched pilot",
            follow_up_actions=["Record retirement"],
            retirement_reason="Mismatched assessment",
            decided_at_utc=BASE + pd.Timedelta(hours=4),
        )


def test_decision_cannot_predate_candidate_approval_or_assessment():
    with pytest.raises(PostPilotDecisionError, match="cannot precede"):
        _decision(decided_at_utc=BASE + pd.Timedelta(hours=2))


def test_decision_hash_detects_tampering():
    decision = _decision()
    decision["decision_reason"] = "Tampered"
    with pytest.raises(PostPilotDecisionError, match="hash is invalid"):
        verify_post_pilot_decision(decision)


def test_decision_write_is_assessment_bound_and_immutable(tmp_path):
    decision = _decision()
    path = write_post_pilot_decision(tmp_path, decision)
    assert path.name == f"post_pilot_decision_{decision['assessment_id']}.json"
    verify_post_pilot_decision(json.loads(path.read_text(encoding="utf-8")))
    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        write_post_pilot_decision(tmp_path, decision)


def test_decision_satisfies_versioned_json_schema():
    decision = _decision()
    schema_path = (
        Path(__file__).resolve().parents[1]
        / "data-contracts"
        / "fabric_post_pilot_decision_schema.json"
    )
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    errors = list(
        Draft202012Validator(
            schema, format_checker=FormatChecker()
        ).iter_errors(decision)
    )
    assert errors == []
    assert decision["decision_contract_version"] == DECISION_CONTRACT_VERSION


def test_cli_loads_verified_candidate_history_and_records_decision(tmp_path):
    candidate, history = _candidate_history()
    candidate_dir = tmp_path / candidate["candidate_id"]
    for manifest, event in history:
        write_candidate_revision(candidate_dir, manifest, event)
    receipt = _receipt(candidate["candidate_id"])
    assessment = _assessment(receipt)
    receipt_path = tmp_path / "receipt.json"
    assessment_path = tmp_path / "assessment.json"
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
    assessment_path.write_text(json.dumps(assessment), encoding="utf-8")
    output = tmp_path / "decisions"

    exit_code = main(
        [
            "record",
            "--candidate-dir",
            str(candidate_dir),
            "--receipt",
            str(receipt_path),
            "--assessment",
            str(assessment_path),
            "--decision",
            "continue_evidence_collection",
            "--decided-by",
            "alexmarinos87",
            "--decision-role",
            "model-owner",
            "--review-ticket",
            "PILOT-REVIEW-007",
            "--reason",
            "Pilot remained within bounds",
            "--follow-up-action",
            "Collect another independent pilot window",
            "--decided-at-utc",
            "2026-01-01T04:00:00Z",
            "--confirm-assessment-id",
            assessment["assessment_id"],
            "--output-dir",
            str(output),
        ]
    )
    assert exit_code == 0
    files = list(output.glob("post_pilot_decision_*.json"))
    assert len(files) == 1
    recorded = json.loads(files[0].read_text(encoding="utf-8"))
    assert recorded["new_pilot_authorized"] is False
    assert main(["verify", "--decision", str(files[0])]) == 0


def test_cli_requires_exact_assessment_confirmation(tmp_path):
    candidate, history = _candidate_history()
    candidate_dir = tmp_path / candidate["candidate_id"]
    for manifest, event in history:
        write_candidate_revision(candidate_dir, manifest, event)
    receipt = _receipt(candidate["candidate_id"])
    assessment = _assessment(receipt)
    receipt_path = tmp_path / "receipt.json"
    assessment_path = tmp_path / "assessment.json"
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
    assessment_path.write_text(json.dumps(assessment), encoding="utf-8")

    with pytest.raises(ValueError, match="exactly match"):
        main(
            [
                "record",
                "--candidate-dir",
                str(candidate_dir),
                "--receipt",
                str(receipt_path),
                "--assessment",
                str(assessment_path),
                "--decision",
                "retire_candidate",
                "--decided-by",
                "owner",
                "--decision-role",
                "model-owner",
                "--review-ticket",
                "PILOT-REVIEW-008",
                "--reason",
                "Retire candidate",
                "--follow-up-action",
                "Record registry retirement",
                "--retirement-reason",
                "Candidate is unsuitable",
                "--confirm-assessment-id",
                "wrong-assessment",
            ]
        )

from datetime import datetime, timezone
import json
from pathlib import Path

import pandas as pd
import pytest
from jsonschema import Draft202012Validator, FormatChecker

from forecasting.model_registry import (
    ModelCandidateRegistryError,
    load_candidate_history,
    register_candidate,
    transition_candidate,
    verify_candidate_history,
    verify_manifest,
    write_candidate_revision,
)
from forecasting.run_model_registry import main


COMMIT_SHA = "a" * 40
TREE_SHA = "b" * 40
CREATED = datetime(2026, 8, 22, 12, 0, tzinfo=timezone.utc)


def promotion(*, status="eligible_for_human_review", failed=0):
    return pd.DataFrame(
        [
            {
                "assessment_id": "assessment-1",
                "assessment_timestamp_utc": "2026-08-22T11:30:00Z",
                "comparison_run_id": "comparison-1",
                "reconciliation_run_id": "reconciliation-1",
                "baseline_model": "ridge_weather_lag",
                "candidate_model": "ridge_target_weather",
                "assessment_status": status,
                "automatic_promotion_allowed": False,
                "failed_check_count": failed,
                "policy_version": "target-weather-promotion-policy-v1",
                "assessment_contract_version": "target-weather-promotion-assessment-v1",
            }
        ]
    )


def health(*, status="healthy", failed_errors=0, failed_warnings=0):
    return pd.DataFrame(
        [
            {
                "monitor_run_id": "monitor-1",
                "monitor_timestamp_utc": "2026-08-22T11:45:00Z",
                "monitor_status": status,
                "automatic_remediation_allowed": False,
                "failed_error_check_count": failed_errors,
                "failed_warning_check_count": failed_warnings,
                "policy_version": "forecast-provider-health-policy-v1",
                "monitoring_contract_version": "forecast-provider-monitoring-v1",
            }
        ]
    )


def register(promotion_frame=None, health_frame=None, **overrides):
    values = {
        "repository": "alexmarinos87/weather-energy-demand-pipeline",
        "code_commit_sha": COMMIT_SHA,
        "code_tree_sha": TREE_SHA,
        "candidate_version": "0.1.0",
        "training_data_boundary_utc": "2026-08-22T10:00:00Z",
        "feature_contract_versions": (
            "time-horizon-v1",
            "rolling-origin-v1",
            "weather-model-comparison-v1",
        ),
        "forecast_weather_contract_version": "target-weather-v1",
        "actor": "alexmarinos87",
        "reason": "Register reviewed evidence package",
        "created_at_utc": CREATED,
    }
    values.update(overrides)
    return register_candidate(
        promotion_frame if promotion_frame is not None else promotion(),
        health_frame if health_frame is not None else health(),
        **values,
    )


def test_registration_is_deterministic_and_does_not_authorize_deployment():
    first_manifest, first_event = register()
    second_manifest, second_event = register()

    assert first_manifest["candidate_id"] == second_manifest["candidate_id"]
    assert first_manifest["candidate_state"] == "draft"
    assert first_manifest["candidate_revision"] == 1
    assert not first_manifest["automatic_promotion_allowed"]
    assert not first_manifest["automatic_remediation_allowed"]
    assert not first_manifest["deployment_authorized"]
    assert first_manifest["active_model_unchanged"]
    assert first_event["action"] == "registered"
    assert first_event["event_hash"] == second_event["event_hash"]
    verify_manifest(first_manifest)


def test_blocked_assessment_can_be_registered_for_audit_but_not_reviewed():
    manifest, _ = register(
        promotion_frame=promotion(status="blocked", failed=2)
    )
    assert manifest["candidate_state"] == "draft"
    with pytest.raises(ModelCandidateRegistryError, match="blocked"):
        transition_candidate(
            manifest,
            action="review_requested",
            actor="alexmarinos87",
            reason="Request review",
            event_timestamp_utc="2026-08-22T12:05:00Z",
        )


def test_warning_provider_health_blocks_review_request():
    manifest, _ = register(health_frame=health(status="warning", failed_warnings=1))
    with pytest.raises(ModelCandidateRegistryError, match="must be healthy"):
        transition_candidate(
            manifest,
            action="review_requested",
            actor="alexmarinos87",
            reason="Request review",
            event_timestamp_utc="2026-08-22T12:05:00Z",
        )


def test_review_request_records_actor_and_preserves_safety_flags():
    manifest, event = register()
    requested, request_event = transition_candidate(
        manifest,
        action="review_requested",
        actor="review-requester",
        reason="Evidence is ready",
        event_timestamp_utc="2026-08-22T12:05:00Z",
    )

    assert requested["candidate_state"] == "review_requested"
    assert requested["candidate_revision"] == 2
    assert requested["review_requested_by"] == "review-requester"
    assert request_event["previous_event_hash"] == event["event_hash"]
    assert not requested["deployment_authorized"]
    assert requested["active_model_unchanged"]


def test_approval_requires_review_request_and_review_ticket():
    manifest, _ = register()
    with pytest.raises(ModelCandidateRegistryError, match="not allowed"):
        transition_candidate(
            manifest,
            action="approved",
            actor="reviewer",
            reason="Approve",
            review_ticket="REVIEW-1",
        )

    requested, _ = transition_candidate(
        manifest,
        action="review_requested",
        actor="requester",
        reason="Ready",
        event_timestamp_utc="2026-08-22T12:05:00Z",
    )
    with pytest.raises(ModelCandidateRegistryError, match="review_ticket"):
        transition_candidate(
            requested,
            action="approved",
            actor="reviewer",
            reason="Approve",
            event_timestamp_utc="2026-08-22T12:10:00Z",
        )


def test_approved_state_is_human_evidence_not_activation():
    draft, registered = register()
    requested, request_event = transition_candidate(
        draft,
        action="review_requested",
        actor="requester",
        reason="Ready for review",
        event_timestamp_utc="2026-08-22T12:05:00Z",
    )
    approved, approval_event = transition_candidate(
        requested,
        action="approved",
        actor="reviewer",
        reason="Approved for controlled manual trial",
        review_ticket="REVIEW-2026-001",
        event_timestamp_utc="2026-08-22T12:10:00Z",
    )

    assert approved["candidate_state"] == "approved"
    assert approved["reviewed_by"] == "reviewer"
    assert approved["review_decision"] == "approved"
    assert approved["review_ticket"] == "REVIEW-2026-001"
    assert not approved["deployment_authorized"]
    assert approved["active_model_unchanged"]
    latest = verify_candidate_history(
        [draft, requested, approved],
        [registered, request_event, approval_event],
    )
    assert latest["manifest_hash"] == approved["manifest_hash"]


def test_rejection_and_retirement_are_append_only_transitions():
    draft, registered = register()
    requested, request_event = transition_candidate(
        draft,
        action="review_requested",
        actor="requester",
        reason="Ready",
        event_timestamp_utc="2026-08-22T12:05:00Z",
    )
    rejected, reject_event = transition_candidate(
        requested,
        action="rejected",
        actor="reviewer",
        reason="Insufficient representative history",
        review_ticket="REVIEW-2",
        event_timestamp_utc="2026-08-22T12:10:00Z",
    )
    retired, retire_event = transition_candidate(
        rejected,
        action="retired",
        actor="owner",
        reason="Superseded",
        review_ticket="REVIEW-3",
        event_timestamp_utc="2026-08-22T12:15:00Z",
    )

    assert rejected["candidate_state"] == "rejected"
    assert retired["candidate_state"] == "retired"
    assert retired["retired_by"] == "owner"
    verify_candidate_history(
        [draft, requested, rejected, retired],
        [registered, request_event, reject_event, retire_event],
    )


def test_tampered_manifest_hash_is_rejected():
    manifest, _ = register()
    tampered = dict(manifest)
    tampered["candidate_version"] = "9.9.9"
    with pytest.raises(ModelCandidateRegistryError, match="manifest hash"):
        verify_manifest(tampered)


def test_tampered_event_chain_is_rejected():
    draft, registered = register()
    requested, request_event = transition_candidate(
        draft,
        action="review_requested",
        actor="requester",
        reason="Ready",
        event_timestamp_utc="2026-08-22T12:05:00Z",
    )
    tampered = dict(request_event)
    tampered["previous_event_hash"] = "0" * 64
    with pytest.raises(ModelCandidateRegistryError, match="event hash"):
        verify_candidate_history([draft, requested], [registered, tampered])


def test_naive_training_boundary_and_invalid_shas_are_rejected():
    with pytest.raises(ModelCandidateRegistryError, match="timezone-aware"):
        register(training_data_boundary_utc="2026-08-22T10:00:00")
    with pytest.raises(ModelCandidateRegistryError, match="40-character"):
        register(code_commit_sha="not-a-sha")


def test_revision_files_are_immutable_and_history_loads(tmp_path):
    manifest, event = register()
    candidate_dir = tmp_path / manifest["candidate_id"]
    write_candidate_revision(candidate_dir, manifest, event)
    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        write_candidate_revision(candidate_dir, manifest, event)

    requested, request_event = transition_candidate(
        manifest,
        action="review_requested",
        actor="requester",
        reason="Ready",
        event_timestamp_utc="2026-08-22T12:05:00Z",
    )
    write_candidate_revision(candidate_dir, requested, request_event)
    manifests, events, latest = load_candidate_history(candidate_dir)
    assert len(manifests) == len(events) == 2
    assert latest["candidate_state"] == "review_requested"


def test_cli_register_review_decide_and_verify(tmp_path):
    promotion_path = tmp_path / "promotion.csv"
    health_path = tmp_path / "health.csv"
    output_root = tmp_path / "registry"
    promotion().to_csv(promotion_path, index=False)
    health().to_csv(health_path, index=False)
    register_args = [
        "register",
        "--promotion-summary",
        str(promotion_path),
        "--provider-health-summary",
        str(health_path),
        "--repository",
        "alexmarinos87/weather-energy-demand-pipeline",
        "--code-commit-sha",
        COMMIT_SHA,
        "--code-tree-sha",
        TREE_SHA,
        "--candidate-version",
        "0.1.0",
        "--training-data-boundary-utc",
        "2026-08-22T10:00:00Z",
        "--feature-contract-version",
        "time-horizon-v1",
        "weather-model-comparison-v1",
        "--forecast-weather-contract-version",
        "target-weather-v1",
        "--actor",
        "owner",
        "--reason",
        "Register",
        "--created-at-utc",
        "2026-08-22T12:00:00Z",
        "--output-root",
        str(output_root),
    ]
    assert main(register_args) == 0
    candidate_dirs = list(output_root.iterdir())
    assert len(candidate_dirs) == 1
    candidate_dir = candidate_dirs[0]
    assert main(
        [
            "request-review",
            "--candidate-dir",
            str(candidate_dir),
            "--actor",
            "requester",
            "--reason",
            "Ready",
            "--event-timestamp-utc",
            "2026-08-22T12:05:00Z",
        ]
    ) == 0
    assert main(
        [
            "decide",
            "--candidate-dir",
            str(candidate_dir),
            "--decision",
            "approved",
            "--reviewer",
            "reviewer",
            "--review-ticket",
            "REVIEW-4",
            "--reason",
            "Approved for manual trial",
            "--event-timestamp-utc",
            "2026-08-22T12:10:00Z",
        ]
    ) == 0
    assert main(["verify", "--candidate-dir", str(candidate_dir)]) == 0
    _, _, latest = load_candidate_history(candidate_dir)
    assert latest["candidate_state"] == "approved"
    assert not latest["deployment_authorized"]


def test_manifest_and_event_satisfy_versioned_schemas():
    manifest, event = register()
    root = Path(__file__).resolve().parents[1] / "data-contracts"
    manifest_schema = json.loads(
        (root / "model_candidate_manifest_schema.json").read_text(encoding="utf-8")
    )
    event_schema = json.loads(
        (root / "model_candidate_event_schema.json").read_text(encoding="utf-8")
    )
    assert list(
        Draft202012Validator(
            manifest_schema, format_checker=FormatChecker()
        ).iter_errors(manifest)
    ) == []
    assert list(
        Draft202012Validator(
            event_schema, format_checker=FormatChecker()
        ).iter_errors(event)
    ) == []

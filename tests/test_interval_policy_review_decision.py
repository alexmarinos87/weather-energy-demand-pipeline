from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
from jsonschema import Draft202012Validator, FormatChecker

from forecasting.interval_policy_review_decision import (
    DECISION_SAFETY_FIELDS,
    IntervalPolicyReviewDecisionError,
    create_policy_review_decision,
    sensitivity_summary_sha256,
    verify_policy_review_decision,
    write_policy_review_decision,
)
from forecasting.run_interval_policy_review_decision import main


ROOT = Path(__file__).resolve().parents[1]
SENSITIVITY_RUN_ID = "ips-" + "1" * 24
TREND_RUN_ID = "iht-" + "2" * 24


def _summary() -> pd.DataFrame:
    rows = []
    statuses = {"healthy": "healthy", "warning": "warning", "failed": "failed"}
    candidate_statuses = {
        "active-reference": statuses,
        "stricter-review": {"healthy": "healthy", "warning": "failed", "failed": "failed"},
        "tolerant-review": {"healthy": "healthy", "warning": "healthy", "failed": "healthy"},
    }
    for scenario, retained in statuses.items():
        for candidate_id, mapping in candidate_statuses.items():
            role = "active_reference" if candidate_id == "active-reference" else "review_candidate"
            candidate_status = mapping[scenario]
            changed = candidate_status != retained
            rows.append(
                {
                    "sensitivity_run_id": SENSITIVITY_RUN_ID,
                    "sensitivity_run_timestamp_utc": "2026-01-20T00:00:00Z",
                    "trend_run_id": TREND_RUN_ID,
                    "scenario": scenario,
                    "candidate_id": candidate_id,
                    "candidate_role": role,
                    "candidate_version": "interval-monitoring-review-candidate-v1",
                    "retained_monitor_status": retained,
                    "active_reference_status": retained,
                    "candidate_status": candidate_status,
                    "status_changed_from_active": changed,
                    "sensitivity_classification": (
                        "active_reference" if role == "active_reference" else "status_sensitive" if changed else "status_robust"
                    ),
                    "slice_count": 96,
                    "changed_slice_count": 48 if changed else 0,
                    "human_review_required": role == "review_candidate" and changed,
                    "sensitivity_contract_version": "interval-policy-sensitivity-v1",
                    "active_policy_updated": False,
                    "candidate_thresholds_activated": False,
                    "retained_evidence_mutated": False,
                    "interval_recalibration_performed": False,
                    "model_change_performed": False,
                    "schedule_change_performed": False,
                    "promotion_change_performed": False,
                    "alert_delivery_performed": False,
                }
            )
    return pd.DataFrame(rows)


def _decision(decision: str, target: str, requested_changes=()):
    return create_policy_review_decision(
        _summary(),
        decision=decision,
        target_candidate_id=target,
        reviewer_name="Alex Reviewer",
        reviewer_role="Data Platform Owner",
        review_ticket="GOV-127",
        rationale="The retained counterfactual evidence was reviewed against the active monitoring contract.",
        requested_changes=requested_changes,
        decision_timestamp_utc="2026-01-20T01:00:00Z",
    )


def test_retain_active_policy_is_named_hashed_and_non_activating():
    decision = _decision("retain_active_policy", "active-reference")
    verify_policy_review_decision(decision, _summary())

    assert decision["decision_effect"] == "active_policy_retained"
    assert decision["target_candidate_role"] == "active_reference"
    assert decision["requested_changes"] == []
    assert decision["named_human_review_confirmed"] is True
    assert decision["follow_up_human_action_required"] is False
    assert all(decision[field] is False for field in DECISION_SAFETY_FIELDS)


def test_reject_and_revision_decisions_target_review_candidates_only():
    rejected = _decision("reject_candidate", "stricter-review")
    revised = _decision(
        "request_candidate_revision",
        "tolerant-review",
        ["Reduce the permitted coverage shortfall and document the operational rationale."],
    )

    assert rejected["decision_effect"] == "candidate_rejected"
    assert revised["decision_effect"] == "candidate_revision_required"
    assert revised["follow_up_human_action_required"] is True
    with pytest.raises(IntervalPolicyReviewDecisionError, match="active-reference"):
        _decision("retain_active_policy", "stricter-review")
    with pytest.raises(IntervalPolicyReviewDecisionError, match="review candidate"):
        _decision("reject_candidate", "active-reference")


def test_revision_requires_meaningful_unique_requested_changes():
    with pytest.raises(IntervalPolicyReviewDecisionError, match="at least one"):
        _decision("request_candidate_revision", "stricter-review")
    with pytest.raises(IntervalPolicyReviewDecisionError, match="at least 10"):
        _decision("request_candidate_revision", "stricter-review", ["short"])
    with pytest.raises(IntervalPolicyReviewDecisionError, match="duplicates"):
        _decision(
            "request_candidate_revision",
            "stricter-review",
            ["Document the revised threshold rationale.", "Document the revised threshold rationale."],
        )


def test_decision_binds_the_complete_summary_and_rejects_tampering():
    summary = _summary()
    decision = _decision("reject_candidate", "stricter-review")
    original_digest = sensitivity_summary_sha256(summary)

    tampered = summary.copy()
    tampered.loc[
        (tampered["scenario"] == "warning")
        & (tampered["candidate_id"] == "stricter-review"),
        "changed_slice_count",
    ] = 47
    assert sensitivity_summary_sha256(tampered) != original_digest
    with pytest.raises(IntervalPolicyReviewDecisionError, match="does not match"):
        verify_policy_review_decision(decision, tampered)


def test_summary_must_retain_active_reference_and_false_authority():
    malformed = _summary()
    malformed.loc[0, "candidate_thresholds_activated"] = True
    with pytest.raises(IntervalPolicyReviewDecisionError, match="authority"):
        create_policy_review_decision(
            malformed,
            decision="retain_active_policy",
            target_candidate_id="active-reference",
            reviewer_name="Reviewer",
            reviewer_role="Owner",
            review_ticket="GOV-1",
            rationale="Reviewed retained evidence.",
        )

    malformed = _summary()
    malformed.loc[
        malformed["candidate_id"] == "active-reference", "candidate_status"
    ] = "healthy"
    with pytest.raises(IntervalPolicyReviewDecisionError, match="Active-reference"):
        create_policy_review_decision(
            malformed,
            decision="retain_active_policy",
            target_candidate_id="active-reference",
            reviewer_name="Reviewer",
            reviewer_role="Owner",
            review_ticket="GOV-1",
            rationale="Reviewed retained evidence.",
        )


def test_decision_rows_satisfy_versioned_schema():
    decision = _decision("request_candidate_revision", "stricter-review", ["Provide a bounded revised candidate with narrower tolerance."])
    schema = json.loads(
        (ROOT / "data-contracts" / "interval_policy_review_decision_schema.json").read_text(encoding="utf-8")
    )
    errors = list(
        Draft202012Validator(schema, format_checker=FormatChecker()).iter_errors(decision)
    )
    assert errors == []


def test_immutable_writer_and_cli(tmp_path):
    summary_path = tmp_path / "summary.csv"
    output_dir = tmp_path / "output"
    _summary().to_csv(summary_path, index=False)
    args = [
        "--sensitivity-summary", str(summary_path),
        "--decision", "reject_candidate",
        "--target-candidate-id", "stricter-review",
        "--reviewer-name", "Alex Reviewer",
        "--reviewer-role", "Data Platform Owner",
        "--review-ticket", "GOV-127",
        "--rationale", "The candidate is rejected after review of the retained sensitivity evidence.",
        "--decision-timestamp-utc", "2026-01-20T01:00:00Z",
        "--output-dir", str(output_dir),
    ]
    assert main(args) == 0
    assert len(list(output_dir.glob("*.json"))) == 1
    assert len(list(output_dir.glob("*.md"))) == 1
    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        main(args)

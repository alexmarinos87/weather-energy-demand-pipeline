from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
from jsonschema import Draft202012Validator, FormatChecker

from forecasting.interval_policy_review_decision import (
    IntervalPolicyReviewDecisionError,
    create_interval_policy_review_decision,
    sensitivity_summary_sha256,
    verify_interval_policy_review_decision,
)
from forecasting.run_interval_policy_review_decision import main

ROOT = Path(__file__).resolve().parents[1]


def summary() -> pd.DataFrame:
    rows = []
    timestamp = pd.Timestamp("2026-01-20T00:00:00Z")
    outcomes = {
        "active-reference": ("active_reference", "policy-v1", {"healthy": "healthy", "warning": "warning", "failed": "failed"}),
        "stricter-review": ("review_candidate", "strict-v1", {"healthy": "healthy", "warning": "failed", "failed": "failed"}),
        "tolerant-review": ("review_candidate", "tolerant-v1", {"healthy": "healthy", "warning": "healthy", "failed": "healthy"}),
    }
    for policy_id, (role, version, statuses) in outcomes.items():
        for scenario, retained in {"healthy": "healthy", "warning": "warning", "failed": "failed"}.items():
            candidate = statuses[scenario]
            changed = candidate != retained
            rows.append({
                "sensitivity_run_id": "ips-" + "1" * 24,
                "sensitivity_run_timestamp_utc": timestamp,
                "trend_run_id": "iht-" + "2" * 24,
                "scenario": scenario,
                "candidate_policy_id": policy_id,
                "candidate_policy_role": role,
                "candidate_policy_version": version,
                "retained_monitor_status": retained,
                "active_reference_status": retained,
                "candidate_status": candidate,
                "sensitivity_classification": "active_reference" if role == "active_reference" else "status_sensitive" if changed else "status_robust",
                "status_changed_from_active": changed,
                "changed_slice_count": 4 if changed else 0,
                "human_review_required": role == "review_candidate",
                "sensitivity_contract_version": "interval-policy-sensitivity-v1",
                "active_policy_updated": False,
                "candidate_thresholds_activated": False,
                "retained_evidence_mutated": False,
                "interval_recalibration_performed": False,
                "model_change_performed": False,
                "schedule_change_performed": False,
                "promotion_change_performed": False,
                "alert_delivery_performed": False,
            })
    return pd.DataFrame(rows)


def make_decision(**overrides):
    values = {
        "decision": "request_revision",
        "target_policy_id": "stricter-review",
        "reviewer_name": "Alex Reviewer",
        "reviewer_role": "Data Platform Owner",
        "review_ticket": "REV-123",
        "rationale": "The candidate needs a narrower, independently reviewed threshold change.",
        "requested_revision": "Retain active freshness limits and revise only the coverage drift threshold.",
        "decided_at_utc": "2026-01-20T01:00:00Z",
    }
    values.update(overrides)
    return create_interval_policy_review_decision(summary(), **values)


def test_request_revision_binds_named_reviewer_and_complete_evidence():
    decision = make_decision()
    assert decision["reviewer_name"] == "Alex Reviewer"
    assert decision["target_policy_id"] == "stricter-review"
    assert decision["sensitivity_summary_sha256"] == sensitivity_summary_sha256(summary())
    assert len(decision["scenario_evidence"]) == 3
    verify_interval_policy_review_decision(decision, summary())


def test_retain_must_target_active_reference():
    with pytest.raises(IntervalPolicyReviewDecisionError, match="active-reference"):
        make_decision(decision="retain_active_policy", requested_revision=None)
    retained = make_decision(
        decision="retain_active_policy",
        target_policy_id="active-reference",
        requested_revision=None,
    )
    assert retained["decision_effect"] == "active_policy_retained_for_review"


def test_reject_must_target_review_candidate():
    with pytest.raises(IntervalPolicyReviewDecisionError, match="review candidate"):
        make_decision(
            decision="reject_candidate",
            target_policy_id="active-reference",
            requested_revision=None,
        )
    rejected = make_decision(decision="reject_candidate", requested_revision=None)
    assert rejected["decision_effect"] == "review_candidate_rejected"


def test_revision_requires_meaningful_requested_change():
    with pytest.raises(IntervalPolicyReviewDecisionError, match="at least 20"):
        make_decision(requested_revision="too short")


def test_decision_cannot_precede_sensitivity_run():
    with pytest.raises(IntervalPolicyReviewDecisionError, match="cannot precede"):
        make_decision(decided_at_utc="2026-01-19T23:59:00Z")


def test_tampered_summary_or_decision_is_rejected():
    decision = make_decision()
    changed = summary()
    changed.loc[changed.index[-1], "changed_slice_count"] = 99
    with pytest.raises(IntervalPolicyReviewDecisionError, match="digest"):
        verify_interval_policy_review_decision(decision, changed)
    tampered = dict(decision)
    tampered["rationale"] = "changed"
    with pytest.raises(IntervalPolicyReviewDecisionError, match="hash"):
        verify_interval_policy_review_decision(tampered)


def test_authority_flags_are_all_false():
    decision = make_decision()
    for field in (
        "threshold_activation_authorized", "candidate_thresholds_activated",
        "active_policy_updated", "retained_evidence_mutated",
        "interval_recalibration_performed", "model_change_performed",
        "schedule_change_performed", "promotion_change_performed",
        "alert_delivery_performed", "deployment_performed",
        "external_publication_performed",
    ):
        assert decision[field] is False


def test_decision_satisfies_json_schema():
    decision = make_decision()
    schema = json.loads((ROOT / "data-contracts/interval_policy_review_decision_schema.json").read_text(encoding="utf-8"))
    errors = list(Draft202012Validator(schema, format_checker=FormatChecker()).iter_errors(decision))
    assert errors == []


def test_cli_writes_immutable_json_and_markdown(tmp_path):
    source = tmp_path / "summary.csv"
    output = tmp_path / "out"
    summary().to_csv(source, index=False)
    arguments = [
        "--sensitivity-summary", str(source), "--decision", "request_revision",
        "--target-policy-id", "stricter-review", "--reviewer-name", "Alex Reviewer",
        "--reviewer-role", "Data Platform Owner", "--review-ticket", "REV-123",
        "--rationale", "The candidate needs a narrower independently reviewed change.",
        "--requested-revision", "Retain active freshness limits and revise only coverage drift.",
        "--decided-at-utc", "2026-01-20T01:00:00Z", "--output-dir", str(output),
    ]
    assert main(arguments) == 0
    assert len(list(output.glob("*.json"))) == 1
    assert len(list(output.glob("*.md"))) == 1
    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        main(arguments)


def test_documentation_keeps_decision_non_activating():
    document = (ROOT / "INTERVAL_POLICY_REVIEW_DECISION.md").read_text(encoding="utf-8")
    assert "sensitivity_summary_sha256" in document
    assert "does not activate candidate thresholds" in document

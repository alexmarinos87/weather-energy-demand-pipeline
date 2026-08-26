from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
import json

import pandas as pd
import pytest
from jsonschema import Draft202012Validator, FormatChecker

import forecasting.interval_policy_candidate_revision as revision
from forecasting.interval_monitoring import PredictionIntervalMonitoringConfig


ROOT = Path(__file__).resolve().parents[1]


def decision() -> dict:
    return {
        "decision_id": "ipd-111111111111111111111111",
        "decision_sha256": "a" * 64,
        "decision": "request_candidate_revision",
        "decision_timestamp_utc": "2026-08-26T10:00:00Z",
        "sensitivity_run_id": "ips-222222222222222222222222",
        "sensitivity_summary_sha256": "b" * 64,
        "target_candidate_id": "stricter-review",
        "target_candidate_role": "review_candidate",
        "target_candidate_version": "interval-monitoring-review-candidate-v1",
        "requested_changes": [
            "Reconsider the calibration-history and width thresholds using retained evidence."
        ],
    }


def summary() -> pd.DataFrame:
    return pd.DataFrame([{"scenario": "warning"}])


def proposed_policy() -> dict:
    policy = asdict(PredictionIntervalMonitoringConfig())
    policy["min_calibration_observation_count"] += 6
    return policy


def create(monkeypatch) -> dict:
    monkeypatch.setattr(revision, "verify_policy_review_decision", lambda *_: None)
    return revision.create_candidate_revision_package(
        decision(),
        summary(),
        proposed_policy=proposed_policy(),
        revised_candidate_id="stricter-review-r2",
        revised_candidate_version="interval-monitoring-review-candidate-v2",
        proposed_by="Alex Reviewer",
        proposer_role="Data platform engineer",
        revision_ticket="GOV-129",
        rationale="Create a revised candidate that addresses the named review request.",
        evidence_notes=["The calibration minimum remains causal and explicit."],
        created_at_utc="2026-08-26T11:00:00Z",
        revision_id="ipcr-333333333333333333333333",
    )


def test_package_is_hash_bound_schema_valid_and_non_activating(monkeypatch):
    package = create(monkeypatch)

    schema = json.loads(
        (ROOT / "data-contracts/interval_policy_candidate_revision_schema.json").read_text(
            encoding="utf-8"
        )
    )
    errors = list(
        Draft202012Validator(
            schema, format_checker=FormatChecker()
        ).iter_errors(package)
    )
    assert errors == []
    assert package["changed_threshold_count"] == 1
    assert package["changed_thresholds"][0]["field"] == (
        "min_calibration_observation_count"
    )
    assert package["human_review_required"] is True
    for field in revision.AUTHORITY_FIELDS:
        assert package[field] is False


def test_package_rejects_non_revision_decision(monkeypatch):
    monkeypatch.setattr(revision, "verify_policy_review_decision", lambda *_: None)
    source = decision()
    source["decision"] = "retain_active_policy"

    with pytest.raises(
        revision.IntervalPolicyCandidateRevisionError,
        match="request-revision",
    ):
        revision.create_candidate_revision_package(
            source,
            summary(),
            proposed_policy=proposed_policy(),
            revised_candidate_id="stricter-review-r2",
            revised_candidate_version="v2",
            proposed_by="Alex Reviewer",
            proposer_role="Engineer",
            revision_ticket="GOV-129",
            rationale="Not a valid revision decision.",
            evidence_notes=["Evidence note"],
        )


def test_package_rejects_unchanged_active_policy(monkeypatch):
    monkeypatch.setattr(revision, "verify_policy_review_decision", lambda *_: None)

    with pytest.raises(
        revision.IntervalPolicyCandidateRevisionError,
        match="must differ",
    ):
        revision.create_candidate_revision_package(
            decision(),
            summary(),
            proposed_policy=asdict(PredictionIntervalMonitoringConfig()),
            revised_candidate_id="stricter-review-r2",
            revised_candidate_version="v2",
            proposed_by="Alex Reviewer",
            proposer_role="Engineer",
            revision_ticket="GOV-129",
            rationale="No effective policy change.",
            evidence_notes=["Evidence note"],
        )


def test_verification_rejects_modified_package(monkeypatch):
    package = create(monkeypatch)
    package["changed_threshold_count"] = 99

    with pytest.raises(
        revision.IntervalPolicyCandidateRevisionError,
        match="hash is invalid",
    ):
        revision.verify_candidate_revision_package(package, decision(), summary())


def test_writer_is_immutable(monkeypatch, tmp_path):
    package = create(monkeypatch)
    json_path, markdown_path = revision.write_candidate_revision_package(
        tmp_path, package
    )

    assert json_path.is_file()
    assert markdown_path.is_file()
    assert "review evidence only" in markdown_path.read_text(encoding="utf-8")
    with pytest.raises(FileExistsError):
        revision.write_candidate_revision_package(tmp_path, package)


def test_missing_or_unknown_policy_fields_fail_closed(monkeypatch):
    monkeypatch.setattr(revision, "verify_policy_review_decision", lambda *_: None)
    policy = proposed_policy()
    policy.pop("min_calibration_observation_count")
    policy["invented_threshold"] = 1

    with pytest.raises(
        revision.IntervalPolicyCandidateRevisionError,
        match="fields are invalid",
    ):
        revision.create_candidate_revision_package(
            decision(),
            summary(),
            proposed_policy=policy,
            revised_candidate_id="stricter-review-r2",
            revised_candidate_version="v2",
            proposed_by="Alex Reviewer",
            proposer_role="Engineer",
            revision_ticket="GOV-129",
            rationale="Invalid proposed policy shape.",
            evidence_notes=["Evidence note"],
        )

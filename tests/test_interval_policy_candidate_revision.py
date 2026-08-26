from __future__ import annotations

from dataclasses import asdict
import json
from pathlib import Path

import pandas as pd
import pytest
from jsonschema import Draft202012Validator, FormatChecker

from forecasting.interval_monitoring import PredictionIntervalMonitoringConfig
from forecasting.interval_policy_candidate_revision import (
    PACKAGE_SAFETY_FIELDS,
    IntervalPolicyCandidateRevisionError,
    create_candidate_revision_package,
    verify_candidate_revision_package,
)
from forecasting.interval_policy_review_decision import create_policy_review_decision
from forecasting.run_interval_policy_candidate_revision import main

ROOT = Path(__file__).resolve().parents[1]
REQUEST = "Reduce the permitted coverage shortfall and document the operational rationale."


def summary() -> pd.DataFrame:
    rows = []
    mappings = {
        "active-reference": {"healthy": "healthy", "warning": "warning", "failed": "failed"},
        "stricter-review": {"healthy": "healthy", "warning": "failed", "failed": "failed"},
    }
    for scenario in ("healthy", "warning", "failed"):
        for candidate_id, candidate_statuses in mappings.items():
            role = "active_reference" if candidate_id == "active-reference" else "review_candidate"
            candidate_status = candidate_statuses[scenario]
            changed = candidate_status != scenario
            rows.append({
                "sensitivity_run_id": "ips-" + "1" * 24,
                "sensitivity_run_timestamp_utc": "2026-01-20T00:00:00Z",
                "trend_run_id": "iht-" + "2" * 24,
                "scenario": scenario,
                "candidate_id": candidate_id,
                "candidate_role": role,
                "candidate_version": "interval-monitoring-review-candidate-v1",
                "retained_monitor_status": scenario,
                "active_reference_status": scenario,
                "candidate_status": candidate_status,
                "status_changed_from_active": changed,
                "sensitivity_classification": "active_reference" if role == "active_reference" else "status_sensitive" if changed else "status_robust",
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
            })
    return pd.DataFrame(rows)


def decision(name: str = "request_candidate_revision") -> dict:
    return create_policy_review_decision(
        summary(),
        decision=name,
        target_candidate_id="stricter-review",
        reviewer_name="Alex Reviewer",
        reviewer_role="Data Platform Owner",
        review_ticket="GOV-128",
        rationale="The retained sensitivity result requires a bounded candidate revision.",
        requested_changes=[REQUEST] if name == "request_candidate_revision" else (),
        decision_timestamp_utc="2026-01-20T01:00:00Z",
    )


def candidate(candidate_id: str, version: str, **overrides) -> dict:
    values = asdict(PredictionIntervalMonitoringConfig())
    values.update({
        "recent_interval_run_count": 3,
        "reference_interval_run_count": 6,
        "min_recent_interval_runs": 3,
        "min_reference_interval_runs": 6,
        "max_interval_run_age_minutes": 5040,
        "max_evaluation_age_minutes": 10080,
        "min_calibration_observation_count": 30,
        "max_recent_coverage_shortfall_pct_points": 2.0,
        "max_coverage_drop_pct_points": 2.0,
        "max_average_interval_width_increase_pct": 15.0,
        "max_calibration_history_drop_pct": 15.0,
        "candidate_id": candidate_id,
        "candidate_role": "review_candidate",
        "candidate_version": version,
        "rationale": "Reviewed candidate thresholds for counterfactual monitoring evidence.",
    })
    values.update(overrides)
    return values


def package() -> dict:
    return create_candidate_revision_package(
        decision(),
        summary(),
        source_candidate=candidate("stricter-review", "interval-monitoring-review-candidate-v1"),
        revised_candidate=candidate(
            "stricter-review-r1",
            "interval-monitoring-review-candidate-v2",
            max_recent_coverage_shortfall_pct_points=3.0,
            max_average_interval_width_increase_pct=20.0,
        ),
        requested_change_responses=[{
            "requested_change": REQUEST,
            "response": "The revised candidate keeps a bounded shortfall and width tolerance for another sensitivity review.",
            "changed_threshold_fields": [
                "max_recent_coverage_shortfall_pct_points",
                "max_average_interval_width_increase_pct",
            ],
        }],
        prepared_by="Alex Engineer",
        preparer_role="Data Platform Owner",
        revision_ticket="GOV-129",
        rationale="This package responds to the named revision request and remains counterfactual until a new sensitivity review.",
        prepared_at_utc="2026-01-20T02:00:00Z",
    )


def test_package_is_hashed_bound_and_non_activating():
    value = package()
    verify_candidate_revision_package(value, decision(), summary())
    assert value["compatibility_status"] == "compatible_for_new_sensitivity_review"
    assert value["next_review_action"] == "named_revision_package_review_required"
    assert value["automatic_sensitivity_rerun_allowed"] is False
    assert {row["field"] for row in value["threshold_changes"]} == {
        "max_recent_coverage_shortfall_pct_points",
        "max_average_interval_width_increase_pct",
    }
    assert all(value[field] is False for field in PACKAGE_SAFETY_FIELDS)


def test_requires_revision_request_and_matching_source_candidate():
    with pytest.raises(IntervalPolicyCandidateRevisionError, match="request_candidate_revision"):
        create_candidate_revision_package(
            decision("reject_candidate"), summary(),
            source_candidate=candidate("stricter-review", "interval-monitoring-review-candidate-v1"),
            revised_candidate=candidate("stricter-review-r1", "v2", max_recent_coverage_shortfall_pct_points=3.0),
            requested_change_responses=[], prepared_by="Engineer", preparer_role="Owner",
            revision_ticket="GOV-129", rationale="A sufficiently long rationale for this invalid revision package.",
        )
    with pytest.raises(IntervalPolicyCandidateRevisionError, match="decision target"):
        create_candidate_revision_package(
            decision(), summary(),
            source_candidate=candidate("other-candidate", "interval-monitoring-review-candidate-v1"),
            revised_candidate=candidate("other-candidate-r1", "v2", max_recent_coverage_shortfall_pct_points=3.0),
            requested_change_responses=[{
                "requested_change": REQUEST,
                "response": "The revised threshold is ready for a separate sensitivity review.",
                "changed_threshold_fields": ["max_recent_coverage_shortfall_pct_points"],
            }], prepared_by="Engineer", preparer_role="Owner", revision_ticket="GOV-129",
            rationale="A sufficiently long rationale for this invalid revision package.",
        )


def test_revision_requires_new_identity_and_real_change():
    source = candidate("stricter-review", "interval-monitoring-review-candidate-v1")
    with pytest.raises(IntervalPolicyCandidateRevisionError, match="new candidate_id"):
        create_candidate_revision_package(
            decision(), summary(), source_candidate=source, revised_candidate=dict(source),
            requested_change_responses=[], prepared_by="Engineer", preparer_role="Owner",
            revision_ticket="GOV-129", rationale="A sufficiently long rationale for this invalid revision package.",
        )
    with pytest.raises(IntervalPolicyCandidateRevisionError, match="at least one"):
        create_candidate_revision_package(
            decision(), summary(), source_candidate=source,
            revised_candidate=candidate("stricter-review-r1", "v2"),
            requested_change_responses=[], prepared_by="Engineer", preparer_role="Owner",
            revision_ticket="GOV-129", rationale="A sufficiently long rationale for this invalid revision package.",
        )


def test_responses_cover_every_request_and_changed_threshold():
    with pytest.raises(IntervalPolicyCandidateRevisionError, match="Every changed"):
        create_candidate_revision_package(
            decision(), summary(),
            source_candidate=candidate("stricter-review", "interval-monitoring-review-candidate-v1"),
            revised_candidate=candidate(
                "stricter-review-r1", "v2",
                max_recent_coverage_shortfall_pct_points=3.0,
                max_average_interval_width_increase_pct=20.0,
            ),
            requested_change_responses=[{
                "requested_change": REQUEST,
                "response": "The response deliberately covers only one changed threshold.",
                "changed_threshold_fields": ["max_recent_coverage_shortfall_pct_points"],
            }], prepared_by="Engineer", preparer_role="Owner", revision_ticket="GOV-129",
            rationale="A sufficiently long rationale for this invalid revision package.",
        )


def test_invalid_monitoring_configuration_is_rejected():
    with pytest.raises(IntervalPolicyCandidateRevisionError, match="monitoring configuration"):
        create_candidate_revision_package(
            decision(), summary(),
            source_candidate=candidate("stricter-review", "interval-monitoring-review-candidate-v1"),
            revised_candidate=candidate(
                "stricter-review-r1", "v2",
                recent_interval_run_count=2, min_recent_interval_runs=3,
            ),
            requested_change_responses=[{
                "requested_change": REQUEST,
                "response": "The response covers the invalid window changes for rejection.",
                "changed_threshold_fields": ["recent_interval_run_count"],
            }], prepared_by="Engineer", preparer_role="Owner", revision_ticket="GOV-129",
            rationale="A sufficiently long rationale for this invalid revision package.",
        )


def test_tampering_and_schema_validation():
    value = package()
    schema = json.loads((ROOT / "data-contracts" / "interval_policy_candidate_revision_schema.json").read_text(encoding="utf-8"))
    assert list(Draft202012Validator(schema, format_checker=FormatChecker()).iter_errors(value)) == []
    value["revised_candidate"]["max_recent_coverage_shortfall_pct_points"] = 99.0
    with pytest.raises(IntervalPolicyCandidateRevisionError, match="hash"):
        verify_candidate_revision_package(value, decision(), summary())


def test_cli_writes_immutable_json_and_markdown(tmp_path):
    summary_path = tmp_path / "summary.csv"
    decision_path = tmp_path / "decision.json"
    plan_path = tmp_path / "plan.json"
    output_dir = tmp_path / "output"
    summary().to_csv(summary_path, index=False)
    decision_path.write_text(json.dumps(decision()), encoding="utf-8")
    plan_path.write_text(json.dumps({
        "source_candidate": candidate("stricter-review", "interval-monitoring-review-candidate-v1"),
        "revised_candidate": candidate("stricter-review-r1", "v2", max_recent_coverage_shortfall_pct_points=3.0),
        "requested_change_responses": [{
            "requested_change": REQUEST,
            "response": "The revised shortfall threshold is bounded for a separate sensitivity review.",
            "changed_threshold_fields": ["max_recent_coverage_shortfall_pct_points"],
        }],
    }), encoding="utf-8")
    args = [
        "--decision", str(decision_path), "--sensitivity-summary", str(summary_path),
        "--revision-plan", str(plan_path), "--prepared-by", "Alex Engineer",
        "--preparer-role", "Data Platform Owner", "--revision-ticket", "GOV-129",
        "--rationale", "This bounded revision remains non-activating pending named review.",
        "--prepared-at-utc", "2026-01-20T02:00:00Z", "--output-dir", str(output_dir),
    ]
    assert main(args) == 0
    assert len(list(output_dir.glob("*.json"))) == 1
    assert len(list(output_dir.glob("*.md"))) == 1
    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        main(args)

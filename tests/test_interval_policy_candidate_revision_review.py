from __future__ import annotations

from dataclasses import asdict
import json
from pathlib import Path

import pandas as pd
import pytest
from jsonschema import Draft202012Validator, FormatChecker

from forecasting.interval_monitoring import PredictionIntervalMonitoringConfig
from forecasting.interval_policy_candidate_revision import (
    create_candidate_revision_package,
)
from forecasting.interval_policy_candidate_revision_review import (
    REVIEW_SAFETY_FIELDS,
    IntervalPolicyCandidateRevisionReviewError,
    create_candidate_revision_review,
    verify_candidate_revision_review,
)
from forecasting.interval_policy_review_decision import (
    create_policy_review_decision,
)
from forecasting.run_interval_policy_candidate_revision_review import main


ROOT = Path(__file__).resolve().parents[1]
REQUEST = (
    "Reduce the permitted coverage shortfall and document the operational "
    "rationale."
)


def summary() -> pd.DataFrame:
    rows = []
    mappings = {
        "active-reference": {
            "healthy": "healthy",
            "warning": "warning",
            "failed": "failed",
        },
        "stricter-review": {
            "healthy": "healthy",
            "warning": "failed",
            "failed": "failed",
        },
    }
    for scenario in ("healthy", "warning", "failed"):
        for candidate_id, candidate_statuses in mappings.items():
            role = (
                "active_reference"
                if candidate_id == "active-reference"
                else "review_candidate"
            )
            candidate_status = candidate_statuses[scenario]
            changed = candidate_status != scenario
            rows.append(
                {
                    "sensitivity_run_id": "ips-" + "1" * 24,
                    "sensitivity_run_timestamp_utc": "2026-01-20T00:00:00Z",
                    "trend_run_id": "iht-" + "2" * 24,
                    "scenario": scenario,
                    "candidate_id": candidate_id,
                    "candidate_role": role,
                    "candidate_version": (
                        "interval-monitoring-review-candidate-v1"
                    ),
                    "retained_monitor_status": scenario,
                    "active_reference_status": scenario,
                    "candidate_status": candidate_status,
                    "status_changed_from_active": changed,
                    "sensitivity_classification": (
                        "active_reference"
                        if role == "active_reference"
                        else "status_sensitive"
                        if changed
                        else "status_robust"
                    ),
                    "slice_count": 96,
                    "changed_slice_count": 48 if changed else 0,
                    "human_review_required": (
                        role == "review_candidate" and changed
                    ),
                    "sensitivity_contract_version": (
                        "interval-policy-sensitivity-v1"
                    ),
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


def policy_decision() -> dict:
    return create_policy_review_decision(
        summary(),
        decision="request_candidate_revision",
        target_candidate_id="stricter-review",
        reviewer_name="Alex Reviewer",
        reviewer_role="Data Platform Owner",
        review_ticket="GOV-128",
        rationale=(
            "The retained sensitivity result requires a bounded candidate "
            "revision."
        ),
        requested_changes=[REQUEST],
        decision_timestamp_utc="2026-01-20T01:00:00Z",
    )


def candidate(candidate_id: str, version: str, **overrides) -> dict:
    values = asdict(PredictionIntervalMonitoringConfig())
    values.update(
        {
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
            "rationale": (
                "Reviewed candidate thresholds for counterfactual monitoring "
                "evidence."
            ),
        }
    )
    values.update(overrides)
    return values


def package() -> dict:
    return create_candidate_revision_package(
        policy_decision(),
        summary(),
        source_candidate=candidate(
            "stricter-review",
            "interval-monitoring-review-candidate-v1",
        ),
        revised_candidate=candidate(
            "stricter-review-r1",
            "interval-monitoring-review-candidate-v2",
            max_recent_coverage_shortfall_pct_points=3.0,
        ),
        requested_change_responses=[
            {
                "requested_change": REQUEST,
                "response": (
                    "The revised shortfall remains bounded for a separate "
                    "sensitivity review."
                ),
                "changed_threshold_fields": [
                    "max_recent_coverage_shortfall_pct_points"
                ],
            }
        ],
        prepared_by="Alex Engineer",
        preparer_role="Data Platform Owner",
        revision_ticket="GOV-129",
        rationale=(
            "This package responds to the named revision request and remains "
            "counterfactual until a new review."
        ),
        prepared_at_utc="2026-01-20T02:00:00Z",
    )


def review(
    decision_name: str = "accept_for_sensitivity_review",
    requested_changes=(),
) -> dict:
    return create_candidate_revision_review(
        package(),
        policy_decision(),
        summary(),
        review_decision=decision_name,
        reviewer_name="Alex Reviewer",
        reviewer_role="Data Platform Owner",
        review_ticket="GOV-130",
        rationale=(
            "The package and complete retained evidence chain were reviewed "
            "without authorising execution."
        ),
        requested_changes=requested_changes,
        reviewed_at_utc="2026-01-20T03:00:00Z",
    )


def test_acceptance_is_hashed_bound_and_non_executing():
    value = review()
    verify_candidate_revision_review(
        value, package(), policy_decision(), summary()
    )
    assert value["review_effect"] == "separate_sensitivity_review_eligible"
    assert (
        value["next_action"]
        == "separate_sensitivity_review_request_required"
    )
    assert value["follow_up_human_action_required"] is True
    assert all(value[field] is False for field in REVIEW_SAFETY_FIELDS)


def test_reject_and_revision_request_have_distinct_follow_up_rules():
    rejected = review("reject_revision_package")
    revised = review(
        "request_package_revision",
        ["Document the rationale for the revised coverage shortfall."],
    )
    assert rejected["next_action"] == "no_further_action_recorded"
    assert rejected["follow_up_human_action_required"] is False
    assert revised["next_action"] == "new_revision_package_required"
    assert revised["follow_up_human_action_required"] is True
    with pytest.raises(
        IntervalPolicyCandidateRevisionReviewError,
        match="cannot include",
    ):
        review(
            "accept_for_sensitivity_review",
            ["This should not be retained on an acceptance decision."],
        )
    with pytest.raises(
        IntervalPolicyCandidateRevisionReviewError,
        match="at least one",
    ):
        review("request_package_revision")


def test_review_rejects_package_or_source_evidence_tampering():
    value = review()
    altered_package = package()
    altered_package["revised_candidate"][
        "max_recent_coverage_shortfall_pct_points"
    ] = 99.0
    with pytest.raises(Exception, match="hash"):
        verify_candidate_revision_review(
            value, altered_package, policy_decision(), summary()
        )

    altered_summary = summary()
    altered_summary.loc[
        (altered_summary["scenario"] == "warning")
        & (altered_summary["candidate_id"] == "stricter-review"),
        "changed_slice_count",
    ] = 47
    with pytest.raises(Exception, match="does not match"):
        verify_candidate_revision_review(
            value, package(), policy_decision(), altered_summary
        )


def test_review_timestamp_cannot_precede_package():
    with pytest.raises(
        IntervalPolicyCandidateRevisionReviewError,
        match="cannot precede",
    ):
        create_candidate_revision_review(
            package(),
            policy_decision(),
            summary(),
            review_decision="accept_for_sensitivity_review",
            reviewer_name="Reviewer",
            reviewer_role="Owner",
            review_ticket="GOV-130",
            rationale=(
                "A sufficiently long rationale for an invalid early review."
            ),
            reviewed_at_utc="2026-01-20T01:30:00Z",
        )


def test_review_satisfies_versioned_schema():
    value = review()
    schema = json.loads(
        (
            ROOT
            / "data-contracts"
            / "interval_policy_candidate_revision_review_schema.json"
        ).read_text(encoding="utf-8")
    )
    assert (
        list(
            Draft202012Validator(
                schema, format_checker=FormatChecker()
            ).iter_errors(value)
        )
        == []
    )


def test_cli_writes_immutable_json_and_markdown(tmp_path):
    summary_path = tmp_path / "summary.csv"
    decision_path = tmp_path / "decision.json"
    package_path = tmp_path / "package.json"
    output_dir = tmp_path / "output"
    summary().to_csv(summary_path, index=False)
    decision_path.write_text(
        json.dumps(policy_decision()), encoding="utf-8"
    )
    package_path.write_text(json.dumps(package()), encoding="utf-8")
    args = [
        "--revision-package",
        str(package_path),
        "--source-decision",
        str(decision_path),
        "--sensitivity-summary",
        str(summary_path),
        "--review-decision",
        "accept_for_sensitivity_review",
        "--reviewer-name",
        "Alex Reviewer",
        "--reviewer-role",
        "Data Platform Owner",
        "--review-ticket",
        "GOV-130",
        "--rationale",
        "The package is eligible only for a separately invoked sensitivity review.",
        "--reviewed-at-utc",
        "2026-01-20T03:00:00Z",
        "--output-dir",
        str(output_dir),
    ]
    assert main(args) == 0
    assert len(list(output_dir.glob("*.json"))) == 1
    assert len(list(output_dir.glob("*.md"))) == 1
    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        main(args)

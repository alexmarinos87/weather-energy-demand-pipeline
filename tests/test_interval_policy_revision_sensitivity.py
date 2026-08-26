from __future__ import annotations

from dataclasses import asdict
import json
from pathlib import Path

import pandas as pd
import pytest
from jsonschema import Draft202012Validator

from forecasting.interval_monitoring import PredictionIntervalMonitoringConfig
from forecasting.interval_policy_candidate_revision import (
    create_candidate_revision_package,
)
from forecasting.interval_policy_candidate_revision_review import (
    create_candidate_revision_review,
)
from forecasting.interval_policy_revision_sensitivity import (
    REVISION_SENSITIVITY_SAFETY_FIELDS,
    IntervalPolicyRevisionSensitivityError,
    run_revision_sensitivity,
    verify_revision_sensitivity,
    write_revision_sensitivity,
)
from forecasting.interval_policy_review_decision import (
    create_policy_review_decision,
)
from forecasting.run_interval_policy_revision_sensitivity import main


ROOT = Path(__file__).resolve().parents[1]
REQUEST = (
    "Reduce the permitted coverage shortfall and document the operational "
    "rationale."
)


def slice_trends() -> pd.DataFrame:
    observations = {
        "healthy": {
            "calibration": 30,
            "shortfall": 1.0,
            "width": 0.0,
        },
        "warning": {
            "calibration": 30,
            "shortfall": 1.0,
            "width": 35.0,
        },
        "failed": {
            "calibration": 20,
            "shortfall": 10.0,
            "width": 0.0,
        },
    }
    rows = []
    for scenario, values in observations.items():
        rows.append(
            {
                "trend_run_id": "iht-" + "2" * 24,
                "trend_run_timestamp_utc": "2026-01-20T00:00:00Z",
                "scenario": scenario,
                "source_area": "east_midlands",
                "resource_id": "resource-1",
                "city": "Nottingham",
                "requested_horizon_minutes": 30,
                "model_name": "ridge_weather_lag",
                "feature_contract_version": "time-horizon-v1",
                "target_coverage_level": 0.90,
                "interval_contract_version": "prediction-interval-v1",
                "monitor_status": scenario,
                "trend_contract_version": "interval-health-trend-v1",
                "recent_interval_run_count": 3,
                "reference_interval_run_count": 6,
                "reference_history_sufficient": True,
                "latest_interval_run_timestamp_utc": (
                    "2026-01-19T23:00:00Z"
                ),
                "latest_evaluation_end_utc": "2026-01-19T22:00:00Z",
                "recent_minimum_calibration_observation_count": values[
                    "calibration"
                ],
                "latest_coverage_shortfall_pct_points": values["shortfall"],
                "coverage_drop_pct_points": 0.0,
                "average_interval_width_increase_pct": values["width"],
                "calibration_history_drop_pct": 0.0,
            }
        )
    return pd.DataFrame(rows)


def source_summary() -> pd.DataFrame:
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
        for candidate_id, mapping in mappings.items():
            role = (
                "active_reference"
                if candidate_id == "active-reference"
                else "review_candidate"
            )
            candidate_status = mapping[scenario]
            changed = candidate_status != scenario
            rows.append(
                {
                    "sensitivity_run_id": "ips-" + "1" * 24,
                    "sensitivity_run_timestamp_utc": (
                        "2026-01-20T00:00:00Z"
                    ),
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
                    "slice_count": 1,
                    "changed_slice_count": 1 if changed else 0,
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
        source_summary(),
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


def revision_package(**revised_overrides) -> dict:
    revised = {
        "max_recent_coverage_shortfall_pct_points": 3.0,
        **revised_overrides,
    }
    changed_fields = sorted(revised)
    return create_candidate_revision_package(
        policy_decision(),
        source_summary(),
        source_candidate=candidate(
            "stricter-review",
            "interval-monitoring-review-candidate-v1",
        ),
        revised_candidate=candidate(
            "stricter-review-r1",
            "interval-monitoring-review-candidate-v2",
            **revised,
        ),
        requested_change_responses=[
            {
                "requested_change": REQUEST,
                "response": (
                    "The revised values remain bounded for a separate "
                    "sensitivity comparison."
                ),
                "changed_threshold_fields": changed_fields,
            }
        ],
        prepared_by="Alex Engineer",
        preparer_role="Data Platform Owner",
        revision_ticket="GOV-129",
        rationale=(
            "This package responds to the named revision request and remains "
            "counterfactual until reviewed."
        ),
        prepared_at_utc="2026-01-20T02:00:00Z",
    )


def revision_review(
    review_decision: str = "accept_for_sensitivity_review",
    package_value: dict | None = None,
) -> dict:
    package_value = package_value or revision_package()
    return create_candidate_revision_review(
        package_value,
        policy_decision(),
        source_summary(),
        review_decision=review_decision,
        reviewer_name="Alex Reviewer",
        reviewer_role="Data Platform Owner",
        review_ticket="GOV-130",
        rationale=(
            "The package and complete retained evidence chain were reviewed "
            "without authorising threshold activation."
        ),
        reviewed_at_utc="2026-01-20T03:00:00Z",
    )


def run(package_value: dict | None = None, review_value: dict | None = None):
    package_value = package_value or revision_package()
    review_value = review_value or revision_review(
        package_value=package_value
    )
    return run_revision_sensitivity(
        slice_trends(),
        review_value,
        package_value,
        policy_decision(),
        source_summary(),
        sensitivity_run_id="ips-" + "3" * 24,
        sensitivity_run_timestamp="2026-01-20T04:00:00Z",
    )


def test_run_reuses_canonical_evaluator_and_preserves_bindings():
    slices, summary, report = run()
    assert set(slices["candidate_id"]) == {
        "active-reference",
        "stricter-review-r1",
    }
    assert set(summary["candidate_id"]) == {
        "active-reference",
        "stricter-review-r1",
    }
    assert set(summary["source_revision_review_id"]) == {
        revision_review()["revision_review_id"]
    }
    assert set(summary["revision_sensitivity_contract_version"]) == {
        "interval-policy-revision-sensitivity-v1"
    }
    assert all(
        not bool(summary[field].any())
        for field in REVISION_SENSITIVITY_SAFETY_FIELDS
    )
    assert "retained evidence only" in report


def test_nonaccepted_review_is_rejected():
    package_value = revision_package()
    rejected = revision_review(
        "reject_revision_package", package_value=package_value
    )
    with pytest.raises(
        IntervalPolicyRevisionSensitivityError,
        match="accept_for_sensitivity_review",
    ):
        run_revision_sensitivity(
            slice_trends(),
            rejected,
            package_value,
            policy_decision(),
            source_summary(),
            sensitivity_run_id="ips-" + "3" * 24,
            sensitivity_run_timestamp="2026-01-20T04:00:00Z",
        )


def test_trend_identity_and_timestamp_must_follow_reviewed_chain():
    wrong = slice_trends()
    wrong["trend_run_id"] = "iht-" + "9" * 24
    with pytest.raises(
        IntervalPolicyRevisionSensitivityError,
        match="do not match",
    ):
        run_revision_sensitivity(
            wrong,
            revision_review(),
            revision_package(),
            policy_decision(),
            source_summary(),
            sensitivity_run_timestamp="2026-01-20T04:00:00Z",
        )
    with pytest.raises(
        IntervalPolicyRevisionSensitivityError,
        match="cannot precede",
    ):
        run_revision_sensitivity(
            slice_trends(),
            revision_review(),
            revision_package(),
            policy_decision(),
            source_summary(),
            sensitivity_run_timestamp="2026-01-20T02:30:00Z",
        )


def test_retained_trends_reject_window_geometry_revisions():
    package_value = revision_package(recent_interval_run_count=4)
    review_value = revision_review(package_value=package_value)
    with pytest.raises(
        IntervalPolicyRevisionSensitivityError,
        match="window geometry",
    ):
        run_revision_sensitivity(
            slice_trends(),
            review_value,
            package_value,
            policy_decision(),
            source_summary(),
            sensitivity_run_timestamp="2026-01-20T04:00:00Z",
        )


def test_output_tampering_is_rejected():
    slices, summary, _ = run()
    slices.loc[0, "source_revision_package_id"] = "ipr-" + "f" * 24
    with pytest.raises(
        IntervalPolicyRevisionSensitivityError,
        match="inconsistent",
    ):
        verify_revision_sensitivity(
            slices,
            summary,
            revision_review(),
            revision_package(),
            policy_decision(),
            source_summary(),
        )


def test_writer_creates_hash_bound_immutable_manifest(tmp_path):
    slices, summary, report = run()
    paths = write_revision_sensitivity(
        tmp_path,
        slices,
        summary,
        report,
        revision_review(),
        revision_package(),
        policy_decision(),
        source_summary(),
        output_format="csv",
    )
    assert len(paths) == 4
    manifest = json.loads(paths[-1].read_text(encoding="utf-8"))
    schema = json.loads(
        (
            ROOT
            / "data-contracts"
            / "interval_policy_revision_sensitivity_manifest_schema.json"
        ).read_text(encoding="utf-8")
    )
    assert list(Draft202012Validator(schema).iter_errors(manifest)) == []
    assert {item["role"] for item in manifest["artifacts"]} == {
        "slices",
        "summary",
        "report",
    }
    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        write_revision_sensitivity(
            tmp_path,
            slices,
            summary,
            report,
            revision_review(),
            revision_package(),
            policy_decision(),
            source_summary(),
            output_format="csv",
        )


def test_cli_writes_review_bound_outputs(tmp_path):
    trends_path = tmp_path / "trends.csv"
    summary_path = tmp_path / "source_summary.csv"
    review_path = tmp_path / "review.json"
    package_path = tmp_path / "package.json"
    decision_path = tmp_path / "decision.json"
    output_dir = tmp_path / "output"
    slice_trends().to_csv(trends_path, index=False)
    source_summary().to_csv(summary_path, index=False)
    review_path.write_text(json.dumps(revision_review()), encoding="utf-8")
    package_path.write_text(json.dumps(revision_package()), encoding="utf-8")
    decision_path.write_text(json.dumps(policy_decision()), encoding="utf-8")
    args = [
        "--slice-trends",
        str(trends_path),
        "--revision-review",
        str(review_path),
        "--revision-package",
        str(package_path),
        "--source-decision",
        str(decision_path),
        "--source-sensitivity-summary",
        str(summary_path),
        "--sensitivity-run-id",
        "ips-" + "3" * 24,
        "--sensitivity-run-timestamp",
        "2026-01-20T04:00:00Z",
        "--output-format",
        "csv",
        "--output-dir",
        str(output_dir),
    ]
    assert main(args) == 0
    assert len(list(output_dir.iterdir())) == 4

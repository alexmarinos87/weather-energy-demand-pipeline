from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
from jsonschema import Draft202012Validator, FormatChecker

from forecasting.interval_policy_compatibility import (
    CURRENT_POLICY_ID,
    PREVIOUS_POLICY_ID,
    SAFETY_FIELDS,
    IntervalPolicyCompatibilityError,
    assess_retained_policy_compatibility,
    prepare_retained_health_checks,
    write_compatibility_assessment,
)
from forecasting.run_interval_policy_compatibility import main


ROOT = Path(__file__).resolve().parents[1]
BASE_TIMESTAMP = pd.Timestamp("2026-01-20T00:00:00Z")


def health_checks(
    *,
    shortfall: float = 4.0,
    retained_threshold: float = 5.0,
    reference_passed: bool = True,
    monitor_run_id: str = "monitor-1",
    source_area: str = "east_midlands",
) -> pd.DataFrame:
    identity = {
        "monitor_run_id": monitor_run_id,
        "monitor_timestamp_utc": BASE_TIMESTAMP,
        "source_area": source_area,
        "resource_id": f"resource-{source_area}",
        "city": "Nottingham,GB",
        "requested_horizon_minutes": 30,
        "model_name": "ridge_weather_lag",
        "feature_contract_version": "time-horizon-v1",
        "target_coverage_level": 0.90,
        "interval_contract_version": "split-conformal-absolute-residual-v1",
        "latest_interval_run_id": f"interval-{monitor_run_id}",
        "policy_version": "prediction-interval-monitoring-policy-v1",
        "monitoring_contract_version": "prediction-interval-monitoring-v1",
    }
    definitions = [
        ("history", "error", "minimum_recent_interval_runs", 3.0, 2.0, ">=", True),
        ("freshness", "error", "latest_interval_run_age_minutes", 60.0, 10080.0, "<=", True),
        ("freshness", "error", "latest_interval_evaluation_age_minutes", 120.0, 20160.0, "<=", True),
        ("calibration", "error", "minimum_recent_calibration_observation_count", 48.0, 24.0, ">=", True),
        ("coverage", "error", "maximum_recent_coverage_shortfall_pct_points", shortfall, retained_threshold, "<=", shortfall <= retained_threshold),
        ("history", "warning", "minimum_reference_interval_runs", 6.0 if reference_passed else 2.0, 3.0, ">=", reference_passed),
        ("coverage", "warning", "maximum_interval_coverage_drop_pct_points", 2.0, 5.0, "<=", True),
        ("width", "warning", "maximum_average_interval_width_increase_pct", 10.0, 25.0, "<=", True),
        ("calibration", "warning", "maximum_calibration_history_drop_pct", 5.0, 25.0, "<=", True),
    ]
    return pd.DataFrame(
        [
            {
                **identity,
                "check_scope": scope,
                "severity": severity,
                "check_name": name,
                "observed_value": observed,
                "threshold_value": threshold,
                "comparator": comparator,
                "passed": passed,
                "details": name,
            }
            for scope, severity, name, observed, threshold, comparator, passed in definitions
        ]
    )


def assess(frame: pd.DataFrame | None = None):
    return assess_retained_policy_compatibility(
        health_checks() if frame is None else frame,
        assessment_run_id="ipc-111111111111111111111111",
        assessment_timestamp_utc="2026-01-20T01:00:00Z",
    )


def json_row(frame: pd.DataFrame) -> dict:
    row = frame.iloc[0].to_dict()
    for key, value in list(row.items()):
        if isinstance(value, pd.Timestamp):
            row[key] = value.isoformat()
        elif hasattr(value, "item"):
            row[key] = value.item()
    return row


def test_four_point_shortfall_is_newly_failed_under_reviewed_policy():
    slices, summary = assess()
    row = slices.iloc[0]
    assert row["previous_policy_id"] == PREVIOUS_POLICY_ID
    assert row["current_policy_id"] == CURRENT_POLICY_ID
    assert row["previous_policy_status"] == "healthy"
    assert row["current_policy_status"] == "failed"
    assert row["status_transition"] == "healthy_to_failed"
    assert row["changed_check_names"] == [
        "maximum_recent_coverage_shortfall_pct_points"
    ]
    assert summary.loc[0, "newly_failed_slice_count"] == 1
    assert summary.loc[0, "compatibility_classification"] == (
        "policy_change_affects_retained_conclusions"
    )
    assert all(not bool(row[field]) for field in SAFETY_FIELDS)


def test_two_point_shortfall_preserves_healthy_conclusion():
    slices, summary = assess(health_checks(shortfall=2.0, retained_threshold=3.0))
    assert slices.loc[0, "previous_policy_status"] == "healthy"
    assert slices.loc[0, "current_policy_status"] == "healthy"
    assert not slices.loc[0, "status_changed"]
    assert summary.loc[0, "changed_monitoring_slice_count"] == 0
    assert summary.loc[0, "compatibility_classification"] == (
        "retained_conclusions_unchanged"
    )


def test_existing_warning_is_preserved_when_coverage_rule_does_not_change():
    slices, summary = assess(
        health_checks(
            shortfall=2.0,
            retained_threshold=3.0,
            reference_passed=False,
        )
    )
    assert slices.loc[0, "previous_policy_status"] == "warning"
    assert slices.loc[0, "current_policy_status"] == "warning"
    assert summary.loc[0, "previous_overall_status"] == "warning"
    assert summary.loc[0, "current_overall_status"] == "warning"


def test_multiple_monitor_runs_and_slices_remain_independent():
    frame = pd.concat(
        [
            health_checks(shortfall=4.0, monitor_run_id="monitor-1"),
            health_checks(
                shortfall=1.0,
                retained_threshold=3.0,
                monitor_run_id="monitor-2",
                source_area="south_wales",
            ),
        ],
        ignore_index=True,
    )
    slices, summary = assess(frame)
    assert len(slices) == 2
    assert len(summary) == 2
    changed = summary.set_index("source_monitor_run_id")[
        "changed_monitoring_slice_count"
    ].to_dict()
    assert changed == {"monitor-1": 1, "monitor-2": 0}


def test_retained_comparator_tampering_and_duplicate_checks_are_rejected():
    tampered = health_checks()
    tampered.loc[
        tampered["check_name"].eq("minimum_recent_interval_runs"), "passed"
    ] = False
    with pytest.raises(IntervalPolicyCompatibilityError, match="contradicts"):
        prepare_retained_health_checks(tampered)

    duplicated = pd.concat(
        [health_checks(), health_checks().iloc[[0]]], ignore_index=True
    )
    with pytest.raises(IntervalPolicyCompatibilityError, match="duplicate"):
        prepare_retained_health_checks(duplicated)


def test_unreviewed_threshold_and_naive_timestamp_are_rejected():
    wrong_threshold = health_checks(retained_threshold=4.0)
    target = wrong_threshold["check_name"].eq(
        "maximum_recent_coverage_shortfall_pct_points"
    )
    wrong_threshold.loc[target, "passed"] = True
    with pytest.raises(IntervalPolicyCompatibilityError, match="three-point"):
        prepare_retained_health_checks(wrong_threshold)

    naive = health_checks()
    naive["monitor_timestamp_utc"] = "2026-01-20T00:00:00"
    with pytest.raises(IntervalPolicyCompatibilityError, match="timezone-aware"):
        prepare_retained_health_checks(naive)


def test_outputs_satisfy_versioned_schemas():
    slices, summary = assess()
    contracts = ROOT / "data-contracts"
    slice_schema = json.loads(
        (contracts / "interval_policy_compatibility_slice_schema.json").read_text(
            encoding="utf-8"
        )
    )
    summary_schema = json.loads(
        (contracts / "interval_policy_compatibility_summary_schema.json").read_text(
            encoding="utf-8"
        )
    )
    assert list(
        Draft202012Validator(
            slice_schema, format_checker=FormatChecker()
        ).iter_errors(json_row(slices))
    ) == []
    assert list(
        Draft202012Validator(
            summary_schema, format_checker=FormatChecker()
        ).iter_errors(json_row(summary))
    ) == []


def test_writer_and_cli_are_immutable(tmp_path):
    slices, summary = assess()
    direct = tmp_path / "direct"
    paths = write_compatibility_assessment(
        direct, slices, summary, output_format="csv"
    )
    assert set(paths) == {"slices", "summary", "report"}
    assert all(path.is_file() for path in paths.values())
    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        write_compatibility_assessment(
            direct, slices, summary, output_format="csv"
        )

    input_path = tmp_path / "checks.csv"
    health_checks().to_csv(input_path, index=False)
    output = tmp_path / "cli"
    assert main(
        [
            "--health-checks",
            str(input_path),
            "--output-dir",
            str(output),
            "--output-format",
            "csv",
            "--assessment-run-id",
            "ipc-222222222222222222222222",
            "--assessment-timestamp-utc",
            "2026-01-20T02:00:00Z",
        ]
    ) == 0
    assert len(list(output.iterdir())) == 3

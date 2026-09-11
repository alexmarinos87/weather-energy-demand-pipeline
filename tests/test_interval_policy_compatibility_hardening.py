"""Regression coverage for issue #96 using real canonical monitor outputs."""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from forecasting.interval_monitoring import (
    PredictionIntervalMonitoringConfig,
    monitor_prediction_interval_health,
)
from forecasting.interval_policy_compatibility import (
    IntervalPolicyCompatibilityError,
    assess_retained_policy_compatibility,
    prepare_retained_health_checks,
    write_compatibility_assessment,
)

RUN_ID = "ipc-" + "a" * 24
TIMESTAMP = pd.Timestamp("2026-01-20T01:00:00Z")
TARGET_CHECK = "maximum_recent_coverage_shortfall_pct_points"
DRIFT_CHECK = "maximum_interval_coverage_drop_pct_points"


@pytest.fixture
def checks() -> pd.DataFrame:
    rows = []
    for index in range(9):
        timestamp = TIMESTAMP - pd.Timedelta(days=9 - index)
        rows.append({
            "interval_run_id": f"interval-{index}",
            "interval_run_timestamp_utc": timestamp,
            "source_area": "east_midlands",
            "resource_id": "92d3431c-15d7-4aa6-ad34-2335596a026c",
            "city": "Nottingham,GB",
            "requested_horizon_minutes": 30,
            "model_name": "ridge_weather_lag",
            "feature_contract_version": "time-horizon-v1",
            "target_coverage_level": 0.90,
            "calibration_observation_count": 48,
            "calibration_radius_mw": 10.0,
            "evaluation_end_utc": timestamp - pd.Timedelta(hours=1),
            "evaluation_observation_count": 100,
            "empirical_coverage_pct": 86.0,
            "average_interval_width_mw": 20.0,
            "interval_contract_version": "split-conformal-absolute-residual-v1",
        })
    frame, _ = monitor_prediction_interval_health(
        pd.DataFrame(rows),
        config=PredictionIntervalMonitoringConfig(
            max_recent_coverage_shortfall_pct_points=5.0
        ),
        as_of_utc=TIMESTAMP,
        run_timestamp=TIMESTAMP,
        run_id="monitor-regression",
    )
    return frame


def assess(checks: pd.DataFrame):
    return assess_retained_policy_compatibility(
        checks, assessment_run_id=RUN_ID,
        assessment_timestamp_utc=TIMESTAMP + pd.Timedelta(hours=1),
    )


@pytest.mark.parametrize("column", ["source_area", "resource_id", "city", "latest_interval_run_id"])
@pytest.mark.parametrize("missing", [float("nan"), pd.NA, pd.NaT, None, " "])
def test_missing_identity_is_rejected(checks, column, missing):
    checks[column] = missing
    with pytest.raises(IntervalPolicyCompatibilityError, match="non-empty"):
        prepare_retained_health_checks(checks)


@pytest.mark.parametrize("column,value", [
    ("severity", "warning"), ("check_scope", "history"), ("comparator", ">="),
])
def test_target_check_metadata_cannot_change_policy_semantics(checks, column, value):
    target = checks["check_name"].eq(TARGET_CHECK)
    checks.loc[target, column] = value
    if column == "comparator":
        checks.loc[target, "passed"] = False
    with pytest.raises(IntervalPolicyCompatibilityError, match="canonical"):
        prepare_retained_health_checks(checks)


def test_non_target_threshold_cannot_silently_change(checks):
    checks.loc[checks["check_name"].eq("latest_interval_run_age_minutes"), "threshold_value"] = 99999.0
    with pytest.raises(IntervalPolicyCompatibilityError, match="threshold"):
        prepare_retained_health_checks(checks)


def test_missing_conditional_drift_check_is_rejected(checks):
    incomplete = checks.loc[~checks["check_name"].eq(DRIFT_CHECK)]
    with pytest.raises(IntervalPolicyCompatibilityError, match="drift"):
        prepare_retained_health_checks(incomplete)


def test_unknown_check_is_not_treated_as_canonical_evidence(checks):
    extra = checks.iloc[[0]].copy()
    extra["check_name"] = "unreviewed_custom_check"
    with pytest.raises(IntervalPolicyCompatibilityError, match="canonical"):
        prepare_retained_health_checks(pd.concat([checks, extra], ignore_index=True))


def test_latest_interval_identity_must_be_consistent_within_slice(checks):
    checks.loc[checks.index[0], "latest_interval_run_id"] = "unrelated-interval"
    with pytest.raises(IntervalPolicyCompatibilityError, match="latest_interval_run_id"):
        prepare_retained_health_checks(checks)


def test_assessment_cannot_precede_retained_monitoring(checks):
    with pytest.raises(IntervalPolicyCompatibilityError, match="precede"):
        assess_retained_policy_compatibility(
            checks, assessment_run_id=RUN_ID,
            assessment_timestamp_utc=TIMESTAMP - pd.Timedelta(seconds=1),
        )


@pytest.mark.parametrize("run_id", ["../escape", "unsafe/name", "", "ipc-123", "a\\b"])
def test_assessment_requires_a_safe_run_id(checks, run_id):
    with pytest.raises(IntervalPolicyCompatibilityError, match="assessment_run_id"):
        assess_retained_policy_compatibility(
            checks, assessment_run_id=run_id, assessment_timestamp_utc=TIMESTAMP,
        )


def test_canonical_input_is_unchanged_and_comparison_is_time_stable(checks):
    original = checks.copy(deep=True)
    slices, summary = assess(checks)
    later_slices, later_summary = assess_retained_policy_compatibility(
        checks, assessment_run_id=RUN_ID,
        assessment_timestamp_utc=TIMESTAMP + pd.Timedelta(days=365),
    )
    pd.testing.assert_frame_equal(checks, original)
    for column in ("previous_policy_status", "current_policy_status"):
        pd.testing.assert_series_equal(slices[column], later_slices[column])
    for column in ("previous_overall_status", "current_overall_status"):
        pd.testing.assert_series_equal(summary[column], later_summary[column])
    assert summary.loc[0, "newly_failed_slice_count"] == 1


@pytest.mark.parametrize("output_format", ["csv", "parquet"])
def test_writer_round_trip(checks, tmp_path, output_format):
    slices, summary = assess(checks)
    paths = write_compatibility_assessment(tmp_path, slices, summary, output_format=output_format)
    read = pd.read_csv if output_format == "csv" else pd.read_parquet
    assert len(read(paths["slices"])) == len(slices)
    assert read(paths["summary"]).loc[0, "newly_failed_slice_count"] == 1
    assert set(paths) == {"slices", "summary", "report"}


@pytest.mark.parametrize("collision", ["slices", "summary", "report"])
def test_preexisting_output_prevents_any_partial_publication(checks, tmp_path, collision):
    slices, summary = assess(checks)
    suffix = "md" if collision == "report" else "csv"
    existing = tmp_path / f"interval_policy_compatibility_{collision}_{RUN_ID}.{suffix}"
    existing.write_bytes(b"existing-evidence")
    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        write_compatibility_assessment(tmp_path, slices, summary, output_format="csv")
    assert existing.read_bytes() == b"existing-evidence"
    assert set(tmp_path.iterdir()) == {existing}


def test_broken_symlink_is_not_followed_or_replaced(checks, tmp_path):
    slices, summary = assess(checks)
    outside = tmp_path / "outside.csv"
    existing = tmp_path / f"interval_policy_compatibility_slices_{RUN_ID}.csv"
    existing.symlink_to(outside)
    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        write_compatibility_assessment(tmp_path, slices, summary, output_format="csv")
    assert existing.is_symlink()
    assert not outside.exists()
    assert set(tmp_path.iterdir()) == {existing}


def test_serialization_failure_leaves_no_published_files(checks, tmp_path, monkeypatch):
    slices, summary = assess(checks)
    original = pd.DataFrame.to_csv

    def fail_on_summary(self, *args, **kwargs):
        if "previous_overall_status" in self.columns:
            raise OSError("simulated serialization failure")
        return original(self, *args, **kwargs)

    monkeypatch.setattr(pd.DataFrame, "to_csv", fail_on_summary)
    with pytest.raises(OSError, match="simulated serialization failure"):
        write_compatibility_assessment(tmp_path, slices, summary, output_format="csv")
    assert list(tmp_path.iterdir()) == []


def test_racing_writer_cannot_be_overwritten(checks, tmp_path, monkeypatch):
    slices, summary = assess(checks)
    original = pd.DataFrame.to_csv
    collision = tmp_path / f"interval_policy_compatibility_summary_{RUN_ID}.csv"

    def competing_publication(self, *args, **kwargs):
        if "previous_overall_status" in self.columns:
            collision.write_bytes(b"concurrent-evidence")
        return original(self, *args, **kwargs)

    monkeypatch.setattr(pd.DataFrame, "to_csv", competing_publication)
    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        write_compatibility_assessment(tmp_path, slices, summary, output_format="csv")
    assert collision.read_bytes() == b"concurrent-evidence"
    assert set(tmp_path.iterdir()) == {collision}


def test_invalid_format_is_rejected_before_directory_creation(checks, tmp_path):
    slices, summary = assess(checks)
    output = tmp_path / "must-not-exist"
    with pytest.raises(IntervalPolicyCompatibilityError, match="output_format"):
        write_compatibility_assessment(output, slices, summary, output_format="json")
    assert not output.exists()

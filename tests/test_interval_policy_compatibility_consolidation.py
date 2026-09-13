"""Differential regressions for #97; sources come from the canonical monitor."""
from __future__ import annotations

import ast
import inspect
from pathlib import Path

import pandas as pd
import pytest

from forecasting.interval_health_trends import build_interval_health_trends
from forecasting.interval_monitoring import (
    PredictionIntervalMonitoringConfig,
    monitor_prediction_interval_health,
)
from forecasting.interval_policy_compatibility import assess_retained_policy_compatibility
from forecasting.interval_policy_retained_compatibility import (
    evaluate_retained_policy_compatibility,
)

ROOT = Path(__file__).resolve().parents[1]
AS_OF = pd.Timestamp("2026-01-20T01:00:00Z")
ASSESSMENT = AS_OF + pd.Timedelta(hours=2)
RUN_ID = "ipca-" + "7" * 24


def canonical_evidence(*, shortfalls=(12.0, 0.0, 0.0), weights=(100, 100, 100),
                       history_count=9, retained_threshold=5.0, areas=("east_midlands",)):
    history_rows = []
    for area in areas:
        for index in range(history_count):
            recent_index = index - (history_count - 3)
            shortfall = shortfalls[recent_index] if recent_index >= 0 else 0.0
            weight = weights[recent_index] if recent_index >= 0 else 100
            timestamp = AS_OF - pd.Timedelta(hours=history_count - index)
            history_rows.append({
                "scenario": "retained",
                "history_sequence": index + 1,
                "interval_run_id": f"interval-{index}",
                "interval_run_timestamp_utc": timestamp,
                "source_area": area,
                "resource_id": f"resource-{area}",
                "city": f"city-{area}",
                "requested_horizon_minutes": 30,
                "model_name": "ridge_weather_lag",
                "feature_contract_version": "time-horizon-v1",
                "target_coverage_level": 0.90,
                "calibration_observation_count": 48,
                "calibration_radius_mw": 10.0,
                "evaluation_end_utc": timestamp - pd.Timedelta(minutes=30),
                "evaluation_observation_count": weight,
                "empirical_coverage_pct": 90.0 - shortfall,
                "average_interval_width_mw": 20.0,
                "interval_contract_version": "split-conformal-absolute-residual-v1",
            })
    history = pd.DataFrame(history_rows)
    checks, monitor_summary = monitor_prediction_interval_health(
        history,
        config=PredictionIntervalMonitoringConfig(
            max_recent_coverage_shortfall_pct_points=retained_threshold
        ),
        as_of_utc=AS_OF, run_timestamp=AS_OF + pd.Timedelta(minutes=5),
        run_id="monitor-retained",
    )
    monitor_summary["scenario"] = "retained"
    _, trends = build_interval_health_trends(
        history, monitor_summary,
        trend_run_id="iht-" + "8" * 24,
        trend_run_timestamp=AS_OF + pd.Timedelta(hours=1),
    )
    return history, checks, trends


def call_adapter(trends, checks, *, timestamp=ASSESSMENT):
    # Exercise the old signature during the red run, not an artificial TypeError.
    kwargs = {"compatibility_run_id": RUN_ID,
              "compatibility_run_timestamp": timestamp}
    if "retained_health_checks" in inspect.signature(
        evaluate_retained_policy_compatibility
    ).parameters:
        kwargs["retained_health_checks"] = checks
    return evaluate_retained_policy_compatibility(trends, **kwargs)


def test_weighted_recent_coverage_matches_canonical_monitor_not_latest_row():
    history, checks, trends = canonical_evidence()
    assert trends.loc[0, "latest_coverage_shortfall_pct_points"] == 0.0
    slices, summary, _ = call_adapter(trends, checks)
    assert summary.loc[0, "previous_policy_status"] == "healthy"
    assert summary.loc[0, "current_policy_status"] == "failed"
    assert summary.loc[0, "newly_failed_slice_count"] == 1
    assert set(slices["observed_coverage_shortfall_pct_points"]) == {4.0}
    for threshold, column in ((5.0, "previous_policy_status"), (3.0, "current_policy_status")):
        _, expected = monitor_prediction_interval_health(
            history,
            config=PredictionIntervalMonitoringConfig(
                max_recent_coverage_shortfall_pct_points=threshold
            ),
            as_of_utc=AS_OF, run_timestamp=AS_OF,
        )
        assert summary.loc[0, column] == expected.loc[0, "monitor_status"]


def test_later_assessment_does_not_re_age_historical_evidence():
    _, checks, trends = canonical_evidence()
    before = call_adapter(trends, checks)
    after = call_adapter(trends, checks, timestamp=ASSESSMENT + pd.Timedelta(days=365))
    for column in ("previous_policy_status", "current_policy_status", "changed_slice_count"):
        pd.testing.assert_series_equal(before[1][column], after[1][column])
    pd.testing.assert_series_equal(
        before[0]["latest_interval_run_age_minutes"], after[0]["latest_interval_run_age_minutes"]
    )


def test_trend_only_evidence_fails_closed_with_migration_guidance():
    _, _, trends = canonical_evidence()
    with pytest.raises(ValueError, match="retained_health_checks"):
        evaluate_retained_policy_compatibility(
            trends, compatibility_run_id=RUN_ID,
            compatibility_run_timestamp=ASSESSMENT,
        )


@pytest.mark.parametrize("column", ["monitor_as_of_utc", "monitor_run_id"])
def test_missing_historical_binding_is_rejected(column):
    _, checks, trends = canonical_evidence()
    if column == "monitor_as_of_utc":
        checks = checks.drop(columns=column)
    else:
        trends = trends.drop(columns=column)
    with pytest.raises(ValueError, match=column):
        call_adapter(trends, checks)


@pytest.mark.parametrize("column,value", [
    ("monitor_run_id", "unrelated-monitor"),
    ("latest_interval_run_id", "unrelated-interval"),
    ("recent_empirical_coverage_pct", 90.0),
    ("recent_interval_run_count", 2),
    ("reference_interval_run_count", 5),
    ("recent_minimum_calibration_observation_count", 47),
    ("monitor_status", "failed"),
    ("coverage_drop_pct_points", 0.0),
])
def test_mismatched_trend_and_check_evidence_is_rejected(column, value):
    _, checks, trends = canonical_evidence()
    trends[column] = value
    with pytest.raises(ValueError):
        call_adapter(trends, checks)


def test_extra_check_slice_cannot_be_silently_dropped():
    _, checks, trends = canonical_evidence(areas=("east_midlands", "south_wales"))
    with pytest.raises(ValueError, match="slice"):
        call_adapter(trends.iloc[[0]], checks)


def test_one_monitor_run_cannot_be_relabelled_as_two_scenarios():
    _, checks, trends = canonical_evidence()
    duplicate = trends.copy()
    duplicate["scenario"] = "other-label"
    with pytest.raises(ValueError, match="scenario"):
        call_adapter(pd.concat([trends, duplicate], ignore_index=True), checks)


@pytest.mark.parametrize("shortfall,previous,current", [
    (3.0, "healthy", "healthy"),
    (3.000001, "healthy", "failed"),
    (5.0, "healthy", "failed"),
    (5.000001, "failed", "failed"),
])
def test_exact_threshold_boundaries(shortfall, previous, current):
    _, checks, trends = canonical_evidence(shortfalls=(shortfall,) * 3)
    _, summary, _ = call_adapter(trends, checks)
    assert summary.loc[0, "previous_policy_status"] == previous
    assert summary.loc[0, "current_policy_status"] == current


def test_observation_weights_are_not_replaced_with_equal_run_weights():
    _, checks, trends = canonical_evidence(
        shortfalls=(12.0, 0.0, 0.0), weights=(50, 200, 50)
    )
    slices, summary, _ = call_adapter(trends, checks)
    assert set(slices["observed_coverage_shortfall_pct_points"]) == {2.0}
    assert summary.loc[0, "current_policy_status"] == "healthy"


def test_insufficient_reference_history_preserves_warning_and_exact_slices():
    _, checks, trends = canonical_evidence(
        shortfalls=(2.0,) * 3, history_count=3,
        areas=("east_midlands", "south_wales"),
    )
    slices, summary, _ = call_adapter(trends, checks)
    assert summary.loc[0, "previous_policy_status"] == "warning"
    assert summary.loc[0, "current_policy_status"] == "warning"
    assert summary.loc[0, "slice_count"] == 2
    assert len(slices) == 4
    assert not slices["drift_rules_evaluated"].any()


def test_original_checks_and_trends_are_unchanged_and_source_digest_is_bound():
    _, checks, trends = canonical_evidence(retained_threshold=3.0)
    checks_before, trends_before = checks.copy(deep=True), trends.copy(deep=True)
    _, direct_summary = assess_retained_policy_compatibility(
        checks, assessment_run_id="ipc-" + "7" * 24,
        assessment_timestamp_utc=ASSESSMENT,
    )
    slices, summary, _ = call_adapter(trends, checks)
    assert set(summary["source_health_checks_sha256"]) == set(
        direct_summary["source_health_checks_sha256"]
    )
    assert set(slices["source_health_checks_sha256"]) == set(summary["source_health_checks_sha256"])
    pd.testing.assert_frame_equal(checks, checks_before)
    pd.testing.assert_frame_equal(trends, trends_before)


def test_adapter_delegates_instead_of_maintaining_a_second_rule_engine():
    module = inspect.getmodule(evaluate_retained_policy_compatibility)
    definitions = {node.name for node in ast.walk(ast.parse(inspect.getsource(module)))
                   if isinstance(node, ast.FunctionDef)}
    assert "_evaluate" not in definitions
    assert "assess_retained_policy_compatibility" in inspect.getsource(module)


def test_legacy_evaluator_import_callers_are_inventoried():
    allowed = {
        "forecasting/run_interval_policy_retained_compatibility.py",
        "tests/test_interval_policy_retained_compatibility.py",
        "tests/test_interval_policy_compatibility_consolidation.py",
    }
    found = set()
    for directory in ("forecasting", "tests", "ingestion", "transformations", "fabric"):
        for path in (ROOT / directory).rglob("*.py"):
            for node in ast.walk(ast.parse(path.read_text(encoding="utf-8")))):
                if isinstance(node, ast.ImportFrom) and node.module == (
                    "forecasting." + "interval_policy_retained_compatibility"
                ):
                    if any(alias.name == "evaluate_retained_policy_compatibility" for alias in node.names):
                        found.add(path.relative_to(ROOT).as_posix())
    assert found == allowed


def test_unimplemented_check_manifest_schema_is_removed_without_code_consumers():
    filename = "interval_policy_" + "compatibility_manifest_schema.json"
    consumers = []
    for directory in ("forecasting", "tests", "ingestion", "transformations", "fabric"):
        for path in (ROOT / directory).rglob("*.py"):
            if filename in path.read_text(encoding="utf-8"):
                consumers.append(path.relative_to(ROOT).as_posix())
    assert consumers == [], f"Unexpected manifest consumers: {consumers}"
    assert not (ROOT / "data-contracts" / filename).exists()

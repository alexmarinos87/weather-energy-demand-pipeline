"""Cross-field summary contracts, backed by actual canonical monitoring output."""
from __future__ import annotations

import pandas as pd
import pytest

from interval_policy_evidence_fixtures import AS_OF, three_scenario_evidence
from forecasting._interval_policy_retained_compatibility_common import (
    IntervalPolicyRetainedCompatibilityError,
    compatibility_summary_sha256,
    prepare_compatibility_summary,
)
from forecasting.interval_health_trends import build_interval_health_trends
from forecasting.interval_monitoring import PredictionIntervalMonitoringConfig, monitor_prediction_interval_health
from forecasting.interval_policy_retained_compatibility import evaluate_retained_policy_compatibility
from forecasting.interval_policy_retained_compatibility_manifest import build_compatibility_manifest


@pytest.fixture(scope="module")
def assessed():
    _, checks, trends = three_scenario_evidence()
    return evaluate_retained_policy_compatibility(
        trends, retained_health_checks=checks,
        compatibility_run_id="ipca-" + "2" * 24,
        compatibility_run_timestamp="2026-01-20T01:00:00Z",
    )


@pytest.mark.parametrize("name,changes", [
    ("tightened", {"changed_slice_count": 2}),
    ("tightened", {"newly_failed_slice_count": 2}),
    ("tightened", {"newly_failed_slice_count": 0}),
    ("tightened", {"changed_slice_count": 0}),
    ("tightened", {"slice_count": True}),
    ("tightened", {"changed_slice_count": True}),
    ("tightened", {"newly_failed_slice_count": True}),
    ("tightened", {"current_policy_status": "healthy"}),
    ("tightened", {"current_policy_status": "warning"}),
    ("tightened", {"compatibility_classification": "fully_compatible"}),
    ("tightened", {"compatibility_classification": "unreviewed-classification"}),
    ("tightened", {"retained_status_compatibility": "matches_both_policies"}),
    ("tightened", {"retained_status_compatibility": "unknown"}),
    ("tightened", {"retained_monitor_status": "warning"}),
    ("tightened", {"human_review_required": False}),
    ("stable", {"human_review_required": True}),
    ("stable", {"current_policy_status": "warning"}),
    ("failed", {"current_policy_status": "healthy"}),
    ("failed", {"changed_slice_count": 1, "newly_failed_slice_count": 1,
                "compatibility_classification": "slice_change_without_scenario_change",
                "human_review_required": True}),
])
def test_impossible_or_contradictory_summary_cannot_receive_a_digest(assessed, name, changes):
    _, summary, _ = assessed
    invalid = summary.loc[summary["scenario"].eq(name)].copy().reset_index(drop=True)
    for column, value in changes.items():
        invalid[column] = value
    before = invalid.copy(deep=True)
    with pytest.raises(IntervalPolicyRetainedCompatibilityError):
        compatibility_summary_sha256(invalid)
    pd.testing.assert_frame_equal(invalid, before)


@pytest.mark.parametrize("output_format", ["csv", "parquet"])
def test_matching_saved_and_supplied_invalid_summaries_are_not_blessed(assessed, tmp_path, output_format):
    slices, summary, report = assessed
    invalid = summary.copy(deep=True)
    invalid.loc[invalid["scenario"].eq("tightened"), "human_review_required"] = False
    paths = {"slices": tmp_path / f"slices.{output_format}",
             "summary": tmp_path / f"summary.{output_format}",
             "report": tmp_path / "report.md"}
    for frame, path in ((slices, paths["slices"]), (invalid, paths["summary"])):
        if output_format == "csv":
            frame.to_csv(path, index=False)
        else:
            frame.to_parquet(path, index=False)
    paths["report"].write_text(report, encoding="utf-8")
    before = {path: path.read_bytes() for path in paths.values()}
    with pytest.raises(IntervalPolicyRetainedCompatibilityError):
        build_compatibility_manifest(invalid, artifacts=paths)
    assert {path: path.read_bytes() for path in paths.values()} == before


def real_summary(shortfalls, run_count):
    history, _, _ = three_scenario_evidence()
    template = history.loc[history["scenario"].eq("stable")].tail(run_count)
    histories = []
    for index, shortfall in enumerate(shortfalls):
        part = template.copy()
        part["scenario"] = "mixed"
        part["resource_id"] = f"resource-{index}"
        # Each retained run contains every slice and has one contiguous sequence.
        part["history_sequence"] = range(1, len(part) + 1)
        part["interval_run_id"] = [f"interval-mixed-{number}" for number in range(1, len(part) + 1)]
        part["empirical_coverage_pct"] = 90.0 - shortfall
        histories.append(part)
    history = pd.concat(histories, ignore_index=True)
    checks, monitoring_summary = monitor_prediction_interval_health(
        history, config=PredictionIntervalMonitoringConfig(max_recent_coverage_shortfall_pct_points=5.0),
        as_of_utc=AS_OF, run_timestamp=AS_OF + pd.Timedelta(minutes=5), run_id="monitor-mixed",
    )
    monitoring_summary["scenario"] = "mixed"
    _, trends = build_interval_health_trends(
        history, monitoring_summary, trend_run_id="iht-" + "5" * 24,
        trend_run_timestamp=AS_OF + pd.Timedelta(minutes=10),
    )
    return evaluate_retained_policy_compatibility(
        trends, retained_health_checks=checks, compatibility_run_id="ipca-" + "6" * 24,
        compatibility_run_timestamp=AS_OF + pd.Timedelta(hours=1),
    )[1]


@pytest.mark.parametrize("shortfalls,run_count,previous,current,changed,classification", [
    ((1.0,), 9, "healthy", "healthy", 0, "fully_compatible"),
    ((4.0,), 9, "healthy", "failed", 1, "scenario_status_escalation"),
    ((10.0,), 9, "failed", "failed", 0, "fully_compatible"),
    ((1.0,), 3, "warning", "warning", 0, "fully_compatible"),
    ((4.0,), 3, "warning", "failed", 1, "scenario_status_escalation"),
    ((10.0, 4.0), 9, "failed", "failed", 1, "slice_change_without_scenario_change"),
    ((1.0, 4.0), 9, "healthy", "failed", 1, "scenario_status_escalation"),
    ((4.0, 4.0), 9, "healthy", "failed", 2, "scenario_status_escalation"),
])
def test_valid_canonical_edge_cases_remain_accepted(shortfalls, run_count, previous, current, changed, classification):
    summary = real_summary(shortfalls, run_count)
    before = summary.copy(deep=True)
    result = prepare_compatibility_summary(summary)
    row = result.iloc[0]
    assert row["previous_policy_status"] == previous
    assert row["current_policy_status"] == current
    assert row["changed_slice_count"] == row["newly_failed_slice_count"] == changed
    assert row["compatibility_classification"] == classification
    assert len(compatibility_summary_sha256(result)) == 64
    pd.testing.assert_frame_equal(summary, before)

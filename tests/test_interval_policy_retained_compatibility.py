from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
from jsonschema import Draft202012Validator, FormatChecker

from forecasting._interval_policy_retained_compatibility_common import (
    COMPATIBILITY_SAFETY_FIELDS,
    CURRENT_POLICY_ID,
    PREVIOUS_POLICY_ID,
    IntervalPolicyRetainedCompatibilityError,
    compatibility_policy_candidates,
)
from forecasting.interval_policy_retained_compatibility import (
    evaluate_retained_policy_compatibility,
)
from forecasting.interval_policy_retained_compatibility_manifest import (
    build_compatibility_manifest,
    verify_compatibility_manifest,
)
from forecasting.interval_policy_sensitivity import evaluate_policy_sensitivity
from forecasting.run_interval_policy_retained_compatibility import main


ROOT = Path(__file__).resolve().parents[1]
RUN_ID = "ipca-" + "1" * 24
RUN_TIMESTAMP = "2026-01-20T01:00:00Z"


def slice_trends() -> pd.DataFrame:
    rows = []
    scenarios = {
        "stable": ("healthy", 1.0),
        "tightened": ("healthy", 4.0),
        "failed": ("failed", 10.0),
    }
    for index, (scenario, (retained_status, shortfall)) in enumerate(
        scenarios.items(), start=1
    ):
        rows.append(
            {
                "trend_run_id": "iht-" + "2" * 24,
                "trend_run_timestamp_utc": "2026-01-20T00:00:00Z",
                "scenario": scenario,
                "source_area": "east_midlands",
                "resource_id": "resource-1",
                "city": "Nottingham,GB",
                "requested_horizon_minutes": 30,
                "model_name": "ridge_weather_lag",
                "feature_contract_version": "time-horizon-v1",
                "target_coverage_level": 0.90,
                "interval_contract_version": "prediction-interval-v1",
                "monitor_status": retained_status,
                "trend_contract_version": "interval-health-trend-v1",
                "recent_interval_run_count": 3,
                "reference_interval_run_count": 6,
                "reference_history_sufficient": True,
                "latest_interval_run_timestamp_utc": (
                    f"2026-01-19T{20 + index:02d}:00:00Z"
                ),
                "latest_evaluation_end_utc": (
                    f"2026-01-19T{19 + index:02d}:00:00Z"
                ),
                "recent_minimum_calibration_observation_count": 30,
                "latest_coverage_shortfall_pct_points": shortfall,
                "coverage_drop_pct_points": 0.0,
                "average_interval_width_increase_pct": 0.0,
                "calibration_history_drop_pct": 0.0,
            }
        )
    return pd.DataFrame(rows)


def run():
    return evaluate_retained_policy_compatibility(
        slice_trends(),
        compatibility_run_id=RUN_ID,
        compatibility_run_timestamp=RUN_TIMESTAMP,
    )


def test_assessment_preserves_retained_status_and_exposes_tightening():
    slices, summary, report = run()
    keyed = summary.set_index("scenario")
    assert keyed.loc["stable", "previous_policy_status"] == "healthy"
    assert keyed.loc["stable", "current_policy_status"] == "healthy"
    assert keyed.loc["tightened", "previous_policy_status"] == "healthy"
    assert keyed.loc["tightened", "current_policy_status"] == "failed"
    assert (
        keyed.loc["tightened", "retained_status_compatibility"]
        == "matches_previous_policy_only"
    )
    assert keyed.loc["tightened", "newly_failed_slice_count"] == 1
    assert bool(keyed.loc["tightened", "human_review_required"])
    assert keyed.loc["failed", "previous_policy_status"] == "failed"
    assert keyed.loc["failed", "current_policy_status"] == "failed"
    assert all(not bool(summary[field].any()) for field in COMPATIBILITY_SAFETY_FIELDS)
    assert all(not bool(slices[field].any()) for field in COMPATIBILITY_SAFETY_FIELDS)
    assert "Historical monitor statuses are preserved" in report


def test_assessment_matches_the_canonical_policy_evaluator():
    slices, summary, _ = run()
    current_status = summary.set_index("scenario")["current_policy_status"]
    canonical_input = slice_trends().copy()
    canonical_input["monitor_status"] = canonical_input["scenario"].map(
        current_status
    )
    previous, current = compatibility_policy_candidates()
    canonical_slices, _, _ = evaluate_policy_sensitivity(
        canonical_input,
        candidates=(previous, current),
        sensitivity_run_id="ips-" + "3" * 24,
        sensitivity_run_timestamp=RUN_TIMESTAMP,
    )
    observed = {
        (row.scenario, row.policy_id): row.evaluated_status
        for row in slices.itertuples(index=False)
    }
    expected = {
        (row.scenario, row.candidate_id): row.candidate_slice_status
        for row in canonical_slices.itertuples(index=False)
    }
    assert observed == expected
    assert set(slices["policy_id"]) == {PREVIOUS_POLICY_ID, CURRENT_POLICY_ID}


def test_manifest_binds_exact_artifacts_and_schema(tmp_path):
    slices, summary, report = run()
    paths = {
        "slices": tmp_path / "slices.csv",
        "summary": tmp_path / "summary.csv",
        "report": tmp_path / "report.md",
    }
    slices.to_csv(paths["slices"], index=False)
    summary.to_csv(paths["summary"], index=False)
    paths["report"].write_text(report, encoding="utf-8")
    manifest = build_compatibility_manifest(summary, artifacts=paths)
    verify_compatibility_manifest(
        manifest,
        summary,
        artifact_directory=tmp_path,
    )
    schema = json.loads(
        (
            ROOT
            / "data-contracts"
            / "interval_policy_retained_compatibility_manifest_schema.json"
        ).read_text(encoding="utf-8")
    )
    assert list(
        Draft202012Validator(
            schema, format_checker=FormatChecker()
        ).iter_errors(manifest)
    ) == []


def test_manifest_rejects_artifact_or_summary_tampering(tmp_path):
    slices, summary, report = run()
    paths = {
        "slices": tmp_path / "slices.csv",
        "summary": tmp_path / "summary.csv",
        "report": tmp_path / "report.md",
    }
    slices.to_csv(paths["slices"], index=False)
    summary.to_csv(paths["summary"], index=False)
    paths["report"].write_text(report, encoding="utf-8")
    manifest = build_compatibility_manifest(summary, artifacts=paths)
    paths["report"].write_text(report + "\nchanged\n", encoding="utf-8")
    with pytest.raises(
        IntervalPolicyRetainedCompatibilityError, match="artifact hash"
    ):
        verify_compatibility_manifest(
            manifest,
            summary,
            artifact_directory=tmp_path,
        )
    paths["report"].write_text(report, encoding="utf-8")
    changed = summary.copy()
    changed.loc[0, "changed_slice_count"] = 99
    with pytest.raises(
        IntervalPolicyRetainedCompatibilityError,
        match="summary digest",
    ):
        verify_compatibility_manifest(
            manifest,
            changed,
            artifact_directory=tmp_path,
        )


def test_cli_writes_immutable_assessment_outputs(tmp_path):
    input_path = tmp_path / "slice-trends.csv"
    output_dir = tmp_path / "output"
    slice_trends().to_csv(input_path, index=False)
    args = [
        "--slice-trends",
        str(input_path),
        "--output-dir",
        str(output_dir),
        "--compatibility-run-id",
        RUN_ID,
        "--compatibility-run-timestamp",
        RUN_TIMESTAMP,
    ]
    assert main(args) == 0
    assert len(list(output_dir.glob("*.csv"))) == 2
    assert len(list(output_dir.glob("*.md"))) == 1
    assert len(list(output_dir.glob("*.json"))) == 1
    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        main(args)

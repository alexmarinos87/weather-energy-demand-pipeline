from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
from jsonschema import Draft202012Validator, FormatChecker

from forecasting.interval_policy_sensitivity import (
    AUTHORITY_FIELDS,
    IntervalPolicySensitivityError,
    PolicyCandidate,
    default_policy_candidates,
    evaluate_policy_sensitivity,
)
from forecasting.run_interval_policy_sensitivity import main

ROOT = Path(__file__).resolve().parents[1]
SENSITIVITY_RUN_ID = "ips-" + "2" * 24
TREND_RUN_ID = "iht-" + "1" * 24
TREND_TIMESTAMP = pd.Timestamp("2026-01-20T00:00:00Z")
REVIEW_TIMESTAMP = "2026-01-20T01:00:00Z"
AREAS = (
    ("east_midlands", "resource-east", "Nottingham"),
    ("south_wales", "resource-wales", "Cardiff"),
)


def _slice_trends() -> pd.DataFrame:
    rows = []
    for scenario in ("healthy", "warning", "failed"):
        for source_area, resource_id, city in AREAS:
            for horizon in (30, 60):
                for model in ("persistence_current_value", "ridge_weather_lag"):
                    for coverage in (0.8, 0.9):
                        if scenario == "healthy":
                            retained, shortfall, width_growth, calibration = (
                                "healthy", 2.0, 0.0, 30
                            )
                            coverage_drop, calibration_drop = 0.0, 0.0
                        elif scenario == "warning":
                            retained, shortfall, width_growth, calibration = (
                                "warning", 3.0, 35.0, 30
                            )
                            coverage_drop, calibration_drop = 2.0, 0.0
                        else:
                            retained, shortfall, width_growth, calibration = (
                                "failed", 10.0, 0.0, 20
                            )
                            coverage_drop, calibration_drop = 1.0, 33.333333
                        rows.append(
                            {
                                "trend_run_id": TREND_RUN_ID,
                                "trend_run_timestamp_utc": TREND_TIMESTAMP,
                                "scenario": scenario,
                                "source_area": source_area,
                                "resource_id": resource_id,
                                "city": city,
                                "requested_horizon_minutes": horizon,
                                "model_name": model,
                                "feature_contract_version": "time-horizon-v1",
                                "target_coverage_level": coverage,
                                "interval_contract_version": (
                                    "split-conformal-absolute-residual-v1"
                                ),
                                "monitor_status": retained,
                                "trend_contract_version": "interval-health-trend-v1",
                                "recent_interval_run_count": 3,
                                "reference_interval_run_count": 6,
                                "reference_history_sufficient": True,
                                "latest_interval_run_timestamp_utc": (
                                    TREND_TIMESTAMP - pd.Timedelta(hours=1)
                                ),
                                "latest_evaluation_end_utc": (
                                    TREND_TIMESTAMP - pd.Timedelta(hours=2)
                                ),
                                "recent_minimum_calibration_observation_count": (
                                    calibration
                                ),
                                "latest_coverage_shortfall_pct_points": shortfall,
                                "coverage_drop_pct_points": coverage_drop,
                                "average_interval_width_increase_pct": width_growth,
                                "calibration_history_drop_pct": calibration_drop,
                            }
                        )
    return pd.DataFrame(rows)


def _json_row(row: pd.Series) -> dict[str, object]:
    result = {}
    for key, value in row.items():
        if isinstance(value, pd.Timestamp):
            result[key] = value.isoformat()
        elif pd.isna(value):
            result[key] = None
        elif hasattr(value, "item"):
            result[key] = value.item()
        else:
            result[key] = value
    return result


def test_default_candidates_are_bounded_and_have_one_exact_active_reference():
    candidates = default_policy_candidates()
    assert 2 <= len(candidates) <= 5
    assert [c.candidate_role for c in candidates].count("active_reference") == 1
    assert {c.candidate_id for c in candidates} == {
        "active-reference", "stricter-review", "tolerant-review"
    }


def test_sensitivity_preserves_exact_slices_and_expected_status_matrix():
    slices, summary, report = evaluate_policy_sensitivity(
        _slice_trends(),
        sensitivity_run_id=SENSITIVITY_RUN_ID,
        sensitivity_run_timestamp=REVIEW_TIMESTAMP,
    )
    expected_slices = len(_slice_trends())
    assert len(slices) == expected_slices * 3
    matrix = {
        (row.scenario, row.candidate_id): row.candidate_status
        for row in summary.itertuples(index=False)
    }
    assert matrix[("healthy", "active-reference")] == "healthy"
    assert matrix[("healthy", "stricter-review")] == "healthy"
    assert matrix[("healthy", "tolerant-review")] == "healthy"
    assert matrix[("warning", "active-reference")] == "warning"
    assert matrix[("warning", "stricter-review")] == "failed"
    assert matrix[("warning", "tolerant-review")] == "healthy"
    assert matrix[("failed", "active-reference")] == "failed"
    assert matrix[("failed", "stricter-review")] == "failed"
    assert matrix[("failed", "tolerant-review")] == "healthy"
    assert set(slices["source_area"]) == {area[0] for area in AREAS}
    assert "counterfactual human-review evidence" in report


def test_active_reference_must_exactly_match_checked_in_policy():
    candidates = list(default_policy_candidates())
    active = candidates[0]
    candidates[0] = PolicyCandidate(
        **{**active.__dict__, "max_recent_coverage_shortfall_pct_points": 6.0}
    )
    with pytest.raises(IntervalPolicySensitivityError, match="exactly match"):
        evaluate_policy_sensitivity(
            _slice_trends(),
            candidates=candidates,
            sensitivity_run_id=SENSITIVITY_RUN_ID,
            sensitivity_run_timestamp=REVIEW_TIMESTAMP,
        )


def test_active_reference_fails_closed_on_retained_status_disagreement():
    trends = _slice_trends()
    trends.loc[trends["scenario"] == "warning", "monitor_status"] = "healthy"
    with pytest.raises(IntervalPolicySensitivityError, match="does not reproduce"):
        evaluate_policy_sensitivity(
            trends,
            sensitivity_run_id=SENSITIVITY_RUN_ID,
            sensitivity_run_timestamp=REVIEW_TIMESTAMP,
        )


def test_reference_insufficient_slices_leave_drift_rules_unevaluated():
    trends = _slice_trends().loc[lambda frame: frame["scenario"] == "healthy"].copy()
    trends["reference_interval_run_count"] = 0
    trends["reference_history_sufficient"] = False
    for column in (
        "coverage_drop_pct_points",
        "average_interval_width_increase_pct",
        "calibration_history_drop_pct",
    ):
        trends[column] = None
    trends["monitor_status"] = "warning"
    slices, summary, _ = evaluate_policy_sensitivity(
        trends,
        sensitivity_run_id=SENSITIVITY_RUN_ID,
        sensitivity_run_timestamp=REVIEW_TIMESTAMP,
    )
    assert not slices["drift_rules_evaluated"].any()
    assert slices["coverage_drop_passed"].isna().all()
    active = summary.loc[summary["candidate_id"] == "active-reference"]
    assert set(active["candidate_status"]) == {"warning"}


def test_duplicate_exact_slice_identity_is_rejected():
    trends = pd.concat([_slice_trends(), _slice_trends().iloc[[0]]], ignore_index=True)
    with pytest.raises(IntervalPolicySensitivityError, match="duplicate"):
        evaluate_policy_sensitivity(
            trends,
            sensitivity_run_id=SENSITIVITY_RUN_ID,
            sensitivity_run_timestamp=REVIEW_TIMESTAMP,
        )


def test_all_authority_flags_remain_false():
    slices, summary, _ = evaluate_policy_sensitivity(
        _slice_trends(),
        sensitivity_run_id=SENSITIVITY_RUN_ID,
        sensitivity_run_timestamp=REVIEW_TIMESTAMP,
    )
    for field in AUTHORITY_FIELDS:
        assert set(slices[field]) == {False}
        assert set(summary[field]) == {False}


def test_rows_satisfy_versioned_json_schemas():
    slices, summary, _ = evaluate_policy_sensitivity(
        _slice_trends(),
        sensitivity_run_id=SENSITIVITY_RUN_ID,
        sensitivity_run_timestamp=REVIEW_TIMESTAMP,
    )
    contracts = (
        ("interval_policy_sensitivity_slice_schema.json", _json_row(slices.iloc[0])),
        ("interval_policy_sensitivity_summary_schema.json", _json_row(summary.iloc[0])),
    )
    for filename, row in contracts:
        schema = json.loads((ROOT / "data-contracts" / filename).read_text())
        errors = list(
            Draft202012Validator(schema, format_checker=FormatChecker()).iter_errors(row)
        )
        assert errors == []


def test_cli_writes_immutable_csv_outputs(tmp_path):
    trend_path = tmp_path / "slice_trends.csv"
    output = tmp_path / "out"
    _slice_trends().to_csv(trend_path, index=False)
    args = [
        "--slice-trends", str(trend_path),
        "--output-dir", str(output),
        "--output-format", "csv",
        "--sensitivity-run-id", SENSITIVITY_RUN_ID,
        "--sensitivity-run-timestamp", REVIEW_TIMESTAMP,
    ]
    assert main(args) == 0
    assert (output / f"interval_policy_sensitivity_slices_{SENSITIVITY_RUN_ID}.csv").is_file()
    assert (output / f"interval_policy_sensitivity_summary_{SENSITIVITY_RUN_ID}.csv").is_file()
    assert (output / f"interval_policy_sensitivity_report_{SENSITIVITY_RUN_ID}.md").is_file()
    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        main(args)


def test_documentation_records_counterfactual_and_non_mutating_boundary():
    document = (ROOT / "INTERVAL_POLICY_SENSITIVITY.md").read_text()
    assert "active-reference" in document
    assert "stricter-review" in document
    assert "tolerant-review" in document
    assert "does not call `monitor_prediction_interval_health`" in document
    assert "active_policy_updated=false" in document
    assert "Candidate outcomes are counterfactual" in document

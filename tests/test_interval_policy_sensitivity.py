from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
from jsonschema import Draft202012Validator, FormatChecker

from forecasting.interval_health_trends import (
    AUTHORITY_FIELDS as TREND_AUTHORITY_FIELDS,
    build_interval_health_trends,
)
from forecasting.interval_policy_sensitivity import (
    ACTIVE_POLICY_ID,
    AUTHORITY_FIELDS,
    IntervalMonitoringPolicyCandidate,
    IntervalPolicySensitivityError,
    build_interval_policy_sensitivity,
    default_policy_candidates,
)
from forecasting.interval_monitoring import PredictionIntervalMonitoringConfig
from forecasting.run_interval_policy_sensitivity import main

ROOT = Path(__file__).resolve().parents[1]
TREND_RUN_ID = "iht-" + "1" * 24
SENSITIVITY_RUN_ID = "ips-" + "2" * 24
RUN_TIMESTAMP = "2026-01-20T00:00:00Z"


def _history_and_summary() -> tuple[pd.DataFrame, pd.DataFrame]:
    anchor = pd.Timestamp(RUN_TIMESTAMP)
    rows: list[dict[str, object]] = []
    for scenario in ("healthy", "warning", "failed"):
        for sequence in range(1, 10):
            run_timestamp = anchor - pd.Timedelta(hours=10 - sequence)
            recent = sequence > 6
            nominal = 90.0
            width = 10.0
            empirical = nominal - 2.0
            calibration = 30
            if scenario == "warning":
                empirical = nominal - (3.0 if recent else 1.0)
                width = 13.5 if recent else 10.0
            elif scenario == "failed" and recent:
                empirical = nominal - 10.0
                calibration = 20
            rows.append(
                {
                    "scenario": scenario,
                    "history_sequence": sequence,
                    "interval_run_id": f"{scenario}-interval-{sequence:02d}",
                    "interval_run_timestamp_utc": run_timestamp,
                    "source_area": "east_midlands",
                    "resource_id": "resource-east",
                    "city": "Nottingham",
                    "requested_horizon_minutes": 30,
                    "model_name": "ridge_weather_lag",
                    "feature_contract_version": "time-horizon-v1",
                    "target_coverage_level": 0.90,
                    "calibration_observation_count": calibration,
                    "calibration_radius_mw": width / 2.0,
                    "evaluation_end_utc": run_timestamp - pd.Timedelta(minutes=30),
                    "evaluation_observation_count": 100,
                    "empirical_coverage_pct": empirical,
                    "average_interval_width_mw": width,
                    "interval_contract_version": "split-conformal-absolute-residual-v1",
                }
            )
    summaries = []
    for scenario in ("healthy", "warning", "failed"):
        summaries.append(
            {
                "scenario": scenario,
                "monitor_run_id": f"monitor-{scenario}",
                "monitor_status": scenario,
                "failed_error_check_count": 2 if scenario == "failed" else 0,
                "failed_warning_check_count": 1 if scenario == "warning" else 0,
                "policy_version": "prediction-interval-monitoring-policy-v1",
                "monitoring_contract_version": "prediction-interval-monitoring-v1",
                **{field: False for field in TREND_AUTHORITY_FIELDS},
            }
        )
    return pd.DataFrame(rows), pd.DataFrame(summaries)


def _slice_trends() -> pd.DataFrame:
    history, summaries = _history_and_summary()
    _, slices = build_interval_health_trends(
        history,
        summaries,
        trend_run_id=TREND_RUN_ID,
        trend_run_timestamp=RUN_TIMESTAMP,
    )
    return slices


def _json_row(row: pd.Series) -> dict[str, object]:
    output: dict[str, object] = {}
    for key, value in row.items():
        if isinstance(value, pd.Timestamp):
            output[key] = value.isoformat()
        elif pd.isna(value):
            output[key] = None
        elif hasattr(value, "item"):
            output[key] = value.item()
        else:
            output[key] = value
    return output


def test_default_candidates_bind_one_exact_active_reference():
    candidates = default_policy_candidates()
    active = [candidate for candidate in candidates if candidate.policy_role == "active_reference"]
    assert len(candidates) == 3
    assert len(active) == 1
    assert active[0].policy_id == ACTIVE_POLICY_ID
    assert active[0].monitoring_config() == PredictionIntervalMonitoringConfig()


def test_expected_counterfactual_statuses_are_exposed():
    slices, summary, report = build_interval_policy_sensitivity(
        _slice_trends(),
        sensitivity_run_id=SENSITIVITY_RUN_ID,
        sensitivity_run_timestamp=RUN_TIMESTAMP,
        as_of_utc=RUN_TIMESTAMP,
    )
    statuses = {
        (row.scenario, row.policy_id): row.candidate_status
        for row in summary.itertuples(index=False)
    }
    assert statuses[("healthy", "active-reference")] == "healthy"
    assert statuses[("warning", "active-reference")] == "warning"
    assert statuses[("failed", "active-reference")] == "failed"
    assert statuses[("warning", "stricter-review")] == "failed"
    assert statuses[("warning", "tolerant-review")] == "healthy"
    assert statuses[("failed", "tolerant-review")] == "healthy"
    assert set(slices["source_area"]) == {"east_midlands"}
    assert "counterfactual" in report.lower()


def test_active_reference_must_reproduce_retained_status():
    trends = _slice_trends()
    trends.loc[trends["scenario"] == "warning", "monitor_status"] = "healthy"
    with pytest.raises(IntervalPolicySensitivityError, match="does not reproduce"):
        build_interval_policy_sensitivity(
            trends,
            sensitivity_run_id=SENSITIVITY_RUN_ID,
            sensitivity_run_timestamp=RUN_TIMESTAMP,
        )


def test_active_reference_thresholds_cannot_be_disguised():
    candidates = list(default_policy_candidates())
    active = candidates[0]
    candidates[0] = IntervalMonitoringPolicyCandidate(
        **{
            **active.__dict__,
            "max_recent_coverage_shortfall_pct_points": 99.0,
        }
    )
    with pytest.raises(IntervalPolicySensitivityError, match="exactly match"):
        build_interval_policy_sensitivity(
            _slice_trends(),
            candidates=tuple(candidates),
            sensitivity_run_id=SENSITIVITY_RUN_ID,
            sensitivity_run_timestamp=RUN_TIMESTAMP,
        )


def test_reference_insufficient_rows_cannot_claim_drift():
    trends = _slice_trends()
    trends.loc[0, "reference_history_sufficient"] = False
    with pytest.raises(IntervalPolicySensitivityError, match="must not fabricate"):
        build_interval_policy_sensitivity(
            trends,
            sensitivity_run_id=SENSITIVITY_RUN_ID,
            sensitivity_run_timestamp=RUN_TIMESTAMP,
        )


def test_outputs_prohibit_automatic_action():
    slices, summary, _ = build_interval_policy_sensitivity(
        _slice_trends(),
        sensitivity_run_id=SENSITIVITY_RUN_ID,
        sensitivity_run_timestamp=RUN_TIMESTAMP,
    )
    for field in AUTHORITY_FIELDS:
        assert set(slices[field]) == {False}
        assert set(summary[field]) == {False}


def test_rows_satisfy_versioned_schemas():
    slices, summary, _ = build_interval_policy_sensitivity(
        _slice_trends(),
        sensitivity_run_id=SENSITIVITY_RUN_ID,
        sensitivity_run_timestamp=RUN_TIMESTAMP,
    )
    cases = (
        ("interval_policy_sensitivity_slice_schema.json", slices.iloc[0]),
        ("interval_policy_sensitivity_summary_schema.json", summary.iloc[0]),
    )
    for filename, row in cases:
        schema = json.loads((ROOT / "data-contracts" / filename).read_text())
        errors = list(
            Draft202012Validator(
                schema, format_checker=FormatChecker()
            ).iter_errors(_json_row(row))
        )
        assert errors == []


def test_cli_writes_immutable_outputs(tmp_path):
    input_path = tmp_path / "slice_trends.csv"
    output_dir = tmp_path / "out"
    _slice_trends().to_csv(input_path, index=False)
    arguments = [
        "--slice-trends", str(input_path),
        "--output-dir", str(output_dir),
        "--output-format", "csv",
        "--sensitivity-run-id", SENSITIVITY_RUN_ID,
        "--sensitivity-run-timestamp", RUN_TIMESTAMP,
    ]
    assert main(arguments) == 0
    assert (output_dir / f"interval_policy_sensitivity_slices_{SENSITIVITY_RUN_ID}.csv").is_file()
    assert (output_dir / f"interval_policy_sensitivity_summary_{SENSITIVITY_RUN_ID}.csv").is_file()
    assert (output_dir / f"interval_policy_sensitivity_report_{SENSITIVITY_RUN_ID}.md").is_file()
    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        main(arguments)


def test_as_of_cannot_precede_retained_evidence():
    with pytest.raises(IntervalPolicySensitivityError, match="cannot precede"):
        build_interval_policy_sensitivity(
            _slice_trends(),
            sensitivity_run_id=SENSITIVITY_RUN_ID,
            sensitivity_run_timestamp=RUN_TIMESTAMP,
            as_of_utc="2026-01-01T00:00:00Z",
        )


def test_documentation_preserves_human_authority():
    document = (ROOT / "INTERVAL_POLICY_SENSITIVITY.md").read_text()
    assert "does not rerun the canonical monitor" in document
    assert "active_policy_updated=false" in document
    assert "separate immutable named human decision" in document

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
from jsonschema import Draft202012Validator, FormatChecker

from forecasting.interval_health_reporting import (
    REPORT_CONTRACT_VERSION,
    IntervalHealthReportError,
    build_interval_health_report,
)
from forecasting.interval_health_trends import build_interval_health_trends
from forecasting.run_interval_health_report import main

ROOT = Path(__file__).resolve().parents[1]
TREND_ID = "iht-" + "1" * 24
REPORT_ID = "ihr-" + "2" * 24
TIMESTAMP = "2026-01-20T00:00:00Z"


def _history() -> pd.DataFrame:
    rows = []
    anchor = pd.Timestamp(TIMESTAMP)
    areas = (("east_midlands", "east", "Nottingham"), ("south_wales", "wales", "Cardiff"))
    for scenario in ("healthy", "warning", "failed"):
        for sequence in range(1, 10):
            recent = sequence > 6
            ts = anchor - pd.Timedelta(hours=10 - sequence)
            for area, resource, city in areas:
                for horizon in (30, 60):
                    for model in ("persistence_current_value", "ridge_weather_lag"):
                        for coverage in (0.8, 0.9):
                            nominal = coverage * 100
                            width = 10.0 + horizon / 30
                            empirical = nominal - 2
                            calibration = 30
                            if scenario == "warning":
                                width *= 1.35 if recent else 1.0
                                empirical = nominal - (3 if recent else 1)
                            elif scenario == "failed" and recent:
                                empirical = nominal - 10
                                calibration = 20
                            rows.append({
                                "scenario": scenario,
                                "history_sequence": sequence,
                                "interval_run_id": f"{scenario}-{sequence:02d}",
                                "interval_run_timestamp_utc": ts,
                                "source_area": area,
                                "resource_id": resource,
                                "city": city,
                                "requested_horizon_minutes": horizon,
                                "model_name": model,
                                "feature_contract_version": "time-horizon-v1",
                                "target_coverage_level": coverage,
                                "calibration_observation_count": calibration,
                                "calibration_radius_mw": width / 2,
                                "evaluation_end_utc": ts - pd.Timedelta(minutes=30),
                                "evaluation_observation_count": 100,
                                "empirical_coverage_pct": empirical,
                                "average_interval_width_mw": width,
                                "interval_contract_version": "split-conformal-absolute-residual-v1",
                            })
    return pd.DataFrame(rows)


def _summaries() -> pd.DataFrame:
    rows = []
    for scenario in ("healthy", "warning", "failed"):
        rows.append({
            "scenario": scenario,
            "monitor_run_id": f"monitor-{scenario}",
            "monitor_status": scenario,
            "failed_error_check_count": 2 if scenario == "failed" else 0,
            "failed_warning_check_count": 1 if scenario != "healthy" else 0,
            "policy_version": "prediction-interval-monitoring-policy-v1",
            "monitoring_contract_version": "prediction-interval-monitoring-v1",
            "automatic_remediation_allowed": False,
            "automatic_recalibration_allowed": False,
            "automatic_model_change_allowed": False,
            "automatic_schedule_change_allowed": False,
            "automatic_promotion_allowed": False,
        })
    return pd.DataFrame(rows)


def _trends():
    return build_interval_health_trends(
        _history(), _summaries(), trend_run_id=TREND_ID,
        trend_run_timestamp=TIMESTAMP,
    )


def _json_row(row: pd.Series):
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


def test_report_preserves_status_and_exact_area_horizon_boundaries():
    run_trends, slice_trends = _trends()
    report = build_interval_health_report(
        run_trends, slice_trends, report_run_id=REPORT_ID,
        report_run_timestamp=TIMESTAMP,
    )
    overview = report["frames"]["interval_health_report_overview"]
    area = report["frames"]["interval_health_report_area_horizon"]
    attention = report["frames"]["interval_health_report_attention_queue"]
    assert dict(zip(overview["scenario"], overview["monitor_status"])) == {
        "healthy": "healthy", "warning": "warning", "failed": "failed"
    }
    assert len(area) == 3 * 2 * 2
    assert set(area["source_area"]) == {"east_midlands", "south_wales"}
    assert set(area["requested_horizon_minutes"]) == {30, 60}
    assert set(attention["monitor_status"]) == {"warning", "failed"}
    assert attention.iloc[0]["monitor_status"] == "failed"
    assert set(overview["report_contract_version"]) == {REPORT_CONTRACT_VERSION}


def test_report_is_thin_client_and_does_not_recalculate_status():
    run_trends, slice_trends = _trends()
    warning = slice_trends["scenario"] == "warning"
    slice_trends.loc[warning, "latest_coverage_shortfall_pct_points"] = 0.0
    slice_trends.loc[warning, "average_interval_width_increase_pct"] = 0.0
    report = build_interval_health_report(
        run_trends, slice_trends, report_run_id=REPORT_ID,
        report_run_timestamp=TIMESTAMP,
    )
    overview = report["frames"]["interval_health_report_overview"]
    retained = overview.loc[overview["scenario"] == "warning", "monitor_status"].item()
    assert retained == "warning"
    assert "does not recalculate monitor status" in report["markdown"]


def test_mismatched_trend_run_id_is_rejected():
    run_trends, slice_trends = _trends()
    slice_trends["trend_run_id"] = "iht-" + "9" * 24
    with pytest.raises(IntervalHealthReportError, match="exactly one"):
        build_interval_health_report(run_trends, slice_trends)


def test_inconsistent_scenario_status_is_rejected():
    run_trends, slice_trends = _trends()
    index = slice_trends.index[slice_trends["scenario"] == "warning"][0]
    slice_trends.loc[index, "monitor_status"] = "healthy"
    with pytest.raises(IntervalHealthReportError, match="inconsistent"):
        build_interval_health_report(run_trends, slice_trends)


def test_duplicate_reporting_slice_is_rejected():
    run_trends, slice_trends = _trends()
    slice_trends = pd.concat([slice_trends, slice_trends.iloc[[0]]], ignore_index=True)
    with pytest.raises(IntervalHealthReportError, match="duplicate"):
        build_interval_health_report(run_trends, slice_trends)


def test_report_frames_satisfy_versioned_schemas():
    run_trends, slice_trends = _trends()
    report = build_interval_health_report(
        run_trends, slice_trends, report_run_id=REPORT_ID,
        report_run_timestamp=TIMESTAMP,
    )
    mapping = {
        "interval_health_report_overview": "interval_health_report_overview_schema.json",
        "interval_health_report_area_horizon": "interval_health_report_area_horizon_schema.json",
        "interval_health_report_attention_queue": "interval_health_report_attention_schema.json",
    }
    for role, filename in mapping.items():
        schema = json.loads((ROOT / "data-contracts" / filename).read_text())
        frame = report["frames"][role]
        errors = list(Draft202012Validator(
            schema, format_checker=FormatChecker()
        ).iter_errors(_json_row(frame.iloc[0])))
        assert errors == []


def test_cli_writes_immutable_report_outputs(tmp_path):
    run_trends, slice_trends = _trends()
    run_path = tmp_path / "run.csv"
    slice_path = tmp_path / "slice.csv"
    run_trends.to_csv(run_path, index=False)
    slice_trends.to_csv(slice_path, index=False)
    output = tmp_path / "report"
    args = [
        "--run-trends", str(run_path), "--slice-trends", str(slice_path),
        "--output-dir", str(output), "--output-format", "csv",
        "--report-run-id", REPORT_ID, "--report-run-timestamp", TIMESTAMP,
    ]
    assert main(args) == 0
    assert (output / f"interval_health_report_overview_{REPORT_ID}.csv").is_file()
    assert (output / f"interval_health_report_area_horizon_{REPORT_ID}.csv").is_file()
    assert (output / f"interval_health_report_attention_queue_{REPORT_ID}.csv").is_file()
    assert (output / f"interval_health_report_{REPORT_ID}.md").is_file()
    assert (output / f"interval_health_report_{REPORT_ID}.html").is_file()
    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        main(args)


def test_notebook_is_thin_and_documentation_keeps_authority_boundary():
    notebook = json.loads(
        (ROOT / "notebooks" / "interval_health_trends.ipynb").read_text()
    )
    source = "\n".join(
        "".join(cell.get("source", [])) for cell in notebook["cells"]
    )
    assert notebook["nbformat"] == 4 and notebook["cells"]
    assert "build_interval_health_report" in source
    assert "monitor_prediction_interval_health" not in source
    assert "max_recent_coverage_shortfall" not in source
    document = (ROOT / "INTERVAL_HEALTH_REPORTING.md").read_text()
    assert "reads only the retained trend datasets" in document
    assert "does not recalculate monitor status" in document
    assert "automatic recalibration" in document

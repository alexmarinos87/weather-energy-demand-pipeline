from datetime import datetime, timezone
import json
from pathlib import Path

import pandas as pd
import pytest
from jsonschema import Draft202012Validator, FormatChecker

from forecasting.interval_monitoring import (
    FAILED_STATUS,
    HEALTHY_STATUS,
    WARNING_STATUS,
    PredictionIntervalMonitoringConfig,
    PredictionIntervalMonitoringError,
    monitor_prediction_interval_health,
    prepare_interval_metric_history,
)
from forecasting.run_interval_monitoring import main


BASE = pd.Timestamp("2026-01-10T00:00:00Z")


def interval_metrics(
    *,
    runs: int = 9,
    recent_coverage: float = 92.0,
    reference_coverage: float = 92.0,
    recent_width: float = 20.0,
    reference_width: float = 20.0,
    recent_calibration: int = 48,
    reference_calibration: int = 48,
    model_name: str = "ridge_weather_lag",
    source_area: str = "east_midlands",
    target_coverage_level: float = 0.90,
) -> pd.DataFrame:
    rows = []
    start = BASE - pd.Timedelta(days=runs - 1)
    reference_count = max(0, runs - 3)
    for index in range(runs):
        recent = index >= reference_count
        run_timestamp = start + pd.Timedelta(days=index)
        rows.append(
            {
                "interval_run_id": f"interval-{source_area}-{model_name}-{index}",
                "interval_run_timestamp_utc": run_timestamp,
                "point_prediction_run_id": f"points-{index}",
                "source_area": source_area,
                "resource_id": f"resource-{source_area}",
                "city": "Nottingham,GB",
                "requested_horizon_minutes": 30,
                "model_name": model_name,
                "feature_contract_version": "time-horizon-v1",
                "evaluation_origin_fold": 4,
                "target_coverage_level": target_coverage_level,
                "calibration_method": "absolute_residual_quantile",
                "calibration_observation_count": (
                    recent_calibration if recent else reference_calibration
                ),
                "calibration_quantile_rank": 44,
                "calibration_radius_mw": 10.0,
                "calibration_feature_start_utc": run_timestamp
                - pd.Timedelta(days=3),
                "calibration_feature_end_utc": run_timestamp
                - pd.Timedelta(days=2),
                "calibration_label_available_through_utc": run_timestamp
                - pd.Timedelta(days=1, hours=2),
                "evaluation_feature_start_utc": run_timestamp
                - pd.Timedelta(days=1),
                "evaluation_feature_end_utc": run_timestamp
                - pd.Timedelta(hours=2),
                "evaluation_start_utc": run_timestamp
                - pd.Timedelta(hours=23, minutes=30),
                "evaluation_end_utc": run_timestamp - pd.Timedelta(hours=1),
                "evaluation_observation_count": 100,
                "empirical_coverage_pct": (
                    recent_coverage if recent else reference_coverage
                ),
                "average_interval_width_mw": (
                    recent_width if recent else reference_width
                ),
                "median_interval_width_mw": (
                    recent_width if recent else reference_width
                ),
                "minimum_interval_width_mw": (
                    recent_width if recent else reference_width
                ),
                "maximum_interval_width_mw": (
                    recent_width if recent else reference_width
                ),
                "interval_contract_version": (
                    "split-conformal-absolute-residual-v1"
                ),
            }
        )
    return pd.DataFrame(rows)


def monitor(metrics=None, config=None, as_of=None):
    return monitor_prediction_interval_health(
        metrics if metrics is not None else interval_metrics(),
        config=config,
        as_of_utc=as_of or BASE + pd.Timedelta(hours=1),
        run_id="interval-monitor-1",
        run_timestamp=datetime(2026, 1, 10, 1, 5, tzinfo=timezone.utc),
    )


def test_healthy_interval_history_passes_without_automatic_authority():
    checks, summary = monitor()
    assert summary.loc[0, "monitor_status"] == HEALTHY_STATUS
    assert not summary.loc[0, "automatic_remediation_allowed"]
    assert not summary.loc[0, "automatic_recalibration_allowed"]
    assert not summary.loc[0, "automatic_model_change_allowed"]
    assert not summary.loc[0, "automatic_schedule_change_allowed"]
    assert not summary.loc[0, "automatic_promotion_allowed"]
    assert summary.loc[0, "failed_error_check_count"] == 0
    assert summary.loc[0, "failed_warning_check_count"] == 0
    assert checks["passed"].all()


def test_stale_interval_run_and_evaluation_are_errors():
    checks, summary = monitor(as_of=BASE + pd.Timedelta(days=20))
    failed = set(checks.loc[~checks["passed"], "check_name"])
    assert summary.loc[0, "monitor_status"] == FAILED_STATUS
    assert "latest_interval_run_age_minutes" in failed
    assert "latest_interval_evaluation_age_minutes" in failed


def test_recent_coverage_shortfall_is_an_error():
    checks, summary = monitor(metrics=interval_metrics(recent_coverage=80.0))
    failed = set(checks.loc[~checks["passed"], "check_name"])
    assert summary.loc[0, "monitor_status"] == FAILED_STATUS
    assert "maximum_recent_coverage_shortfall_pct_points" in failed


def test_insufficient_recent_calibration_history_is_an_error():
    checks, summary = monitor(metrics=interval_metrics(recent_calibration=20))
    failed = set(checks.loc[~checks["passed"], "check_name"])
    assert summary.loc[0, "monitor_status"] == FAILED_STATUS
    assert "minimum_recent_calibration_observation_count" in failed


def test_longitudinal_interval_regression_is_warning_when_hard_limits_pass():
    metrics = interval_metrics(
        recent_coverage=91.0,
        reference_coverage=98.0,
        recent_width=130.0,
        reference_width=100.0,
        recent_calibration=30,
        reference_calibration=50,
    )
    checks, summary = monitor(metrics=metrics)
    failed_warnings = set(
        checks.loc[
            (~checks["passed"]) & (checks["severity"] == "warning"),
            "check_name",
        ]
    )
    assert summary.loc[0, "monitor_status"] == WARNING_STATUS
    assert failed_warnings.issuperset(
        {
            "maximum_interval_coverage_drop_pct_points",
            "maximum_average_interval_width_increase_pct",
            "maximum_calibration_history_drop_pct",
        }
    )


def test_insufficient_reference_history_is_explicit_warning():
    checks, summary = monitor(metrics=interval_metrics(runs=3))
    reference = checks.loc[
        checks["check_name"] == "minimum_reference_interval_runs"
    ].iloc[0]
    assert reference["severity"] == "warning"
    assert not reference["passed"]
    assert summary.loc[0, "monitor_status"] == WARNING_STATUS
    assert not checks["check_name"].str.contains("width_increase").any()
    assert not checks["check_name"].str.contains("coverage_drop").any()


def test_multiple_interval_slices_are_monitored_independently():
    metrics = pd.concat(
        [
            interval_metrics(),
            interval_metrics(
                model_name="seasonal_previous_week",
                source_area="south_wales",
            ),
        ],
        ignore_index=True,
    )
    _, summary = monitor(metrics=metrics)
    assert summary.loc[0, "monitored_interval_slice_count"] == 2
    assert summary.loc[0, "retained_interval_run_count"] == 18


def test_coverage_uses_evaluation_observation_weighting():
    metrics = interval_metrics(runs=2, recent_coverage=90.0)
    metrics.loc[0, "empirical_coverage_pct"] = 0.0
    metrics.loc[0, "evaluation_observation_count"] = 1
    metrics.loc[1, "empirical_coverage_pct"] = 100.0
    metrics.loc[1, "evaluation_observation_count"] = 100
    config = PredictionIntervalMonitoringConfig(
        recent_interval_run_count=2,
        min_recent_interval_runs=2,
        min_reference_interval_runs=1,
    )
    checks, summary = monitor(metrics=metrics, config=config)
    coverage = checks.loc[
        checks["check_name"]
        == "maximum_recent_coverage_shortfall_pct_points"
    ].iloc[0]
    assert coverage["observed_value"] == pytest.approx(0.0)
    assert summary.loc[0, "monitor_status"] == WARNING_STATUS


def test_naive_interval_run_timestamp_is_rejected():
    metrics = interval_metrics()
    metrics["interval_run_timestamp_utc"] = metrics[
        "interval_run_timestamp_utc"
    ].astype(object)
    metrics.loc[0, "interval_run_timestamp_utc"] = "2026-01-01T00:00:00"
    with pytest.raises(
        PredictionIntervalMonitoringError, match="timezone-aware"
    ):
        prepare_interval_metric_history(metrics)


def test_duplicate_interval_run_slice_is_rejected():
    metrics = interval_metrics()
    metrics = pd.concat([metrics, metrics.iloc[[0]]], ignore_index=True)
    with pytest.raises(PredictionIntervalMonitoringError, match="duplicate"):
        prepare_interval_metric_history(metrics)


def test_as_of_cannot_precede_retained_interval_evidence():
    with pytest.raises(PredictionIntervalMonitoringError, match="cannot precede"):
        monitor(as_of=BASE - pd.Timedelta(days=10))


def test_config_rejects_impossible_recent_window():
    with pytest.raises(PredictionIntervalMonitoringError, match="cannot exceed"):
        PredictionIntervalMonitoringConfig(
            recent_interval_run_count=2,
            min_recent_interval_runs=3,
        ).validate()


def test_cli_writes_evidence_before_returning_failed_exit(tmp_path):
    metrics_path = tmp_path / "interval_metrics.csv"
    interval_metrics(recent_coverage=80.0).to_csv(metrics_path, index=False)
    output = tmp_path / "output"
    exit_code = main(
        [
            "--interval-metrics",
            str(metrics_path),
            "--as-of-utc",
            "2026-01-10T01:00:00Z",
            "--output-dir",
            str(output),
            "--output-format",
            "csv",
            "--fail-on-error",
        ]
    )
    assert exit_code == 2
    summaries = list(output.glob("prediction_interval_health_summary_*.csv"))
    checks = list(output.glob("prediction_interval_health_checks_*.csv"))
    assert len(summaries) == len(checks) == 1
    assert pd.read_csv(summaries[0]).loc[0, "monitor_status"] == FAILED_STATUS


def _json_row(frame: pd.DataFrame) -> dict:
    row = frame.iloc[0].to_dict()
    for key, value in list(row.items()):
        if isinstance(value, pd.Timestamp):
            row[key] = value.isoformat()
        elif pd.isna(value):
            row[key] = None
    return row


def test_interval_health_rows_satisfy_versioned_schemas():
    checks, summary = monitor()
    root = Path(__file__).resolve().parents[1] / "data-contracts"
    check_schema = json.loads(
        (root / "prediction_interval_health_check_schema.json").read_text(
            encoding="utf-8"
        )
    )
    summary_schema = json.loads(
        (root / "prediction_interval_health_summary_schema.json").read_text(
            encoding="utf-8"
        )
    )
    assert list(
        Draft202012Validator(
            check_schema, format_checker=FormatChecker()
        ).iter_errors(_json_row(checks))
    ) == []
    assert list(
        Draft202012Validator(
            summary_schema, format_checker=FormatChecker()
        ).iter_errors(_json_row(summary))
    ) == []

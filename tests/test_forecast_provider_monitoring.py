from datetime import datetime, timezone
import json
from pathlib import Path

import pandas as pd
import pytest
from jsonschema import Draft202012Validator, FormatChecker

from forecasting.provider_monitoring import (
    FAILED_STATUS,
    HEALTHY_STATUS,
    WARNING_STATUS,
    ForecastProviderMonitoringConfig,
    ForecastProviderMonitoringError,
    monitor_forecast_provider_health,
    prepare_forecast_snapshot_evidence,
)
from forecasting.run_provider_monitoring import main


BASE = pd.Timestamp("2026-01-02T00:00:00Z")


def forecasts(
    *,
    ingestion_times=None,
    slots_per_snapshot: int = 8,
    provider: str = "openweather",
    model: str = "5-day-3-hour",
):
    ingestion_times = ingestion_times or [
        BASE,
        BASE + pd.Timedelta(hours=3),
        BASE + pd.Timedelta(hours=6),
    ]
    rows = []
    for snapshot_index, ingested in enumerate(ingestion_times):
        for slot in range(1, slots_per_snapshot + 1):
            rows.append(
                {
                    "source_area": "east_midlands",
                    "city": "Nottingham",
                    "forecast_issued_at_utc": ingested,
                    "forecast_ingested_at_utc": ingested,
                    "forecast_valid_at_utc": ingested
                    + pd.Timedelta(hours=3 * slot),
                    "forecast_temperature_c": 8.0 + slot,
                    "forecast_humidity_pct": 55.0 + slot,
                    "forecast_provider": provider,
                    "forecast_model": model,
                    "forecast_issue_basis": "retrieval_time_surrogate",
                    "forecast_provider_record_id": str(slot),
                    "raw_snapshot_id": f"snapshot-{provider}-{snapshot_index}",
                }
            )
    return pd.DataFrame(rows)


def reconciliation(
    *,
    runs: int = 9,
    recent_coverage: float = 95.0,
    reference_coverage: float = 95.0,
    recent_temperature_mae: float = 1.0,
    reference_temperature_mae: float = 1.0,
    recent_humidity_mae: float = 5.0,
    reference_humidity_mae: float = 5.0,
    provider: str = "openweather",
    model: str = "5-day-3-hour",
):
    rows = []
    start = BASE - pd.Timedelta(hours=runs - 3)
    reference_count = max(0, runs - 3)
    for index in range(runs):
        recent = index >= reference_count
        coverage = recent_coverage if recent else reference_coverage
        eligible = 100
        matched = int(round(coverage))
        rows.append(
            {
                "reconciliation_run_id": f"run-{provider}-{index}",
                "reconciliation_run_timestamp_utc": start
                + pd.Timedelta(hours=index),
                "source_area": "east_midlands",
                "city": "Nottingham",
                "forecast_provider": provider,
                "forecast_model": model,
                "forecast_issue_basis": "retrieval_time_surrogate",
                "forecast_lead_time_bucket": "00-06h",
                "eligible_forecast_count": eligible,
                "matched_forecast_count": matched,
                "forecast_observation_coverage_pct": coverage,
                "temperature_mae_c": (
                    recent_temperature_mae
                    if recent
                    else reference_temperature_mae
                ),
                "humidity_mae_pct": (
                    recent_humidity_mae
                    if recent
                    else reference_humidity_mae
                ),
            }
        )
    return pd.DataFrame(rows)


def monitor(forecast=None, metrics=None, config=None, as_of=None):
    return monitor_forecast_provider_health(
        forecast if forecast is not None else forecasts(),
        metrics if metrics is not None else reconciliation(),
        config=config,
        as_of_utc=as_of or BASE + pd.Timedelta(hours=7),
        run_id="monitor-1",
        run_timestamp=datetime(2026, 1, 2, 7, 5, tzinfo=timezone.utc),
    )


def test_healthy_provider_evidence_passes_without_automatic_remediation():
    checks, summary = monitor()
    assert summary.loc[0, "monitor_status"] == HEALTHY_STATUS
    assert not summary.loc[0, "automatic_remediation_allowed"]
    assert summary.loc[0, "failed_error_check_count"] == 0
    assert summary.loc[0, "failed_warning_check_count"] == 0
    assert checks["passed"].all()


def test_stale_latest_forecast_snapshot_is_an_error():
    checks, summary = monitor(as_of=BASE + pd.Timedelta(hours=12))
    failed = checks.loc[~checks["passed"], "check_name"].tolist()
    assert summary.loc[0, "monitor_status"] == FAILED_STATUS
    assert "latest_forecast_ingestion_age_minutes" in failed


def test_large_snapshot_gap_is_an_error_even_when_latest_is_fresh():
    evidence = forecasts(
        ingestion_times=[BASE, BASE + pd.Timedelta(hours=10)]
    )
    checks, summary = monitor(
        forecast=evidence,
        metrics=reconciliation(),
        as_of=BASE + pd.Timedelta(hours=11),
    )
    failed = set(checks.loc[~checks["passed"], "check_name"])
    assert summary.loc[0, "monitor_status"] == FAILED_STATUS
    assert "maximum_snapshot_ingestion_gap_minutes" in failed


def test_short_latest_snapshot_fails_slot_and_horizon_checks():
    full = forecasts(ingestion_times=[BASE, BASE + pd.Timedelta(hours=3)])
    short = forecasts(
        ingestion_times=[BASE + pd.Timedelta(hours=6)],
        slots_per_snapshot=4,
    )
    evidence = pd.concat([full, short], ignore_index=True)
    checks, summary = monitor(forecast=evidence)
    failed = set(checks.loc[~checks["passed"], "check_name"])
    assert summary.loc[0, "monitor_status"] == FAILED_STATUS
    assert "latest_snapshot_minimum_slot_count" in failed
    assert "latest_snapshot_minimum_horizon_minutes" in failed


def test_one_snapshot_produces_warning_not_false_cadence_health():
    checks, summary = monitor(
        forecast=forecasts(ingestion_times=[BASE + pd.Timedelta(hours=6)]),
    )
    warning = checks.loc[
        checks["check_name"] == "minimum_snapshot_history_for_cadence"
    ].iloc[0]
    assert not warning["passed"]
    assert warning["severity"] == "warning"
    assert summary.loc[0, "monitor_status"] == WARNING_STATUS


def test_hard_reconciliation_quality_failures_set_failed_status():
    metrics = reconciliation(
        recent_coverage=80.0,
        recent_temperature_mae=4.0,
        recent_humidity_mae=20.0,
    )
    checks, summary = monitor(metrics=metrics)
    failed = set(checks.loc[~checks["passed"], "check_name"])
    assert summary.loc[0, "monitor_status"] == FAILED_STATUS
    assert failed.issuperset(
        {
            "minimum_recent_reconciliation_coverage_pct",
            "maximum_recent_temperature_mae_c",
            "maximum_recent_humidity_mae_pct",
        }
    )


def test_longitudinal_regression_is_warning_when_hard_limits_still_pass():
    metrics = reconciliation(
        recent_coverage=90.0,
        reference_coverage=98.0,
        recent_temperature_mae=2.0,
        reference_temperature_mae=1.0,
        recent_humidity_mae=10.0,
        reference_humidity_mae=5.0,
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
            "maximum_reconciliation_coverage_drop_pct_points",
            "maximum_temperature_mae_increase_c",
            "maximum_humidity_mae_increase_pct",
        }
    )


def test_insufficient_reference_history_is_explicit_warning():
    checks, summary = monitor(metrics=reconciliation(runs=3))
    check = checks.loc[
        checks["check_name"] == "minimum_reference_reconciliation_runs"
    ].iloc[0]
    assert check["severity"] == "warning"
    assert not check["passed"]
    assert summary.loc[0, "monitor_status"] == WARNING_STATUS
    assert not checks["check_name"].str.contains("mae_increase").any()


def test_stale_reconciliation_run_is_an_error():
    checks, summary = monitor(as_of=BASE + pd.Timedelta(days=3))
    assert summary.loc[0, "monitor_status"] == FAILED_STATUS
    assert "latest_reconciliation_age_minutes" in set(
        checks.loc[~checks["passed"], "check_name"]
    )


def test_multiple_provider_identities_are_monitored_independently():
    forecast = pd.concat(
        [
            forecasts(),
            forecasts(provider="provider-b", model="model-b"),
        ],
        ignore_index=True,
    )
    metrics = pd.concat(
        [
            reconciliation(),
            reconciliation(provider="provider-b", model="model-b"),
        ],
        ignore_index=True,
    )
    _, summary = monitor(forecast=forecast, metrics=metrics)
    assert summary.loc[0, "monitored_forecast_identity_count"] == 2
    assert summary.loc[0, "monitored_reconciliation_slice_count"] == 2


def test_naive_forecast_timestamps_are_rejected():
    evidence = forecasts()
    evidence.loc[0, "forecast_ingested_at_utc"] = "2026-01-02T00:00:00"
    with pytest.raises(ForecastProviderMonitoringError, match="timezone-aware"):
        prepare_forecast_snapshot_evidence(evidence)


def test_as_of_cannot_precede_retained_evidence():
    with pytest.raises(ForecastProviderMonitoringError, match="cannot precede"):
        monitor(as_of=BASE - pd.Timedelta(minutes=1))


def test_config_rejects_impossible_reconciliation_window():
    with pytest.raises(ForecastProviderMonitoringError, match="cannot exceed"):
        ForecastProviderMonitoringConfig(
            recent_reconciliation_run_count=2,
            min_recent_reconciliation_runs=3,
        ).validate()


def test_cli_writes_evidence_before_returning_failed_exit(tmp_path):
    forecast_path = tmp_path / "forecast.csv"
    reconciliation_path = tmp_path / "reconciliation.csv"
    forecasts().to_csv(forecast_path, index=False)
    reconciliation(recent_coverage=80.0).to_csv(reconciliation_path, index=False)
    output = tmp_path / "output"
    exit_code = main(
        [
            "--forecast-input",
            str(forecast_path),
            "--reconciliation-metrics",
            str(reconciliation_path),
            "--as-of-utc",
            "2026-01-02T07:00:00Z",
            "--output-dir",
            str(output),
            "--output-format",
            "csv",
            "--fail-on-error",
        ]
    )
    assert exit_code == 2
    summaries = list(output.glob("forecast_provider_health_summary_*.csv"))
    checks = list(output.glob("forecast_provider_health_checks_*.csv"))
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


def test_health_check_and_summary_satisfy_versioned_schemas():
    checks, summary = monitor()
    root = Path(__file__).resolve().parents[1] / "data-contracts"
    check_schema = json.loads(
        (root / "forecast_provider_health_check_schema.json").read_text(
            encoding="utf-8"
        )
    )
    summary_schema = json.loads(
        (root / "forecast_provider_health_summary_schema.json").read_text(
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

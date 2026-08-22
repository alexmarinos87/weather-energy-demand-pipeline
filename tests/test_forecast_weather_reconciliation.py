from datetime import datetime, timezone
import json
from pathlib import Path

import pandas as pd
import pytest
from jsonschema import Draft202012Validator, FormatChecker

from forecasting.run_weather_reconciliation import main
from forecasting.weather_reconciliation import (
    ForecastWeatherReconciliationConfig,
    ForecastWeatherReconciliationError,
    prepare_observed_weather_input,
    reconcile_forecast_weather,
)


def _forecast(valid_times: list[pd.Timestamp]) -> pd.DataFrame:
    issued = pd.Timestamp("2026-01-01T00:00:00Z")
    return pd.DataFrame(
        {
            "source_area": "east_midlands",
            "city": "Nottingham",
            "forecast_issued_at_utc": issued,
            "forecast_ingested_at_utc": issued,
            "forecast_valid_at_utc": valid_times,
            "forecast_temperature_c": [
                10.0 + index for index in range(len(valid_times))
            ],
            "forecast_humidity_pct": [
                60.0 + index for index in range(len(valid_times))
            ],
            "forecast_provider": "openweather",
            "forecast_model": "5-day-3-hour",
            "forecast_issue_basis": "retrieval_time_surrogate",
            "forecast_provider_record_id": [
                str(index) for index in range(len(valid_times))
            ],
            "raw_snapshot_id": "snapshot-1",
        }
    )


def _observed(times: list[pd.Timestamp]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "source_area": "east_midlands",
            "city": "Nottingham",
            "event_timestamp_utc": times,
            "ingestion_timestamp_utc": [
                time + pd.Timedelta(minutes=5) for time in times
            ],
            "temperature_c": [8.0 + index for index in range(len(times))],
            "humidity_pct": [55.0 + index for index in range(len(times))],
            "source_file": [
                f"weather_{index}.parquet" for index in range(len(times))
            ],
        }
    )


def test_exact_reconciliation_records_signed_and_absolute_errors():
    valid = [
        pd.Timestamp("2026-01-01T03:00:00Z"),
        pd.Timestamp("2026-01-01T06:00:00Z"),
    ]
    rows, metrics = reconcile_forecast_weather(
        _forecast(valid),
        _observed(valid),
        config=ForecastWeatherReconciliationConfig(min_coverage=1.0),
        run_id="reconciliation-test",
        run_timestamp=datetime(2026, 1, 2, tzinfo=timezone.utc),
    )

    assert set(rows["reconciliation_status"]) == {"matched"}
    assert rows["temperature_error_c"].tolist() == [2.0, 2.0]
    assert rows["humidity_error_pct"].tolist() == [5.0, 5.0]
    assert set(rows["observation_valid_time_delta_minutes"]) == {0.0}
    assert metrics.loc[0, "temperature_mae_c"] == 2.0
    assert metrics.loc[0, "temperature_rmse_c"] == 2.0
    assert metrics.loc[0, "humidity_mae_pct"] == 5.0
    assert metrics.loc[0, "forecast_observation_coverage_pct"] == 100.0


def test_future_unmatured_slots_are_excluded_from_coverage_denominator():
    valid = [
        pd.Timestamp("2026-01-01T03:00:00Z"),
        pd.Timestamp("2026-01-02T03:00:00Z"),
    ]
    observed = _observed(
        [
            pd.Timestamp("2026-01-01T02:00:00Z"),
            pd.Timestamp("2026-01-01T03:00:00Z"),
            pd.Timestamp("2026-01-01T04:00:00Z"),
        ]
    )

    rows, metrics = reconcile_forecast_weather(
        _forecast(valid),
        observed,
        config=ForecastWeatherReconciliationConfig(min_coverage=1.0),
    )

    assert len(rows) == 1
    assert rows.loc[0, "forecast_valid_at_utc"] == valid[0]
    assert metrics.loc[0, "eligible_forecast_count"] == 1


def test_internal_missing_observation_lowers_coverage_and_fails_gate():
    valid = [
        pd.Timestamp("2026-01-01T03:00:00Z"),
        pd.Timestamp("2026-01-01T06:00:00Z"),
        pd.Timestamp("2026-01-01T09:00:00Z"),
    ]
    observed = _observed(
        [
            pd.Timestamp("2026-01-01T03:00:00Z"),
            pd.Timestamp("2026-01-01T09:00:00Z"),
        ]
    )

    with pytest.raises(ForecastWeatherReconciliationError, match="matched 1/2"):
        reconcile_forecast_weather(
            _forecast(valid),
            observed,
            config=ForecastWeatherReconciliationConfig(
                observation_tolerance_minutes=30,
                min_coverage=0.90,
            ),
        )


def test_cross_area_observations_do_not_match():
    valid = [pd.Timestamp("2026-01-01T03:00:00Z")]
    observed = _observed(valid)
    observed["source_area"] = "south_west"

    with pytest.raises(ForecastWeatherReconciliationError, match="matched 0/1"):
        reconcile_forecast_weather(
            _forecast(valid),
            observed,
            config=ForecastWeatherReconciliationConfig(min_coverage=1.0),
        )


def test_equal_distance_tie_prefers_observation_before_valid_time():
    valid = [pd.Timestamp("2026-01-01T03:00:00Z")]
    observed = _observed(
        [
            pd.Timestamp("2026-01-01T02:30:00Z"),
            pd.Timestamp("2026-01-01T03:30:00Z"),
        ]
    )

    rows, _ = reconcile_forecast_weather(
        _forecast(valid),
        observed,
        config=ForecastWeatherReconciliationConfig(
            observation_tolerance_minutes=30,
            min_coverage=1.0,
        ),
    )

    assert rows.loc[0, "observation_event_timestamp_utc"] == pd.Timestamp(
        "2026-01-01T02:30:00Z"
    )
    assert rows.loc[0, "observation_valid_time_delta_minutes"] == -30.0


def test_duplicate_observation_event_uses_latest_ingested_record():
    event = pd.Timestamp("2026-01-01T03:00:00Z")
    observed = pd.DataFrame(
        [
            {
                "source_area": "east_midlands",
                "city": "Nottingham",
                "event_timestamp_utc": event,
                "ingestion_timestamp_utc": event + pd.Timedelta(minutes=2),
                "temperature_c": 7.0,
                "humidity_pct": 50.0,
                "source_file": "a.parquet",
            },
            {
                "source_area": "east_midlands",
                "city": "Nottingham",
                "event_timestamp_utc": event,
                "ingestion_timestamp_utc": event + pd.Timedelta(minutes=5),
                "temperature_c": 9.0,
                "humidity_pct": 52.0,
                "source_file": "b.parquet",
            },
        ]
    )

    prepared = prepare_observed_weather_input(observed)
    assert len(prepared) == 1
    assert prepared.loc[0, "temperature_c"] == 9.0
    assert prepared.loc[0, "source_file"] == "b.parquet"


def test_observed_ingestion_before_event_is_rejected():
    event = pd.Timestamp("2026-01-01T03:00:00Z")
    observed = _observed([event])
    observed.loc[0, "ingestion_timestamp_utc"] = event - pd.Timedelta(minutes=1)

    with pytest.raises(ForecastWeatherReconciliationError, match="must not precede"):
        prepare_observed_weather_input(observed)


def test_naive_reconciliation_timestamps_are_rejected():
    forecast = _forecast([pd.Timestamp("2026-01-01T03:00:00Z")])
    forecast["forecast_valid_at_utc"] = ["2026-01-01T03:00:00"]

    with pytest.raises(ForecastWeatherReconciliationError, match="timezone-aware"):
        reconcile_forecast_weather(
            forecast,
            _observed([pd.Timestamp("2026-01-01T03:00:00Z")]),
        )


def test_no_mature_forecast_slots_is_distinct_from_low_coverage():
    forecast = _forecast([pd.Timestamp("2026-01-02T03:00:00Z")])
    observed = _observed([pd.Timestamp("2026-01-01T03:00:00Z")])

    with pytest.raises(
        ForecastWeatherReconciliationError,
        match="No forecast slots are mature",
    ):
        reconcile_forecast_weather(forecast, observed)


def test_reconciliation_row_satisfies_versioned_schema():
    valid = [pd.Timestamp("2026-01-01T03:00:00Z")]
    rows, _ = reconcile_forecast_weather(
        _forecast(valid),
        _observed(valid),
        config=ForecastWeatherReconciliationConfig(min_coverage=1.0),
        run_id="schema-test",
        run_timestamp=datetime(2026, 1, 2, tzinfo=timezone.utc),
    )
    contract_path = (
        Path(__file__).resolve().parents[1]
        / "data-contracts"
        / "forecast_weather_reconciliation_schema.json"
    )
    contract = json.loads(contract_path.read_text(encoding="utf-8"))
    row = rows.iloc[0].to_dict()
    for column in (
        "reconciliation_run_timestamp_utc",
        "forecast_issued_at_utc",
        "forecast_ingested_at_utc",
        "forecast_valid_at_utc",
        "observation_event_timestamp_utc",
        "observation_ingested_at_utc",
    ):
        row[column] = pd.Timestamp(row[column]).isoformat()
    errors = list(
        Draft202012Validator(
            contract, format_checker=FormatChecker()
        ).iter_errors(row)
    )
    assert errors == []


def test_quality_metric_row_satisfies_versioned_schema():
    valid = [pd.Timestamp("2026-01-01T03:00:00Z")]
    _, metrics = reconcile_forecast_weather(
        _forecast(valid),
        _observed(valid),
        config=ForecastWeatherReconciliationConfig(min_coverage=1.0),
        run_id="metric-schema-test",
        run_timestamp=datetime(2026, 1, 2, tzinfo=timezone.utc),
    )
    contract_path = (
        Path(__file__).resolve().parents[1]
        / "data-contracts"
        / "forecast_weather_quality_metrics_schema.json"
    )
    contract = json.loads(contract_path.read_text(encoding="utf-8"))
    row = metrics.iloc[0].to_dict()
    for column in (
        "reconciliation_run_timestamp_utc",
        "forecast_valid_start_utc",
        "forecast_valid_end_utc",
    ):
        row[column] = pd.Timestamp(row[column]).isoformat()
    errors = list(
        Draft202012Validator(
            contract, format_checker=FormatChecker()
        ).iter_errors(row)
    )
    assert errors == []


def test_cli_reads_partition_directories_and_writes_distinct_outputs(tmp_path):
    forecast_dir = tmp_path / "forecast" / "ingestion_date=2026-01-01"
    observed_dir = tmp_path / "observed" / "dt=2026-01-01"
    output_dir = tmp_path / "output"
    forecast_dir.mkdir(parents=True)
    observed_dir.mkdir(parents=True)
    valid = [pd.Timestamp("2026-01-01T03:00:00Z")]
    _forecast(valid).to_csv(forecast_dir / "forecast.csv", index=False)
    _observed(valid).to_csv(observed_dir / "observed.csv", index=False)

    exit_code = main(
        [
            "--forecast-input",
            str(tmp_path / "forecast"),
            "--observed-input",
            str(tmp_path / "observed"),
            "--min-coverage",
            "1.0",
            "--output-dir",
            str(output_dir),
            "--output-format",
            "csv",
        ]
    )

    assert exit_code == 0
    rows_files = list(output_dir.glob("forecast_weather_reconciliation_*.csv"))
    metric_files = list(output_dir.glob("forecast_weather_quality_metrics_*.csv"))
    assert len(rows_files) == 1
    assert len(metric_files) == 1
    assert pd.read_csv(rows_files[0]).loc[0, "reconciliation_status"] == "matched"
    assert pd.read_csv(metric_files[0]).loc[0, "temperature_mae_c"] == 2.0


def test_lead_time_buckets_are_reported_separately():
    forecast = pd.concat(
        [
            _forecast([pd.Timestamp("2026-01-01T03:00:00Z")]),
            _forecast([pd.Timestamp("2026-01-02T06:00:00Z")]).assign(
                raw_snapshot_id="snapshot-2"
            ),
        ],
        ignore_index=True,
    )
    observed = _observed(
        [
            pd.Timestamp("2026-01-01T03:00:00Z"),
            pd.Timestamp("2026-01-02T06:00:00Z"),
        ]
    )

    _, metrics = reconcile_forecast_weather(
        forecast,
        observed,
        config=ForecastWeatherReconciliationConfig(min_coverage=1.0),
    )

    assert set(metrics["forecast_lead_time_bucket"]) == {"00-06h", "24-48h"}

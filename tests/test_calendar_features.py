from datetime import datetime, timezone

import pandas as pd
import pytest

from forecasting.calendar_features import (
    CALENDAR_FEATURE_CONTRACT_VERSION,
    UK_LOCAL_TIMEZONE,
    CalendarFeatureError,
    add_uk_local_calendar_features,
)
from forecasting.contracts import (
    UK_LOCAL_FEATURE_COLUMNS,
    UK_LOCAL_FEATURE_CONTRACT_VERSION,
    BacktestConfig,
    ForecastingContractError,
    prepare_feature_frame,
)
from forecasting.demo import (
    build_demo_feature_frame,
    build_multi_area_demo_feature_frame,
)
from forecasting.evaluation import run_chronological_backtest
from forecasting.forecast_weather import (
    ForecastWeatherConfig,
    build_demo_forecast_weather_frame,
)
from forecasting.run_baseline import main
from forecasting.weather_comparison import run_weather_model_comparison


def _timestamps(*values: str) -> pd.DataFrame:
    return pd.DataFrame({"event_timestamp_utc": list(values)})


def test_winter_and_summer_offsets_are_explicit():
    prepared = add_uk_local_calendar_features(
        _timestamps("2026-01-15T12:00:00Z", "2026-07-15T12:00:00Z")
    )
    assert prepared["hour_of_day_local"].tolist() == [12, 13]
    assert prepared["local_utc_offset_minutes"].tolist() == [0, 60]
    assert prepared["is_dst_local"].tolist() == [0, 1]
    assert set(prepared["calendar_timezone"]) == {UK_LOCAL_TIMEZONE}
    assert set(prepared["calendar_feature_contract_version"]) == {
        CALENDAR_FEATURE_CONTRACT_VERSION
    }


def test_spring_forward_does_not_invent_the_missing_local_hour():
    prepared = add_uk_local_calendar_features(
        _timestamps("2026-03-29T00:30:00Z", "2026-03-29T01:30:00Z")
    )
    assert prepared["hour_of_day_local"].tolist() == [0, 2]
    assert prepared["local_utc_offset_minutes"].tolist() == [0, 60]
    assert prepared["is_dst_local"].tolist() == [0, 1]


def test_autumn_repeated_local_hour_remains_distinguishable():
    prepared = add_uk_local_calendar_features(
        _timestamps("2026-10-25T00:30:00Z", "2026-10-25T01:30:00Z")
    )
    local_labels = prepared["event_timestamp_local"].dt.strftime(
        "%Y-%m-%d %H:%M"
    )
    assert local_labels.tolist() == ["2026-10-25 01:30", "2026-10-25 01:30"]
    assert prepared["local_utc_offset_minutes"].tolist() == [60, 0]
    assert prepared["is_dst_local"].tolist() == [1, 0]
    assert prepared["event_timestamp_utc"].nunique() == 2


def test_calendar_day_numbers_use_iso_monday_one():
    prepared = add_uk_local_calendar_features(
        _timestamps("2026-01-05T12:00:00Z", "2026-01-11T12:00:00Z")
    )
    assert prepared["day_of_week_utc"].tolist() == [1, 7]
    assert prepared["day_of_week_local"].tolist() == [1, 7]
    assert prepared["is_weekend_local"].tolist() == [0, 1]


def test_naive_timestamps_are_rejected_at_calendar_and_forecast_boundaries():
    with pytest.raises(CalendarFeatureError, match="timezone-aware"):
        add_uk_local_calendar_features(
            _timestamps("2026-01-01T00:00:00")
        )

    frame = build_demo_feature_frame(periods=120)
    frame["event_timestamp_utc"] = frame["event_timestamp_utc"].astype(object)
    frame.loc[0, "event_timestamp_utc"] = "2026-01-01T00:05:00"
    with pytest.raises(ForecastingContractError, match="timezone-aware"):
        prepare_feature_frame(frame, BacktestConfig())


def test_demo_features_include_consistent_utc_and_local_calendar_evidence():
    frame = build_multi_area_demo_feature_frame(
        periods=120,
        start="2026-07-01T00:00:00Z",
    )
    assert set(frame["local_utc_offset_minutes"]) == {60}
    assert set(frame["is_dst_local"]) == {1}
    assert (
        frame["hour_of_day_local"]
        == (frame["hour_of_day_utc"] + 1) % 24
    ).all()
    assert set(frame["calendar_feature_contract_version"]) == {
        CALENDAR_FEATURE_CONTRACT_VERSION
    }


def test_local_calendar_backtest_records_distinct_feature_contract():
    frame = build_demo_feature_frame(
        periods=180,
        start="2026-07-01T00:00:00Z",
    )
    predictions, metrics = run_chronological_backtest(
        frame,
        config=BacktestConfig(
            horizon_minutes=(30,),
            feature_columns=tuple(UK_LOCAL_FEATURE_COLUMNS),
            feature_contract_version=UK_LOCAL_FEATURE_CONTRACT_VERSION,
        ),
        run_id="local-calendar-test",
        run_timestamp=datetime(2026, 8, 23, tzinfo=timezone.utc),
    )
    assert set(predictions["feature_contract_version"]) == {
        UK_LOCAL_FEATURE_CONTRACT_VERSION
    }
    assert set(metrics["feature_contract_version"]) == {
        UK_LOCAL_FEATURE_CONTRACT_VERSION
    }
    assert (
        predictions["trained_through_utc"]
        < predictions["feature_timestamp_utc"]
    ).all()


def test_target_weather_comparison_accepts_the_same_local_calendar_contract():
    frame = build_demo_feature_frame(
        periods=180,
        start="2026-07-01T00:00:00Z",
    )
    forecast_weather = build_demo_forecast_weather_frame(
        frame, horizon_minutes=(30,)
    )
    predictions, metrics = run_weather_model_comparison(
        frame,
        forecast_weather,
        backtest_config=BacktestConfig(
            horizon_minutes=(30,),
            feature_columns=tuple(UK_LOCAL_FEATURE_COLUMNS),
            feature_contract_version=UK_LOCAL_FEATURE_CONTRACT_VERSION,
        ),
        forecast_config=ForecastWeatherConfig(
            valid_time_tolerance_minutes=0,
            min_coverage=1.0,
        ),
        evaluation_mode="holdout",
        run_id="local-weather-comparison",
        run_timestamp=datetime(2026, 8, 23, tzinfo=timezone.utc),
    )
    assert set(predictions["model_name"]) == {
        "ridge_weather_lag",
        "ridge_target_weather",
    }
    assert set(predictions["feature_contract_version"]) == {
        UK_LOCAL_FEATURE_CONTRACT_VERSION
    }
    assert set(metrics["feature_contract_version"]) == {
        UK_LOCAL_FEATURE_CONTRACT_VERSION
    }


def test_cli_writes_distinct_local_calendar_outputs(tmp_path):
    assert (
        main(
            [
                "--demo",
                "--calendar-mode",
                "uk-local",
                "--horizon-minutes",
                "30",
                "--output-dir",
                str(tmp_path),
                "--output-format",
                "csv",
            ]
        )
        == 0
    )
    predictions_path = tmp_path / "baseline_uk_local_calendar_predictions.csv"
    metrics_path = tmp_path / "baseline_uk_local_calendar_metrics.csv"
    assert predictions_path.is_file()
    assert metrics_path.is_file()
    assert set(pd.read_csv(predictions_path)["feature_contract_version"]) == {
        UK_LOCAL_FEATURE_CONTRACT_VERSION
    }
    assert not (tmp_path / "baseline_predictions.csv").exists()

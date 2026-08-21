from datetime import datetime, timezone
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from jsonschema import Draft202012Validator, FormatChecker

from forecasting import (
    BacktestConfig,
    ForecastWeatherConfig,
    ForecastingContractError,
    run_weather_model_comparison,
)
from forecasting.run_baseline import main


def _weather_driven_features(periods: int = 300) -> pd.DataFrame:
    timestamps = pd.date_range(
        "2026-01-01T00:00:00Z", periods=periods, freq="5min"
    )
    index = np.arange(periods)
    temperature = 5.0 + ((index * 37) % 101) / 10.0
    humidity = 45.0 + ((index * 17) % 40)
    demand = 800.0 + 35.0 * temperature + 4.0 * humidity + index % 7
    frame = pd.DataFrame(
        {
            "source_area": "east_midlands",
            "resource_id": "resource-1",
            "city": "Nottingham",
            "event_timestamp_utc": timestamps,
            "demand_mw": demand,
            "temperature": temperature,
            "humidity": humidity,
            "hour_of_day_utc": [value.hour for value in timestamps],
            "day_of_week_utc": [
                value.dayofweek + 1 for value in timestamps
            ],
            "is_weekend_utc": [
                int(value.dayofweek >= 5) for value in timestamps
            ],
            "weather_age_minutes": np.zeros(periods),
        }
    )
    frame["demand_lag_1"] = frame["demand_mw"].shift(1)
    frame["demand_rolling_mean_12"] = (
        frame["demand_mw"].shift(1).rolling(12, min_periods=1).mean()
    )
    return frame.iloc[1:].reset_index(drop=True)


def _forecast_weather(
    features: pd.DataFrame,
    horizons: tuple[int, ...] = (30, 60),
) -> pd.DataFrame:
    lookup = features.set_index("event_timestamp_utc")
    records: list[dict[str, object]] = []
    for row in features.itertuples(index=False):
        for horizon in horizons:
            valid_at = row.event_timestamp_utc + pd.Timedelta(
                minutes=horizon
            )
            if valid_at not in lookup.index:
                continue
            target = lookup.loc[valid_at]
            records.append(
                {
                    "source_area": row.source_area,
                    "city": row.city,
                    "forecast_issued_at_utc": row.event_timestamp_utc,
                    "forecast_ingested_at_utc": row.event_timestamp_utc,
                    "forecast_valid_at_utc": valid_at,
                    "forecast_temperature_c": float(
                        target["temperature"]
                    )
                    + 0.05,
                    "forecast_humidity_pct": float(target["humidity"]) + 0.1,
                    "forecast_provider": "demo-provider",
                    "forecast_model": "deterministic-v1",
                }
            )
    return pd.DataFrame(records)


def _run(
    features: pd.DataFrame,
    weather: pd.DataFrame,
    *,
    mode: str = "holdout",
    horizons: tuple[int, ...] = (30, 60),
):
    return run_weather_model_comparison(
        features,
        weather,
        backtest_config=BacktestConfig(horizon_minutes=horizons),
        forecast_config=ForecastWeatherConfig(min_coverage=1.0),
        evaluation_mode=mode,
        origin_count=3,
        run_id=f"comparison-{mode}",
        run_timestamp=datetime(2026, 8, 21, tzinfo=timezone.utc),
    )


def test_public_api_exposes_weather_model_comparison():
    from forecasting.baseline import (
        prepare_weather_model_comparison,
        run_weather_model_comparison as baseline_comparison,
    )

    assert baseline_comparison is run_weather_model_comparison
    assert callable(prepare_weather_model_comparison)


def test_holdout_models_use_identical_rows_and_training_boundaries():
    features = _weather_driven_features()
    predictions, metrics = _run(features, _forecast_weather(features))

    assert set(predictions["model_name"]) == {
        "ridge_weather_lag",
        "ridge_target_weather",
    }
    assert set(predictions["weather_feature_mode"]) == {
        "observed_at_feature",
        "target_forecast",
    }
    assert set(predictions["weather_comparison_contract_version"]) == {
        "weather-model-comparison-v1"
    }
    for _, cohort in predictions.groupby(
        ["requested_horizon_minutes", "split"]
    ):
        timestamps = [
            set(group["feature_timestamp_utc"])
            for _, group in cohort.groupby("model_name")
        ]
        assert timestamps[0] == timestamps[1]
        assert cohort["trained_through_utc"].nunique() == 1
        assert cohort["training_observation_count"].nunique() == 1

    assert set(metrics["forecast_weather_coverage_pct"]) == {100.0}


def test_target_weather_model_beats_observed_weather_on_target_driven_data():
    features = _weather_driven_features()
    _, metrics = _run(features, _forecast_weather(features))
    test_metrics = metrics.loc[metrics["split"] == "test"]

    for _, horizon in test_metrics.groupby("requested_horizon_minutes"):
        by_model = horizon.set_index("model_name")
        assert (
            by_model.loc["ridge_target_weather", "mae_mw"]
            < by_model.loc["ridge_weather_lag", "mae_mw"]
        )


def test_target_model_does_not_use_observed_weather_columns():
    features = _weather_driven_features()
    weather = _forecast_weather(features)
    original, _ = _run(features, weather, horizons=(30,))

    changed = features.copy()
    random = np.random.default_rng(42)
    changed["temperature"] = random.permutation(
        changed["temperature"].to_numpy()
    )
    changed["humidity"] = random.permutation(
        changed["humidity"].to_numpy()
    )
    changed["weather_age_minutes"] = random.integers(
        0, 100, len(changed)
    )
    altered, _ = _run(changed, weather, horizons=(30,))

    sort_columns = ["split", "feature_timestamp_utc"]
    original_target = (
        original.loc[original["model_name"] == "ridge_target_weather"]
        .sort_values(sort_columns)["predicted_demand_mw"]
        .to_numpy()
    )
    altered_target = (
        altered.loc[altered["model_name"] == "ridge_target_weather"]
        .sort_values(sort_columns)["predicted_demand_mw"]
        .to_numpy()
    )
    original_observed = (
        original.loc[original["model_name"] == "ridge_weather_lag"]
        .sort_values(sort_columns)["predicted_demand_mw"]
        .to_numpy()
    )
    altered_observed = (
        altered.loc[altered["model_name"] == "ridge_weather_lag"]
        .sort_values(sort_columns)["predicted_demand_mw"]
        .to_numpy()
    )

    assert np.allclose(original_target, altered_target)
    assert not np.allclose(original_observed, altered_observed)


def test_rolling_origin_comparison_preserves_cutoffs_and_fold_sequence():
    features = _weather_driven_features()
    predictions, metrics = _run(
        features,
        _forecast_weather(features, horizons=(30,)),
        mode="rolling-origin",
        horizons=(30,),
    )

    assert set(predictions["origin_fold"]) == {1, 2, 3}
    assert set(predictions["origin_count"]) == {3}
    assert (
        predictions["trained_through_utc"]
        < predictions["origin_cutoff_utc"]
    ).all()
    assert (
        predictions["origin_cutoff_utc"]
        <= predictions["feature_timestamp_utc"]
    ).all()
    assert set(
        predictions.loc[predictions["origin_fold"] < 3, "split"]
    ) == {"validation"}
    assert set(
        predictions.loc[predictions["origin_fold"] == 3, "split"]
    ) == {"test"}
    assert set(metrics["evaluation_contract_version"]) == {
        "rolling-origin-v1"
    }


def test_forecast_weather_gap_fails_paired_comparison_coverage():
    features = _weather_driven_features()
    weather = _forecast_weather(features, horizons=(30,))
    missing_valid_times = set(
        weather["forecast_valid_at_utc"].drop_duplicates().iloc[::2]
    )
    weather = weather.loc[
        ~weather["forecast_valid_at_utc"].isin(missing_valid_times)
    ].reset_index(drop=True)

    with pytest.raises(ForecastingContractError, match="minimum coverage"):
        _run(features, weather, horizons=(30,))


def test_comparison_prediction_satisfies_versioned_schema():
    features = _weather_driven_features()
    predictions, _ = _run(
        features,
        _forecast_weather(features, horizons=(30,)),
        horizons=(30,),
    )
    contract = json.loads(
        (
            Path(__file__).resolve().parents[1]
            / "data-contracts"
            / "forecast_weather_model_comparison_schema.json"
        ).read_text(encoding="utf-8")
    )
    validator = Draft202012Validator(
        contract, format_checker=FormatChecker()
    )

    for model_name in ("ridge_weather_lag", "ridge_target_weather"):
        row = predictions.loc[
            predictions["model_name"] == model_name
        ].iloc[0].to_dict()
        for column in (
            "run_timestamp_utc",
            "feature_timestamp_utc",
            "event_timestamp_utc",
            "trained_through_utc",
            "target_weather_forecast_issued_at_utc",
            "target_weather_forecast_ingested_at_utc",
            "target_weather_forecast_valid_at_utc",
        ):
            row[column] = pd.Timestamp(row[column]).isoformat()
        for column in ("origin_fold", "origin_count", "origin_cutoff_utc"):
            if pd.isna(row[column]):
                row[column] = None
        assert list(validator.iter_errors(row)) == []


def test_cli_demo_writes_distinct_weather_comparison_evidence(tmp_path):
    exit_code = main(
        [
            "--demo",
            "--model-set",
            "weather-comparison",
            "--horizon-minutes",
            "30",
            "--min-forecast-weather-coverage",
            "1.0",
            "--output-dir",
            str(tmp_path),
            "--output-format",
            "csv",
        ]
    )

    assert exit_code == 0
    predictions_path = tmp_path / "weather_comparison_predictions.csv"
    metrics_path = tmp_path / "weather_comparison_metrics.csv"
    assert predictions_path.is_file()
    assert metrics_path.is_file()
    predictions = pd.read_csv(predictions_path)
    assert set(predictions["model_name"]) == {
        "ridge_weather_lag",
        "ridge_target_weather",
    }
    assert not (tmp_path / "baseline_predictions.csv").exists()


def test_cli_requires_forecast_input_for_real_weather_comparison(tmp_path):
    feature_path = tmp_path / "features.csv"
    _weather_driven_features(120).to_csv(feature_path, index=False)

    with pytest.raises(SystemExit):
        main(
            [
                "--input",
                str(feature_path),
                "--model-set",
                "weather-comparison",
                "--horizon-minutes",
                "30",
            ]
        )

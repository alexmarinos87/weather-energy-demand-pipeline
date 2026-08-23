from datetime import datetime, timezone
import json
from pathlib import Path

import pandas as pd
from jsonschema import Draft202012Validator, FormatChecker

from forecasting.contracts import (
    UK_LOCAL_FEATURE_COLUMNS,
    UK_LOCAL_FEATURE_CONTRACT_VERSION,
    BacktestConfig,
)
from forecasting.demo import build_demo_feature_frame
from forecasting.evaluation import run_chronological_backtest
from forecasting.forecast_weather import (
    ForecastWeatherConfig,
    build_demo_forecast_weather_frame,
)
from forecasting.weather_comparison import run_weather_model_comparison


ROOT = Path(__file__).resolve().parents[1]


def _json_row(row: pd.Series) -> dict:
    payload = row.to_dict()
    for key, value in list(payload.items()):
        if isinstance(value, pd.Timestamp):
            payload[key] = value.isoformat()
        elif pd.isna(value):
            payload[key] = None
    return payload


def _local_config() -> BacktestConfig:
    return BacktestConfig(
        horizon_minutes=(30,),
        feature_columns=tuple(UK_LOCAL_FEATURE_COLUMNS),
        feature_contract_version=UK_LOCAL_FEATURE_CONTRACT_VERSION,
    )


def test_local_calendar_baseline_prediction_satisfies_shared_schema():
    frame = build_demo_feature_frame(
        periods=180,
        start="2026-07-01T00:00:00Z",
    )
    predictions, _ = run_chronological_backtest(
        frame,
        config=_local_config(),
        run_id="local-schema-baseline",
        run_timestamp=datetime(2026, 8, 23, tzinfo=timezone.utc),
    )
    schema = json.loads(
        (ROOT / "data-contracts" / "forecast_evaluation_schema.json").read_text(
            encoding="utf-8"
        )
    )
    errors = list(
        Draft202012Validator(
            schema, format_checker=FormatChecker()
        ).iter_errors(_json_row(predictions.iloc[0]))
    )
    assert errors == []


def test_local_calendar_weather_comparison_satisfies_shared_schema():
    frame = build_demo_feature_frame(
        periods=180,
        start="2026-07-01T00:00:00Z",
    )
    weather = build_demo_forecast_weather_frame(frame, horizon_minutes=(30,))
    predictions, _ = run_weather_model_comparison(
        frame,
        weather,
        backtest_config=_local_config(),
        forecast_config=ForecastWeatherConfig(
            valid_time_tolerance_minutes=0,
            min_coverage=1.0,
        ),
        evaluation_mode="holdout",
        run_id="local-schema-comparison",
        run_timestamp=datetime(2026, 8, 23, tzinfo=timezone.utc),
    )
    schema = json.loads(
        (
            ROOT
            / "data-contracts"
            / "forecast_weather_model_comparison_schema.json"
        ).read_text(encoding="utf-8")
    )
    errors = list(
        Draft202012Validator(
            schema, format_checker=FormatChecker()
        ).iter_errors(_json_row(predictions.iloc[0]))
    )
    assert errors == []

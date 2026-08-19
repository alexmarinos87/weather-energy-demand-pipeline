from datetime import datetime, timezone

import json
from pathlib import Path

import pandas as pd
import pytest
from jsonschema import Draft202012Validator, FormatChecker

from forecasting.baseline import (
    BacktestConfig,
    ForecastingContractError,
    build_demo_feature_frame,
    prepare_feature_frame,
    run_chronological_backtest,
)
from forecasting.run_baseline import main


def test_chronological_backtest_never_trains_on_evaluation_rows():
    frame = build_demo_feature_frame(periods=120)
    predictions, metrics = run_chronological_backtest(
        frame,
        run_id="test-run",
        run_timestamp=datetime(2026, 8, 19, tzinfo=timezone.utc),
    )

    assert set(predictions["split"]) == {"validation", "test"}
    assert set(predictions["model_name"]) == {
        "persistence_lag_1",
        "ridge_weather_lag",
    }
    assert (
        predictions["trained_through_utc"] < predictions["event_timestamp_utc"]
    ).all()
    assert set(metrics["split"]) == {"validation", "test"}
    assert (metrics["observation_count"] > 0).all()


def test_ridge_baseline_beats_persistence_on_weather_sensitive_demo():
    frame = build_demo_feature_frame(periods=144)
    _, metrics = run_chronological_backtest(frame, run_id="comparison")
    test_metrics = metrics.loc[metrics["split"] == "test"].set_index("model_name")

    assert (
        test_metrics.loc["ridge_weather_lag", "mae_mw"]
        < test_metrics.loc["persistence_lag_1", "mae_mw"]
    )


def test_duplicate_group_timestamps_are_rejected():
    frame = build_demo_feature_frame(periods=80)
    duplicate = pd.concat([frame, frame.iloc[[10]]], ignore_index=True)

    with pytest.raises(ForecastingContractError, match="duplicate event timestamps"):
        prepare_feature_frame(duplicate, BacktestConfig())


def test_non_finite_features_are_rejected():
    frame = build_demo_feature_frame(periods=80)
    frame.loc[10, "temperature"] = float("inf")

    with pytest.raises(ForecastingContractError, match="finite values"):
        prepare_feature_frame(frame, BacktestConfig())


def test_insufficient_history_is_rejected():
    frame = build_demo_feature_frame(periods=60).iloc[:30]

    with pytest.raises(ForecastingContractError, match="training rows"):
        run_chronological_backtest(frame)


def test_cli_demo_writes_predictions_and_metrics(tmp_path):
    exit_code = main(
        [
            "--demo",
            "--output-dir",
            str(tmp_path),
            "--output-format",
            "csv",
        ]
    )

    assert exit_code == 0
    assert (tmp_path / "baseline_predictions.csv").is_file()
    assert (tmp_path / "baseline_metrics.csv").is_file()


def test_prediction_rows_satisfy_versioned_evaluation_contract():
    frame = build_demo_feature_frame(periods=96)
    predictions, _ = run_chronological_backtest(
        frame,
        run_id="contract-test",
        run_timestamp=datetime(2026, 8, 19, tzinfo=timezone.utc),
    )
    contract_path = (
        Path(__file__).resolve().parents[1]
        / "data-contracts"
        / "forecast_evaluation_schema.json"
    )
    contract = json.loads(contract_path.read_text(encoding="utf-8"))
    validator = Draft202012Validator(contract, format_checker=FormatChecker())
    row = predictions.iloc[0].to_dict()
    for timestamp_column in (
        "run_timestamp_utc",
        "event_timestamp_utc",
        "trained_through_utc",
    ):
        row[timestamp_column] = pd.Timestamp(row[timestamp_column]).isoformat()

    errors = list(validator.iter_errors(row))
    assert errors == []

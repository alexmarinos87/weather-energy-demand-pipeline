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
    build_supervised_frame,
    prepare_feature_frame,
    run_chronological_backtest,
)
from forecasting.run_baseline import main


def test_future_horizon_predictions_use_strictly_earlier_features_and_labels():
    frame = build_demo_feature_frame(periods=144)
    predictions, metrics = run_chronological_backtest(
        frame,
        config=BacktestConfig(horizon_steps=3),
        run_id="horizon-test",
        run_timestamp=datetime(2026, 8, 21, tzinfo=timezone.utc),
    )

    assert set(predictions["horizon_steps"]) == {3}
    assert (
        predictions["trained_through_utc"] < predictions["feature_timestamp_utc"]
    ).all()
    assert (
        predictions["feature_timestamp_utc"] < predictions["event_timestamp_utc"]
    ).all()
    assert set(predictions["horizon_minutes"]) == {180.0}
    assert set(metrics["horizon_steps"]) == {3}
    assert (
        metrics["trained_through_utc"] < metrics["evaluation_feature_start_utc"]
    ).all()


def test_persistence_uses_current_demand_for_future_target():
    frame = build_demo_feature_frame(periods=120)
    predictions, _ = run_chronological_backtest(
        frame,
        config=BacktestConfig(horizon_steps=2),
        run_id="persistence-test",
    )
    persistence = predictions.loc[
        predictions["model_name"] == "persistence_lag_1"
    ]

    assert (
        persistence["predicted_demand_mw"] == persistence["current_demand_mw"]
    ).all()


def test_supervised_frame_shifts_targets_within_each_group():
    frame = build_demo_feature_frame(periods=80)
    prepared = prepare_feature_frame(frame, BacktestConfig(horizon_steps=4))
    supervised = build_supervised_frame(prepared, BacktestConfig(horizon_steps=4))

    assert len(supervised) == len(prepared) - 4
    assert (
        supervised["target_timestamp_utc"] - supervised["feature_timestamp_utc"]
        == pd.Timedelta(hours=4)
    ).all()
    assert supervised.iloc[0]["target_demand_mw"] == prepared.iloc[4]["demand_mw"]


def test_invalid_horizon_is_rejected():
    frame = build_demo_feature_frame(periods=80)

    with pytest.raises(ForecastingContractError, match="horizon_steps"):
        prepare_feature_frame(frame, BacktestConfig(horizon_steps=0))


def test_cli_accepts_explicit_horizon(tmp_path):
    exit_code = main(
        [
            "--demo",
            "--horizon-steps",
            "3",
            "--output-dir",
            str(tmp_path),
            "--output-format",
            "csv",
        ]
    )

    assert exit_code == 0
    predictions = pd.read_csv(tmp_path / "baseline_predictions.csv")
    assert set(predictions["horizon_steps"]) == {3}


def test_prediction_rows_satisfy_horizon_contract():
    frame = build_demo_feature_frame(periods=96)
    predictions, _ = run_chronological_backtest(
        frame,
        config=BacktestConfig(horizon_steps=2),
        run_id="contract-test",
        run_timestamp=datetime(2026, 8, 21, tzinfo=timezone.utc),
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
        "feature_timestamp_utc",
        "event_timestamp_utc",
        "trained_through_utc",
    ):
        row[timestamp_column] = pd.Timestamp(row[timestamp_column]).isoformat()

    errors = list(validator.iter_errors(row))
    assert errors == []

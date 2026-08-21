from datetime import datetime, timezone

import pandas as pd
import pytest

from forecasting.baseline import (
    BacktestConfig,
    ForecastingContractError,
    build_demo_feature_frame,
    build_supervised_frame,
    prepare_feature_frame,
    run_chronological_backtest,
)
from forecasting.run_baseline import main


def test_predictions_cover_explicit_30_and_60_minute_targets():
    frame = build_demo_feature_frame(periods=180)
    predictions, metrics = run_chronological_backtest(
        frame,
        run_id="time-horizon-test",
        run_timestamp=datetime(2026, 8, 21, tzinfo=timezone.utc),
    )

    assert set(predictions["requested_horizon_minutes"]) == {30, 60}
    assert set(predictions["target_delay_minutes"]) == {0.0}
    assert set(
        predictions.loc[
            predictions["requested_horizon_minutes"] == 30, "horizon_steps"
        ]
    ) == {6}
    assert set(
        predictions.loc[
            predictions["requested_horizon_minutes"] == 60, "horizon_steps"
        ]
    ) == {12}
    assert set(metrics["target_coverage_pct"]) == {100.0}


def test_missing_exact_target_uses_first_later_observation_within_tolerance():
    frame = build_demo_feature_frame(periods=160)
    missing_timestamp = frame.loc[40, "event_timestamp_utc"]
    frame = frame.loc[frame["event_timestamp_utc"] != missing_timestamp].reset_index(
        drop=True
    )
    config = BacktestConfig(
        horizon_minutes=(30,),
        target_tolerance_minutes=5,
        min_target_coverage=0.95,
    )
    supervised = build_supervised_frame(prepare_feature_frame(frame, config), config)

    delayed = supervised.loc[supervised["target_delay_minutes"] == 5.0]
    assert not delayed.empty
    assert (delayed["horizon_minutes"] == 35.0).all()


def test_irregular_observation_count_does_not_change_requested_time_horizon():
    frame = build_demo_feature_frame(periods=160)
    frame = frame.drop(index=[25]).reset_index(drop=True)
    config = BacktestConfig(
        horizon_minutes=(30,),
        target_tolerance_minutes=5,
        min_target_coverage=0.95,
    )
    supervised = build_supervised_frame(prepare_feature_frame(frame, config), config)

    exact = supervised.loc[supervised["target_delay_minutes"] == 0.0]
    assert set(exact["horizon_minutes"]) == {30.0}
    assert set(exact["horizon_steps"]) == {5, 6}


def test_gap_beyond_tolerance_fails_target_coverage_contract():
    frame = build_demo_feature_frame(periods=160)
    frame = frame.drop(index=[40, 41]).reset_index(drop=True)
    config = BacktestConfig(
        horizon_minutes=(30,),
        target_tolerance_minutes=5,
        min_target_coverage=1.0,
    )

    with pytest.raises(ForecastingContractError, match="minimum coverage"):
        build_supervised_frame(prepare_feature_frame(frame, config), config)


def test_persistence_uses_current_demand_for_future_target():
    frame = build_demo_feature_frame(periods=180)
    predictions, _ = run_chronological_backtest(
        frame,
        config=BacktestConfig(horizon_minutes=(60,)),
        run_id="persistence-test",
    )
    persistence = predictions.loc[
        predictions["model_name"] == "persistence_current_value"
    ]

    assert (
        persistence["predicted_demand_mw"] == persistence["current_demand_mw"]
    ).all()


def test_unsupported_time_horizon_is_rejected():
    frame = build_demo_feature_frame(periods=120)

    with pytest.raises(ForecastingContractError, match="Unsupported horizon_minutes=45"):
        prepare_feature_frame(frame, BacktestConfig(horizon_minutes=(45,)))


def test_cli_accepts_both_approved_time_horizons(tmp_path):
    exit_code = main(
        [
            "--demo",
            "--horizon-minutes",
            "30",
            "60",
            "--target-tolerance-minutes",
            "5",
            "--output-dir",
            str(tmp_path),
            "--output-format",
            "csv",
        ]
    )

    assert exit_code == 0
    predictions = pd.read_csv(tmp_path / "baseline_predictions.csv")
    assert set(predictions["requested_horizon_minutes"]) == {30, 60}

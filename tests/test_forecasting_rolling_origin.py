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
    build_rolling_origin_folds,
    build_supervised_frame,
    prepare_feature_frame,
    run_rolling_origin_backtest,
)
from forecasting.run_baseline import main


def test_rolling_origins_expand_history_and_reserve_final_test_window():
    frame = build_demo_feature_frame(periods=240)
    predictions, metrics = run_rolling_origin_backtest(
        frame,
        config=BacktestConfig(horizon_minutes=(30,)),
        origin_count=3,
        run_id="rolling-test",
        run_timestamp=datetime(2026, 8, 21, tzinfo=timezone.utc),
    )

    assert set(predictions["origin_fold"]) == {1, 2, 3}
    assert set(predictions["origin_count"]) == {3}
    assert set(predictions.loc[predictions["origin_fold"] < 3, "split"]) == {
        "validation"
    }
    assert set(predictions.loc[predictions["origin_fold"] == 3, "split"]) == {
        "test"
    }
    evidence = (
        metrics.loc[metrics["model_name"] == "persistence_current_value"]
        .sort_values("origin_fold")
        .drop_duplicates("origin_fold")
    )
    assert evidence["origin_cutoff_utc"].is_monotonic_increasing
    assert evidence["training_observation_count"].is_monotonic_increasing
    assert evidence["training_observation_count"].iloc[-1] > evidence[
        "training_observation_count"
    ].iloc[0]


def test_rolling_origin_windows_do_not_reuse_evaluation_rows():
    frame = build_demo_feature_frame(periods=240)
    config = BacktestConfig(horizon_minutes=(30,))
    supervised = build_supervised_frame(prepare_feature_frame(frame, config), config)
    group = supervised.sort_values("feature_timestamp_utc").reset_index(drop=True)
    folds = build_rolling_origin_folds(group, config, origin_count=4)

    evidence = pd.concat(
        [
            fold.evaluation[["feature_timestamp_utc"]].assign(
                origin_fold=fold.origin_fold,
                split=fold.split,
            )
            for fold in folds
        ],
        ignore_index=True,
    )
    assert evidence["feature_timestamp_utc"].is_unique
    assert [fold.split for fold in folds] == [
        "validation",
        "validation",
        "validation",
        "test",
    ]


def test_every_origin_purges_labels_unavailable_at_cutoff():
    frame = build_demo_feature_frame(periods=240)
    predictions, _ = run_rolling_origin_backtest(
        frame,
        config=BacktestConfig(horizon_minutes=(60,)),
        origin_count=3,
        run_id="purge-test",
    )

    assert (
        predictions["trained_through_utc"] < predictions["origin_cutoff_utc"]
    ).all()
    assert (
        predictions["origin_cutoff_utc"] <= predictions["feature_timestamp_utc"]
    ).all()
    assert set(predictions["evaluation_contract_version"]) == {
        "rolling-origin-v1"
    }


def test_invalid_origin_count_is_rejected():
    frame = build_demo_feature_frame(periods=160)

    with pytest.raises(ForecastingContractError, match="origin_count"):
        run_rolling_origin_backtest(frame, origin_count=1)


def test_too_many_origins_for_validation_history_is_rejected():
    frame = build_demo_feature_frame(periods=120)

    with pytest.raises(ForecastingContractError, match="rolling origins require"):
        run_rolling_origin_backtest(
            frame,
            config=BacktestConfig(horizon_minutes=(60,)),
            origin_count=8,
        )


def test_cli_preserves_fixed_holdout_as_default(tmp_path):
    exit_code = main(
        [
            "--demo",
            "--horizon-minutes",
            "30",
            "--output-dir",
            str(tmp_path),
            "--output-format",
            "csv",
        ]
    )

    assert exit_code == 0
    predictions = pd.read_csv(tmp_path / "baseline_predictions.csv")
    metrics = pd.read_csv(tmp_path / "baseline_metrics.csv")
    assert set(predictions["split"]) == {"validation", "test"}
    assert "origin_fold" not in predictions.columns
    assert "origin_fold" not in metrics.columns


def test_cli_accepts_rolling_origin_count(tmp_path):
    exit_code = main(
        [
            "--demo",
            "--evaluation-mode",
            "rolling-origin",
            "--horizon-minutes",
            "30",
            "--rolling-origin-folds",
            "4",
            "--output-dir",
            str(tmp_path),
            "--output-format",
            "csv",
        ]
    )

    assert exit_code == 0
    predictions = pd.read_csv(tmp_path / "rolling_origin_predictions.csv")
    metrics = pd.read_csv(tmp_path / "rolling_origin_metrics.csv")
    assert set(predictions["origin_fold"]) == {1, 2, 3, 4}
    assert set(predictions["origin_count"]) == {4}
    assert set(metrics["evaluation_contract_version"]) == {"rolling-origin-v1"}
    assert not (tmp_path / "baseline_predictions.csv").exists()


def test_prediction_rows_satisfy_rolling_origin_contract():
    frame = build_demo_feature_frame(periods=180)
    predictions, _ = run_rolling_origin_backtest(
        frame,
        config=BacktestConfig(horizon_minutes=(30,)),
        origin_count=3,
        run_id="contract-test",
        run_timestamp=datetime(2026, 8, 21, tzinfo=timezone.utc),
    )
    contract_path = (
        Path(__file__).resolve().parents[1]
        / "data-contracts"
        / "rolling_origin_evaluation_schema.json"
    )
    contract = json.loads(contract_path.read_text(encoding="utf-8"))
    validator = Draft202012Validator(contract, format_checker=FormatChecker())
    row = predictions.iloc[0].to_dict()
    row["origin_cutoff_utc"] = pd.Timestamp(row["origin_cutoff_utc"]).isoformat()

    assert list(validator.iter_errors(row)) == []

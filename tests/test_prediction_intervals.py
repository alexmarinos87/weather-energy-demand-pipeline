from datetime import datetime, timezone
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from jsonschema import Draft202012Validator, FormatChecker

from forecasting.prediction_intervals import (
    PredictionIntervalConfig,
    PredictionIntervalError,
    calibrate_prediction_intervals,
    prepare_point_prediction_evidence,
)
from forecasting.run_prediction_intervals import main


ROOT = Path(__file__).resolve().parents[1]


def point_predictions(
    *,
    run_id: str = "point-run-1",
    model_name: str = "ridge_weather_lag",
    horizon: int = 30,
    validation_rows: int = 30,
    test_rows: int = 10,
    rolling: bool = False,
) -> pd.DataFrame:
    rows = []
    start = pd.Timestamp("2026-01-01T00:00:00Z")
    for index in range(validation_rows):
        feature = start + pd.Timedelta(minutes=5 * index)
        actual = 100.0
        rows.append(
            {
                "run_id": run_id,
                "source_area": "east_midlands",
                "resource_id": "resource-1",
                "city": "Nottingham",
                "feature_timestamp_utc": feature,
                "event_timestamp_utc": feature + pd.Timedelta(minutes=horizon),
                "requested_horizon_minutes": horizon,
                "split": "validation",
                "model_name": model_name,
                "actual_demand_mw": actual,
                "predicted_demand_mw": actual + index + 1,
                "trained_through_utc": start - pd.Timedelta(hours=1),
                "feature_contract_version": "time-horizon-v1",
                "origin_fold": 1 + int(rolling and index >= validation_rows // 2),
            }
        )
    test_start = start + pd.Timedelta(minutes=5 * validation_rows)
    for index in range(test_rows):
        feature = test_start + pd.Timedelta(minutes=5 * index)
        rows.append(
            {
                "run_id": run_id,
                "source_area": "east_midlands",
                "resource_id": "resource-1",
                "city": "Nottingham",
                "feature_timestamp_utc": feature,
                "event_timestamp_utc": feature + pd.Timedelta(minutes=horizon),
                "requested_horizon_minutes": horizon,
                "split": "test",
                "model_name": model_name,
                "actual_demand_mw": 100.0,
                "predicted_demand_mw": 100.0,
                "trained_through_utc": start - pd.Timedelta(minutes=30),
                "feature_contract_version": "time-horizon-v1",
                "origin_fold": 3 if rolling else None,
            }
        )
    return pd.DataFrame(rows)


def calibrate(frame=None, config=None):
    return calibrate_prediction_intervals(
        frame if frame is not None else point_predictions(),
        config=config
        or PredictionIntervalConfig(
            coverage_levels=(0.90,),
            min_calibration_rows=24,
        ),
        interval_run_id="interval-run-1",
        interval_run_timestamp=datetime(2026, 1, 2, tzinfo=timezone.utc),
    )


def test_overlapping_validation_labels_are_excluded_before_calibration():
    intervals, metrics = calibrate()
    assert set(intervals["calibration_observation_count"]) == {24}
    assert set(intervals["calibration_quantile_rank"]) == {23}
    assert set(intervals["calibration_radius_mw"]) == {23.0}
    assert (
        intervals["calibration_label_available_through_utc"]
        < intervals["feature_timestamp_utc"]
    ).all()
    assert metrics.loc[0, "calibration_observation_count"] == 24


def test_finite_sample_quantile_uses_ceil_n_plus_one_rank():
    intervals, _ = calibrate(
        config=PredictionIntervalConfig(
            coverage_levels=(0.50, 0.90),
            min_calibration_rows=24,
        )
    )
    radii = (
        intervals.groupby("target_coverage_level")["calibration_radius_mw"]
        .first()
        .to_dict()
    )
    assert radii == {0.5: 13.0, 0.9: 23.0}


def test_test_labels_do_not_set_interval_radius():
    original, _ = calibrate()
    changed = point_predictions()
    changed.loc[changed["split"] == "test", "actual_demand_mw"] = 1000.0
    tampered, _ = calibrate(changed)
    assert set(original["calibration_radius_mw"]) == set(
        tampered["calibration_radius_mw"]
    )
    assert original["interval_covered"].all()
    assert not tampered["interval_covered"].any()


def test_insufficient_causally_available_calibration_rows_fail():
    with pytest.raises(PredictionIntervalError, match="24 causally available"):
        calibrate(
            config=PredictionIntervalConfig(
                coverage_levels=(0.90,),
                min_calibration_rows=25,
            )
        )


def test_models_and_horizons_are_calibrated_independently():
    frame = pd.concat(
        [
            point_predictions(model_name="ridge_weather_lag", horizon=30),
            point_predictions(model_name="seasonal_previous_week", horizon=30),
            point_predictions(
                model_name="ridge_weather_lag",
                horizon=60,
                validation_rows=36,
            ),
        ],
        ignore_index=True,
    )
    intervals, metrics = calibrate(frame)
    assert intervals.groupby(
        ["requested_horizon_minutes", "model_name"]
    ).ngroups == 3
    assert metrics.groupby(
        ["requested_horizon_minutes", "model_name"]
    ).ngroups == 3


def test_rolling_origin_validation_rows_calibrate_the_final_test_origin():
    intervals, metrics = calibrate(point_predictions(rolling=True))
    assert set(intervals["evaluation_origin_fold"].astype(int)) == {3}
    assert set(metrics["evaluation_origin_fold"].astype(int)) == {3}
    assert set(intervals["calibration_observation_count"]) == {24}


def test_timezone_naive_prediction_evidence_is_rejected():
    frame = point_predictions()
    frame["feature_timestamp_utc"] = frame["feature_timestamp_utc"].astype(object)
    frame.loc[0, "feature_timestamp_utc"] = "2026-01-01T00:00:00"
    with pytest.raises(PredictionIntervalError, match="timezone-aware"):
        prepare_point_prediction_evidence(frame)


def test_duplicate_evaluation_identity_is_rejected():
    frame = point_predictions()
    frame = pd.concat([frame, frame.iloc[[0]]], ignore_index=True)
    with pytest.raises(PredictionIntervalError, match="duplicate"):
        prepare_point_prediction_evidence(frame)


def test_interval_metrics_report_empirical_coverage_and_width():
    intervals, metrics = calibrate()
    assert metrics.loc[0, "empirical_coverage_pct"] == 100.0
    assert metrics.loc[0, "average_interval_width_mw"] == 46.0
    assert set(intervals["interval_width_mw"]) == {46.0}
    assert (
        intervals["lower_prediction_mw"]
        <= intervals["point_prediction_mw"]
    ).all()
    assert (
        intervals["point_prediction_mw"]
        <= intervals["upper_prediction_mw"]
    ).all()


def _json_row(row: pd.Series) -> dict:
    payload = row.to_dict()
    for key, value in list(payload.items()):
        if isinstance(value, pd.Timestamp):
            payload[key] = value.isoformat()
        elif isinstance(value, np.generic):
            payload[key] = value.item()
        elif pd.isna(value):
            payload[key] = None
    return payload


def test_interval_and_metric_rows_satisfy_versioned_schemas():
    intervals, metrics = calibrate()
    interval_schema = json.loads(
        (ROOT / "data-contracts" / "prediction_interval_schema.json").read_text(
            encoding="utf-8"
        )
    )
    metric_schema = json.loads(
        (
            ROOT
            / "data-contracts"
            / "prediction_interval_metrics_schema.json"
        ).read_text(encoding="utf-8")
    )
    assert list(
        Draft202012Validator(
            interval_schema, format_checker=FormatChecker()
        ).iter_errors(_json_row(intervals.iloc[0]))
    ) == []
    assert list(
        Draft202012Validator(
            metric_schema, format_checker=FormatChecker()
        ).iter_errors(_json_row(metrics.iloc[0]))
    ) == []


def test_cli_reads_point_predictions_and_writes_immutable_outputs(tmp_path):
    predictions_path = tmp_path / "point_predictions.csv"
    output = tmp_path / "output"
    point_predictions().to_csv(predictions_path, index=False)
    assert (
        main(
            [
                "--predictions-input",
                str(predictions_path),
                "--coverage-levels",
                "0.9",
                "--min-calibration-rows",
                "24",
                "--output-dir",
                str(output),
                "--output-format",
                "csv",
            ]
        )
        == 0
    )
    interval_files = list(output.glob("prediction_intervals_*.csv"))
    metric_files = list(output.glob("prediction_interval_metrics_*.csv"))
    assert len(interval_files) == len(metric_files) == 1
    assert set(pd.read_csv(interval_files[0])["target_coverage_level"]) == {0.9}


def test_multiple_point_runs_require_explicit_selection_in_cli(tmp_path):
    predictions_path = tmp_path / "point_predictions.csv"
    frame = pd.concat(
        [point_predictions(run_id="one"), point_predictions(run_id="two")],
        ignore_index=True,
    )
    frame.to_csv(predictions_path, index=False)
    with pytest.raises(ValueError, match="select one"):
        main(
            [
                "--predictions-input",
                str(predictions_path),
                "--output-dir",
                str(tmp_path / "output"),
            ]
        )

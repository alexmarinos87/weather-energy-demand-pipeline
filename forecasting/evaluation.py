from __future__ import annotations

from datetime import datetime, timezone
from math import sqrt
from typing import Sequence
from uuid import uuid4

import pandas as pd

from forecasting.contracts import (
    FEATURE_TIMESTAMP_COLUMN,
    GROUP_COLUMNS,
    SUPERVISED_TARGET_COLUMN,
    TARGET_COLUMN,
    TARGET_TIMESTAMP_COLUMN,
    TIMESTAMP_COLUMN,
    BacktestConfig,
    ForecastingContractError,
    build_supervised_frame,
    prepare_feature_frame,
    purge_overlapping_training_rows,
    split_group,
)
from forecasting.ridge import fit_ridge_model


PREDICTION_COLUMNS = [
    "run_id",
    "run_timestamp_utc",
    *GROUP_COLUMNS,
    FEATURE_TIMESTAMP_COLUMN,
    TIMESTAMP_COLUMN,
    "horizon_steps",
    "horizon_minutes",
    "split",
    "model_name",
    "current_demand_mw",
    "actual_demand_mw",
    "predicted_demand_mw",
    "absolute_error_mw",
    "squared_error_mw2",
    "trained_through_utc",
    "feature_contract_version",
]
METRIC_COLUMNS = [
    "run_id",
    "run_timestamp_utc",
    *GROUP_COLUMNS,
    "horizon_steps",
    "split",
    "model_name",
    "observation_count",
    "mae_mw",
    "rmse_mw",
    "mape_pct",
    "bias_mw",
    "trained_through_utc",
    "evaluation_feature_start_utc",
    "evaluation_feature_end_utc",
    "evaluation_start_utc",
    "evaluation_end_utc",
    "feature_contract_version",
]


def _prediction_rows(
    evaluation: pd.DataFrame,
    *,
    split: str,
    model_name: str,
    predicted: Sequence[float],
    trained_through: pd.Timestamp,
    run_id: str,
    run_timestamp: datetime,
    config: BacktestConfig,
) -> list[dict[str, object]]:
    predicted_values = list(predicted)
    if len(predicted_values) != len(evaluation):
        raise ForecastingContractError(
            "Prediction count does not match the evaluation row count."
        )
    rows: list[dict[str, object]] = []
    for source_row, prediction in zip(
        evaluation.itertuples(index=False), predicted_values
    ):
        actual = float(getattr(source_row, SUPERVISED_TARGET_COLUMN))
        error = float(prediction) - actual
        rows.append(
            {
                "run_id": run_id,
                "run_timestamp_utc": run_timestamp,
                "source_area": source_row.source_area,
                "resource_id": source_row.resource_id,
                "city": source_row.city,
                FEATURE_TIMESTAMP_COLUMN: getattr(source_row, FEATURE_TIMESTAMP_COLUMN),
                TIMESTAMP_COLUMN: getattr(source_row, TARGET_TIMESTAMP_COLUMN),
                "horizon_steps": config.horizon_steps,
                "horizon_minutes": float(source_row.horizon_minutes),
                "split": split,
                "model_name": model_name,
                "current_demand_mw": float(getattr(source_row, TARGET_COLUMN)),
                "actual_demand_mw": actual,
                "predicted_demand_mw": float(prediction),
                "absolute_error_mw": abs(error),
                "squared_error_mw2": error * error,
                "trained_through_utc": trained_through,
                "feature_contract_version": config.feature_contract_version,
            }
        )
    return rows


def _metric_row(group: pd.DataFrame) -> dict[str, object]:
    errors = group["predicted_demand_mw"] - group["actual_demand_mw"]
    nonzero = group["actual_demand_mw"].abs() > 1e-12
    mape = None
    if nonzero.any():
        mape = float(
            (
                errors[nonzero].abs()
                / group.loc[nonzero, "actual_demand_mw"].abs()
            ).mean()
            * 100
        )
    first = group.iloc[0]
    return {
        "run_id": first["run_id"],
        "run_timestamp_utc": first["run_timestamp_utc"],
        "source_area": first["source_area"],
        "resource_id": first["resource_id"],
        "city": first["city"],
        "horizon_steps": int(first["horizon_steps"]),
        "split": first["split"],
        "model_name": first["model_name"],
        "observation_count": int(len(group)),
        "mae_mw": float(errors.abs().mean()),
        "rmse_mw": float(sqrt((errors * errors).mean())),
        "mape_pct": mape,
        "bias_mw": float(errors.mean()),
        "trained_through_utc": first["trained_through_utc"],
        "evaluation_feature_start_utc": group[FEATURE_TIMESTAMP_COLUMN].min(),
        "evaluation_feature_end_utc": group[FEATURE_TIMESTAMP_COLUMN].max(),
        "evaluation_start_utc": group[TIMESTAMP_COLUMN].min(),
        "evaluation_end_utc": group[TIMESTAMP_COLUMN].max(),
        "feature_contract_version": first["feature_contract_version"],
    }


def _evaluate_split(
    training: pd.DataFrame,
    evaluation: pd.DataFrame,
    *,
    split: str,
    config: BacktestConfig,
    run_id: str,
    run_timestamp: datetime,
) -> list[dict[str, object]]:
    training = purge_overlapping_training_rows(training, evaluation, config)
    trained_through = training[TARGET_TIMESTAMP_COLUMN].max()
    ridge = fit_ridge_model(
        training,
        feature_columns=config.feature_columns,
        target_column=SUPERVISED_TARGET_COLUMN,
        alpha=config.ridge_alpha,
    ).predict(
        evaluation.loc[:, config.feature_columns].itertuples(index=False, name=None)
    )
    rows = _prediction_rows(
        evaluation,
        split=split,
        model_name="persistence_lag_1",
        predicted=evaluation[TARGET_COLUMN].astype(float).tolist(),
        trained_through=trained_through,
        run_id=run_id,
        run_timestamp=run_timestamp,
        config=config,
    )
    rows.extend(
        _prediction_rows(
            evaluation,
            split=split,
            model_name="ridge_weather_lag",
            predicted=ridge,
            trained_through=trained_through,
            run_id=run_id,
            run_timestamp=run_timestamp,
            config=config,
        )
    )
    return rows


def run_chronological_backtest(
    frame: pd.DataFrame,
    *,
    config: BacktestConfig | None = None,
    run_id: str | None = None,
    run_timestamp: datetime | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Evaluate explicit future-horizon baselines without fitting on future labels."""
    config = config or BacktestConfig()
    prepared = prepare_feature_frame(frame, config)
    supervised = build_supervised_frame(prepared, config)
    run_id = run_id or str(uuid4())
    run_timestamp = run_timestamp or datetime.now(timezone.utc)
    if run_timestamp.tzinfo is None:
        raise ForecastingContractError("run_timestamp must be timezone-aware.")
    run_timestamp = run_timestamp.astimezone(timezone.utc)

    all_predictions: list[dict[str, object]] = []
    for _, group in supervised.groupby(GROUP_COLUMNS, sort=True, dropna=False):
        group = group.sort_values(FEATURE_TIMESTAMP_COLUMN).reset_index(drop=True)
        train, validation, test = split_group(group, config)
        all_predictions.extend(
            _evaluate_split(
                train,
                validation,
                split="validation",
                config=config,
                run_id=run_id,
                run_timestamp=run_timestamp,
            )
        )
        all_predictions.extend(
            _evaluate_split(
                pd.concat([train, validation], ignore_index=True),
                test,
                split="test",
                config=config,
                run_id=run_id,
                run_timestamp=run_timestamp,
            )
        )

    predictions = pd.DataFrame(all_predictions, columns=PREDICTION_COLUMNS)
    if predictions.empty:
        raise ForecastingContractError("No groups produced forecast predictions.")
    if not (
        predictions["trained_through_utc"] < predictions[FEATURE_TIMESTAMP_COLUMN]
    ).all():
        raise ForecastingContractError(
            "A prediction used labels that were not known at feature time."
        )
    if not (
        predictions[FEATURE_TIMESTAMP_COLUMN] < predictions[TIMESTAMP_COLUMN]
    ).all():
        raise ForecastingContractError(
            "A forecast target did not occur after its feature timestamp."
        )
    metrics = pd.DataFrame(
        [
            _metric_row(group)
            for _, group in predictions.groupby(
                [*GROUP_COLUMNS, "horizon_steps", "split", "model_name"], sort=True
            )
        ],
        columns=METRIC_COLUMNS,
    )
    return predictions.reset_index(drop=True), metrics.reset_index(drop=True)

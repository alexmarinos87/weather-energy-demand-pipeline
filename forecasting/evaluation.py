from __future__ import annotations

from datetime import datetime, timezone
from math import sqrt
from typing import Sequence
from uuid import uuid4

import pandas as pd

from forecasting.contracts import (
    GROUP_COLUMNS,
    TARGET_COLUMN,
    TIMESTAMP_COLUMN,
    BacktestConfig,
    ForecastingContractError,
    prepare_feature_frame,
    split_group,
)
from forecasting.ridge import fit_ridge_model


PREDICTION_COLUMNS = [
    "run_id",
    "run_timestamp_utc",
    *GROUP_COLUMNS,
    TIMESTAMP_COLUMN,
    "split",
    "model_name",
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
    "split",
    "model_name",
    "observation_count",
    "mae_mw",
    "rmse_mw",
    "mape_pct",
    "bias_mw",
    "trained_through_utc",
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
    feature_contract_version: str,
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
        actual = float(getattr(source_row, TARGET_COLUMN))
        error = float(prediction) - actual
        rows.append(
            {
                "run_id": run_id,
                "run_timestamp_utc": run_timestamp,
                "source_area": source_row.source_area,
                "resource_id": source_row.resource_id,
                "city": source_row.city,
                TIMESTAMP_COLUMN: getattr(source_row, TIMESTAMP_COLUMN),
                "split": split,
                "model_name": model_name,
                "actual_demand_mw": actual,
                "predicted_demand_mw": float(prediction),
                "absolute_error_mw": abs(error),
                "squared_error_mw2": error * error,
                "trained_through_utc": trained_through,
                "feature_contract_version": feature_contract_version,
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
        "split": first["split"],
        "model_name": first["model_name"],
        "observation_count": int(len(group)),
        "mae_mw": float(errors.abs().mean()),
        "rmse_mw": float(sqrt((errors * errors).mean())),
        "mape_pct": mape,
        "bias_mw": float(errors.mean()),
        "trained_through_utc": first["trained_through_utc"],
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
    trained_through = training[TIMESTAMP_COLUMN].max()
    ridge = fit_ridge_model(
        training,
        feature_columns=config.feature_columns,
        alpha=config.ridge_alpha,
    ).predict(
        evaluation.loc[:, config.feature_columns].itertuples(index=False, name=None)
    )
    rows = _prediction_rows(
        evaluation,
        split=split,
        model_name="persistence_lag_1",
        predicted=evaluation["demand_lag_1"].astype(float).tolist(),
        trained_through=trained_through,
        run_id=run_id,
        run_timestamp=run_timestamp,
        feature_contract_version=config.feature_contract_version,
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
            feature_contract_version=config.feature_contract_version,
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
    """Evaluate persistence and ridge baselines without fitting on future rows."""
    config = config or BacktestConfig()
    prepared = prepare_feature_frame(frame, config)
    run_id = run_id or str(uuid4())
    run_timestamp = run_timestamp or datetime.now(timezone.utc)
    if run_timestamp.tzinfo is None:
        raise ForecastingContractError("run_timestamp must be timezone-aware.")
    run_timestamp = run_timestamp.astimezone(timezone.utc)

    all_predictions: list[dict[str, object]] = []
    for _, group in prepared.groupby(GROUP_COLUMNS, sort=True, dropna=False):
        group = group.sort_values(TIMESTAMP_COLUMN).reset_index(drop=True)
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
        predictions["trained_through_utc"] < predictions[TIMESTAMP_COLUMN]
    ).all():
        raise ForecastingContractError(
            "A prediction was evaluated at or before its training boundary."
        )
    metrics = pd.DataFrame(
        [
            _metric_row(group)
            for _, group in predictions.groupby(
                [*GROUP_COLUMNS, "split", "model_name"], sort=True
            )
        ],
        columns=METRIC_COLUMNS,
    )
    return predictions.reset_index(drop=True), metrics.reset_index(drop=True)

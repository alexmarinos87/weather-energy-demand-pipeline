from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import uuid4

import pandas as pd

from math import sqrt
from typing import Sequence

from forecasting.contracts import (
    FEATURE_TIMESTAMP_COLUMN,
    GROUP_COLUMNS,
    REQUESTED_HORIZON_COLUMN,
    SUPERVISED_TARGET_COLUMN,
    TARGET_COLUMN,
    TARGET_DELAY_COLUMN,
    TARGET_TIMESTAMP_COLUMN,
    TARGET_TOLERANCE_COLUMN,
    TIMESTAMP_COLUMN,
    BacktestConfig,
    ForecastingContractError,
    build_supervised_frame,
    prepare_feature_frame,
    purge_overlapping_training_rows,
)
from forecasting.ridge import fit_ridge_model


ORIGIN_FOLD_COLUMN = "origin_fold"
ORIGIN_COUNT_COLUMN = "origin_count"
ORIGIN_CUTOFF_COLUMN = "origin_cutoff_utc"
TRAINING_OBSERVATION_COUNT_COLUMN = "training_observation_count"
EVALUATION_CONTRACT_VERSION_COLUMN = "evaluation_contract_version"
EVALUATION_CONTRACT_VERSION = "rolling-origin-v1"
PREDICTION_COLUMNS = [
    "run_id",
    "run_timestamp_utc",
    *GROUP_COLUMNS,
    FEATURE_TIMESTAMP_COLUMN,
    TIMESTAMP_COLUMN,
    REQUESTED_HORIZON_COLUMN,
    TARGET_TOLERANCE_COLUMN,
    "horizon_steps",
    "horizon_minutes",
    TARGET_DELAY_COLUMN,
    "split",
    "model_name",
    "current_demand_mw",
    "actual_demand_mw",
    "predicted_demand_mw",
    "absolute_error_mw",
    "squared_error_mw2",
    "trained_through_utc",
    "feature_contract_version",
    ORIGIN_FOLD_COLUMN,
    ORIGIN_COUNT_COLUMN,
    ORIGIN_CUTOFF_COLUMN,
    TRAINING_OBSERVATION_COUNT_COLUMN,
    EVALUATION_CONTRACT_VERSION_COLUMN,
]
METRIC_COLUMNS = [
    "run_id",
    "run_timestamp_utc",
    *GROUP_COLUMNS,
    REQUESTED_HORIZON_COLUMN,
    TARGET_TOLERANCE_COLUMN,
    "split",
    "model_name",
    "observation_count",
    "eligible_target_count",
    "matched_target_count",
    "target_coverage_pct",
    "horizon_steps_avg",
    "horizon_minutes_avg",
    "target_delay_minutes_avg",
    "target_delay_minutes_max",
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
    ORIGIN_FOLD_COLUMN,
    ORIGIN_COUNT_COLUMN,
    ORIGIN_CUTOFF_COLUMN,
    TRAINING_OBSERVATION_COUNT_COLUMN,
    EVALUATION_CONTRACT_VERSION_COLUMN,
]


@dataclass(frozen=True)
class RollingOriginFold:
    """One expanding-window validation or final-test origin."""

    origin_fold: int
    origin_count: int
    split: str
    training_candidates: pd.DataFrame
    evaluation: pd.DataFrame

    @property
    def origin_cutoff_utc(self) -> pd.Timestamp:
        if self.evaluation.empty:
            raise ForecastingContractError(
                "Rolling-origin evaluation window must contain at least one row."
            )
        return pd.Timestamp(self.evaluation[FEATURE_TIMESTAMP_COLUMN].min())


def _validate_origin_count(origin_count: int) -> None:
    if isinstance(origin_count, bool) or not isinstance(origin_count, int):
        raise ForecastingContractError("origin_count must be an integer of at least 2.")
    if origin_count < 2:
        raise ForecastingContractError("origin_count must be an integer of at least 2.")


def _balanced_partition_sizes(total: int, parts: int) -> list[int]:
    base, remainder = divmod(total, parts)
    return [base + (1 if index < remainder else 0) for index in range(parts)]


def build_rolling_origin_folds(
    group: pd.DataFrame,
    config: BacktestConfig,
    *,
    origin_count: int = 3,
) -> list[RollingOriginFold]:
    """Build expanding validation origins followed by one untouched test origin."""
    config.validate()
    _validate_origin_count(origin_count)
    if group.empty:
        raise ForecastingContractError("Forecast group has no supervised rows.")

    ordered = group.sort_values(FEATURE_TIMESTAMP_COLUMN).reset_index(drop=True)
    row_count = len(ordered)
    train_end = int(row_count * config.train_fraction)
    validation_end = int(
        row_count * (config.train_fraction + config.validation_fraction)
    )
    validation_count = validation_end - train_end
    test_count = row_count - validation_end
    validation_origin_count = origin_count - 1

    if train_end < config.min_train_rows:
        raise ForecastingContractError(
            f"Group has {train_end} initial training rows; minimum is "
            f"{config.min_train_rows}."
        )
    required_validation_rows = (
        validation_origin_count * config.min_validation_rows
    )
    if validation_count < required_validation_rows:
        raise ForecastingContractError(
            f"Group has {validation_count} validation rows; {origin_count} rolling "
            f"origins require at least {required_validation_rows}."
        )
    if test_count < config.min_test_rows:
        raise ForecastingContractError(
            f"Group has {test_count} test rows; minimum is {config.min_test_rows}."
        )

    folds: list[RollingOriginFold] = []
    cursor = train_end
    for fold_number, window_size in enumerate(
        _balanced_partition_sizes(validation_count, validation_origin_count),
        start=1,
    ):
        evaluation_end = cursor + window_size
        folds.append(
            RollingOriginFold(
                origin_fold=fold_number,
                origin_count=origin_count,
                split="validation",
                training_candidates=ordered.iloc[:cursor].copy(),
                evaluation=ordered.iloc[cursor:evaluation_end].copy(),
            )
        )
        cursor = evaluation_end

    folds.append(
        RollingOriginFold(
            origin_fold=origin_count,
            origin_count=origin_count,
            split="test",
            training_candidates=ordered.iloc[:validation_end].copy(),
            evaluation=ordered.iloc[validation_end:].copy(),
        )
    )

    cutoffs = [fold.origin_cutoff_utc for fold in folds]
    if not all(earlier < later for earlier, later in zip(cutoffs, cutoffs[1:])):
        raise ForecastingContractError(
            "Rolling-origin cutoffs must be strictly increasing."
        )
    return folds


def _prediction_rows(
    evaluation: pd.DataFrame,
    *,
    fold: RollingOriginFold,
    model_name: str,
    predicted: Sequence[float],
    trained_through: pd.Timestamp,
    training_observation_count: int,
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
                REQUESTED_HORIZON_COLUMN: int(
                    getattr(source_row, REQUESTED_HORIZON_COLUMN)
                ),
                TARGET_TOLERANCE_COLUMN: config.target_tolerance_minutes,
                "horizon_steps": int(source_row.horizon_steps),
                "horizon_minutes": float(source_row.horizon_minutes),
                TARGET_DELAY_COLUMN: float(getattr(source_row, TARGET_DELAY_COLUMN)),
                "split": fold.split,
                "model_name": model_name,
                "current_demand_mw": float(getattr(source_row, TARGET_COLUMN)),
                "actual_demand_mw": actual,
                "predicted_demand_mw": float(prediction),
                "absolute_error_mw": abs(error),
                "squared_error_mw2": error * error,
                "trained_through_utc": trained_through,
                "feature_contract_version": config.feature_contract_version,
                ORIGIN_FOLD_COLUMN: fold.origin_fold,
                ORIGIN_COUNT_COLUMN: fold.origin_count,
                ORIGIN_CUTOFF_COLUMN: fold.origin_cutoff_utc,
                TRAINING_OBSERVATION_COUNT_COLUMN: training_observation_count,
                EVALUATION_CONTRACT_VERSION_COLUMN: EVALUATION_CONTRACT_VERSION,
            }
        )
    return rows


def _metric_row(
    group: pd.DataFrame,
    supervised_group: pd.DataFrame,
) -> dict[str, object]:
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
    source_first = supervised_group.iloc[0]
    return {
        "run_id": first["run_id"],
        "run_timestamp_utc": first["run_timestamp_utc"],
        "source_area": first["source_area"],
        "resource_id": first["resource_id"],
        "city": first["city"],
        REQUESTED_HORIZON_COLUMN: int(first[REQUESTED_HORIZON_COLUMN]),
        TARGET_TOLERANCE_COLUMN: int(first[TARGET_TOLERANCE_COLUMN]),
        "split": first["split"],
        "model_name": first["model_name"],
        "observation_count": int(len(group)),
        "eligible_target_count": int(source_first["eligible_target_count"]),
        "matched_target_count": int(source_first["matched_target_count"]),
        "target_coverage_pct": float(source_first["target_coverage_pct"]),
        "horizon_steps_avg": float(group["horizon_steps"].mean()),
        "horizon_minutes_avg": float(group["horizon_minutes"].mean()),
        "target_delay_minutes_avg": float(group[TARGET_DELAY_COLUMN].mean()),
        "target_delay_minutes_max": float(group[TARGET_DELAY_COLUMN].max()),
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
        ORIGIN_FOLD_COLUMN: int(first[ORIGIN_FOLD_COLUMN]),
        ORIGIN_COUNT_COLUMN: int(first[ORIGIN_COUNT_COLUMN]),
        ORIGIN_CUTOFF_COLUMN: first[ORIGIN_CUTOFF_COLUMN],
        TRAINING_OBSERVATION_COUNT_COLUMN: int(
            first[TRAINING_OBSERVATION_COUNT_COLUMN]
        ),
        EVALUATION_CONTRACT_VERSION_COLUMN: EVALUATION_CONTRACT_VERSION,
    }


def _validate_origin_evidence(predictions: pd.DataFrame) -> None:
    identity = [
        *GROUP_COLUMNS,
        REQUESTED_HORIZON_COLUMN,
        "model_name",
    ]
    for _, model_group in predictions.groupby(identity, sort=True, dropna=False):
        first = model_group.iloc[0]
        origin_count = int(first[ORIGIN_COUNT_COLUMN])
        folds = sorted(model_group[ORIGIN_FOLD_COLUMN].astype(int).unique().tolist())
        if folds != list(range(1, origin_count + 1)):
            raise ForecastingContractError(
                "Rolling-origin evidence does not contain a complete fold sequence."
            )
        by_fold = (
            model_group.sort_values(ORIGIN_FOLD_COLUMN)
            .drop_duplicates(ORIGIN_FOLD_COLUMN)
            .reset_index(drop=True)
        )
        if set(by_fold.loc[by_fold[ORIGIN_FOLD_COLUMN] < origin_count, "split"]) != {
            "validation"
        }:
            raise ForecastingContractError(
                "Only the final rolling origin may use the test split."
            )
        if set(by_fold.loc[by_fold[ORIGIN_FOLD_COLUMN] == origin_count, "split"]) != {
            "test"
        }:
            raise ForecastingContractError(
                "The final rolling origin must be the untouched test split."
            )
        if not by_fold[ORIGIN_CUTOFF_COLUMN].is_monotonic_increasing:
            raise ForecastingContractError(
                "Rolling-origin cutoffs must be monotonically increasing."
            )
        if not by_fold[TRAINING_OBSERVATION_COUNT_COLUMN].is_monotonic_increasing:
            raise ForecastingContractError(
                "Available rolling-origin training history must not decrease."
            )


def run_rolling_origin_backtest(
    frame: pd.DataFrame,
    *,
    config: BacktestConfig | None = None,
    origin_count: int = 3,
    run_id: str | None = None,
    run_timestamp: datetime | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Evaluate repeated expanding cutoffs plus one final untouched test origin."""
    config = config or BacktestConfig()
    _validate_origin_count(origin_count)
    prepared = prepare_feature_frame(frame, config)
    supervised = build_supervised_frame(prepared, config)
    run_id = run_id or str(uuid4())
    run_timestamp = run_timestamp or datetime.now(timezone.utc)
    if run_timestamp.tzinfo is None:
        raise ForecastingContractError("run_timestamp must be timezone-aware.")
    run_timestamp = run_timestamp.astimezone(timezone.utc)

    all_predictions: list[dict[str, object]] = []
    supervised_lookup: dict[tuple[object, ...], pd.DataFrame] = {}
    grouping_columns = [*GROUP_COLUMNS, REQUESTED_HORIZON_COLUMN]
    for key, group in supervised.groupby(grouping_columns, sort=True, dropna=False):
        group = group.sort_values(FEATURE_TIMESTAMP_COLUMN).reset_index(drop=True)
        normalized_key = key if isinstance(key, tuple) else (key,)
        supervised_lookup[normalized_key] = group
        for fold in build_rolling_origin_folds(
            group,
            config,
            origin_count=origin_count,
        ):
            purged = purge_overlapping_training_rows(
                fold.training_candidates,
                fold.evaluation,
                config,
            )
            trained_through = purged[TARGET_TIMESTAMP_COLUMN].max()
            ridge_predictions = fit_ridge_model(
                purged,
                feature_columns=config.feature_columns,
                target_column=SUPERVISED_TARGET_COLUMN,
                alpha=config.ridge_alpha,
            ).predict(
                fold.evaluation.loc[:, config.feature_columns].itertuples(
                    index=False, name=None
                )
            )
            all_predictions.extend(
                _prediction_rows(
                    fold.evaluation,
                    fold=fold,
                    model_name="persistence_current_value",
                    predicted=fold.evaluation[TARGET_COLUMN].astype(float).tolist(),
                    trained_through=trained_through,
                    training_observation_count=len(purged),
                    run_id=run_id,
                    run_timestamp=run_timestamp,
                    config=config,
                )
            )
            all_predictions.extend(
                _prediction_rows(
                    fold.evaluation,
                    fold=fold,
                    model_name="ridge_weather_lag",
                    predicted=ridge_predictions,
                    trained_through=trained_through,
                    training_observation_count=len(purged),
                    run_id=run_id,
                    run_timestamp=run_timestamp,
                    config=config,
                )
            )

    predictions = pd.DataFrame(
        all_predictions,
        columns=PREDICTION_COLUMNS,
    )
    if predictions.empty:
        raise ForecastingContractError(
            "No groups produced rolling-origin forecast predictions."
        )
    if not (
        predictions["trained_through_utc"] < predictions[ORIGIN_CUTOFF_COLUMN]
    ).all():
        raise ForecastingContractError(
            "A rolling origin used labels unavailable at its cutoff."
        )
    if not (
        predictions[ORIGIN_CUTOFF_COLUMN]
        <= predictions[FEATURE_TIMESTAMP_COLUMN]
    ).all():
        raise ForecastingContractError(
            "A prediction occurred before its rolling-origin cutoff."
        )
    if not (
        predictions[FEATURE_TIMESTAMP_COLUMN]
        < predictions["event_timestamp_utc"]
    ).all():
        raise ForecastingContractError(
            "A forecast target did not occur after its feature timestamp."
        )
    _validate_origin_evidence(predictions)

    metric_rows: list[dict[str, object]] = []
    metric_grouping = [
        *GROUP_COLUMNS,
        REQUESTED_HORIZON_COLUMN,
        ORIGIN_FOLD_COLUMN,
        "split",
        "model_name",
    ]
    for key, prediction_group in predictions.groupby(
        metric_grouping,
        sort=True,
        dropna=False,
    ):
        lookup_key = tuple(key[: len(GROUP_COLUMNS) + 1])
        metric_rows.append(
            _metric_row(
                prediction_group,
                supervised_lookup[lookup_key],
            )
        )

    metrics = pd.DataFrame(metric_rows, columns=METRIC_COLUMNS)
    return predictions.reset_index(drop=True), metrics.reset_index(drop=True)

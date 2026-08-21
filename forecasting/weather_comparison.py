from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from math import isfinite, sqrt
from uuid import uuid4

import pandas as pd

from forecasting.contracts import (
    FEATURE_TIMESTAMP_COLUMN,
    GROUP_COLUMNS,
    REQUESTED_HORIZON_COLUMN,
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
from forecasting.forecast_weather import (
    ForecastWeatherConfig,
    ForecastWeatherContractError,
    attach_target_forecast_weather,
)
from forecasting.ridge import fit_ridge_model
from forecasting.rolling_origin import (
    EVALUATION_CONTRACT_VERSION,
    EVALUATION_CONTRACT_VERSION_COLUMN,
    ORIGIN_COUNT_COLUMN,
    ORIGIN_CUTOFF_COLUMN,
    ORIGIN_FOLD_COLUMN,
    TRAINING_OBSERVATION_COUNT_COLUMN,
    build_rolling_origin_folds,
)


OBSERVED_MODEL = "ridge_weather_lag"
TARGET_MODEL = "ridge_target_weather"
MODEL_MODES = {
    OBSERVED_MODEL: "observed_at_feature",
    TARGET_MODEL: "target_forecast",
}
COMPARISON_VERSION = "weather-model-comparison-v1"
COMPARISON_VERSION_COLUMN = "weather_comparison_contract_version"
FIXED_HOLDOUT_VERSION = "fixed-holdout-v1"
WEATHER_FEATURE_MAP = {
    "temperature": "target_weather_temperature_c",
    "humidity": "target_weather_humidity_pct",
    "weather_age_minutes": "target_weather_availability_age_minutes",
}
FORECAST_EVIDENCE_COLUMNS = (
    "forecast_weather_contract_version",
    "forecast_weather_eligible_count",
    "forecast_weather_matched_count",
    "forecast_weather_coverage_pct",
    "minimum_forecast_weather_coverage_pct",
    "target_weather_forecast_issued_at_utc",
    "target_weather_forecast_ingested_at_utc",
    "target_weather_forecast_valid_at_utc",
    "target_weather_temperature_c",
    "target_weather_humidity_pct",
    "target_weather_provider",
    "target_weather_model",
    "target_weather_valid_delta_minutes",
    "target_weather_feature_lead_minutes",
    "target_weather_availability_age_minutes",
)


@dataclass(frozen=True)
class ComparisonWindow:
    split: str
    training: pd.DataFrame
    evaluation: pd.DataFrame
    evaluation_contract_version: str
    origin_fold: int | None = None
    origin_count: int | None = None
    origin_cutoff_utc: pd.Timestamp | None = None


def _feature_columns(
    config: BacktestConfig,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    observed = tuple(config.feature_columns)
    missing = sorted(set(WEATHER_FEATURE_MAP) - set(observed))
    if missing:
        raise ForecastingContractError(
            "Weather comparison requires observed features: "
            + ", ".join(missing)
            + "."
        )
    target = tuple(WEATHER_FEATURE_MAP.get(column, column) for column in observed)
    if len(set(target)) != len(target):
        raise ForecastingContractError(
            "Target-weather feature mapping produced duplicate columns."
        )
    return observed, target


def _require_finite(frame: pd.DataFrame, columns: tuple[str, ...]) -> pd.DataFrame:
    prepared = frame.copy()
    missing = sorted(set(columns) - set(prepared.columns))
    if missing:
        raise ForecastingContractError(
            "Weather-comparison frame is missing features: "
            + ", ".join(missing)
            + "."
        )
    for column in columns:
        prepared[column] = pd.to_numeric(prepared[column], errors="coerce")
    values = prepared[list(columns)]
    if values.isna().any().any() or not values.apply(
        lambda series: series.map(lambda value: isfinite(float(value)))
    ).all().all():
        raise ForecastingContractError(
            "Weather-comparison features must contain finite non-null values."
        )
    return prepared


def prepare_weather_model_comparison(
    feature_frame: pd.DataFrame,
    forecast_weather: pd.DataFrame,
    *,
    backtest_config: BacktestConfig,
    forecast_config: ForecastWeatherConfig,
) -> tuple[pd.DataFrame, tuple[str, ...], tuple[str, ...]]:
    """Build one paired cohort used by both ridge models."""
    observed, target = _feature_columns(backtest_config)
    supervised = build_supervised_frame(
        prepare_feature_frame(feature_frame, backtest_config),
        backtest_config,
    )
    try:
        paired = attach_target_forecast_weather(
            supervised,
            forecast_weather,
            config=forecast_config,
        )
    except ForecastWeatherContractError as exc:
        raise ForecastingContractError(str(exc)) from exc
    paired = _require_finite(
        paired,
        tuple(dict.fromkeys([*observed, *target])),
    )
    if paired.empty:
        raise ForecastingContractError(
            "No paired target-weather comparison rows are available."
        )
    return paired, observed, target


def _comparison_windows(
    group: pd.DataFrame,
    config: BacktestConfig,
    *,
    evaluation_mode: str,
    origin_count: int,
) -> list[ComparisonWindow]:
    if evaluation_mode == "holdout":
        train, validation, test = split_group(group, config)
        return [
            ComparisonWindow(
                "validation",
                train,
                validation,
                FIXED_HOLDOUT_VERSION,
            ),
            ComparisonWindow(
                "test",
                pd.concat([train, validation], ignore_index=True),
                test,
                FIXED_HOLDOUT_VERSION,
            ),
        ]
    return [
        ComparisonWindow(
            fold.split,
            fold.training_candidates,
            fold.evaluation,
            EVALUATION_CONTRACT_VERSION,
            fold.origin_fold,
            fold.origin_count,
            fold.origin_cutoff_utc,
        )
        for fold in build_rolling_origin_folds(
            group,
            config,
            origin_count=origin_count,
        )
    ]


def _prediction_rows(
    evaluation: pd.DataFrame,
    *,
    window: ComparisonWindow,
    model_name: str,
    predictions: list[float],
    trained_through: pd.Timestamp,
    training_rows: int,
    run_id: str,
    run_timestamp: datetime,
    backtest_config: BacktestConfig,
    forecast_config: ForecastWeatherConfig,
) -> list[dict[str, object]]:
    if len(predictions) != len(evaluation):
        raise ForecastingContractError(
            "Prediction count does not match the comparison window."
        )
    rows: list[dict[str, object]] = []
    for source, prediction in zip(evaluation.itertuples(index=False), predictions):
        actual = float(getattr(source, SUPERVISED_TARGET_COLUMN))
        error = float(prediction) - actual
        row = {
            "run_id": run_id,
            "run_timestamp_utc": run_timestamp,
            "source_area": source.source_area,
            "resource_id": source.resource_id,
            "city": source.city,
            FEATURE_TIMESTAMP_COLUMN: getattr(source, FEATURE_TIMESTAMP_COLUMN),
            TIMESTAMP_COLUMN: getattr(source, TARGET_TIMESTAMP_COLUMN),
            REQUESTED_HORIZON_COLUMN: int(
                getattr(source, REQUESTED_HORIZON_COLUMN)
            ),
            "split": window.split,
            "model_name": model_name,
            "weather_feature_mode": MODEL_MODES[model_name],
            "actual_demand_mw": actual,
            "predicted_demand_mw": float(prediction),
            "absolute_error_mw": abs(error),
            "squared_error_mw2": error * error,
            "trained_through_utc": trained_through,
            "feature_contract_version": backtest_config.feature_contract_version,
            COMPARISON_VERSION_COLUMN: COMPARISON_VERSION,
            "forecast_weather_valid_time_tolerance_minutes": (
                forecast_config.valid_time_tolerance_minutes
            ),
            "forecast_weather_max_availability_age_minutes": (
                forecast_config.max_availability_age_minutes
            ),
            ORIGIN_FOLD_COLUMN: window.origin_fold,
            ORIGIN_COUNT_COLUMN: window.origin_count,
            ORIGIN_CUTOFF_COLUMN: window.origin_cutoff_utc,
            TRAINING_OBSERVATION_COUNT_COLUMN: training_rows,
            EVALUATION_CONTRACT_VERSION_COLUMN: (
                window.evaluation_contract_version
            ),
        }
        row.update(
            {
                column: getattr(source, column)
                for column in FORECAST_EVIDENCE_COLUMNS
            }
        )
        rows.append(row)
    return rows


def _evaluate(
    window: ComparisonWindow,
    *,
    observed_features: tuple[str, ...],
    target_features: tuple[str, ...],
    backtest_config: BacktestConfig,
    forecast_config: ForecastWeatherConfig,
    run_id: str,
    run_timestamp: datetime,
) -> list[dict[str, object]]:
    training = purge_overlapping_training_rows(
        window.training,
        window.evaluation,
        backtest_config,
    )
    trained_through = training[TARGET_TIMESTAMP_COLUMN].max()
    feature_sets = {
        OBSERVED_MODEL: observed_features,
        TARGET_MODEL: target_features,
    }
    rows: list[dict[str, object]] = []
    for model_name, feature_columns in feature_sets.items():
        predicted = fit_ridge_model(
            training,
            feature_columns=feature_columns,
            target_column=SUPERVISED_TARGET_COLUMN,
            alpha=backtest_config.ridge_alpha,
        ).predict(
            window.evaluation.loc[:, feature_columns].itertuples(
                index=False, name=None
            )
        )
        rows.extend(
            _prediction_rows(
                window.evaluation,
                window=window,
                model_name=model_name,
                predictions=list(predicted),
                trained_through=trained_through,
                training_rows=len(training),
                run_id=run_id,
                run_timestamp=run_timestamp,
                backtest_config=backtest_config,
                forecast_config=forecast_config,
            )
        )
    return rows


def _validate(
    predictions: pd.DataFrame,
    *,
    evaluation_mode: str,
    forecast_config: ForecastWeatherConfig,
) -> None:
    if predictions.empty:
        raise ForecastingContractError(
            "No weather-model comparison predictions were produced."
        )
    feature = predictions[FEATURE_TIMESTAMP_COLUMN]
    target = predictions[TIMESTAMP_COLUMN]
    issued = predictions["target_weather_forecast_issued_at_utc"]
    ingested = predictions["target_weather_forecast_ingested_at_utc"]
    valid = predictions["target_weather_forecast_valid_at_utc"]
    if not (
        (predictions["trained_through_utc"] < feature)
        & (feature < target)
        & (issued <= ingested)
        & (ingested <= feature)
        & (valid > feature)
    ).all():
        raise ForecastingContractError(
            "Weather-model comparison evidence violated a causal boundary."
        )
    if (
        predictions["target_weather_valid_delta_minutes"]
        > forecast_config.valid_time_tolerance_minutes
    ).any():
        raise ForecastingContractError(
            "Target-weather valid times exceeded comparison tolerance."
        )
    if (
        predictions["target_weather_availability_age_minutes"]
        > forecast_config.max_availability_age_minutes
    ).any():
        raise ForecastingContractError(
            "Target-weather evidence exceeded maximum availability age."
        )
    if set(predictions[COMPARISON_VERSION_COLUMN]) != {COMPARISON_VERSION}:
        raise ForecastingContractError(
            "Weather-model comparison contract version is invalid."
        )
    if not (
        predictions["weather_feature_mode"]
        == predictions["model_name"].map(MODEL_MODES)
    ).all():
        raise ForecastingContractError(
            "Weather feature modes do not match model identities."
        )

    cohort_columns = [
        *GROUP_COLUMNS,
        REQUESTED_HORIZON_COLUMN,
        "split",
        ORIGIN_FOLD_COLUMN,
    ]
    for _, cohort in predictions.groupby(
        cohort_columns, sort=True, dropna=False
    ):
        if set(cohort["model_name"]) != set(MODEL_MODES):
            raise ForecastingContractError(
                "Each comparison cohort must contain both ridge models."
            )
        timestamps = [
            frozenset(group[FEATURE_TIMESTAMP_COLUMN].tolist())
            for _, group in cohort.groupby("model_name")
        ]
        if len(set(timestamps)) != 1:
            raise ForecastingContractError(
                "Comparison models evaluated different target rows."
            )
        if cohort["trained_through_utc"].nunique() != 1:
            raise ForecastingContractError(
                "Comparison models used different training boundaries."
            )

    if evaluation_mode == "holdout":
        if predictions[
            [ORIGIN_FOLD_COLUMN, ORIGIN_COUNT_COLUMN, ORIGIN_CUTOFF_COLUMN]
        ].notna().any().any():
            raise ForecastingContractError(
                "Holdout comparison rows must not contain origin evidence."
            )
        return

    required = [ORIGIN_FOLD_COLUMN, ORIGIN_COUNT_COLUMN, ORIGIN_CUTOFF_COLUMN]
    if predictions[required].isna().any().any():
        raise ForecastingContractError(
            "Rolling comparison rows require complete origin evidence."
        )
    for _, group in predictions.groupby(
        [*GROUP_COLUMNS, REQUESTED_HORIZON_COLUMN, "model_name"],
        sort=True,
        dropna=False,
    ):
        origin_count = int(group[ORIGIN_COUNT_COLUMN].iloc[0])
        folds = sorted(group[ORIGIN_FOLD_COLUMN].astype(int).unique().tolist())
        if folds != list(range(1, origin_count + 1)):
            raise ForecastingContractError(
                "Rolling comparison has an incomplete fold sequence."
            )


def _metric_row(group: pd.DataFrame) -> dict[str, object]:
    errors = group["predicted_demand_mw"] - group["actual_demand_mw"]
    first = group.iloc[0]
    return {
        "run_id": first["run_id"],
        "run_timestamp_utc": first["run_timestamp_utc"],
        "source_area": first["source_area"],
        "resource_id": first["resource_id"],
        "city": first["city"],
        REQUESTED_HORIZON_COLUMN: int(first[REQUESTED_HORIZON_COLUMN]),
        "split": first["split"],
        "model_name": first["model_name"],
        "weather_feature_mode": first["weather_feature_mode"],
        "observation_count": int(len(group)),
        "forecast_weather_eligible_count": int(
            first["forecast_weather_eligible_count"]
        ),
        "forecast_weather_matched_count": int(
            first["forecast_weather_matched_count"]
        ),
        "forecast_weather_coverage_pct": float(
            first["forecast_weather_coverage_pct"]
        ),
        "minimum_forecast_weather_coverage_pct": float(
            first["minimum_forecast_weather_coverage_pct"]
        ),
        "target_weather_valid_delta_minutes_avg": float(
            group["target_weather_valid_delta_minutes"].mean()
        ),
        "target_weather_availability_age_minutes_avg": float(
            group["target_weather_availability_age_minutes"].mean()
        ),
        "mae_mw": float(errors.abs().mean()),
        "rmse_mw": float(sqrt((errors * errors).mean())),
        "bias_mw": float(errors.mean()),
        "trained_through_utc": first["trained_through_utc"],
        "evaluation_feature_start_utc": group[
            FEATURE_TIMESTAMP_COLUMN
        ].min(),
        "evaluation_feature_end_utc": group[
            FEATURE_TIMESTAMP_COLUMN
        ].max(),
        "evaluation_start_utc": group[TIMESTAMP_COLUMN].min(),
        "evaluation_end_utc": group[TIMESTAMP_COLUMN].max(),
        "feature_contract_version": first["feature_contract_version"],
        "forecast_weather_contract_version": first[
            "forecast_weather_contract_version"
        ],
        COMPARISON_VERSION_COLUMN: first[COMPARISON_VERSION_COLUMN],
        ORIGIN_FOLD_COLUMN: first[ORIGIN_FOLD_COLUMN],
        ORIGIN_COUNT_COLUMN: first[ORIGIN_COUNT_COLUMN],
        ORIGIN_CUTOFF_COLUMN: first[ORIGIN_CUTOFF_COLUMN],
        TRAINING_OBSERVATION_COUNT_COLUMN: int(
            first[TRAINING_OBSERVATION_COUNT_COLUMN]
        ),
        EVALUATION_CONTRACT_VERSION_COLUMN: first[
            EVALUATION_CONTRACT_VERSION_COLUMN
        ],
    }


def run_weather_model_comparison(
    feature_frame: pd.DataFrame,
    forecast_weather: pd.DataFrame,
    *,
    backtest_config: BacktestConfig | None = None,
    forecast_config: ForecastWeatherConfig | None = None,
    evaluation_mode: str = "holdout",
    origin_count: int = 3,
    run_id: str | None = None,
    run_timestamp: datetime | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Compare observed-at-feature and target-valid weather on paired rows."""
    if evaluation_mode not in {"holdout", "rolling-origin"}:
        raise ForecastingContractError(
            "evaluation_mode must be 'holdout' or 'rolling-origin'."
        )
    backtest_config = backtest_config or BacktestConfig()
    forecast_config = forecast_config or ForecastWeatherConfig()
    paired, observed_features, target_features = (
        prepare_weather_model_comparison(
            feature_frame,
            forecast_weather,
            backtest_config=backtest_config,
            forecast_config=forecast_config,
        )
    )
    run_id = run_id or str(uuid4())
    run_timestamp = run_timestamp or datetime.now(timezone.utc)
    if run_timestamp.tzinfo is None:
        raise ForecastingContractError(
            "run_timestamp must be timezone-aware."
        )
    run_timestamp = run_timestamp.astimezone(timezone.utc)

    rows: list[dict[str, object]] = []
    for _, group in paired.groupby(
        [*GROUP_COLUMNS, REQUESTED_HORIZON_COLUMN],
        sort=True,
        dropna=False,
    ):
        ordered = group.sort_values(
            FEATURE_TIMESTAMP_COLUMN
        ).reset_index(drop=True)
        for window in _comparison_windows(
            ordered,
            backtest_config,
            evaluation_mode=evaluation_mode,
            origin_count=origin_count,
        ):
            rows.extend(
                _evaluate(
                    window,
                    observed_features=observed_features,
                    target_features=target_features,
                    backtest_config=backtest_config,
                    forecast_config=forecast_config,
                    run_id=run_id,
                    run_timestamp=run_timestamp,
                )
            )

    predictions = pd.DataFrame(rows)
    _validate(
        predictions,
        evaluation_mode=evaluation_mode,
        forecast_config=forecast_config,
    )
    metrics = pd.DataFrame(
        [
            _metric_row(group)
            for _, group in predictions.groupby(
                [
                    *GROUP_COLUMNS,
                    REQUESTED_HORIZON_COLUMN,
                    "split",
                    "model_name",
                    ORIGIN_FOLD_COLUMN,
                ],
                sort=True,
                dropna=False,
            )
        ]
    )
    return predictions.reset_index(drop=True), metrics.reset_index(drop=True)

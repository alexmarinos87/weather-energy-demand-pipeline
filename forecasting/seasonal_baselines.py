from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from math import cos, pi, sin, sqrt
from typing import Any, Sequence
from uuid import uuid4

import pandas as pd

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
    split_group,
)
from forecasting.demo import build_demo_feature_frame
from forecasting.ridge import fit_ridge_model
from forecasting.rolling_origin import build_rolling_origin_folds


SEASONAL_BASELINE_CONTRACT_VERSION = "elapsed-seasonal-v1"
HOLDOUT_EVALUATION_CONTRACT_VERSION = "fixed-holdout-v1"
ROLLING_EVALUATION_CONTRACT_VERSION = "rolling-origin-v1"
SEASONAL_REFERENCES = {
    "previous_day": 24 * 60,
    "previous_week": 7 * 24 * 60,
}
SEASONAL_MODELS = {
    "seasonal_previous_day": "previous_day",
    "seasonal_previous_week": "previous_week",
}
ALL_MODELS = (
    "persistence_current_value",
    "seasonal_previous_day",
    "seasonal_previous_week",
    "ridge_weather_lag",
)


@dataclass(frozen=True)
class SeasonalBaselineConfig:
    reference_tolerance_minutes: int = 15
    min_reference_coverage: float = 0.90
    contract_version: str = SEASONAL_BASELINE_CONTRACT_VERSION

    def validate(self) -> None:
        if (
            isinstance(self.reference_tolerance_minutes, bool)
            or not isinstance(self.reference_tolerance_minutes, int)
            or self.reference_tolerance_minutes < 0
        ):
            raise ForecastingContractError(
                "reference_tolerance_minutes must be a non-negative integer."
            )
        if not 0 < self.min_reference_coverage <= 1:
            raise ForecastingContractError(
                "min_reference_coverage must be greater than 0 and at most 1."
            )
        if not isinstance(self.contract_version, str) or not self.contract_version.strip():
            raise ForecastingContractError("contract_version must be non-empty.")


def _group_identity(group: pd.DataFrame) -> str:
    first = group.iloc[0]
    return "/".join(str(first[column]) for column in GROUP_COLUMNS)


def _nearest_reference(
    source: pd.DataFrame,
    *,
    ideal_timestamp: pd.Timestamp,
    feature_timestamp: pd.Timestamp,
    tolerance: pd.Timedelta,
) -> pd.Series | None:
    timestamps = pd.DatetimeIndex(source[TIMESTAMP_COLUMN])
    right = int(timestamps.searchsorted(ideal_timestamp, side="left"))
    positions = {right - 1, right}
    candidates: list[tuple[pd.Timedelta, bool, int, pd.Series]] = []
    for position in positions:
        if position < 0 or position >= len(source):
            continue
        row = source.iloc[position]
        timestamp = pd.Timestamp(row[TIMESTAMP_COLUMN])
        difference = timestamp - ideal_timestamp
        if abs(difference) > tolerance or timestamp > feature_timestamp:
            continue
        candidates.append(
            (
                abs(difference),
                timestamp > ideal_timestamp,
                -timestamp.value,
                row,
            )
        )
    if not candidates:
        return None
    candidates.sort(key=lambda item: (item[0], item[1], item[2]))
    return candidates[0][3]


def attach_seasonal_references(
    prepared: pd.DataFrame,
    supervised: pd.DataFrame,
    *,
    config: SeasonalBaselineConfig | None = None,
) -> pd.DataFrame:
    """Attach previous-day/week demand by elapsed UTC time, never row offset."""
    config = config or SeasonalBaselineConfig()
    config.validate()
    tolerance = pd.Timedelta(minutes=config.reference_tolerance_minutes)
    source_groups = {
        key if isinstance(key, tuple) else (key,): group.sort_values(
            TIMESTAMP_COLUMN
        ).reset_index(drop=True)
        for key, group in prepared.groupby(GROUP_COLUMNS, sort=True, dropna=False)
    }
    output_groups: list[pd.DataFrame] = []
    grouping = [*GROUP_COLUMNS, REQUESTED_HORIZON_COLUMN]
    for key, demand_group in supervised.groupby(grouping, sort=True, dropna=False):
        normalized_key = tuple(key[: len(GROUP_COLUMNS)])
        source = source_groups.get(normalized_key)
        if source is None or source.empty:
            raise ForecastingContractError(
                f"Missing source history for seasonal group {_group_identity(demand_group)}."
            )
        earliest = pd.Timestamp(source[TIMESTAMP_COLUMN].min())
        latest = pd.Timestamp(source[TIMESTAMP_COLUMN].max())
        enriched = demand_group.copy()
        for reference_name, reference_minutes in SEASONAL_REFERENCES.items():
            eligible_count = 0
            matched_count = 0
            matches: dict[int, dict[str, Any]] = {}
            reference_delta = pd.Timedelta(minutes=reference_minutes)
            for row_index, row in demand_group.iterrows():
                target_timestamp = pd.Timestamp(row[TARGET_TIMESTAMP_COLUMN])
                feature_timestamp = pd.Timestamp(row[FEATURE_TIMESTAMP_COLUMN])
                ideal_timestamp = target_timestamp - reference_delta
                if ideal_timestamp < earliest or ideal_timestamp > latest:
                    continue
                eligible_count += 1
                match = _nearest_reference(
                    source,
                    ideal_timestamp=ideal_timestamp,
                    feature_timestamp=feature_timestamp,
                    tolerance=tolerance,
                )
                if match is None:
                    continue
                source_timestamp = pd.Timestamp(match[TIMESTAMP_COLUMN])
                if source_timestamp > feature_timestamp:
                    raise ForecastingContractError(
                        "A seasonal reference was not available at feature time."
                    )
                offset = (
                    source_timestamp - ideal_timestamp
                ).total_seconds() / 60.0
                matches[row_index] = {
                    f"{reference_name}_ideal_timestamp_utc": ideal_timestamp,
                    f"{reference_name}_source_timestamp_utc": source_timestamp,
                    f"{reference_name}_demand_mw": float(match[TARGET_COLUMN]),
                    f"{reference_name}_offset_minutes": float(offset),
                    f"{reference_name}_absolute_offset_minutes": abs(float(offset)),
                    f"{reference_name}_source_age_minutes": (
                        feature_timestamp - source_timestamp
                    ).total_seconds()
                    / 60.0,
                }
                matched_count += 1
            if eligible_count == 0:
                raise ForecastingContractError(
                    f"Group {_group_identity(demand_group)} has no eligible "
                    f"{reference_name.replace('_', '-')} references."
                )
            coverage = matched_count / eligible_count
            if coverage < config.min_reference_coverage:
                raise ForecastingContractError(
                    f"Group {_group_identity(demand_group)} matched "
                    f"{matched_count}/{eligible_count} eligible "
                    f"{reference_name.replace('_', '-')} references "
                    f"({coverage:.1%}); minimum coverage is "
                    f"{config.min_reference_coverage:.1%}."
                )
            for column in (
                f"{reference_name}_ideal_timestamp_utc",
                f"{reference_name}_source_timestamp_utc",
                f"{reference_name}_demand_mw",
                f"{reference_name}_offset_minutes",
                f"{reference_name}_absolute_offset_minutes",
                f"{reference_name}_source_age_minutes",
            ):
                enriched[column] = pd.Series(
                    {index: values[column] for index, values in matches.items()}
                )
            enriched[f"{reference_name}_eligible_count"] = eligible_count
            enriched[f"{reference_name}_matched_count"] = matched_count
            enriched[f"{reference_name}_coverage_pct"] = coverage * 100.0
        complete_columns = [
            f"{name}_source_timestamp_utc" for name in SEASONAL_REFERENCES
        ]
        enriched = enriched.dropna(subset=complete_columns)
        if enriched.empty:
            raise ForecastingContractError(
                f"Group {_group_identity(demand_group)} has no rows with both "
                "previous-day and previous-week references."
            )
        output_groups.append(enriched)
    if not output_groups:
        raise ForecastingContractError("No seasonal comparison groups are available.")
    cohort = pd.concat(output_groups, ignore_index=True)
    for reference_name in SEASONAL_REFERENCES:
        if not (
            cohort[f"{reference_name}_source_timestamp_utc"]
            <= cohort[FEATURE_TIMESTAMP_COLUMN]
        ).all():
            raise ForecastingContractError(
                "A seasonal source timestamp occurred after feature time."
            )
        if not (
            cohort[f"{reference_name}_absolute_offset_minutes"]
            <= config.reference_tolerance_minutes
        ).all():
            raise ForecastingContractError(
                "A seasonal source match exceeded the configured tolerance."
            )
    return cohort.sort_values(
        [*GROUP_COLUMNS, REQUESTED_HORIZON_COLUMN, FEATURE_TIMESTAMP_COLUMN]
    ).reset_index(drop=True)


def build_seasonal_demo_feature_frame(
    *,
    periods: int = 12 * 288,
    start: str = "2026-01-01T00:00:00Z",
    source_areas: tuple[str, ...] | None = None,
) -> pd.DataFrame:
    """Build deterministic daily/weekly demand structure for seasonal evidence."""
    if periods < 9 * 288:
        raise ForecastingContractError(
            "Seasonal demo data requires at least nine days of five-minute rows."
        )
    frame = build_demo_feature_frame(
        periods=periods,
        start=start,
        source_areas=source_areas,
    )
    area_bases = {
        area: 1100.0 + 175.0 * index
        for index, area in enumerate(sorted(frame["source_area"].unique()))
    }
    local = pd.DatetimeIndex(frame["event_timestamp_local"])
    minute_of_day = local.hour * 60 + local.minute
    radians = minute_of_day.map(lambda value: 2.0 * pi * value / 1440.0)
    frame["temperature"] = [
        10.0 + 7.0 * sin(value - pi / 2.0) for value in radians
    ]
    frame["humidity"] = [
        62.0 - 12.0 * sin(value - pi / 2.0) for value in radians
    ]
    frame[TARGET_COLUMN] = [
        area_bases[area]
        + 135.0 * sin(angle)
        + 45.0 * cos(2.0 * angle)
        + 70.0 * int(weekend)
        + 12.0 * int(day_of_week)
        - 8.0 * temperature
        for area, angle, weekend, day_of_week, temperature in zip(
            frame["source_area"],
            radians,
            frame["is_weekend_local"],
            frame["day_of_week_local"],
            frame["temperature"],
        )
    ]
    frame["demand_lag_1"] = frame.groupby(GROUP_COLUMNS, sort=False)[
        TARGET_COLUMN
    ].shift(1)
    frame["demand_rolling_mean_12"] = (
        frame.groupby(GROUP_COLUMNS, sort=False)[TARGET_COLUMN]
        .transform(lambda series: series.shift(1).rolling(12, min_periods=1).mean())
    )
    return frame.dropna(subset=["demand_lag_1"]).reset_index(drop=True)


def _model_prediction(
    evaluation: pd.DataFrame,
    model_name: str,
    ridge_predictions: Sequence[float],
) -> list[float]:
    if model_name == "persistence_current_value":
        return evaluation[TARGET_COLUMN].astype(float).tolist()
    if model_name == "seasonal_previous_day":
        return evaluation["previous_day_demand_mw"].astype(float).tolist()
    if model_name == "seasonal_previous_week":
        return evaluation["previous_week_demand_mw"].astype(float).tolist()
    if model_name == "ridge_weather_lag":
        return [float(value) for value in ridge_predictions]
    raise ForecastingContractError(f"Unsupported seasonal model {model_name!r}.")


def _prediction_rows(
    evaluation: pd.DataFrame,
    *,
    model_name: str,
    predicted: Sequence[float],
    split: str,
    trained_through: pd.Timestamp,
    training_observation_count: int,
    run_id: str,
    run_timestamp: datetime,
    backtest_config: BacktestConfig,
    seasonal_config: SeasonalBaselineConfig,
    evaluation_contract_version: str,
    origin_fold: int | None = None,
    origin_count: int | None = None,
    origin_cutoff: pd.Timestamp | None = None,
) -> list[dict[str, Any]]:
    predicted_values = list(predicted)
    if len(predicted_values) != len(evaluation):
        raise ForecastingContractError(
            "Prediction count does not match the seasonal evaluation row count."
        )
    reference_name = SEASONAL_MODELS.get(model_name)
    rows: list[dict[str, Any]] = []
    for source_row, prediction in zip(
        evaluation.itertuples(index=False), predicted_values
    ):
        actual = float(getattr(source_row, SUPERVISED_TARGET_COLUMN))
        error = float(prediction) - actual
        reference_period = (
            SEASONAL_REFERENCES[reference_name] if reference_name else None
        )
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
                TARGET_TOLERANCE_COLUMN: backtest_config.target_tolerance_minutes,
                "horizon_steps": int(source_row.horizon_steps),
                "horizon_minutes": float(source_row.horizon_minutes),
                TARGET_DELAY_COLUMN: float(getattr(source_row, TARGET_DELAY_COLUMN)),
                "split": split,
                "model_name": model_name,
                "model_fit_required": model_name == "ridge_weather_lag",
                "current_demand_mw": float(getattr(source_row, TARGET_COLUMN)),
                "actual_demand_mw": actual,
                "predicted_demand_mw": float(prediction),
                "absolute_error_mw": abs(error),
                "squared_error_mw2": error * error,
                "trained_through_utc": trained_through,
                "training_observation_count": training_observation_count,
                "feature_contract_version": backtest_config.feature_contract_version,
                "seasonal_reference_tolerance_minutes": (
                    seasonal_config.reference_tolerance_minutes
                ),
                "previous_day_eligible_count": int(
                    source_row.previous_day_eligible_count
                ),
                "previous_day_matched_count": int(
                    source_row.previous_day_matched_count
                ),
                "previous_day_coverage_pct": float(
                    source_row.previous_day_coverage_pct
                ),
                "previous_week_eligible_count": int(
                    source_row.previous_week_eligible_count
                ),
                "previous_week_matched_count": int(
                    source_row.previous_week_matched_count
                ),
                "previous_week_coverage_pct": float(
                    source_row.previous_week_coverage_pct
                ),
                "seasonal_reference_period_minutes": reference_period,
                "seasonal_reference_ideal_timestamp_utc": (
                    getattr(source_row, f"{reference_name}_ideal_timestamp_utc")
                    if reference_name
                    else None
                ),
                "seasonal_reference_timestamp_utc": (
                    getattr(source_row, f"{reference_name}_source_timestamp_utc")
                    if reference_name
                    else None
                ),
                "seasonal_reference_demand_mw": (
                    float(getattr(source_row, f"{reference_name}_demand_mw"))
                    if reference_name
                    else None
                ),
                "seasonal_reference_offset_minutes": (
                    float(getattr(source_row, f"{reference_name}_offset_minutes"))
                    if reference_name
                    else None
                ),
                "seasonal_reference_absolute_offset_minutes": (
                    float(
                        getattr(
                            source_row,
                            f"{reference_name}_absolute_offset_minutes",
                        )
                    )
                    if reference_name
                    else None
                ),
                "seasonal_reference_source_age_minutes": (
                    float(getattr(source_row, f"{reference_name}_source_age_minutes"))
                    if reference_name
                    else None
                ),
                "seasonal_baseline_contract_version": seasonal_config.contract_version,
                "evaluation_contract_version": evaluation_contract_version,
                "origin_fold": origin_fold,
                "origin_count": origin_count,
                "origin_cutoff_utc": origin_cutoff,
            }
        )
    return rows


def _evaluate_window(
    training_candidates: pd.DataFrame,
    evaluation: pd.DataFrame,
    *,
    split: str,
    run_id: str,
    run_timestamp: datetime,
    backtest_config: BacktestConfig,
    seasonal_config: SeasonalBaselineConfig,
    evaluation_contract_version: str,
    origin_fold: int | None = None,
    origin_count: int | None = None,
    origin_cutoff: pd.Timestamp | None = None,
) -> list[dict[str, Any]]:
    training = purge_overlapping_training_rows(
        training_candidates, evaluation, backtest_config
    )
    trained_through = training[TARGET_TIMESTAMP_COLUMN].max()
    ridge = fit_ridge_model(
        training,
        feature_columns=backtest_config.feature_columns,
        target_column=SUPERVISED_TARGET_COLUMN,
        alpha=backtest_config.ridge_alpha,
    ).predict(
        evaluation.loc[:, backtest_config.feature_columns].itertuples(
            index=False, name=None
        )
    )
    rows: list[dict[str, Any]] = []
    for model_name in ALL_MODELS:
        rows.extend(
            _prediction_rows(
                evaluation,
                model_name=model_name,
                predicted=_model_prediction(evaluation, model_name, ridge),
                split=split,
                trained_through=trained_through,
                training_observation_count=len(training),
                run_id=run_id,
                run_timestamp=run_timestamp,
                backtest_config=backtest_config,
                seasonal_config=seasonal_config,
                evaluation_contract_version=evaluation_contract_version,
                origin_fold=origin_fold,
                origin_count=origin_count,
                origin_cutoff=origin_cutoff,
            )
        )
    return rows


def _metric_row(group: pd.DataFrame) -> dict[str, Any]:
    errors = group["predicted_demand_mw"] - group["actual_demand_mw"]
    nonzero = group["actual_demand_mw"].abs() > 1e-12
    mape = None
    if nonzero.any():
        mape = float(
            (
                errors[nonzero].abs()
                / group.loc[nonzero, "actual_demand_mw"].abs()
            ).mean()
            * 100.0
        )
    first = group.iloc[0]
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
        "model_fit_required": bool(first["model_fit_required"]),
        "observation_count": int(len(group)),
        "mae_mw": float(errors.abs().mean()),
        "rmse_mw": float(sqrt((errors * errors).mean())),
        "mape_pct": mape,
        "bias_mw": float(errors.mean()),
        "trained_through_utc": first["trained_through_utc"],
        "training_observation_count": int(first["training_observation_count"]),
        "evaluation_feature_start_utc": group[FEATURE_TIMESTAMP_COLUMN].min(),
        "evaluation_feature_end_utc": group[FEATURE_TIMESTAMP_COLUMN].max(),
        "evaluation_start_utc": group[TIMESTAMP_COLUMN].min(),
        "evaluation_end_utc": group[TIMESTAMP_COLUMN].max(),
        "feature_contract_version": first["feature_contract_version"],
        "seasonal_reference_tolerance_minutes": int(
            first["seasonal_reference_tolerance_minutes"]
        ),
        "previous_day_eligible_count": int(first["previous_day_eligible_count"]),
        "previous_day_matched_count": int(first["previous_day_matched_count"]),
        "previous_day_coverage_pct": float(first["previous_day_coverage_pct"]),
        "previous_week_eligible_count": int(first["previous_week_eligible_count"]),
        "previous_week_matched_count": int(first["previous_week_matched_count"]),
        "previous_week_coverage_pct": float(first["previous_week_coverage_pct"]),
        "seasonal_baseline_contract_version": first[
            "seasonal_baseline_contract_version"
        ],
        "evaluation_contract_version": first["evaluation_contract_version"],
        "origin_fold": first["origin_fold"],
        "origin_count": first["origin_count"],
        "origin_cutoff_utc": first["origin_cutoff_utc"],
    }


def _validate_predictions(predictions: pd.DataFrame) -> None:
    if predictions.empty:
        raise ForecastingContractError("No seasonal baseline predictions were produced.")
    identity = [
        *GROUP_COLUMNS,
        REQUESTED_HORIZON_COLUMN,
        "split",
        "origin_fold",
        FEATURE_TIMESTAMP_COLUMN,
        TIMESTAMP_COLUMN,
    ]
    pairs = predictions.groupby(identity, dropna=False).agg(
        row_count=("model_name", "size"),
        model_count=("model_name", "nunique"),
        actual_count=("actual_demand_mw", "nunique"),
        boundary_count=("trained_through_utc", "nunique"),
    )
    if (
        (pairs["row_count"] != len(ALL_MODELS))
        | (pairs["model_count"] != len(ALL_MODELS))
        | (pairs["actual_count"] != 1)
        | (pairs["boundary_count"] != 1)
    ).any():
        raise ForecastingContractError(
            "Seasonal baselines do not form exact paired evaluation rows."
        )
    if set(predictions["model_name"]) != set(ALL_MODELS):
        raise ForecastingContractError("Seasonal model identities are incomplete.")
    if not (
        predictions["trained_through_utc"] < predictions[FEATURE_TIMESTAMP_COLUMN]
    ).all():
        raise ForecastingContractError(
            "Seasonal prediction evidence used unavailable training labels."
        )
    seasonal = predictions.loc[
        predictions["model_name"].isin(SEASONAL_MODELS)
    ]
    if not (
        seasonal["seasonal_reference_timestamp_utc"]
        <= seasonal[FEATURE_TIMESTAMP_COLUMN]
    ).all():
        raise ForecastingContractError(
            "A seasonal prediction used a reference unavailable at feature time."
        )
    if not (
        seasonal["seasonal_reference_absolute_offset_minutes"]
        <= seasonal["seasonal_reference_tolerance_minutes"]
    ).all():
        raise ForecastingContractError(
            "Seasonal prediction evidence exceeded reference tolerance."
        )
    expected_fit = predictions["model_name"].eq("ridge_weather_lag")
    if not (predictions["model_fit_required"].astype(bool) == expected_fit).all():
        raise ForecastingContractError("Seasonal model-fit evidence is inconsistent.")


def run_seasonal_backtest(
    frame: pd.DataFrame,
    *,
    backtest_config: BacktestConfig | None = None,
    seasonal_config: SeasonalBaselineConfig | None = None,
    evaluation_mode: str = "holdout",
    origin_count: int = 3,
    run_id: str | None = None,
    run_timestamp: datetime | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Compare persistence, elapsed seasonal baselines, and ridge on paired rows."""
    backtest_config = backtest_config or BacktestConfig()
    seasonal_config = seasonal_config or SeasonalBaselineConfig()
    seasonal_config.validate()
    if evaluation_mode not in {"holdout", "rolling-origin"}:
        raise ForecastingContractError(
            "evaluation_mode must be holdout or rolling-origin."
        )
    prepared = prepare_feature_frame(frame, backtest_config)
    supervised = build_supervised_frame(prepared, backtest_config)
    cohort = attach_seasonal_references(
        prepared,
        supervised,
        config=seasonal_config,
    )
    run_id = run_id or str(uuid4())
    run_timestamp = run_timestamp or datetime.now(timezone.utc)
    if run_timestamp.tzinfo is None:
        raise ForecastingContractError("run_timestamp must be timezone-aware.")
    run_timestamp = run_timestamp.astimezone(timezone.utc)
    rows: list[dict[str, Any]] = []
    grouping = [*GROUP_COLUMNS, REQUESTED_HORIZON_COLUMN]
    for _, group in cohort.groupby(grouping, sort=True, dropna=False):
        group = group.sort_values(FEATURE_TIMESTAMP_COLUMN).reset_index(drop=True)
        if evaluation_mode == "rolling-origin":
            for fold in build_rolling_origin_folds(
                group,
                backtest_config,
                origin_count=origin_count,
            ):
                rows.extend(
                    _evaluate_window(
                        fold.training_candidates,
                        fold.evaluation,
                        split=fold.split,
                        run_id=run_id,
                        run_timestamp=run_timestamp,
                        backtest_config=backtest_config,
                        seasonal_config=seasonal_config,
                        evaluation_contract_version=ROLLING_EVALUATION_CONTRACT_VERSION,
                        origin_fold=fold.origin_fold,
                        origin_count=fold.origin_count,
                        origin_cutoff=fold.origin_cutoff_utc,
                    )
                )
        else:
            train, validation, test = split_group(group, backtest_config)
            rows.extend(
                _evaluate_window(
                    train,
                    validation,
                    split="validation",
                    run_id=run_id,
                    run_timestamp=run_timestamp,
                    backtest_config=backtest_config,
                    seasonal_config=seasonal_config,
                    evaluation_contract_version=HOLDOUT_EVALUATION_CONTRACT_VERSION,
                )
            )
            rows.extend(
                _evaluate_window(
                    pd.concat([train, validation], ignore_index=True),
                    test,
                    split="test",
                    run_id=run_id,
                    run_timestamp=run_timestamp,
                    backtest_config=backtest_config,
                    seasonal_config=seasonal_config,
                    evaluation_contract_version=HOLDOUT_EVALUATION_CONTRACT_VERSION,
                )
            )
    predictions = pd.DataFrame(rows)
    _validate_predictions(predictions)
    metric_grouping = [
        *GROUP_COLUMNS,
        REQUESTED_HORIZON_COLUMN,
        "split",
        "origin_fold",
        "model_name",
    ]
    metrics = pd.DataFrame(
        [
            _metric_row(group)
            for _, group in predictions.groupby(
                metric_grouping, sort=True, dropna=False
            )
        ]
    )
    return predictions.reset_index(drop=True), metrics.reset_index(drop=True)

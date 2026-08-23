from __future__ import annotations

from dataclasses import dataclass
from math import isfinite

import pandas as pd

from forecasting.calendar_features import (
    CalendarFeatureError,
    LOCAL_CALENDAR_COLUMNS,
    UTC_CALENDAR_COLUMNS,
    add_uk_local_calendar_features,
)


GROUP_COLUMNS = ["source_area", "resource_id", "city"]
TIMESTAMP_COLUMN = "event_timestamp_utc"
FEATURE_TIMESTAMP_COLUMN = "feature_timestamp_utc"
TARGET_TIMESTAMP_COLUMN = "target_timestamp_utc"
TARGET_COLUMN = "demand_mw"
SUPERVISED_TARGET_COLUMN = "target_demand_mw"
REQUESTED_HORIZON_COLUMN = "requested_horizon_minutes"
TARGET_TOLERANCE_COLUMN = "target_tolerance_minutes"
TARGET_DELAY_COLUMN = "target_delay_minutes"
SUPPORTED_HORIZON_MINUTES = (30, 60)
NON_CALENDAR_FEATURE_COLUMNS = (
    "demand_mw",
    "demand_lag_1",
    "demand_rolling_mean_12",
    "temperature",
    "humidity",
)
DEFAULT_FEATURE_COLUMNS = [
    *NON_CALENDAR_FEATURE_COLUMNS,
    *UTC_CALENDAR_COLUMNS,
    "weather_age_minutes",
]
UK_LOCAL_FEATURE_COLUMNS = [
    *NON_CALENDAR_FEATURE_COLUMNS,
    *LOCAL_CALENDAR_COLUMNS,
    "weather_age_minutes",
]
UTC_FEATURE_CONTRACT_VERSION = "time-horizon-v1"
UK_LOCAL_FEATURE_CONTRACT_VERSION = "time-horizon-uk-calendar-v1"


class ForecastingContractError(ValueError):
    """Raised when feature data cannot support a leakage-safe backtest."""


@dataclass(frozen=True)
class BacktestConfig:
    train_fraction: float = 0.60
    validation_fraction: float = 0.20
    min_train_rows: int = 24
    min_validation_rows: int = 6
    min_test_rows: int = 6
    ridge_alpha: float = 1.0
    horizon_minutes: tuple[int, ...] = SUPPORTED_HORIZON_MINUTES
    target_tolerance_minutes: int = 5
    min_target_coverage: float = 0.90
    feature_columns: tuple[str, ...] = tuple(DEFAULT_FEATURE_COLUMNS)
    feature_contract_version: str = UTC_FEATURE_CONTRACT_VERSION

    def validate(self) -> None:
        if not 0 < self.train_fraction < 1:
            raise ForecastingContractError("train_fraction must be between 0 and 1.")
        if not 0 < self.validation_fraction < 1:
            raise ForecastingContractError(
                "validation_fraction must be between 0 and 1."
            )
        if self.train_fraction + self.validation_fraction >= 1:
            raise ForecastingContractError(
                "train_fraction + validation_fraction must leave a test split."
            )
        for name in ("min_train_rows", "min_validation_rows", "min_test_rows"):
            if getattr(self, name) < 1:
                raise ForecastingContractError(f"{name} must be at least 1.")
        if self.ridge_alpha <= 0:
            raise ForecastingContractError("ridge_alpha must be positive.")
        if not self.horizon_minutes:
            raise ForecastingContractError("At least one time horizon is required.")
        if len(set(self.horizon_minutes)) != len(self.horizon_minutes):
            raise ForecastingContractError("horizon_minutes must not contain duplicates.")
        for horizon in self.horizon_minutes:
            if isinstance(horizon, bool) or not isinstance(horizon, int):
                raise ForecastingContractError(
                    "horizon_minutes must contain positive integers."
                )
            if horizon not in SUPPORTED_HORIZON_MINUTES:
                supported = ", ".join(str(value) for value in SUPPORTED_HORIZON_MINUTES)
                raise ForecastingContractError(
                    f"Unsupported horizon_minutes={horizon}; supported values: {supported}."
                )
        if (
            isinstance(self.target_tolerance_minutes, bool)
            or not isinstance(self.target_tolerance_minutes, int)
            or self.target_tolerance_minutes < 0
        ):
            raise ForecastingContractError(
                "target_tolerance_minutes must be a non-negative integer."
            )
        if not 0 < self.min_target_coverage <= 1:
            raise ForecastingContractError(
                "min_target_coverage must be greater than 0 and at most 1."
            )
        if not self.feature_columns:
            raise ForecastingContractError("At least one feature column is required.")
        if len(set(self.feature_columns)) != len(self.feature_columns):
            raise ForecastingContractError("feature_columns must not contain duplicates.")
        if not isinstance(self.feature_contract_version, str) or not self.feature_contract_version.strip():
            raise ForecastingContractError(
                "feature_contract_version must be non-empty."
            )

    @property
    def ordered_horizons(self) -> tuple[int, ...]:
        return tuple(sorted(self.horizon_minutes))


def prepare_feature_frame(
    frame: pd.DataFrame,
    config: BacktestConfig,
) -> pd.DataFrame:
    config.validate()
    try:
        prepared = add_uk_local_calendar_features(
            frame,
            timestamp_column=TIMESTAMP_COLUMN,
        )
    except CalendarFeatureError as exc:
        raise ForecastingContractError(str(exc)) from exc
    required = {
        *GROUP_COLUMNS,
        TIMESTAMP_COLUMN,
        TARGET_COLUMN,
        *config.feature_columns,
    }
    missing = sorted(required - set(prepared.columns))
    if missing:
        raise ForecastingContractError(
            f"Feature frame is missing required columns: {', '.join(missing)}."
        )
    if prepared[GROUP_COLUMNS].isna().any().any():
        raise ForecastingContractError("Forecast groups must have non-null identity.")

    numeric_columns = list(dict.fromkeys([TARGET_COLUMN, *config.feature_columns]))
    for column in numeric_columns:
        prepared[column] = pd.to_numeric(prepared[column], errors="coerce")
    prepared = prepared.dropna(subset=numeric_columns)
    if prepared.empty:
        raise ForecastingContractError(
            "No complete rows remain after applying the forecasting contract."
        )
    finite_mask = prepared[numeric_columns].apply(
        lambda column: column.map(lambda value: isfinite(float(value)))
    )
    if not finite_mask.all().all():
        raise ForecastingContractError(
            "Forecast target and feature columns must contain only finite values."
        )

    prepared = prepared.sort_values([*GROUP_COLUMNS, TIMESTAMP_COLUMN])
    if prepared.duplicated(
        subset=[*GROUP_COLUMNS, TIMESTAMP_COLUMN], keep=False
    ).any():
        raise ForecastingContractError(
            "Forecast groups contain duplicate event timestamps."
        )
    return prepared.reset_index(drop=True)


def _group_identity(group: pd.DataFrame) -> str:
    first = group.iloc[0]
    return "/".join(str(first[column]) for column in GROUP_COLUMNS)


def _match_horizon(
    ordered: pd.DataFrame,
    *,
    requested_horizon_minutes: int,
    config: BacktestConfig,
) -> pd.DataFrame:
    timestamps = pd.DatetimeIndex(ordered[TIMESTAMP_COLUMN])
    maximum_timestamp = timestamps[-1]
    requested_delta = pd.Timedelta(minutes=requested_horizon_minutes)
    tolerance_delta = pd.Timedelta(minutes=config.target_tolerance_minutes)

    eligible_count = 0
    matches: list[dict[str, object]] = []
    for feature_position, feature_timestamp in enumerate(timestamps):
        ideal_target_timestamp = feature_timestamp + requested_delta
        if ideal_target_timestamp > maximum_timestamp:
            continue
        eligible_count += 1
        target_position = int(timestamps.searchsorted(ideal_target_timestamp, side="left"))
        if target_position >= len(timestamps) or target_position <= feature_position:
            continue
        target_timestamp = timestamps[target_position]
        target_delay = target_timestamp - ideal_target_timestamp
        if target_delay < pd.Timedelta(0) or target_delay > tolerance_delta:
            continue
        source_row = ordered.iloc[feature_position].to_dict()
        source_row.update(
            {
                FEATURE_TIMESTAMP_COLUMN: feature_timestamp,
                TARGET_TIMESTAMP_COLUMN: target_timestamp,
                SUPERVISED_TARGET_COLUMN: float(
                    ordered.iloc[target_position][TARGET_COLUMN]
                ),
                REQUESTED_HORIZON_COLUMN: requested_horizon_minutes,
                TARGET_TOLERANCE_COLUMN: config.target_tolerance_minutes,
                "horizon_steps": target_position - feature_position,
                "horizon_minutes": (
                    target_timestamp - feature_timestamp
                ).total_seconds()
                / 60.0,
                TARGET_DELAY_COLUMN: target_delay.total_seconds() / 60.0,
            }
        )
        matches.append(source_row)

    if eligible_count == 0:
        raise ForecastingContractError(
            f"Group {_group_identity(ordered)} has no eligible rows for the "
            f"{requested_horizon_minutes}-minute horizon."
        )
    matched_count = len(matches)
    coverage = matched_count / eligible_count
    if coverage < config.min_target_coverage:
        raise ForecastingContractError(
            f"Group {_group_identity(ordered)} matched {matched_count}/{eligible_count} "
            f"eligible targets for the {requested_horizon_minutes}-minute horizon "
            f"({coverage:.1%}); minimum coverage is {config.min_target_coverage:.1%}."
        )
    matched = pd.DataFrame(matches)
    if matched.empty:
        raise ForecastingContractError(
            f"Group {_group_identity(ordered)} produced no target matches for the "
            f"{requested_horizon_minutes}-minute horizon."
        )
    matched["eligible_target_count"] = eligible_count
    matched["matched_target_count"] = matched_count
    matched["target_coverage_pct"] = coverage * 100.0
    return matched


def build_supervised_frame(
    prepared: pd.DataFrame,
    config: BacktestConfig,
) -> pd.DataFrame:
    """Match causal feature rows to explicit future targets in elapsed time."""
    config.validate()
    supervised_groups: list[pd.DataFrame] = []
    for _, group in prepared.groupby(GROUP_COLUMNS, sort=True, dropna=False):
        ordered = group.sort_values(TIMESTAMP_COLUMN).reset_index(drop=True)
        for requested_horizon in config.ordered_horizons:
            supervised_groups.append(
                _match_horizon(
                    ordered,
                    requested_horizon_minutes=requested_horizon,
                    config=config,
                )
            )

    if not supervised_groups:
        raise ForecastingContractError("No forecast groups are available.")
    supervised = pd.concat(supervised_groups, ignore_index=True)
    if not (
        supervised[FEATURE_TIMESTAMP_COLUMN] < supervised[TARGET_TIMESTAMP_COLUMN]
    ).all():
        raise ForecastingContractError(
            "Forecast targets must occur after their feature timestamps."
        )
    if not (
        supervised["horizon_minutes"] >= supervised[REQUESTED_HORIZON_COLUMN]
    ).all():
        raise ForecastingContractError(
            "Matched forecast targets must not occur before the requested horizon."
        )
    if not (
        supervised[TARGET_DELAY_COLUMN].between(
            0, config.target_tolerance_minutes, inclusive="both"
        )
    ).all():
        raise ForecastingContractError(
            "Matched forecast targets exceeded the configured tolerance."
        )
    return supervised.sort_values(
        [*GROUP_COLUMNS, REQUESTED_HORIZON_COLUMN, FEATURE_TIMESTAMP_COLUMN]
    ).reset_index(drop=True)


def split_group(
    group: pd.DataFrame,
    config: BacktestConfig,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    row_count = len(group)
    train_end = int(row_count * config.train_fraction)
    validation_end = int(
        row_count * (config.train_fraction + config.validation_fraction)
    )
    train = group.iloc[:train_end].copy()
    validation = group.iloc[train_end:validation_end].copy()
    test = group.iloc[validation_end:].copy()

    requirements = (
        ("training", len(train), config.min_train_rows),
        ("validation", len(validation), config.min_validation_rows),
        ("test", len(test), config.min_test_rows),
    )
    for split_name, actual, minimum in requirements:
        if actual < minimum:
            raise ForecastingContractError(
                f"Group has {actual} {split_name} rows; minimum is {minimum}."
            )
    return train, validation, test


def purge_overlapping_training_rows(
    training: pd.DataFrame,
    evaluation: pd.DataFrame,
    config: BacktestConfig,
) -> pd.DataFrame:
    """Remove labels that would not yet be known at evaluation feature time."""
    if evaluation.empty:
        raise ForecastingContractError("Evaluation split has no rows.")
    evaluation_start = evaluation[FEATURE_TIMESTAMP_COLUMN].min()
    purged = training.loc[
        training[TARGET_TIMESTAMP_COLUMN] < evaluation_start
    ].copy()
    if len(purged) < config.min_train_rows:
        raise ForecastingContractError(
            "Purging overlapping horizon labels left "
            f"{len(purged)} training rows; minimum is {config.min_train_rows}."
        )
    return purged

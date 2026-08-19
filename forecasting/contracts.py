from __future__ import annotations

from dataclasses import dataclass
from math import isfinite

import pandas as pd


GROUP_COLUMNS = ["source_area", "resource_id", "city"]
TIMESTAMP_COLUMN = "event_timestamp_utc"
TARGET_COLUMN = "demand_mw"
DEFAULT_FEATURE_COLUMNS = [
    "demand_lag_1",
    "demand_rolling_mean_12",
    "temperature",
    "humidity",
    "hour_of_day_utc",
    "day_of_week_utc",
    "is_weekend_utc",
    "weather_age_minutes",
]


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
    feature_columns: tuple[str, ...] = tuple(DEFAULT_FEATURE_COLUMNS)
    feature_contract_version: str = "baseline-v1"

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
        if not self.feature_columns:
            raise ForecastingContractError("At least one feature column is required.")


def prepare_feature_frame(
    frame: pd.DataFrame,
    config: BacktestConfig,
) -> pd.DataFrame:
    config.validate()
    required = {
        *GROUP_COLUMNS,
        TIMESTAMP_COLUMN,
        TARGET_COLUMN,
        *config.feature_columns,
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ForecastingContractError(
            f"Feature frame is missing required columns: {', '.join(missing)}."
        )

    prepared = frame.copy()
    prepared[TIMESTAMP_COLUMN] = pd.to_datetime(
        prepared[TIMESTAMP_COLUMN], utc=True, errors="coerce"
    )
    if prepared[TIMESTAMP_COLUMN].isna().any():
        raise ForecastingContractError("event_timestamp_utc contains invalid values.")
    if prepared[GROUP_COLUMNS].isna().any().any():
        raise ForecastingContractError("Forecast groups must have non-null identity.")

    numeric_columns = [TARGET_COLUMN, *config.feature_columns]
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

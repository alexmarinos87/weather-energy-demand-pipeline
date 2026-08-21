from __future__ import annotations

from dataclasses import dataclass
from math import isfinite
from typing import Iterable

import pandas as pd


SOURCE_AREA_COLUMN = "source_area"
CITY_COLUMN = "city"
FEATURE_TIMESTAMP_COLUMN = "feature_timestamp_utc"
TARGET_TIMESTAMP_COLUMN = "target_timestamp_utc"
REQUESTED_HORIZON_COLUMN = "requested_horizon_minutes"
FORECAST_ISSUED_COLUMN = "forecast_issued_at_utc"
FORECAST_INGESTED_COLUMN = "forecast_ingested_at_utc"
FORECAST_VALID_COLUMN = "forecast_valid_at_utc"
FORECAST_TEMPERATURE_COLUMN = "forecast_temperature_c"
FORECAST_HUMIDITY_COLUMN = "forecast_humidity_pct"
FORECAST_PROVIDER_COLUMN = "forecast_provider"
FORECAST_MODEL_COLUMN = "forecast_model"
TARGET_WEATHER_PREFIX = "target_weather_"
FORECAST_WEATHER_CONTRACT_VERSION = "target-weather-v1"

REQUIRED_FORECAST_COLUMNS = (
    SOURCE_AREA_COLUMN,
    CITY_COLUMN,
    FORECAST_ISSUED_COLUMN,
    FORECAST_INGESTED_COLUMN,
    FORECAST_VALID_COLUMN,
    FORECAST_TEMPERATURE_COLUMN,
    FORECAST_HUMIDITY_COLUMN,
    FORECAST_PROVIDER_COLUMN,
    FORECAST_MODEL_COLUMN,
)


class ForecastWeatherContractError(ValueError):
    """Raised when forecast-weather evidence is invalid or cannot be matched safely."""


@dataclass(frozen=True)
class ForecastWeatherConfig:
    valid_time_tolerance_minutes: int = 15
    max_availability_age_minutes: int = 180
    min_coverage: float = 0.90
    contract_version: str = FORECAST_WEATHER_CONTRACT_VERSION

    def validate(self) -> None:
        for name in (
            "valid_time_tolerance_minutes",
            "max_availability_age_minutes",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ForecastWeatherContractError(
                    f"{name} must be a non-negative integer."
                )
        if self.max_availability_age_minutes < 1:
            raise ForecastWeatherContractError(
                "max_availability_age_minutes must be at least 1."
            )
        if not 0 < self.min_coverage <= 1:
            raise ForecastWeatherContractError(
                "min_coverage must be greater than 0 and at most 1."
            )
        if (
            not isinstance(self.contract_version, str)
            or not self.contract_version.strip()
        ):
            raise ForecastWeatherContractError("contract_version must be non-empty.")


def _aware_utc(value: object, column: str) -> pd.Timestamp:
    try:
        timestamp = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise ForecastWeatherContractError(
            f"{column} contains an invalid timestamp: {value!r}."
        ) from exc
    if pd.isna(timestamp):
        raise ForecastWeatherContractError(f"{column} contains a null timestamp.")
    if timestamp.tzinfo is None:
        raise ForecastWeatherContractError(
            f"{column} must contain timezone-aware timestamps."
        )
    return timestamp.tz_convert("UTC")


def _non_empty_identity(frame: pd.DataFrame, columns: Iterable[str]) -> None:
    for column in columns:
        if frame[column].isna().any() or frame[column].map(
            lambda value: not str(value).strip()
        ).any():
            raise ForecastWeatherContractError(
                f"{column} must contain non-empty identity values."
            )


def prepare_forecast_weather_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """Normalize provider forecast records into a strict, causal UTC contract."""
    missing = sorted(set(REQUIRED_FORECAST_COLUMNS) - set(frame.columns))
    if missing:
        raise ForecastWeatherContractError(
            "Forecast-weather frame is missing required columns: "
            + ", ".join(missing)
            + "."
        )
    prepared = frame.copy()
    _non_empty_identity(
        prepared,
        (
            SOURCE_AREA_COLUMN,
            CITY_COLUMN,
            FORECAST_PROVIDER_COLUMN,
            FORECAST_MODEL_COLUMN,
        ),
    )
    for column in (
        FORECAST_ISSUED_COLUMN,
        FORECAST_INGESTED_COLUMN,
        FORECAST_VALID_COLUMN,
    ):
        prepared[column] = prepared[column].map(
            lambda value, name=column: _aware_utc(value, name)
        )

    for column in (FORECAST_TEMPERATURE_COLUMN, FORECAST_HUMIDITY_COLUMN):
        prepared[column] = pd.to_numeric(prepared[column], errors="coerce")
        if prepared[column].isna().any() or not prepared[column].map(
            lambda value: isfinite(float(value))
        ).all():
            raise ForecastWeatherContractError(
                f"{column} must contain only finite numeric values."
            )
    if not prepared[FORECAST_HUMIDITY_COLUMN].between(
        0, 100, inclusive="both"
    ).all():
        raise ForecastWeatherContractError(
            "forecast_humidity_pct must be between 0 and 100."
        )
    if not (
        prepared[FORECAST_ISSUED_COLUMN] <= prepared[FORECAST_INGESTED_COLUMN]
    ).all():
        raise ForecastWeatherContractError(
            "Forecast ingestion must not occur before provider issuance."
        )
    if not (
        prepared[FORECAST_INGESTED_COLUMN] < prepared[FORECAST_VALID_COLUMN]
    ).all():
        raise ForecastWeatherContractError(
            "Forecast records must be ingested before their valid time."
        )
    duplicate_columns = [
        SOURCE_AREA_COLUMN,
        CITY_COLUMN,
        FORECAST_PROVIDER_COLUMN,
        FORECAST_MODEL_COLUMN,
        FORECAST_ISSUED_COLUMN,
        FORECAST_VALID_COLUMN,
    ]
    if prepared.duplicated(subset=duplicate_columns, keep=False).any():
        raise ForecastWeatherContractError(
            "Forecast-weather records contain duplicate provider "
            "issue/valid identities."
        )
    return prepared.sort_values(
        [
            SOURCE_AREA_COLUMN,
            CITY_COLUMN,
            FORECAST_VALID_COLUMN,
            FORECAST_ISSUED_COLUMN,
            FORECAST_INGESTED_COLUMN,
            FORECAST_PROVIDER_COLUMN,
            FORECAST_MODEL_COLUMN,
        ]
    ).reset_index(drop=True)


def _required_supervised_frame(supervised: pd.DataFrame) -> pd.DataFrame:
    required = {
        SOURCE_AREA_COLUMN,
        CITY_COLUMN,
        FEATURE_TIMESTAMP_COLUMN,
        TARGET_TIMESTAMP_COLUMN,
        REQUESTED_HORIZON_COLUMN,
    }
    missing = sorted(required - set(supervised.columns))
    if missing:
        raise ForecastWeatherContractError(
            "Supervised demand frame is missing required columns: "
            + ", ".join(missing)
            + "."
        )
    prepared = supervised.copy()
    for column in (FEATURE_TIMESTAMP_COLUMN, TARGET_TIMESTAMP_COLUMN):
        prepared[column] = prepared[column].map(
            lambda value, name=column: _aware_utc(value, name)
        )
    _non_empty_identity(prepared, (SOURCE_AREA_COLUMN, CITY_COLUMN))
    prepared[REQUESTED_HORIZON_COLUMN] = pd.to_numeric(
        prepared[REQUESTED_HORIZON_COLUMN], errors="coerce"
    )
    if prepared[REQUESTED_HORIZON_COLUMN].isna().any() or (
        prepared[REQUESTED_HORIZON_COLUMN] <= 0
    ).any():
        raise ForecastWeatherContractError(
            "requested_horizon_minutes must contain positive values."
        )
    if not (
        prepared[FEATURE_TIMESTAMP_COLUMN] < prepared[TARGET_TIMESTAMP_COLUMN]
    ).all():
        raise ForecastWeatherContractError(
            "Target timestamps must occur after feature timestamps."
        )
    return prepared


def _group_identity(group: pd.DataFrame) -> str:
    first = group.iloc[0]
    identity = f"{first[SOURCE_AREA_COLUMN]}"
    if "resource_id" in group.columns:
        identity += f"/{first['resource_id']}"
    identity += f"/{first[CITY_COLUMN]}"
    return f"{identity} horizon={int(first[REQUESTED_HORIZON_COLUMN])}m"


def attach_target_forecast_weather(
    supervised: pd.DataFrame,
    forecast_weather: pd.DataFrame,
    *,
    config: ForecastWeatherConfig | None = None,
) -> pd.DataFrame:
    """Attach the closest target-valid forecast available at each feature timestamp."""
    config = config or ForecastWeatherConfig()
    config.validate()
    demand = _required_supervised_frame(supervised)
    weather = prepare_forecast_weather_frame(forecast_weather)
    weather_groups = {
        key if isinstance(key, tuple) else (key,): group.reset_index(drop=True)
        for key, group in weather.groupby(
            [SOURCE_AREA_COLUMN, CITY_COLUMN], sort=True, dropna=False
        )
    }
    valid_tolerance = pd.Timedelta(minutes=config.valid_time_tolerance_minutes)
    max_age = pd.Timedelta(minutes=config.max_availability_age_minutes)
    output_groups: list[pd.DataFrame] = []

    grouping_columns = [SOURCE_AREA_COLUMN]
    if "resource_id" in demand.columns:
        grouping_columns.append("resource_id")
    grouping_columns.extend([CITY_COLUMN, REQUESTED_HORIZON_COLUMN])
    for _, demand_group in demand.groupby(
        grouping_columns, sort=True, dropna=False
    ):
        eligible_count = len(demand_group)
        identity_key = (
            demand_group.iloc[0][SOURCE_AREA_COLUMN],
            demand_group.iloc[0][CITY_COLUMN],
        )
        weather_group = weather_groups.get(identity_key)
        matched_rows: list[dict[str, object]] = []
        for _, demand_row in demand_group.iterrows():
            feature_timestamp = demand_row[FEATURE_TIMESTAMP_COLUMN]
            target_timestamp = demand_row[TARGET_TIMESTAMP_COLUMN]
            if weather_group is None:
                continue
            candidates = weather_group.loc[
                (weather_group[FORECAST_ISSUED_COLUMN] <= feature_timestamp)
                & (weather_group[FORECAST_INGESTED_COLUMN] <= feature_timestamp)
                & (weather_group[FORECAST_VALID_COLUMN] > feature_timestamp)
                & (
                    feature_timestamp - weather_group[FORECAST_INGESTED_COLUMN]
                    <= max_age
                )
            ].copy()
            if candidates.empty:
                continue
            candidates["_valid_delta"] = (
                candidates[FORECAST_VALID_COLUMN] - target_timestamp
            ).abs()
            candidates = candidates.loc[candidates["_valid_delta"] <= valid_tolerance]
            if candidates.empty:
                continue
            candidates = candidates.sort_values(
                [
                    "_valid_delta",
                    FORECAST_INGESTED_COLUMN,
                    FORECAST_ISSUED_COLUMN,
                    FORECAST_PROVIDER_COLUMN,
                    FORECAST_MODEL_COLUMN,
                ],
                ascending=[True, False, False, True, True],
            )
            selected = candidates.iloc[0]
            source_row = demand_row.to_dict()
            provider_lead = (
                selected[FORECAST_VALID_COLUMN] - selected[FORECAST_ISSUED_COLUMN]
            ).total_seconds() / 60.0
            feature_lead = (
                selected[FORECAST_VALID_COLUMN] - feature_timestamp
            ).total_seconds() / 60.0
            availability_age = (
                feature_timestamp - selected[FORECAST_INGESTED_COLUMN]
            ).total_seconds() / 60.0
            source_row.update(
                {
                    f"{TARGET_WEATHER_PREFIX}forecast_issued_at_utc": selected[
                        FORECAST_ISSUED_COLUMN
                    ],
                    f"{TARGET_WEATHER_PREFIX}forecast_ingested_at_utc": selected[
                        FORECAST_INGESTED_COLUMN
                    ],
                    f"{TARGET_WEATHER_PREFIX}forecast_valid_at_utc": selected[
                        FORECAST_VALID_COLUMN
                    ],
                    f"{TARGET_WEATHER_PREFIX}temperature_c": float(
                        selected[FORECAST_TEMPERATURE_COLUMN]
                    ),
                    f"{TARGET_WEATHER_PREFIX}humidity_pct": float(
                        selected[FORECAST_HUMIDITY_COLUMN]
                    ),
                    f"{TARGET_WEATHER_PREFIX}provider": selected[
                        FORECAST_PROVIDER_COLUMN
                    ],
                    f"{TARGET_WEATHER_PREFIX}model": selected[FORECAST_MODEL_COLUMN],
                    f"{TARGET_WEATHER_PREFIX}valid_delta_minutes": selected[
                        "_valid_delta"
                    ].total_seconds()
                    / 60.0,
                    f"{TARGET_WEATHER_PREFIX}provider_lead_minutes": provider_lead,
                    f"{TARGET_WEATHER_PREFIX}feature_lead_minutes": feature_lead,
                    f"{TARGET_WEATHER_PREFIX}availability_age_minutes": (
                        availability_age
                    ),
                    "weather_feature_mode": "target_forecast",
                    "forecast_weather_contract_version": config.contract_version,
                }
            )
            matched_rows.append(source_row)

        matched_count = len(matched_rows)
        coverage = matched_count / eligible_count if eligible_count else 0.0
        if coverage < config.min_coverage:
            raise ForecastWeatherContractError(
                f"Forecast weather for {_group_identity(demand_group)} matched "
                f"{matched_count}/{eligible_count} eligible targets ({coverage:.1%}); "
                f"minimum coverage is {config.min_coverage:.1%}."
            )
        matched = pd.DataFrame(matched_rows)
        if matched.empty:
            raise ForecastWeatherContractError(
                "Forecast weather for "
                f"{_group_identity(demand_group)} produced no matches."
            )
        matched["forecast_weather_eligible_count"] = eligible_count
        matched["forecast_weather_matched_count"] = matched_count
        matched["forecast_weather_coverage_pct"] = coverage * 100.0
        matched["minimum_forecast_weather_coverage_pct"] = (
            config.min_coverage * 100.0
        )
        output_groups.append(matched)

    if not output_groups:
        raise ForecastWeatherContractError(
            "No supervised demand groups produced target-weather features."
        )
    result = pd.concat(output_groups, ignore_index=True)
    issued = result[f"{TARGET_WEATHER_PREFIX}forecast_issued_at_utc"]
    ingested = result[f"{TARGET_WEATHER_PREFIX}forecast_ingested_at_utc"]
    valid = result[f"{TARGET_WEATHER_PREFIX}forecast_valid_at_utc"]
    feature = result[FEATURE_TIMESTAMP_COLUMN]
    target = result[TARGET_TIMESTAMP_COLUMN]
    if not ((issued <= ingested) & (ingested <= feature) & (feature < target)).all():
        raise ForecastWeatherContractError(
            "Matched forecast weather violated issue, availability, "
            "feature, or target ordering."
        )
    if not ((valid > issued) & (valid > feature)).all():
        raise ForecastWeatherContractError(
            "Matched forecast valid times must occur after issuance and feature time."
        )
    if not (
        (result[f"{TARGET_WEATHER_PREFIX}provider_lead_minutes"] > 0)
        & (result[f"{TARGET_WEATHER_PREFIX}feature_lead_minutes"] > 0)
    ).all():
        raise ForecastWeatherContractError(
            "Matched forecast lead times must be positive."
        )
    if not (
        result[f"{TARGET_WEATHER_PREFIX}valid_delta_minutes"]
        <= config.valid_time_tolerance_minutes
    ).all():
        raise ForecastWeatherContractError(
            "Matched forecast valid times exceeded the configured tolerance."
        )
    if not (
        result[f"{TARGET_WEATHER_PREFIX}availability_age_minutes"]
        <= config.max_availability_age_minutes
    ).all():
        raise ForecastWeatherContractError(
            "Matched forecast weather exceeded the configured availability age."
        )
    sort_columns = [SOURCE_AREA_COLUMN]
    if "resource_id" in result.columns:
        sort_columns.append("resource_id")
    sort_columns.extend(
        [CITY_COLUMN, REQUESTED_HORIZON_COLUMN, FEATURE_TIMESTAMP_COLUMN]
    )
    return result.sort_values(sort_columns).reset_index(drop=True)


def build_demo_forecast_weather_frame(
    feature_frame: pd.DataFrame,
    *,
    horizon_minutes: tuple[int, ...] = (30, 60),
    provider: str = "demo",
    model: str = "deterministic-v1",
) -> pd.DataFrame:
    """Build deterministic target-valid forecast records without API credentials."""
    required = {
        SOURCE_AREA_COLUMN,
        CITY_COLUMN,
        "event_timestamp_utc",
        "temperature",
        "humidity",
    }
    missing = sorted(required - set(feature_frame.columns))
    if missing:
        raise ForecastWeatherContractError(
            "Demo feature frame is missing required columns: "
            + ", ".join(missing)
            + "."
        )
    if not horizon_minutes or any(
        isinstance(value, bool) or not isinstance(value, int) or value < 1
        for value in horizon_minutes
    ):
        raise ForecastWeatherContractError(
            "horizon_minutes must contain positive integers."
        )
    if len(set(horizon_minutes)) != len(horizon_minutes):
        raise ForecastWeatherContractError(
            "horizon_minutes must not contain duplicates."
        )
    features = feature_frame.copy()
    features["event_timestamp_utc"] = features["event_timestamp_utc"].map(
        lambda value: _aware_utc(value, "event_timestamp_utc")
    )
    records: list[dict[str, object]] = []
    for _, group in features.groupby(
        [SOURCE_AREA_COLUMN, CITY_COLUMN], sort=True, dropna=False
    ):
        ordered = group.sort_values("event_timestamp_utc").reset_index(drop=True)
        lookup = ordered.set_index("event_timestamp_utc")
        for source_row in ordered.itertuples(index=False):
            issued_at = source_row.event_timestamp_utc
            for horizon in sorted(horizon_minutes):
                valid_at = issued_at + pd.Timedelta(minutes=horizon)
                if valid_at not in lookup.index:
                    continue
                target_row = lookup.loc[valid_at]
                if isinstance(target_row, pd.DataFrame):
                    raise ForecastWeatherContractError(
                        "Demo feature frame contains duplicate event timestamps."
                    )
                records.append(
                    {
                        SOURCE_AREA_COLUMN: source_row.source_area,
                        CITY_COLUMN: source_row.city,
                        FORECAST_ISSUED_COLUMN: issued_at,
                        FORECAST_INGESTED_COLUMN: issued_at,
                        FORECAST_VALID_COLUMN: valid_at,
                        FORECAST_TEMPERATURE_COLUMN: float(target_row["temperature"])
                        + 0.2,
                        FORECAST_HUMIDITY_COLUMN: min(
                            100.0, max(0.0, float(target_row["humidity"]) + 1.0)
                        ),
                        FORECAST_PROVIDER_COLUMN: provider,
                        FORECAST_MODEL_COLUMN: model,
                    }
                )
    if not records:
        raise ForecastWeatherContractError(
            "Demo feature history is too short for the configured forecast horizons."
        )
    return prepare_forecast_weather_frame(pd.DataFrame(records))

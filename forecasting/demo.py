from __future__ import annotations

import pandas as pd

from forecasting.contracts import (
    GROUP_COLUMNS,
    TARGET_COLUMN,
    TIMESTAMP_COLUMN,
    ForecastingContractError,
)
from ingestion.common.source_area import load_source_area_contract


DEMO_AREA_DEMAND_BASE_MW = {
    "east_midlands": 1500.0,
    "south_wales": 1225.0,
    "south_west": 1125.0,
    "west_midlands": 1650.0,
}
DEMO_AREA_TEMPERATURE_OFFSET_C = {
    "east_midlands": 0.0,
    "south_wales": 0.8,
    "south_west": 1.6,
    "west_midlands": 0.4,
}
DEMO_AREA_HUMIDITY_OFFSET_PCT = {
    "east_midlands": 0.0,
    "south_wales": 5.0,
    "south_west": 3.0,
    "west_midlands": 1.0,
}


def _source_bindings() -> dict[str, dict[str, object]]:
    contract = load_source_area_contract()
    areas = contract["areas"]
    supported = set(DEMO_AREA_DEMAND_BASE_MW)
    if set(areas) != supported:
        raise ForecastingContractError(
            "The deterministic demo parameters must cover every source-area "
            "contract key exactly."
        )
    return areas


def _selected_source_areas(
    requested: tuple[str, ...] | None,
) -> tuple[str, ...]:
    available = tuple(sorted(_source_bindings()))
    if requested is None:
        return ("east_midlands",)
    normalized = tuple(
        str(value).strip().lower().replace("-", "_").replace(" ", "_")
        for value in requested
    )
    if not normalized or any(not value for value in normalized):
        raise ForecastingContractError(
            "source_areas must contain at least one non-empty source-area key."
        )
    if len(set(normalized)) != len(normalized):
        raise ForecastingContractError("source_areas must not contain duplicates.")
    unsupported = sorted(set(normalized) - set(available))
    if unsupported:
        raise ForecastingContractError(
            "Unsupported demo source areas: "
            + ", ".join(unsupported)
            + ". Supported values: "
            + ", ".join(available)
            + "."
        )
    return tuple(sorted(normalized))


def _build_area_frame(
    source_area: str,
    binding: dict[str, object],
    *,
    periods: int,
    start: str,
) -> pd.DataFrame:
    timestamps = pd.date_range(start=start, periods=periods, freq="5min", tz="UTC")
    demand_values: list[float] = []
    temperatures: list[float] = []
    humidities: list[float] = []
    base_demand = DEMO_AREA_DEMAND_BASE_MW[source_area]
    temperature_offset = DEMO_AREA_TEMPERATURE_OFFSET_C[source_area]
    humidity_offset = DEMO_AREA_HUMIDITY_OFFSET_PCT[source_area]
    for index, timestamp in enumerate(timestamps):
        hour = timestamp.hour
        day_of_week = timestamp.dayofweek + 1
        is_weekend = int(timestamp.dayofweek >= 5)
        temperature = (
            5.0
            + temperature_offset
            + 0.18 * (index % 48)
            + 0.08 * hour
        )
        humidity = 52.0 + humidity_offset + float((index * 7) % 22)
        demand = (
            base_demand
            + 1.8 * index
            - 21.0 * temperature
            + 2.7 * humidity
            + 3.0 * hour
            + 5.0 * day_of_week
            + 55.0 * is_weekend
        )
        temperatures.append(float(temperature))
        humidities.append(float(humidity))
        demand_values.append(float(demand))

    city = str(binding["weather_proxy_city"]).split(",", 1)[0]
    frame = pd.DataFrame(
        {
            "source_area": source_area,
            "resource_id": str(binding["nged_resource_id"]),
            "city": city,
            TIMESTAMP_COLUMN: timestamps,
            TARGET_COLUMN: demand_values,
            "temperature": temperatures,
            "humidity": humidities,
            "hour_of_day_utc": [timestamp.hour for timestamp in timestamps],
            "day_of_week_utc": [
                timestamp.dayofweek + 1 for timestamp in timestamps
            ],
            "is_weekend_utc": [
                int(timestamp.dayofweek >= 5) for timestamp in timestamps
            ],
            "weather_age_minutes": [0.0 for _ in timestamps],
        }
    )
    frame["demand_lag_1"] = frame[TARGET_COLUMN].shift(1)
    frame["demand_rolling_mean_12"] = (
        frame[TARGET_COLUMN].shift(1).rolling(window=12, min_periods=1).mean()
    )
    return frame.iloc[1:].reset_index(drop=True)


def build_demo_feature_frame(
    *,
    periods: int = 288,
    start: str = "2026-01-01T00:00:00Z",
    source_areas: tuple[str, ...] | None = None,
) -> pd.DataFrame:
    """Build deterministic five-minute features for one or more source areas."""
    if periods < 96:
        raise ForecastingContractError("Demo data requires at least 96 periods.")
    bindings = _source_bindings()
    selected = _selected_source_areas(source_areas)
    frame = pd.concat(
        [
            _build_area_frame(
                source_area,
                bindings[source_area],
                periods=periods,
                start=start,
            )
            for source_area in selected
        ],
        ignore_index=True,
    )
    if frame.duplicated(subset=[*GROUP_COLUMNS, TIMESTAMP_COLUMN], keep=False).any():
        raise ForecastingContractError(
            "Deterministic demo groups contain duplicate event timestamps."
        )
    return frame.sort_values([*GROUP_COLUMNS, TIMESTAMP_COLUMN]).reset_index(drop=True)


def build_multi_area_demo_feature_frame(
    *,
    periods: int = 288,
    start: str = "2026-01-01T00:00:00Z",
) -> pd.DataFrame:
    """Build one deterministic group for every contracted NGED source area."""
    return build_demo_feature_frame(
        periods=periods,
        start=start,
        source_areas=tuple(sorted(_source_bindings())),
    )

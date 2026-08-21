from __future__ import annotations

import pandas as pd

from forecasting.contracts import (
    TARGET_COLUMN,
    TIMESTAMP_COLUMN,
    ForecastingContractError,
)


def build_demo_feature_frame(
    *,
    periods: int = 288,
    start: str = "2026-01-01T00:00:00Z",
) -> pd.DataFrame:
    """Build deterministic five-minute features for 30/60-minute demos."""
    if periods < 96:
        raise ForecastingContractError("Demo data requires at least 96 periods.")
    timestamps = pd.date_range(start=start, periods=periods, freq="5min", tz="UTC")
    demand_values: list[float] = []
    temperatures: list[float] = []
    humidities: list[float] = []
    for index, timestamp in enumerate(timestamps):
        hour = timestamp.hour
        day_of_week = timestamp.dayofweek + 1
        is_weekend = int(timestamp.dayofweek >= 5)
        temperature = 5.0 + 0.18 * (index % 48) + 0.08 * hour
        humidity = 52.0 + float((index * 7) % 22)
        demand = (
            1500.0
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

    frame = pd.DataFrame(
        {
            "source_area": "east_midlands",
            "resource_id": "92d3431c-15d7-4aa6-ad34-2335596a026c",
            "city": "Nottingham",
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

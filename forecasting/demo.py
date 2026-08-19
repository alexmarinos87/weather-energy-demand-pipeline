from __future__ import annotations

import pandas as pd

from forecasting.contracts import (
    TARGET_COLUMN,
    TIMESTAMP_COLUMN,
    ForecastingContractError,
)


def build_demo_feature_frame(
    *,
    periods: int = 120,
    start: str = "2026-01-01T00:00:00Z",
) -> pd.DataFrame:
    """Build deterministic weather-sensitive features for a credential-free demo."""
    if periods < 60:
        raise ForecastingContractError("Demo data requires at least 60 periods.")
    timestamps = pd.date_range(start=start, periods=periods, freq="h", tz="UTC")
    demand_values: list[float] = []
    temperatures: list[float] = []
    humidities: list[float] = []
    for index, timestamp in enumerate(timestamps):
        hour = timestamp.hour
        day_of_week = timestamp.dayofweek + 1
        is_weekend = int(timestamp.dayofweek >= 5)
        temperature = 4.0 + 0.75 * ((index * 7) % 21) + 0.15 * hour
        humidity = 55.0 + float((index * 11) % 24)
        demand = (
            1550.0
            - 24.0 * temperature
            + 3.5 * humidity
            + 4.0 * hour
            + 7.0 * day_of_week
            + 65.0 * is_weekend
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

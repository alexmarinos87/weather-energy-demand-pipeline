from __future__ import annotations

from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import pandas as pd


UK_LOCAL_TIMEZONE = "Europe/London"
CALENDAR_FEATURE_CONTRACT_VERSION = "uk-local-calendar-v1"
LOCAL_CALENDAR_COLUMNS = (
    "hour_of_day_local",
    "day_of_week_local",
    "is_weekend_local",
    "local_utc_offset_minutes",
    "is_dst_local",
)
UTC_CALENDAR_COLUMNS = (
    "hour_of_day_utc",
    "day_of_week_utc",
    "is_weekend_utc",
)


class CalendarFeatureError(ValueError):
    """Raised when canonical UTC timestamps cannot produce safe UK calendar fields."""


def _aware_utc_series(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        raise CalendarFeatureError(f"Missing timestamp column {column}.")
    values: list[pd.Timestamp] = []
    for raw in frame[column]:
        try:
            timestamp = pd.Timestamp(raw)
        except (TypeError, ValueError) as exc:
            raise CalendarFeatureError(
                f"{column} must contain valid timezone-aware timestamps."
            ) from exc
        if pd.isna(timestamp) or timestamp.tzinfo is None:
            raise CalendarFeatureError(
                f"{column} must contain timezone-aware timestamps."
            )
        values.append(timestamp.tz_convert("UTC"))
    return pd.Series(values, index=frame.index, dtype="datetime64[ns, UTC]")


def add_uk_local_calendar_features(
    frame: pd.DataFrame,
    *,
    timestamp_column: str = "event_timestamp_utc",
    timezone_name: str = UK_LOCAL_TIMEZONE,
) -> pd.DataFrame:
    """Derive explicit UTC and Europe/London calendar evidence from UTC identity."""
    if timezone_name != UK_LOCAL_TIMEZONE:
        raise CalendarFeatureError(
            f"timezone_name must be {UK_LOCAL_TIMEZONE!r} for this contract."
        )
    try:
        timezone = ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError as exc:
        raise CalendarFeatureError(
            f"Timezone database does not contain {timezone_name}."
        ) from exc

    prepared = frame.copy()
    utc = _aware_utc_series(prepared, timestamp_column)
    local = utc.dt.tz_convert(timezone)
    offsets = local.map(
        lambda timestamp: int(timestamp.utcoffset().total_seconds() / 60)
    ).astype(int)
    dst_flags = local.map(
        lambda timestamp: int(bool(timestamp.dst().total_seconds()))
    ).astype(int)
    if not offsets.isin({0, 60}).all():
        raise CalendarFeatureError(
            "Europe/London UTC offsets must be either 0 or 60 minutes."
        )
    if not ((offsets == 60).astype(int) == dst_flags).all():
        raise CalendarFeatureError(
            "Europe/London daylight-saving flags disagree with UTC offsets."
        )

    prepared[timestamp_column] = utc
    prepared["event_timestamp_local"] = local
    prepared["event_date_local"] = local.dt.strftime("%Y-%m-%d")
    prepared["hour_of_day_utc"] = utc.dt.hour.astype(int)
    prepared["day_of_week_utc"] = (utc.dt.dayofweek + 1).astype(int)
    prepared["is_weekend_utc"] = (utc.dt.dayofweek >= 5).astype(int)
    prepared["hour_of_day_local"] = local.dt.hour.astype(int)
    prepared["day_of_week_local"] = (local.dt.dayofweek + 1).astype(int)
    prepared["is_weekend_local"] = (local.dt.dayofweek >= 5).astype(int)
    prepared["local_utc_offset_minutes"] = offsets
    prepared["is_dst_local"] = dst_flags
    prepared["calendar_timezone"] = timezone_name
    prepared["calendar_feature_contract_version"] = (
        CALENDAR_FEATURE_CONTRACT_VERSION
    )
    return prepared

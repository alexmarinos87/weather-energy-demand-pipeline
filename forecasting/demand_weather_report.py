from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from math import floor, isfinite
from typing import Any
from uuid import uuid4

import pandas as pd


ANALYSIS_CONTRACT_VERSION = "demand-weather-analytics-v1"
GROUP_COLUMNS = ["source_area", "resource_id", "city"]
TIMESTAMP_COLUMN = "event_timestamp_utc"
DEMAND_COLUMN = "demand_mw"
TEMPERATURE_COLUMN = "temperature"
HUMIDITY_COLUMN = "humidity"
WEATHER_AGE_COLUMN = "weather_age_minutes"
REQUIRED_COLUMNS = {
    *GROUP_COLUMNS,
    TIMESTAMP_COLUMN,
    DEMAND_COLUMN,
    TEMPERATURE_COLUMN,
    HUMIDITY_COLUMN,
    WEATHER_AGE_COLUMN,
}


class DemandWeatherAnalysisError(ValueError):
    """Raised when retained feature data cannot support a trustworthy report."""


@dataclass(frozen=True)
class DemandWeatherAnalysisConfig:
    top_peak_count: int = 10
    temperature_bin_width_c: float = 5.0
    contract_version: str = ANALYSIS_CONTRACT_VERSION

    def validate(self) -> None:
        if (
            isinstance(self.top_peak_count, bool)
            or not isinstance(self.top_peak_count, int)
            or self.top_peak_count < 1
        ):
            raise DemandWeatherAnalysisError(
                "top_peak_count must be a positive integer."
            )
        width = float(self.temperature_bin_width_c)
        if not isfinite(width) or width <= 0:
            raise DemandWeatherAnalysisError(
                "temperature_bin_width_c must be finite and positive."
            )
        if not isinstance(self.contract_version, str) or not self.contract_version.strip():
            raise DemandWeatherAnalysisError("contract_version must be non-empty.")


def _required_text(frame: pd.DataFrame, column: str) -> pd.Series:
    if frame[column].isna().any():
        raise DemandWeatherAnalysisError(
            f"{column} must contain non-empty identity values."
        )
    values = frame[column].astype(str).str.strip()
    if values.eq("").any():
        raise DemandWeatherAnalysisError(
            f"{column} must contain non-empty identity values."
        )
    return values


def _utc_series(frame: pd.DataFrame, column: str) -> pd.Series:
    values: list[pd.Timestamp] = []
    for raw in frame[column]:
        try:
            timestamp = pd.Timestamp(raw)
        except (TypeError, ValueError) as exc:
            raise DemandWeatherAnalysisError(
                f"{column} must contain valid timezone-aware timestamps."
            ) from exc
        if pd.isna(timestamp) or timestamp.tzinfo is None:
            raise DemandWeatherAnalysisError(
                f"{column} must contain timezone-aware timestamps."
            )
        values.append(timestamp.tz_convert("UTC"))
    return pd.Series(values, index=frame.index)


def _numeric_series(frame: pd.DataFrame, column: str) -> pd.Series:
    values = pd.to_numeric(frame[column], errors="coerce")
    if values.isna().any() or not values.map(
        lambda value: isfinite(float(value))
    ).all():
        raise DemandWeatherAnalysisError(
            f"{column} must contain finite numeric values."
        )
    return values.astype(float)


def prepare_demand_weather_features(frame: pd.DataFrame) -> pd.DataFrame:
    """Normalize and validate retained demand/weather feature evidence."""
    missing = sorted(REQUIRED_COLUMNS - set(frame.columns))
    if missing:
        raise DemandWeatherAnalysisError(
            "Demand/weather input is missing required columns: "
            + ", ".join(missing)
            + "."
        )
    prepared = frame.copy()
    for column in GROUP_COLUMNS:
        prepared[column] = _required_text(prepared, column)
    prepared[TIMESTAMP_COLUMN] = _utc_series(prepared, TIMESTAMP_COLUMN)
    for column in (
        DEMAND_COLUMN,
        TEMPERATURE_COLUMN,
        HUMIDITY_COLUMN,
        WEATHER_AGE_COLUMN,
    ):
        prepared[column] = _numeric_series(prepared, column)
    if not prepared[HUMIDITY_COLUMN].between(0, 100, inclusive="both").all():
        raise DemandWeatherAnalysisError("humidity must be between 0 and 100.")
    if (prepared[WEATHER_AGE_COLUMN] < 0).any():
        raise DemandWeatherAnalysisError(
            "weather_age_minutes must be non-negative."
        )
    if prepared.duplicated(
        subset=[*GROUP_COLUMNS, TIMESTAMP_COLUMN], keep=False
    ).any():
        raise DemandWeatherAnalysisError(
            "Demand/weather groups contain duplicate event timestamps."
        )
    prepared = prepared.sort_values(
        [*GROUP_COLUMNS, TIMESTAMP_COLUMN]
    ).reset_index(drop=True)
    counts = prepared.groupby(GROUP_COLUMNS, dropna=False).size()
    if (counts < 2).any():
        raise DemandWeatherAnalysisError(
            "Every demand/weather group requires at least two observations."
        )
    return prepared


def _correlation(left: pd.Series, right: pd.Series) -> float | None:
    if left.nunique(dropna=True) < 2 or right.nunique(dropna=True) < 2:
        return None
    value = float(left.corr(right))
    return value if isfinite(value) else None


def _identity(first: pd.Series) -> dict[str, str]:
    return {column: str(first[column]) for column in GROUP_COLUMNS}


def _run_columns(run_id: str, run_timestamp: pd.Timestamp) -> dict[str, Any]:
    return {
        "analysis_run_id": run_id,
        "analysis_timestamp_utc": run_timestamp,
    }


def _overview(
    prepared: pd.DataFrame,
    *,
    run_id: str,
    run_timestamp: pd.Timestamp,
    contract_version: str,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for _, group in prepared.groupby(GROUP_COLUMNS, sort=True, dropna=False):
        group = group.sort_values(TIMESTAMP_COLUMN).reset_index(drop=True)
        first = group.iloc[0]
        intervals = (
            group[TIMESTAMP_COLUMN].diff().dropna().dt.total_seconds() / 60.0
        )
        if intervals.empty or (intervals <= 0).any():
            raise DemandWeatherAnalysisError(
                "Demand/weather timestamps must be strictly increasing."
            )
        rows.append(
            {
                **_run_columns(run_id, run_timestamp),
                **_identity(first),
                "observation_count": int(len(group)),
                "observation_start_utc": group[TIMESTAMP_COLUMN].min(),
                "observation_end_utc": group[TIMESTAMP_COLUMN].max(),
                "median_interval_minutes": float(intervals.median()),
                "demand_mean_mw": float(group[DEMAND_COLUMN].mean()),
                "demand_min_mw": float(group[DEMAND_COLUMN].min()),
                "demand_max_mw": float(group[DEMAND_COLUMN].max()),
                "demand_p95_mw": float(group[DEMAND_COLUMN].quantile(0.95)),
                "temperature_mean_c": float(group[TEMPERATURE_COLUMN].mean()),
                "temperature_min_c": float(group[TEMPERATURE_COLUMN].min()),
                "temperature_max_c": float(group[TEMPERATURE_COLUMN].max()),
                "humidity_mean_pct": float(group[HUMIDITY_COLUMN].mean()),
                "weather_age_mean_minutes": float(
                    group[WEATHER_AGE_COLUMN].mean()
                ),
                "weather_age_max_minutes": float(
                    group[WEATHER_AGE_COLUMN].max()
                ),
                "demand_temperature_pearson": _correlation(
                    group[DEMAND_COLUMN], group[TEMPERATURE_COLUMN]
                ),
                "demand_humidity_pearson": _correlation(
                    group[DEMAND_COLUMN], group[HUMIDITY_COLUMN]
                ),
                "analysis_contract_version": contract_version,
            }
        )
    return pd.DataFrame(rows)


def _hourly_profile(
    prepared: pd.DataFrame,
    *,
    run_id: str,
    run_timestamp: pd.Timestamp,
) -> pd.DataFrame:
    working = prepared.copy()
    working["hour_of_day_utc"] = working[TIMESTAMP_COLUMN].dt.hour.astype(int)
    rows: list[dict[str, Any]] = []
    for _, group in working.groupby(
        [*GROUP_COLUMNS, "hour_of_day_utc"], sort=True, dropna=False
    ):
        first = group.iloc[0]
        rows.append(
            {
                **_run_columns(run_id, run_timestamp),
                **_identity(first),
                "hour_of_day_utc": int(first["hour_of_day_utc"]),
                "observation_count": int(len(group)),
                "demand_mean_mw": float(group[DEMAND_COLUMN].mean()),
                "demand_p50_mw": float(group[DEMAND_COLUMN].quantile(0.50)),
                "demand_p95_mw": float(group[DEMAND_COLUMN].quantile(0.95)),
                "demand_min_mw": float(group[DEMAND_COLUMN].min()),
                "demand_max_mw": float(group[DEMAND_COLUMN].max()),
                "temperature_mean_c": float(group[TEMPERATURE_COLUMN].mean()),
                "humidity_mean_pct": float(group[HUMIDITY_COLUMN].mean()),
            }
        )
    return pd.DataFrame(rows)


def _temperature_profile(
    prepared: pd.DataFrame,
    *,
    run_id: str,
    run_timestamp: pd.Timestamp,
    bin_width: float,
) -> pd.DataFrame:
    working = prepared.copy()
    working["temperature_bin_lower_c"] = working[TEMPERATURE_COLUMN].map(
        lambda value: floor(float(value) / bin_width) * bin_width
    )
    working["temperature_bin_upper_c"] = (
        working["temperature_bin_lower_c"] + bin_width
    )
    rows: list[dict[str, Any]] = []
    for _, group in working.groupby(
        [*GROUP_COLUMNS, "temperature_bin_lower_c", "temperature_bin_upper_c"],
        sort=True,
        dropna=False,
    ):
        first = group.iloc[0]
        lower = float(first["temperature_bin_lower_c"])
        upper = float(first["temperature_bin_upper_c"])
        rows.append(
            {
                **_run_columns(run_id, run_timestamp),
                **_identity(first),
                "temperature_bin_lower_c": lower,
                "temperature_bin_upper_c": upper,
                "temperature_bin_label": f"[{lower:g}, {upper:g})",
                "observation_count": int(len(group)),
                "demand_mean_mw": float(group[DEMAND_COLUMN].mean()),
                "demand_p50_mw": float(group[DEMAND_COLUMN].quantile(0.50)),
                "demand_p95_mw": float(group[DEMAND_COLUMN].quantile(0.95)),
                "demand_min_mw": float(group[DEMAND_COLUMN].min()),
                "demand_max_mw": float(group[DEMAND_COLUMN].max()),
                "humidity_mean_pct": float(group[HUMIDITY_COLUMN].mean()),
            }
        )
    return pd.DataFrame(rows)


def _peak_events(
    prepared: pd.DataFrame,
    *,
    run_id: str,
    run_timestamp: pd.Timestamp,
    top_peak_count: int,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for _, group in prepared.groupby(GROUP_COLUMNS, sort=True, dropna=False):
        group_mean = float(group[DEMAND_COLUMN].mean())
        ranked = group.sort_values(
            [DEMAND_COLUMN, TIMESTAMP_COLUMN], ascending=[False, True]
        ).head(top_peak_count)
        for rank, (_, source) in enumerate(ranked.iterrows(), start=1):
            rows.append(
                {
                    **_run_columns(run_id, run_timestamp),
                    **_identity(source),
                    "peak_rank": rank,
                    TIMESTAMP_COLUMN: source[TIMESTAMP_COLUMN],
                    "demand_mw": float(source[DEMAND_COLUMN]),
                    "demand_above_group_mean_mw": float(
                        source[DEMAND_COLUMN] - group_mean
                    ),
                    "temperature_c": float(source[TEMPERATURE_COLUMN]),
                    "humidity_pct": float(source[HUMIDITY_COLUMN]),
                    "weather_age_minutes": float(source[WEATHER_AGE_COLUMN]),
                }
            )
    return pd.DataFrame(rows)


def render_demand_weather_markdown(
    overview: pd.DataFrame,
    peak_events: pd.DataFrame,
) -> str:
    """Render a descriptive Markdown report without making causal claims."""
    if overview.empty or peak_events.empty:
        raise DemandWeatherAnalysisError(
            "Overview and peak-event evidence must not be empty."
        )
    run_id = str(overview.iloc[0]["analysis_run_id"])
    run_timestamp = pd.Timestamp(
        overview.iloc[0]["analysis_timestamp_utc"]
    ).isoformat()
    lines = [
        "# Demand and weather analysis",
        "",
        f"- Analysis run: `{run_id}`",
        f"- Generated at: `{run_timestamp}`",
        f"- Contract: `{ANALYSIS_CONTRACT_VERSION}`",
        "",
        "This report is descriptive. Correlation does not establish causation, and each city is a representative project weather proxy for its source area.",
        "",
    ]
    for _, row in overview.sort_values(GROUP_COLUMNS).iterrows():
        identity = (
            f"{row['source_area']} / {row['resource_id']} / {row['city']}"
        )
        temperature_correlation = row["demand_temperature_pearson"]
        humidity_correlation = row["demand_humidity_pearson"]
        temperature_text = (
            "not estimable"
            if pd.isna(temperature_correlation)
            else f"{float(temperature_correlation):.3f}"
        )
        humidity_text = (
            "not estimable"
            if pd.isna(humidity_correlation)
            else f"{float(humidity_correlation):.3f}"
        )
        lines.extend(
            [
                f"## {identity}",
                "",
                f"- Observations: **{int(row['observation_count'])}** from `{pd.Timestamp(row['observation_start_utc']).isoformat()}` to `{pd.Timestamp(row['observation_end_utc']).isoformat()}`.",
                f"- Median source interval: **{float(row['median_interval_minutes']):.1f} minutes**.",
                f"- Demand: mean **{float(row['demand_mean_mw']):.2f} MW**, p95 **{float(row['demand_p95_mw']):.2f} MW**, maximum **{float(row['demand_max_mw']):.2f} MW**.",
                f"- Temperature: mean **{float(row['temperature_mean_c']):.2f}°C**, range **{float(row['temperature_min_c']):.2f}–{float(row['temperature_max_c']):.2f}°C**.",
                f"- Pearson correlation with demand: temperature **{temperature_text}**, humidity **{humidity_text}**.",
                "",
                "### Highest-demand observations",
                "",
                "| Rank | Timestamp (UTC) | Demand MW | Temperature °C | Humidity % | Weather age min |",
                "| ---: | --- | ---: | ---: | ---: | ---: |",
            ]
        )
        peaks = peak_events.loc[
            (peak_events["source_area"] == row["source_area"])
            & (peak_events["resource_id"] == row["resource_id"])
            & (peak_events["city"] == row["city"])
        ].sort_values("peak_rank")
        for _, peak in peaks.iterrows():
            lines.append(
                f"| {int(peak['peak_rank'])} | {pd.Timestamp(peak[TIMESTAMP_COLUMN]).isoformat()} | {float(peak['demand_mw']):.2f} | {float(peak['temperature_c']):.2f} | {float(peak['humidity_pct']):.2f} | {float(peak['weather_age_minutes']):.2f} |"
            )
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def build_demand_weather_analysis(
    frame: pd.DataFrame,
    *,
    config: DemandWeatherAnalysisConfig | None = None,
    run_id: str | None = None,
    run_timestamp: Any | None = None,
) -> dict[str, Any]:
    """Build reproducible overview, profile, peak, and Markdown evidence."""
    config = config or DemandWeatherAnalysisConfig()
    config.validate()
    prepared = prepare_demand_weather_features(frame)
    run_id = run_id or "dwa-" + uuid4().hex[:24]
    if not isinstance(run_id, str) or not run_id.startswith("dwa-"):
        raise DemandWeatherAnalysisError("run_id must use the dwa- prefix.")
    timestamp = pd.Timestamp(run_timestamp or datetime.now(timezone.utc))
    if timestamp.tzinfo is None:
        raise DemandWeatherAnalysisError("run_timestamp must be timezone-aware.")
    timestamp = timestamp.tz_convert("UTC")
    overview = _overview(
        prepared,
        run_id=run_id,
        run_timestamp=timestamp,
        contract_version=config.contract_version,
    )
    hourly = _hourly_profile(
        prepared, run_id=run_id, run_timestamp=timestamp
    )
    temperature = _temperature_profile(
        prepared,
        run_id=run_id,
        run_timestamp=timestamp,
        bin_width=float(config.temperature_bin_width_c),
    )
    peaks = _peak_events(
        prepared,
        run_id=run_id,
        run_timestamp=timestamp,
        top_peak_count=config.top_peak_count,
    )
    return {
        "prepared_features": prepared,
        "overview": overview,
        "hourly_load_profile": hourly,
        "temperature_demand_profile": temperature,
        "peak_demand_events": peaks,
        "markdown": render_demand_weather_markdown(overview, peaks),
    }

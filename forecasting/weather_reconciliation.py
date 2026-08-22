from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from math import sqrt
from typing import Any
from uuid import uuid4

import pandas as pd


SOURCE_AREA_COLUMN = "source_area"
CITY_COLUMN = "city"
FORECAST_ISSUED_COLUMN = "forecast_issued_at_utc"
FORECAST_INGESTED_COLUMN = "forecast_ingested_at_utc"
FORECAST_VALID_COLUMN = "forecast_valid_at_utc"
FORECAST_TEMPERATURE_COLUMN = "forecast_temperature_c"
FORECAST_HUMIDITY_COLUMN = "forecast_humidity_pct"
FORECAST_PROVIDER_COLUMN = "forecast_provider"
FORECAST_MODEL_COLUMN = "forecast_model"
OBSERVED_EVENT_COLUMN = "event_timestamp_utc"
OBSERVED_INGESTED_COLUMN = "ingestion_timestamp_utc"
OBSERVED_TEMPERATURE_COLUMN = "temperature_c"
OBSERVED_HUMIDITY_COLUMN = "humidity_pct"
RECONCILIATION_CONTRACT_VERSION = "forecast-observation-reconciliation-v1"

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
REQUIRED_OBSERVED_COLUMNS = (
    SOURCE_AREA_COLUMN,
    CITY_COLUMN,
    OBSERVED_EVENT_COLUMN,
    OBSERVED_INGESTED_COLUMN,
    OBSERVED_TEMPERATURE_COLUMN,
    OBSERVED_HUMIDITY_COLUMN,
)


class ForecastWeatherReconciliationError(ValueError):
    """Raised when forecast-versus-observed evidence is unsafe or incomplete."""


@dataclass(frozen=True)
class ForecastWeatherReconciliationConfig:
    observation_tolerance_minutes: int = 90
    min_coverage: float = 0.80
    contract_version: str = RECONCILIATION_CONTRACT_VERSION

    def validate(self) -> None:
        if (
            isinstance(self.observation_tolerance_minutes, bool)
            or not isinstance(self.observation_tolerance_minutes, int)
            or self.observation_tolerance_minutes < 0
        ):
            raise ForecastWeatherReconciliationError(
                "observation_tolerance_minutes must be a non-negative integer."
            )
        if not 0 < self.min_coverage <= 1:
            raise ForecastWeatherReconciliationError(
                "min_coverage must be greater than 0 and at most 1."
            )
        if not isinstance(self.contract_version, str) or not self.contract_version.strip():
            raise ForecastWeatherReconciliationError(
                "contract_version must be non-empty."
            )


def _utc_series(frame: pd.DataFrame, column: str) -> pd.Series:
    def parse(value: Any) -> pd.Timestamp:
        try:
            timestamp = pd.Timestamp(value)
        except (TypeError, ValueError) as exc:
            raise ForecastWeatherReconciliationError(
                f"{column} must contain valid timezone-aware timestamps."
            ) from exc
        if pd.isna(timestamp):
            raise ForecastWeatherReconciliationError(
                f"{column} must not contain null timestamps."
            )
        if timestamp.tzinfo is None:
            raise ForecastWeatherReconciliationError(
                f"{column} must contain timezone-aware timestamps."
            )
        return timestamp.tz_convert("UTC")

    return frame[column].map(parse)


def _numeric_series(frame: pd.DataFrame, column: str) -> pd.Series:
    parsed = pd.to_numeric(frame[column], errors="coerce")
    if parsed.isna().any():
        raise ForecastWeatherReconciliationError(
            f"{column} must contain finite numeric values."
        )
    finite = parsed.map(
        lambda value: pd.notna(value)
        and float("-inf") < float(value) < float("inf")
    )
    if not finite.all():
        raise ForecastWeatherReconciliationError(
            f"{column} must contain finite numeric values."
        )
    return parsed.astype(float)


def _identity_series(frame: pd.DataFrame, column: str) -> pd.Series:
    if frame[column].isna().any():
        raise ForecastWeatherReconciliationError(
            f"{column} must contain non-empty identity values."
        )
    values = frame[column].astype(str).str.strip()
    if values.eq("").any():
        raise ForecastWeatherReconciliationError(
            f"{column} must contain non-empty identity values."
        )
    return values


def prepare_forecast_reconciliation_input(frame: pd.DataFrame) -> pd.DataFrame:
    """Normalize provider-neutral forecast evidence for retrospective scoring."""
    missing = sorted(set(REQUIRED_FORECAST_COLUMNS) - set(frame.columns))
    if missing:
        raise ForecastWeatherReconciliationError(
            "Forecast input is missing required columns: " + ", ".join(missing) + "."
        )
    prepared = frame.copy()
    for column in (
        SOURCE_AREA_COLUMN,
        CITY_COLUMN,
        FORECAST_PROVIDER_COLUMN,
        FORECAST_MODEL_COLUMN,
    ):
        prepared[column] = _identity_series(prepared, column)
    for column in (
        FORECAST_ISSUED_COLUMN,
        FORECAST_INGESTED_COLUMN,
        FORECAST_VALID_COLUMN,
    ):
        prepared[column] = _utc_series(prepared, column)
    prepared[FORECAST_TEMPERATURE_COLUMN] = _numeric_series(
        prepared, FORECAST_TEMPERATURE_COLUMN
    )
    prepared[FORECAST_HUMIDITY_COLUMN] = _numeric_series(
        prepared, FORECAST_HUMIDITY_COLUMN
    )
    if not prepared[FORECAST_HUMIDITY_COLUMN].between(
        0, 100, inclusive="both"
    ).all():
        raise ForecastWeatherReconciliationError(
            "forecast_humidity_pct must be between 0 and 100."
        )
    if not (
        (prepared[FORECAST_ISSUED_COLUMN] <= prepared[FORECAST_INGESTED_COLUMN])
        & (prepared[FORECAST_INGESTED_COLUMN] < prepared[FORECAST_VALID_COLUMN])
    ).all():
        raise ForecastWeatherReconciliationError(
            "Forecast issue, ingestion, and valid timestamps are misordered."
        )

    identity_columns = [
        SOURCE_AREA_COLUMN,
        CITY_COLUMN,
        FORECAST_PROVIDER_COLUMN,
        FORECAST_MODEL_COLUMN,
        FORECAST_ISSUED_COLUMN,
        FORECAST_VALID_COLUMN,
    ]
    if "raw_snapshot_id" in prepared.columns:
        identity_columns.append("raw_snapshot_id")
    if prepared.duplicated(subset=identity_columns, keep=False).any():
        raise ForecastWeatherReconciliationError(
            "Forecast input contains duplicate issue/valid identities."
        )
    prepared["_source_area_key"] = prepared[SOURCE_AREA_COLUMN].str.casefold()
    prepared["_city_key"] = prepared[CITY_COLUMN].str.casefold()
    return prepared.sort_values(
        [
            "_source_area_key",
            "_city_key",
            FORECAST_PROVIDER_COLUMN,
            FORECAST_MODEL_COLUMN,
            FORECAST_VALID_COLUMN,
            FORECAST_ISSUED_COLUMN,
        ]
    ).reset_index(drop=True)


def prepare_observed_weather_input(frame: pd.DataFrame) -> pd.DataFrame:
    """Normalize silver observed-weather evidence and deduplicate event identities."""
    missing = sorted(set(REQUIRED_OBSERVED_COLUMNS) - set(frame.columns))
    if missing:
        raise ForecastWeatherReconciliationError(
            "Observed-weather input is missing required columns: "
            + ", ".join(missing)
            + "."
        )
    prepared = frame.copy()
    for column in (SOURCE_AREA_COLUMN, CITY_COLUMN):
        prepared[column] = _identity_series(prepared, column)
    for column in (OBSERVED_EVENT_COLUMN, OBSERVED_INGESTED_COLUMN):
        prepared[column] = _utc_series(prepared, column)
    prepared[OBSERVED_TEMPERATURE_COLUMN] = _numeric_series(
        prepared, OBSERVED_TEMPERATURE_COLUMN
    )
    prepared[OBSERVED_HUMIDITY_COLUMN] = _numeric_series(
        prepared, OBSERVED_HUMIDITY_COLUMN
    )
    if not prepared[OBSERVED_HUMIDITY_COLUMN].between(
        0, 100, inclusive="both"
    ).all():
        raise ForecastWeatherReconciliationError(
            "humidity_pct must be between 0 and 100."
        )
    if not (
        prepared[OBSERVED_INGESTED_COLUMN] >= prepared[OBSERVED_EVENT_COLUMN]
    ).all():
        raise ForecastWeatherReconciliationError(
            "Observed-weather ingestion must not precede its event timestamp."
        )
    prepared["_source_area_key"] = prepared[SOURCE_AREA_COLUMN].str.casefold()
    prepared["_city_key"] = prepared[CITY_COLUMN].str.casefold()
    if "source_file" not in prepared.columns:
        prepared["source_file"] = ""
    prepared["source_file"] = prepared["source_file"].fillna("").astype(str)
    prepared = prepared.sort_values(
        [
            "_source_area_key",
            "_city_key",
            OBSERVED_EVENT_COLUMN,
            OBSERVED_INGESTED_COLUMN,
            "source_file",
        ]
    )
    prepared = prepared.drop_duplicates(
        subset=["_source_area_key", "_city_key", OBSERVED_EVENT_COLUMN],
        keep="last",
    )
    return prepared.reset_index(drop=True)


def _lead_time_bucket(minutes: float) -> str:
    if minutes < 0:
        raise ForecastWeatherReconciliationError(
            "Forecast lead time must not be negative."
        )
    if minutes < 360:
        return "00-06h"
    if minutes < 720:
        return "06-12h"
    if minutes < 1440:
        return "12-24h"
    if minutes < 2880:
        return "24-48h"
    return "48h+"


def _optional(row: pd.Series, column: str) -> Any:
    value = row.get(column)
    return None if pd.isna(value) else value


def _group_description(group: pd.DataFrame) -> str:
    first = group.iloc[0]
    return (
        f"{first[SOURCE_AREA_COLUMN]}/{first[CITY_COLUMN]}/"
        f"{first[FORECAST_PROVIDER_COLUMN]}/{first[FORECAST_MODEL_COLUMN]}/"
        f"{first['forecast_lead_time_bucket']}"
    )


def reconcile_forecast_weather(
    forecast_weather: pd.DataFrame,
    observed_weather: pd.DataFrame,
    *,
    config: ForecastWeatherReconciliationConfig | None = None,
    run_id: str | None = None,
    run_timestamp: datetime | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Reconcile mature forecast slots with bounded same-area observed weather."""
    config = config or ForecastWeatherReconciliationConfig()
    config.validate()
    forecast = prepare_forecast_reconciliation_input(forecast_weather)
    observed = prepare_observed_weather_input(observed_weather)
    run_id = run_id or str(uuid4())
    run_timestamp = run_timestamp or datetime.now(timezone.utc)
    if run_timestamp.tzinfo is None:
        raise ForecastWeatherReconciliationError(
            "run_timestamp must be timezone-aware."
        )
    run_timestamp = run_timestamp.astimezone(timezone.utc)
    tolerance = pd.Timedelta(minutes=config.observation_tolerance_minutes)

    observed_groups = {
        key if isinstance(key, tuple) else (key,): group.reset_index(drop=True)
        for key, group in observed.groupby(
            ["_source_area_key", "_city_key"], sort=True, dropna=False
        )
    }
    evidence_rows: list[dict[str, Any]] = []

    for _, forecast_group in forecast.groupby(
        [
            "_source_area_key",
            "_city_key",
            FORECAST_PROVIDER_COLUMN,
            FORECAST_MODEL_COLUMN,
        ],
        sort=True,
        dropna=False,
    ):
        group_key = (
            forecast_group.iloc[0]["_source_area_key"],
            forecast_group.iloc[0]["_city_key"],
        )
        observed_group = observed_groups.get(group_key)
        if observed_group is None or observed_group.empty:
            mature = forecast_group.copy()
        else:
            observed_min = observed_group[OBSERVED_EVENT_COLUMN].min()
            observed_max = observed_group[OBSERVED_EVENT_COLUMN].max()
            mature = forecast_group.loc[
                forecast_group[FORECAST_VALID_COLUMN].between(
                    observed_min, observed_max, inclusive="both"
                )
            ].copy()
        for _, forecast_row in mature.iterrows():
            issued = forecast_row[FORECAST_ISSUED_COLUMN]
            ingested = forecast_row[FORECAST_INGESTED_COLUMN]
            valid = forecast_row[FORECAST_VALID_COLUMN]
            provider_lead = (valid - issued).total_seconds() / 60.0
            pipeline_lead = (valid - ingested).total_seconds() / 60.0
            base = {
                "reconciliation_run_id": run_id,
                "reconciliation_run_timestamp_utc": run_timestamp,
                SOURCE_AREA_COLUMN: forecast_row[SOURCE_AREA_COLUMN],
                CITY_COLUMN: forecast_row[CITY_COLUMN],
                FORECAST_PROVIDER_COLUMN: forecast_row[FORECAST_PROVIDER_COLUMN],
                FORECAST_MODEL_COLUMN: forecast_row[FORECAST_MODEL_COLUMN],
                "forecast_issue_basis": _optional(
                    forecast_row, "forecast_issue_basis"
                ),
                "raw_snapshot_id": _optional(forecast_row, "raw_snapshot_id"),
                "forecast_provider_record_id": _optional(
                    forecast_row, "forecast_provider_record_id"
                ),
                FORECAST_ISSUED_COLUMN: issued,
                FORECAST_INGESTED_COLUMN: ingested,
                FORECAST_VALID_COLUMN: valid,
                FORECAST_TEMPERATURE_COLUMN: float(
                    forecast_row[FORECAST_TEMPERATURE_COLUMN]
                ),
                FORECAST_HUMIDITY_COLUMN: float(
                    forecast_row[FORECAST_HUMIDITY_COLUMN]
                ),
                "forecast_provider_lead_minutes": provider_lead,
                "forecast_pipeline_lead_minutes": pipeline_lead,
                "forecast_lead_time_bucket": _lead_time_bucket(pipeline_lead),
                "observation_tolerance_minutes": (
                    config.observation_tolerance_minutes
                ),
                "reconciliation_contract_version": config.contract_version,
            }
            candidates = pd.DataFrame()
            if observed_group is not None and not observed_group.empty:
                candidates = observed_group.copy()
                candidates["_signed_delta"] = (
                    candidates[OBSERVED_EVENT_COLUMN] - valid
                )
                candidates["_absolute_delta"] = candidates["_signed_delta"].abs()
                candidates = candidates.loc[
                    candidates["_absolute_delta"] <= tolerance
                ]
            if candidates.empty:
                base.update(
                    {
                        "reconciliation_status": "unmatched",
                        "observation_event_timestamp_utc": None,
                        "observation_ingested_at_utc": None,
                        "observation_source_file": None,
                        "observed_temperature_c": None,
                        "observed_humidity_pct": None,
                        "observation_valid_time_delta_minutes": None,
                        "observation_absolute_valid_time_delta_minutes": None,
                        "observation_ingestion_lag_minutes": None,
                        "temperature_error_c": None,
                        "temperature_absolute_error_c": None,
                        "temperature_squared_error_c2": None,
                        "humidity_error_pct": None,
                        "humidity_absolute_error_pct": None,
                        "humidity_squared_error_pct2": None,
                    }
                )
                evidence_rows.append(base)
                continue

            candidates["_after_valid"] = candidates[OBSERVED_EVENT_COLUMN] > valid
            candidates = candidates.sort_values(
                [
                    "_absolute_delta",
                    "_after_valid",
                    OBSERVED_INGESTED_COLUMN,
                    "source_file",
                ],
                ascending=[True, True, False, True],
            )
            matched = candidates.iloc[0]
            observed_temperature = float(matched[OBSERVED_TEMPERATURE_COLUMN])
            observed_humidity = float(matched[OBSERVED_HUMIDITY_COLUMN])
            temperature_error = (
                float(forecast_row[FORECAST_TEMPERATURE_COLUMN])
                - observed_temperature
            )
            humidity_error = (
                float(forecast_row[FORECAST_HUMIDITY_COLUMN]) - observed_humidity
            )
            signed_delta_minutes = (
                matched[OBSERVED_EVENT_COLUMN] - valid
            ).total_seconds() / 60.0
            ingestion_lag = (
                matched[OBSERVED_INGESTED_COLUMN]
                - matched[OBSERVED_EVENT_COLUMN]
            ).total_seconds() / 60.0
            base.update(
                {
                    "reconciliation_status": "matched",
                    "observation_event_timestamp_utc": matched[
                        OBSERVED_EVENT_COLUMN
                    ],
                    "observation_ingested_at_utc": matched[
                        OBSERVED_INGESTED_COLUMN
                    ],
                    "observation_source_file": matched["source_file"] or None,
                    "observed_temperature_c": observed_temperature,
                    "observed_humidity_pct": observed_humidity,
                    "observation_valid_time_delta_minutes": signed_delta_minutes,
                    "observation_absolute_valid_time_delta_minutes": abs(
                        signed_delta_minutes
                    ),
                    "observation_ingestion_lag_minutes": ingestion_lag,
                    "temperature_error_c": temperature_error,
                    "temperature_absolute_error_c": abs(temperature_error),
                    "temperature_squared_error_c2": temperature_error**2,
                    "humidity_error_pct": humidity_error,
                    "humidity_absolute_error_pct": abs(humidity_error),
                    "humidity_squared_error_pct2": humidity_error**2,
                }
            )
            evidence_rows.append(base)

    if not evidence_rows:
        raise ForecastWeatherReconciliationError(
            "No forecast slots are mature within retained observed-weather history."
        )
    evidence = pd.DataFrame(evidence_rows)
    metric_rows: list[dict[str, Any]] = []
    metric_group_columns = [
        SOURCE_AREA_COLUMN,
        CITY_COLUMN,
        FORECAST_PROVIDER_COLUMN,
        FORECAST_MODEL_COLUMN,
        "forecast_issue_basis",
        "forecast_lead_time_bucket",
    ]
    failures: list[str] = []
    for _, group in evidence.groupby(
        metric_group_columns, sort=True, dropna=False
    ):
        matched = group.loc[group["reconciliation_status"] == "matched"]
        eligible_count = len(group)
        matched_count = len(matched)
        coverage = matched_count / eligible_count if eligible_count else 0.0
        if coverage < config.min_coverage:
            failures.append(
                f"{_group_description(group)} matched "
                f"{matched_count}/{eligible_count} ({coverage:.1%})"
            )
        first = group.iloc[0]
        metric: dict[str, Any] = {
            "reconciliation_run_id": run_id,
            "reconciliation_run_timestamp_utc": run_timestamp,
            SOURCE_AREA_COLUMN: first[SOURCE_AREA_COLUMN],
            CITY_COLUMN: first[CITY_COLUMN],
            FORECAST_PROVIDER_COLUMN: first[FORECAST_PROVIDER_COLUMN],
            FORECAST_MODEL_COLUMN: first[FORECAST_MODEL_COLUMN],
            "forecast_issue_basis": first["forecast_issue_basis"],
            "forecast_lead_time_bucket": first["forecast_lead_time_bucket"],
            "eligible_forecast_count": eligible_count,
            "matched_forecast_count": matched_count,
            "forecast_observation_coverage_pct": coverage * 100.0,
            "minimum_forecast_observation_coverage_pct": (
                config.min_coverage * 100.0
            ),
            "observation_tolerance_minutes": (
                config.observation_tolerance_minutes
            ),
            "forecast_provider_lead_minutes_avg": float(
                group["forecast_provider_lead_minutes"].mean()
            ),
            "forecast_pipeline_lead_minutes_avg": float(
                group["forecast_pipeline_lead_minutes"].mean()
            ),
            "forecast_valid_start_utc": group[FORECAST_VALID_COLUMN].min(),
            "forecast_valid_end_utc": group[FORECAST_VALID_COLUMN].max(),
            "reconciliation_contract_version": config.contract_version,
        }
        if matched.empty:
            metric.update(
                {
                    "temperature_mae_c": None,
                    "temperature_rmse_c": None,
                    "temperature_bias_c": None,
                    "humidity_mae_pct": None,
                    "humidity_rmse_pct": None,
                    "humidity_bias_pct": None,
                    "observation_valid_time_delta_minutes_avg": None,
                    "observation_absolute_valid_time_delta_minutes_avg": None,
                    "observation_absolute_valid_time_delta_minutes_max": None,
                    "observation_ingestion_lag_minutes_avg": None,
                    "observation_ingestion_lag_minutes_max": None,
                }
            )
        else:
            metric.update(
                {
                    "temperature_mae_c": float(
                        matched["temperature_absolute_error_c"].mean()
                    ),
                    "temperature_rmse_c": float(
                        sqrt(matched["temperature_squared_error_c2"].mean())
                    ),
                    "temperature_bias_c": float(
                        matched["temperature_error_c"].mean()
                    ),
                    "humidity_mae_pct": float(
                        matched["humidity_absolute_error_pct"].mean()
                    ),
                    "humidity_rmse_pct": float(
                        sqrt(matched["humidity_squared_error_pct2"].mean())
                    ),
                    "humidity_bias_pct": float(
                        matched["humidity_error_pct"].mean()
                    ),
                    "observation_valid_time_delta_minutes_avg": float(
                        matched["observation_valid_time_delta_minutes"].mean()
                    ),
                    "observation_absolute_valid_time_delta_minutes_avg": float(
                        matched[
                            "observation_absolute_valid_time_delta_minutes"
                        ].mean()
                    ),
                    "observation_absolute_valid_time_delta_minutes_max": float(
                        matched[
                            "observation_absolute_valid_time_delta_minutes"
                        ].max()
                    ),
                    "observation_ingestion_lag_minutes_avg": float(
                        matched["observation_ingestion_lag_minutes"].mean()
                    ),
                    "observation_ingestion_lag_minutes_max": float(
                        matched["observation_ingestion_lag_minutes"].max()
                    ),
                }
            )
        metric_rows.append(metric)

    metrics = pd.DataFrame(metric_rows)
    if failures:
        raise ForecastWeatherReconciliationError(
            "Forecast-versus-observed coverage is below the configured minimum: "
            + "; ".join(failures[:5])
        )
    return (
        evidence.sort_values(
            [
                SOURCE_AREA_COLUMN,
                CITY_COLUMN,
                FORECAST_PROVIDER_COLUMN,
                FORECAST_MODEL_COLUMN,
                FORECAST_VALID_COLUMN,
            ]
        ).reset_index(drop=True),
        metrics.sort_values(metric_group_columns).reset_index(drop=True),
    )

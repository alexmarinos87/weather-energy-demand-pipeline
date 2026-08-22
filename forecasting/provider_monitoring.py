from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from math import isfinite
from typing import Any
from uuid import uuid4

import pandas as pd


POLICY_VERSION = "forecast-provider-health-policy-v1"
MONITORING_VERSION = "forecast-provider-monitoring-v1"
HEALTHY_STATUS = "healthy"
WARNING_STATUS = "warning"
FAILED_STATUS = "failed"
SUPPORTED_LEAD_BUCKETS = ("00-06h", "06-12h", "12-24h", "24-48h", "48h+")

FORECAST_REQUIRED = {
    "source_area",
    "city",
    "forecast_issued_at_utc",
    "forecast_ingested_at_utc",
    "forecast_valid_at_utc",
    "forecast_provider",
    "forecast_model",
    "raw_snapshot_id",
}
RECONCILIATION_REQUIRED = {
    "reconciliation_run_id",
    "reconciliation_run_timestamp_utc",
    "source_area",
    "city",
    "forecast_provider",
    "forecast_model",
    "forecast_lead_time_bucket",
    "eligible_forecast_count",
    "matched_forecast_count",
    "forecast_observation_coverage_pct",
    "temperature_mae_c",
    "humidity_mae_pct",
}


class ForecastProviderMonitoringError(ValueError):
    """Raised when provider health evidence cannot be interpreted safely."""


@dataclass(frozen=True)
class ForecastProviderMonitoringConfig:
    max_forecast_ingestion_age_minutes: int = 240
    max_snapshot_gap_minutes: int = 360
    min_slots_per_latest_snapshot: int = 8
    min_latest_snapshot_horizon_minutes: int = 1440
    recent_reconciliation_run_count: int = 3
    reference_reconciliation_run_count: int = 6
    min_recent_reconciliation_runs: int = 2
    min_reference_reconciliation_runs: int = 3
    max_reconciliation_age_minutes: int = 1440
    min_reconciliation_coverage_pct: float = 90.0
    max_temperature_mae_c: float = 2.5
    max_humidity_mae_pct: float = 15.0
    max_coverage_drop_pct_points: float = 5.0
    max_temperature_mae_increase_c: float = 0.5
    max_humidity_mae_increase_pct: float = 3.0
    policy_version: str = POLICY_VERSION

    def validate(self) -> None:
        integer_fields = (
            "max_forecast_ingestion_age_minutes",
            "max_snapshot_gap_minutes",
            "min_slots_per_latest_snapshot",
            "min_latest_snapshot_horizon_minutes",
            "recent_reconciliation_run_count",
            "reference_reconciliation_run_count",
            "min_recent_reconciliation_runs",
            "min_reference_reconciliation_runs",
            "max_reconciliation_age_minutes",
        )
        for name in integer_fields:
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ForecastProviderMonitoringError(
                    f"{name} must be a positive integer."
                )
        if self.min_recent_reconciliation_runs > self.recent_reconciliation_run_count:
            raise ForecastProviderMonitoringError(
                "min_recent_reconciliation_runs cannot exceed the recent window."
            )
        if self.min_reference_reconciliation_runs > self.reference_reconciliation_run_count:
            raise ForecastProviderMonitoringError(
                "min_reference_reconciliation_runs cannot exceed the reference window."
            )
        if not 0 < self.min_reconciliation_coverage_pct <= 100:
            raise ForecastProviderMonitoringError(
                "min_reconciliation_coverage_pct must be in (0, 100]."
            )
        non_negative_fields = (
            "max_temperature_mae_c",
            "max_humidity_mae_pct",
            "max_coverage_drop_pct_points",
            "max_temperature_mae_increase_c",
            "max_humidity_mae_increase_pct",
        )
        for name in non_negative_fields:
            value = float(getattr(self, name))
            if not isfinite(value) or value < 0:
                raise ForecastProviderMonitoringError(
                    f"{name} must be finite and non-negative."
                )
        if not str(self.policy_version).strip():
            raise ForecastProviderMonitoringError("policy_version must be non-empty.")


def _require(frame: pd.DataFrame, columns: set[str], label: str) -> None:
    missing = sorted(columns - set(frame.columns))
    if missing:
        raise ForecastProviderMonitoringError(
            f"{label} is missing required columns: {', '.join(missing)}."
        )


def _text(frame: pd.DataFrame, column: str) -> pd.Series:
    values = frame[column].fillna("").astype(str).str.strip()
    if values.eq("").any():
        raise ForecastProviderMonitoringError(
            f"{column} must contain non-empty values."
        )
    return values


def _utc(frame: pd.DataFrame, column: str) -> pd.Series:
    values: list[pd.Timestamp] = []
    for raw in frame[column]:
        try:
            timestamp = pd.Timestamp(raw)
        except (TypeError, ValueError) as exc:
            raise ForecastProviderMonitoringError(
                f"{column} must contain valid timezone-aware timestamps."
            ) from exc
        if pd.isna(timestamp) or timestamp.tzinfo is None:
            raise ForecastProviderMonitoringError(
                f"{column} must contain timezone-aware timestamps."
            )
        values.append(timestamp.tz_convert("UTC"))
    return pd.Series(values, index=frame.index)


def _number(
    frame: pd.DataFrame,
    column: str,
    *,
    minimum: float | None = None,
) -> pd.Series:
    values = pd.to_numeric(frame[column], errors="coerce")
    if values.isna().any() or not values.map(
        lambda value: isfinite(float(value))
    ).all():
        raise ForecastProviderMonitoringError(
            f"{column} must contain finite numeric values."
        )
    values = values.astype(float)
    if minimum is not None and (values < minimum).any():
        raise ForecastProviderMonitoringError(
            f"{column} must be at least {minimum}."
        )
    return values


def _as_utc(value: datetime | pd.Timestamp | str, name: str) -> pd.Timestamp:
    try:
        timestamp = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise ForecastProviderMonitoringError(
            f"{name} must be a valid timezone-aware timestamp."
        ) from exc
    if pd.isna(timestamp) or timestamp.tzinfo is None:
        raise ForecastProviderMonitoringError(
            f"{name} must be timezone-aware."
        )
    return timestamp.tz_convert("UTC")


def prepare_forecast_snapshot_evidence(frame: pd.DataFrame) -> pd.DataFrame:
    """Normalize forecast rows and build one summary row per immutable snapshot."""
    _require(frame, FORECAST_REQUIRED, "Forecast evidence")
    prepared = frame.copy()
    for column in (
        "source_area",
        "city",
        "forecast_provider",
        "forecast_model",
        "raw_snapshot_id",
    ):
        prepared[column] = _text(prepared, column)
    if "forecast_issue_basis" not in prepared.columns:
        prepared["forecast_issue_basis"] = None
    else:
        prepared["forecast_issue_basis"] = (
            prepared["forecast_issue_basis"].where(
                prepared["forecast_issue_basis"].notna(), None
            )
        )
    for column in (
        "forecast_issued_at_utc",
        "forecast_ingested_at_utc",
        "forecast_valid_at_utc",
    ):
        prepared[column] = _utc(prepared, column)
    if not (
        (prepared["forecast_issued_at_utc"] <= prepared["forecast_ingested_at_utc"])
        & (prepared["forecast_ingested_at_utc"] < prepared["forecast_valid_at_utc"])
    ).all():
        raise ForecastProviderMonitoringError(
            "Forecast issue, ingestion, and valid timestamps are misordered."
        )
    identity = [
        "source_area",
        "city",
        "forecast_provider",
        "forecast_model",
        "raw_snapshot_id",
        "forecast_valid_at_utc",
    ]
    if prepared.duplicated(subset=identity, keep=False).any():
        raise ForecastProviderMonitoringError(
            "Forecast evidence contains duplicate snapshot/valid-time rows."
        )
    prepared["_source_area_key"] = prepared["source_area"].str.casefold()
    prepared["_city_key"] = prepared["city"].str.casefold()
    prepared["_provider_key"] = prepared["forecast_provider"].str.casefold()
    prepared["_model_key"] = prepared["forecast_model"].str.casefold()
    snapshot_identity = [
        "_source_area_key",
        "_city_key",
        "_provider_key",
        "_model_key",
        "raw_snapshot_id",
    ]
    summaries: list[dict[str, Any]] = []
    for _, group in prepared.groupby(snapshot_identity, sort=True, dropna=False):
        issue_values = group["forecast_issued_at_utc"].unique()
        ingestion_values = group["forecast_ingested_at_utc"].unique()
        if len(issue_values) != 1 or len(ingestion_values) != 1:
            raise ForecastProviderMonitoringError(
                "A raw snapshot must have one issue and ingestion timestamp."
            )
        first = group.iloc[0]
        ingested = pd.Timestamp(ingestion_values[0])
        valid_start = group["forecast_valid_at_utc"].min()
        valid_end = group["forecast_valid_at_utc"].max()
        summaries.append(
            {
                "source_area": first["source_area"],
                "city": first["city"],
                "forecast_provider": first["forecast_provider"],
                "forecast_model": first["forecast_model"],
                "forecast_issue_basis": first["forecast_issue_basis"],
                "raw_snapshot_id": first["raw_snapshot_id"],
                "forecast_issued_at_utc": pd.Timestamp(issue_values[0]),
                "forecast_ingested_at_utc": ingested,
                "forecast_valid_start_utc": valid_start,
                "forecast_valid_end_utc": valid_end,
                "forecast_slot_count": int(len(group)),
                "forecast_snapshot_horizon_minutes": float(
                    (valid_end - ingested).total_seconds() / 60.0
                ),
                "_source_area_key": first["_source_area_key"],
                "_city_key": first["_city_key"],
                "_provider_key": first["_provider_key"],
                "_model_key": first["_model_key"],
            }
        )
    if not summaries:
        raise ForecastProviderMonitoringError(
            "Forecast evidence produced no snapshot summaries."
        )
    return pd.DataFrame(summaries).sort_values(
        [
            "_source_area_key",
            "_city_key",
            "_provider_key",
            "_model_key",
            "forecast_ingested_at_utc",
            "raw_snapshot_id",
        ]
    ).reset_index(drop=True)


def prepare_reconciliation_history(frame: pd.DataFrame) -> pd.DataFrame:
    """Normalize and aggregate reconciliation metrics into one row per run/slice."""
    _require(frame, RECONCILIATION_REQUIRED, "Reconciliation metrics")
    prepared = frame.copy()
    for column in (
        "reconciliation_run_id",
        "source_area",
        "city",
        "forecast_provider",
        "forecast_model",
        "forecast_lead_time_bucket",
    ):
        prepared[column] = _text(prepared, column)
    unsupported = sorted(
        set(prepared["forecast_lead_time_bucket"]) - set(SUPPORTED_LEAD_BUCKETS)
    )
    if unsupported:
        raise ForecastProviderMonitoringError(
            "Unsupported forecast lead buckets: " + ", ".join(unsupported) + "."
        )
    if "forecast_issue_basis" not in prepared.columns:
        prepared["forecast_issue_basis"] = None
    prepared["reconciliation_run_timestamp_utc"] = _utc(
        prepared, "reconciliation_run_timestamp_utc"
    )
    for column in (
        "eligible_forecast_count",
        "matched_forecast_count",
        "forecast_observation_coverage_pct",
        "temperature_mae_c",
        "humidity_mae_pct",
    ):
        prepared[column] = _number(prepared, column, minimum=0)
    if (
        (prepared["eligible_forecast_count"] < 1).any()
        or (prepared["matched_forecast_count"] < 1).any()
        or (
            prepared["matched_forecast_count"]
            > prepared["eligible_forecast_count"]
        ).any()
        or not prepared["forecast_observation_coverage_pct"].between(0, 100).all()
    ):
        raise ForecastProviderMonitoringError(
            "Reconciliation counts or coverage are invalid."
        )
    prepared["_source_area_key"] = prepared["source_area"].str.casefold()
    prepared["_city_key"] = prepared["city"].str.casefold()
    prepared["_provider_key"] = prepared["forecast_provider"].str.casefold()
    prepared["_model_key"] = prepared["forecast_model"].str.casefold()
    group_columns = [
        "_source_area_key",
        "_city_key",
        "_provider_key",
        "_model_key",
        "forecast_issue_basis",
        "forecast_lead_time_bucket",
        "reconciliation_run_id",
        "reconciliation_run_timestamp_utc",
    ]
    rows: list[dict[str, Any]] = []
    for _, group in prepared.groupby(group_columns, sort=True, dropna=False):
        first = group.iloc[0]
        eligible = float(group["eligible_forecast_count"].sum())
        matched = float(group["matched_forecast_count"].sum())
        rows.append(
            {
                "reconciliation_run_id": first["reconciliation_run_id"],
                "reconciliation_run_timestamp_utc": first[
                    "reconciliation_run_timestamp_utc"
                ],
                "source_area": first["source_area"],
                "city": first["city"],
                "forecast_provider": first["forecast_provider"],
                "forecast_model": first["forecast_model"],
                "forecast_issue_basis": first["forecast_issue_basis"],
                "forecast_lead_time_bucket": first["forecast_lead_time_bucket"],
                "eligible_forecast_count": eligible,
                "matched_forecast_count": matched,
                "forecast_observation_coverage_pct": matched / eligible * 100.0,
                "temperature_mae_c": float(
                    (group["temperature_mae_c"] * group["matched_forecast_count"]).sum()
                    / matched
                ),
                "humidity_mae_pct": float(
                    (group["humidity_mae_pct"] * group["matched_forecast_count"]).sum()
                    / matched
                ),
                "_source_area_key": first["_source_area_key"],
                "_city_key": first["_city_key"],
                "_provider_key": first["_provider_key"],
                "_model_key": first["_model_key"],
            }
        )
    if not rows:
        raise ForecastProviderMonitoringError(
            "Reconciliation metrics produced no run history."
        )
    return pd.DataFrame(rows).sort_values(
        [
            "_source_area_key",
            "_city_key",
            "_provider_key",
            "_model_key",
            "forecast_lead_time_bucket",
            "reconciliation_run_timestamp_utc",
            "reconciliation_run_id",
        ]
    ).reset_index(drop=True)


def _check(
    run_id: str,
    run_timestamp: pd.Timestamp,
    as_of: pd.Timestamp,
    scope: str,
    severity: str,
    name: str,
    observed: float,
    threshold: float,
    comparator: str,
    passed: bool,
    details: str,
    config: ForecastProviderMonitoringConfig,
    identity: dict[str, Any],
) -> dict[str, Any]:
    return {
        "monitor_run_id": run_id,
        "monitor_timestamp_utc": run_timestamp,
        "monitor_as_of_utc": as_of,
        "check_scope": scope,
        "severity": severity,
        "check_name": name,
        "observed_value": float(observed),
        "threshold_value": float(threshold),
        "comparator": comparator,
        "passed": bool(passed),
        "details": details,
        "source_area": identity["source_area"],
        "city": identity["city"],
        "forecast_provider": identity["forecast_provider"],
        "forecast_model": identity["forecast_model"],
        "forecast_issue_basis": identity.get("forecast_issue_basis"),
        "forecast_lead_time_bucket": identity.get(
            "forecast_lead_time_bucket"
        ),
        "policy_version": config.policy_version,
        "monitoring_contract_version": MONITORING_VERSION,
    }


def _window_quality(frame: pd.DataFrame) -> dict[str, float]:
    eligible = float(frame["eligible_forecast_count"].sum())
    matched = float(frame["matched_forecast_count"].sum())
    if eligible <= 0 or matched <= 0:
        raise ForecastProviderMonitoringError(
            "Reconciliation window must contain positive eligible and matched counts."
        )
    return {
        "coverage": matched / eligible * 100.0,
        "temperature_mae": float(
            (frame["temperature_mae_c"] * frame["matched_forecast_count"]).sum()
            / matched
        ),
        "humidity_mae": float(
            (frame["humidity_mae_pct"] * frame["matched_forecast_count"]).sum()
            / matched
        ),
    }


def monitor_forecast_provider_health(
    forecast_weather: pd.DataFrame,
    reconciliation_metrics: pd.DataFrame,
    *,
    config: ForecastProviderMonitoringConfig | None = None,
    as_of_utc: datetime | pd.Timestamp | str | None = None,
    run_id: str | None = None,
    run_timestamp: datetime | pd.Timestamp | str | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Evaluate provider freshness, cadence, quality, and longitudinal drift."""
    config = config or ForecastProviderMonitoringConfig()
    config.validate()
    snapshots = prepare_forecast_snapshot_evidence(forecast_weather)
    history = prepare_reconciliation_history(reconciliation_metrics)
    as_of = _as_utc(as_of_utc or datetime.now(timezone.utc), "as_of_utc")
    timestamp = _as_utc(
        run_timestamp or datetime.now(timezone.utc), "run_timestamp"
    )
    run_id = run_id or str(uuid4())
    checks: list[dict[str, Any]] = []
    forecast_identity_columns = [
        "_source_area_key",
        "_city_key",
        "_provider_key",
        "_model_key",
    ]
    for _, group in snapshots.groupby(
        forecast_identity_columns, sort=True, dropna=False
    ):
        group = group.sort_values("forecast_ingested_at_utc").reset_index(drop=True)
        first = group.iloc[-1]
        identity = {
            "source_area": first["source_area"],
            "city": first["city"],
            "forecast_provider": first["forecast_provider"],
            "forecast_model": first["forecast_model"],
            "forecast_issue_basis": first["forecast_issue_basis"],
            "forecast_lead_time_bucket": None,
        }
        latest_ingested = first["forecast_ingested_at_utc"]
        ingestion_age = (as_of - latest_ingested).total_seconds() / 60.0
        if ingestion_age < 0:
            raise ForecastProviderMonitoringError(
                "as_of_utc cannot precede a retained forecast ingestion."
            )
        values = [
            (
                "forecast_snapshot",
                "error",
                "latest_forecast_ingestion_age_minutes",
                ingestion_age,
                config.max_forecast_ingestion_age_minutes,
                "<=",
                ingestion_age <= config.max_forecast_ingestion_age_minutes,
                f"Latest forecast snapshot was ingested at {latest_ingested.isoformat()}.",
            ),
            (
                "forecast_snapshot",
                "error",
                "latest_snapshot_minimum_slot_count",
                float(first["forecast_slot_count"]),
                config.min_slots_per_latest_snapshot,
                ">=",
                first["forecast_slot_count"] >= config.min_slots_per_latest_snapshot,
                "Forecast slot count in the newest retained snapshot.",
            ),
            (
                "forecast_snapshot",
                "error",
                "latest_snapshot_minimum_horizon_minutes",
                float(first["forecast_snapshot_horizon_minutes"]),
                config.min_latest_snapshot_horizon_minutes,
                ">=",
                first["forecast_snapshot_horizon_minutes"]
                >= config.min_latest_snapshot_horizon_minutes,
                "Future horizon represented by the newest retained snapshot.",
            ),
            (
                "forecast_snapshot",
                "warning",
                "minimum_snapshot_history_for_cadence",
                float(len(group)),
                2.0,
                ">=",
                len(group) >= 2,
                "At least two retained snapshots are required to measure cadence.",
            ),
        ]
        if len(group) >= 2:
            gaps = group["forecast_ingested_at_utc"].diff().dropna().dt.total_seconds() / 60.0
            maximum_gap = float(gaps.max())
            values.append(
                (
                    "forecast_snapshot",
                    "error",
                    "maximum_snapshot_ingestion_gap_minutes",
                    maximum_gap,
                    config.max_snapshot_gap_minutes,
                    "<=",
                    maximum_gap <= config.max_snapshot_gap_minutes,
                    "Maximum ingestion gap across retained snapshots.",
                )
            )
        for scope, severity, name, observed, threshold, comparator, passed, details in values:
            checks.append(
                _check(
                    run_id,
                    timestamp,
                    as_of,
                    scope,
                    severity,
                    name,
                    observed,
                    threshold,
                    comparator,
                    passed,
                    details,
                    config,
                    identity,
                )
            )

    reconciliation_identity_columns = [
        "_source_area_key",
        "_city_key",
        "_provider_key",
        "_model_key",
        "forecast_issue_basis",
        "forecast_lead_time_bucket",
    ]
    for _, group in history.groupby(
        reconciliation_identity_columns, sort=True, dropna=False
    ):
        group = group.sort_values(
            ["reconciliation_run_timestamp_utc", "reconciliation_run_id"]
        ).reset_index(drop=True)
        recent = group.tail(config.recent_reconciliation_run_count)
        reference_end = len(group) - len(recent)
        reference_start = max(
            0, reference_end - config.reference_reconciliation_run_count
        )
        reference = group.iloc[reference_start:reference_end]
        first = group.iloc[-1]
        identity = {
            "source_area": first["source_area"],
            "city": first["city"],
            "forecast_provider": first["forecast_provider"],
            "forecast_model": first["forecast_model"],
            "forecast_issue_basis": first["forecast_issue_basis"],
            "forecast_lead_time_bucket": first["forecast_lead_time_bucket"],
        }
        latest_reconciliation = first["reconciliation_run_timestamp_utc"]
        reconciliation_age = (
            as_of - latest_reconciliation
        ).total_seconds() / 60.0
        if reconciliation_age < 0:
            raise ForecastProviderMonitoringError(
                "as_of_utc cannot precede a reconciliation run timestamp."
            )
        recent_quality = _window_quality(recent)
        values = [
            (
                "reconciliation",
                "error",
                "minimum_recent_reconciliation_runs",
                float(len(recent)),
                config.min_recent_reconciliation_runs,
                ">=",
                len(recent) >= config.min_recent_reconciliation_runs,
                "Number of reconciliation runs in the recent window.",
            ),
            (
                "reconciliation",
                "error",
                "latest_reconciliation_age_minutes",
                reconciliation_age,
                config.max_reconciliation_age_minutes,
                "<=",
                reconciliation_age <= config.max_reconciliation_age_minutes,
                f"Latest reconciliation run was {latest_reconciliation.isoformat()}.",
            ),
            (
                "reconciliation",
                "error",
                "minimum_recent_reconciliation_coverage_pct",
                recent_quality["coverage"],
                config.min_reconciliation_coverage_pct,
                ">=",
                recent_quality["coverage"]
                >= config.min_reconciliation_coverage_pct,
                "Matched-count coverage across the recent reconciliation window.",
            ),
            (
                "reconciliation",
                "error",
                "maximum_recent_temperature_mae_c",
                recent_quality["temperature_mae"],
                config.max_temperature_mae_c,
                "<=",
                recent_quality["temperature_mae"]
                <= config.max_temperature_mae_c,
                "Matched-count-weighted temperature MAE in the recent window.",
            ),
            (
                "reconciliation",
                "error",
                "maximum_recent_humidity_mae_pct",
                recent_quality["humidity_mae"],
                config.max_humidity_mae_pct,
                "<=",
                recent_quality["humidity_mae"] <= config.max_humidity_mae_pct,
                "Matched-count-weighted humidity MAE in the recent window.",
            ),
            (
                "drift",
                "warning",
                "minimum_reference_reconciliation_runs",
                float(len(reference)),
                config.min_reference_reconciliation_runs,
                ">=",
                len(reference) >= config.min_reference_reconciliation_runs,
                "Reference history required before calculating longitudinal drift.",
            ),
        ]
        if len(reference) >= config.min_reference_reconciliation_runs:
            reference_quality = _window_quality(reference)
            coverage_drop = (
                reference_quality["coverage"] - recent_quality["coverage"]
            )
            temperature_increase = (
                recent_quality["temperature_mae"]
                - reference_quality["temperature_mae"]
            )
            humidity_increase = (
                recent_quality["humidity_mae"]
                - reference_quality["humidity_mae"]
            )
            values.extend(
                [
                    (
                        "drift",
                        "warning",
                        "maximum_reconciliation_coverage_drop_pct_points",
                        coverage_drop,
                        config.max_coverage_drop_pct_points,
                        "<=",
                        coverage_drop <= config.max_coverage_drop_pct_points,
                        "Reference coverage minus recent coverage.",
                    ),
                    (
                        "drift",
                        "warning",
                        "maximum_temperature_mae_increase_c",
                        temperature_increase,
                        config.max_temperature_mae_increase_c,
                        "<=",
                        temperature_increase
                        <= config.max_temperature_mae_increase_c,
                        "Recent temperature MAE minus reference temperature MAE.",
                    ),
                    (
                        "drift",
                        "warning",
                        "maximum_humidity_mae_increase_pct",
                        humidity_increase,
                        config.max_humidity_mae_increase_pct,
                        "<=",
                        humidity_increase
                        <= config.max_humidity_mae_increase_pct,
                        "Recent humidity MAE minus reference humidity MAE.",
                    ),
                ]
            )
        for scope, severity, name, observed, threshold, comparator, passed, details in values:
            checks.append(
                _check(
                    run_id,
                    timestamp,
                    as_of,
                    scope,
                    severity,
                    name,
                    observed,
                    threshold,
                    comparator,
                    passed,
                    details,
                    config,
                    identity,
                )
            )

    check_frame = pd.DataFrame(checks)
    failed_errors = int(
        ((check_frame["severity"] == "error") & (~check_frame["passed"])).sum()
    )
    failed_warnings = int(
        ((check_frame["severity"] == "warning") & (~check_frame["passed"])).sum()
    )
    status = (
        FAILED_STATUS
        if failed_errors
        else WARNING_STATUS
        if failed_warnings
        else HEALTHY_STATUS
    )
    summary = pd.DataFrame(
        [
            {
                "monitor_run_id": run_id,
                "monitor_timestamp_utc": timestamp,
                "monitor_as_of_utc": as_of,
                "monitor_status": status,
                "automatic_remediation_allowed": False,
                "check_count": int(len(check_frame)),
                "passed_check_count": int(check_frame["passed"].sum()),
                "failed_error_check_count": failed_errors,
                "failed_warning_check_count": failed_warnings,
                "monitored_forecast_identity_count": int(
                    snapshots.groupby(forecast_identity_columns).ngroups
                ),
                "monitored_reconciliation_slice_count": int(
                    history.groupby(reconciliation_identity_columns, dropna=False).ngroups
                ),
                "policy_version": config.policy_version,
                "monitoring_contract_version": MONITORING_VERSION,
            }
        ]
    )
    return check_frame.reset_index(drop=True), summary

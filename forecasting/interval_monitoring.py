from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from math import isfinite
from typing import Any
from uuid import uuid4

import pandas as pd


POLICY_VERSION = "prediction-interval-monitoring-policy-v1"
MONITORING_VERSION = "prediction-interval-monitoring-v1"
HEALTHY_STATUS = "healthy"
WARNING_STATUS = "warning"
FAILED_STATUS = "failed"

INTERVAL_METRIC_REQUIRED = {
    "interval_run_id",
    "interval_run_timestamp_utc",
    "source_area",
    "resource_id",
    "city",
    "requested_horizon_minutes",
    "model_name",
    "feature_contract_version",
    "target_coverage_level",
    "calibration_observation_count",
    "calibration_radius_mw",
    "evaluation_end_utc",
    "evaluation_observation_count",
    "empirical_coverage_pct",
    "average_interval_width_mw",
    "interval_contract_version",
}


class PredictionIntervalMonitoringError(ValueError):
    """Raised when retained interval metrics cannot be monitored safely."""


@dataclass(frozen=True)
class PredictionIntervalMonitoringConfig:
    recent_interval_run_count: int = 3
    reference_interval_run_count: int = 6
    min_recent_interval_runs: int = 2
    min_reference_interval_runs: int = 3
    max_interval_run_age_minutes: int = 10080
    max_evaluation_age_minutes: int = 20160
    min_calibration_observation_count: int = 24
    max_recent_coverage_shortfall_pct_points: float = 5.0
    max_coverage_drop_pct_points: float = 5.0
    max_average_interval_width_increase_pct: float = 25.0
    max_calibration_history_drop_pct: float = 25.0
    policy_version: str = POLICY_VERSION

    def validate(self) -> None:
        integer_fields = (
            "recent_interval_run_count",
            "reference_interval_run_count",
            "min_recent_interval_runs",
            "min_reference_interval_runs",
            "max_interval_run_age_minutes",
            "max_evaluation_age_minutes",
            "min_calibration_observation_count",
        )
        for name in integer_fields:
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise PredictionIntervalMonitoringError(
                    f"{name} must be a positive integer."
                )
        if self.min_recent_interval_runs > self.recent_interval_run_count:
            raise PredictionIntervalMonitoringError(
                "min_recent_interval_runs cannot exceed the recent window."
            )
        if self.min_reference_interval_runs > self.reference_interval_run_count:
            raise PredictionIntervalMonitoringError(
                "min_reference_interval_runs cannot exceed the reference window."
            )
        threshold_fields = (
            "max_recent_coverage_shortfall_pct_points",
            "max_coverage_drop_pct_points",
            "max_average_interval_width_increase_pct",
            "max_calibration_history_drop_pct",
        )
        for name in threshold_fields:
            value = float(getattr(self, name))
            if not isfinite(value) or value < 0:
                raise PredictionIntervalMonitoringError(
                    f"{name} must be finite and non-negative."
                )
        if not str(self.policy_version).strip():
            raise PredictionIntervalMonitoringError(
                "policy_version must be non-empty."
            )


def _require(frame: pd.DataFrame, columns: set[str], label: str) -> None:
    missing = sorted(columns - set(frame.columns))
    if missing:
        raise PredictionIntervalMonitoringError(
            f"{label} is missing required columns: {', '.join(missing)}."
        )


def _text(frame: pd.DataFrame, column: str) -> pd.Series:
    values = frame[column].fillna("").astype(str).str.strip()
    if values.eq("").any():
        raise PredictionIntervalMonitoringError(
            f"{column} must contain non-empty values."
        )
    return values


def _utc(frame: pd.DataFrame, column: str) -> pd.Series:
    values: list[pd.Timestamp] = []
    for raw in frame[column]:
        try:
            timestamp = pd.Timestamp(raw)
        except (TypeError, ValueError) as exc:
            raise PredictionIntervalMonitoringError(
                f"{column} must contain valid timezone-aware timestamps."
            ) from exc
        if pd.isna(timestamp) or timestamp.tzinfo is None:
            raise PredictionIntervalMonitoringError(
                f"{column} must contain timezone-aware timestamps."
            )
        values.append(timestamp.tz_convert("UTC"))
    return pd.Series(values, index=frame.index, dtype="datetime64[ns, UTC]")


def _number(
    frame: pd.DataFrame,
    column: str,
    *,
    minimum: float | None = None,
    maximum: float | None = None,
) -> pd.Series:
    values = pd.to_numeric(frame[column], errors="coerce")
    if values.isna().any() or not values.map(
        lambda value: isfinite(float(value))
    ).all():
        raise PredictionIntervalMonitoringError(
            f"{column} must contain finite numeric values."
        )
    values = values.astype(float)
    if minimum is not None and (values < minimum).any():
        raise PredictionIntervalMonitoringError(
            f"{column} must be at least {minimum}."
        )
    if maximum is not None and (values > maximum).any():
        raise PredictionIntervalMonitoringError(
            f"{column} must be at most {maximum}."
        )
    return values


def _positive_integer(frame: pd.DataFrame, column: str) -> pd.Series:
    numeric = _number(frame, column, minimum=1)
    if not (numeric % 1 == 0).all():
        raise PredictionIntervalMonitoringError(
            f"{column} must contain positive integers."
        )
    return numeric.astype(int)


def _as_utc(value: datetime | pd.Timestamp | str, name: str) -> pd.Timestamp:
    try:
        timestamp = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise PredictionIntervalMonitoringError(
            f"{name} must be a valid timezone-aware timestamp."
        ) from exc
    if pd.isna(timestamp) or timestamp.tzinfo is None:
        raise PredictionIntervalMonitoringError(
            f"{name} must be timezone-aware."
        )
    return timestamp.tz_convert("UTC")


def prepare_interval_metric_history(frame: pd.DataFrame) -> pd.DataFrame:
    """Normalize one row per retained interval run and monitoring slice."""
    _require(frame, INTERVAL_METRIC_REQUIRED, "Prediction interval metrics")
    prepared = frame.copy()
    for column in (
        "interval_run_id",
        "source_area",
        "resource_id",
        "city",
        "model_name",
        "feature_contract_version",
        "interval_contract_version",
    ):
        prepared[column] = _text(prepared, column)
    for column in ("interval_run_timestamp_utc", "evaluation_end_utc"):
        prepared[column] = _utc(prepared, column)
    prepared["requested_horizon_minutes"] = _positive_integer(
        prepared, "requested_horizon_minutes"
    )
    prepared["calibration_observation_count"] = _positive_integer(
        prepared, "calibration_observation_count"
    )
    prepared["evaluation_observation_count"] = _positive_integer(
        prepared, "evaluation_observation_count"
    )
    prepared["target_coverage_level"] = _number(
        prepared, "target_coverage_level", minimum=0, maximum=1
    )
    if not prepared["target_coverage_level"].between(
        0, 1, inclusive="neither"
    ).all():
        raise PredictionIntervalMonitoringError(
            "target_coverage_level must be strictly between 0 and 1."
        )
    prepared["calibration_radius_mw"] = _number(
        prepared, "calibration_radius_mw", minimum=0
    )
    prepared["empirical_coverage_pct"] = _number(
        prepared, "empirical_coverage_pct", minimum=0, maximum=100
    )
    prepared["average_interval_width_mw"] = _number(
        prepared, "average_interval_width_mw", minimum=0
    )
    if not (
        prepared["evaluation_end_utc"]
        <= prepared["interval_run_timestamp_utc"]
    ).all():
        raise PredictionIntervalMonitoringError(
            "Interval evaluation evidence cannot end after its interval run."
        )
    timestamp_counts = prepared.groupby(
        "interval_run_id", sort=False
    )["interval_run_timestamp_utc"].nunique()
    if (timestamp_counts != 1).any():
        raise PredictionIntervalMonitoringError(
            "Each interval_run_id must have exactly one run timestamp."
        )
    identity = [
        "interval_run_id",
        "source_area",
        "resource_id",
        "city",
        "requested_horizon_minutes",
        "model_name",
        "feature_contract_version",
        "target_coverage_level",
        "interval_contract_version",
    ]
    if prepared.duplicated(subset=identity, keep=False).any():
        raise PredictionIntervalMonitoringError(
            "Prediction interval metrics contain duplicate run/slice identities."
        )
    for column in (
        "source_area",
        "resource_id",
        "city",
        "model_name",
        "feature_contract_version",
        "interval_contract_version",
    ):
        prepared[f"_{column}_key"] = prepared[column].str.casefold()
    return prepared.sort_values(
        [
            "_source_area_key",
            "_resource_id_key",
            "_city_key",
            "requested_horizon_minutes",
            "_model_name_key",
            "_feature_contract_version_key",
            "target_coverage_level",
            "_interval_contract_version_key",
            "interval_run_timestamp_utc",
            "interval_run_id",
        ]
    ).reset_index(drop=True)


def _weighted_mean(
    frame: pd.DataFrame,
    value_column: str,
    weight_column: str = "evaluation_observation_count",
) -> float:
    weights = pd.to_numeric(frame[weight_column], errors="coerce").astype(float)
    values = pd.to_numeric(frame[value_column], errors="coerce").astype(float)
    total = float(weights.sum())
    if total <= 0:
        raise PredictionIntervalMonitoringError(
            "Monitoring windows require positive evaluation observation counts."
        )
    return float((values * weights).sum() / total)


def _increase_pct(recent: float, reference: float) -> float:
    if reference <= 0:
        return 0.0 if recent <= 0 else 100.0
    return (recent - reference) / reference * 100.0


def _drop_pct(recent: float, reference: float) -> float:
    if reference <= 0:
        return 0.0
    return (reference - recent) / reference * 100.0


def _check(
    *,
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
    identity: dict[str, Any],
    config: PredictionIntervalMonitoringConfig,
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
        "resource_id": identity["resource_id"],
        "city": identity["city"],
        "requested_horizon_minutes": int(
            identity["requested_horizon_minutes"]
        ),
        "model_name": identity["model_name"],
        "feature_contract_version": identity["feature_contract_version"],
        "target_coverage_level": float(identity["target_coverage_level"]),
        "interval_contract_version": identity["interval_contract_version"],
        "latest_interval_run_id": identity["latest_interval_run_id"],
        "policy_version": config.policy_version,
        "monitoring_contract_version": MONITORING_VERSION,
    }


def monitor_prediction_interval_health(
    interval_metrics: pd.DataFrame,
    *,
    config: PredictionIntervalMonitoringConfig | None = None,
    as_of_utc: datetime | pd.Timestamp | str | None = None,
    run_id: str | None = None,
    run_timestamp: datetime | pd.Timestamp | str | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Evaluate interval freshness, calibration, coverage, width, and drift."""
    config = config or PredictionIntervalMonitoringConfig()
    config.validate()
    history = prepare_interval_metric_history(interval_metrics)
    as_of = _as_utc(as_of_utc or datetime.now(timezone.utc), "as_of_utc")
    timestamp = _as_utc(
        run_timestamp or datetime.now(timezone.utc), "run_timestamp"
    )
    run_id = run_id or str(uuid4())
    checks: list[dict[str, Any]] = []
    grouping = [
        "_source_area_key",
        "_resource_id_key",
        "_city_key",
        "requested_horizon_minutes",
        "_model_name_key",
        "_feature_contract_version_key",
        "target_coverage_level",
        "_interval_contract_version_key",
    ]
    for _, group in history.groupby(grouping, sort=True, dropna=False):
        group = group.sort_values(
            ["interval_run_timestamp_utc", "interval_run_id"]
        ).reset_index(drop=True)
        recent = group.tail(config.recent_interval_run_count)
        reference_end = len(group) - len(recent)
        reference_start = max(
            0, reference_end - config.reference_interval_run_count
        )
        reference = group.iloc[reference_start:reference_end]
        latest = group.iloc[-1]
        identity = {
            "source_area": latest["source_area"],
            "resource_id": latest["resource_id"],
            "city": latest["city"],
            "requested_horizon_minutes": latest[
                "requested_horizon_minutes"
            ],
            "model_name": latest["model_name"],
            "feature_contract_version": latest["feature_contract_version"],
            "target_coverage_level": latest["target_coverage_level"],
            "interval_contract_version": latest["interval_contract_version"],
            "latest_interval_run_id": latest["interval_run_id"],
        }
        latest_run = latest["interval_run_timestamp_utc"]
        latest_evaluation = latest["evaluation_end_utc"]
        run_age = (as_of - latest_run).total_seconds() / 60.0
        evaluation_age = (as_of - latest_evaluation).total_seconds() / 60.0
        if run_age < 0 or evaluation_age < 0:
            raise PredictionIntervalMonitoringError(
                "as_of_utc cannot precede retained interval evidence."
            )
        recent_coverage = _weighted_mean(recent, "empirical_coverage_pct")
        recent_width = _weighted_mean(recent, "average_interval_width_mw")
        recent_calibration = float(
            recent["calibration_observation_count"].mean()
        )
        minimum_recent_calibration = float(
            recent["calibration_observation_count"].min()
        )
        nominal_coverage = float(latest["target_coverage_level"]) * 100.0
        coverage_shortfall = max(0.0, nominal_coverage - recent_coverage)
        values = [
            (
                "history",
                "error",
                "minimum_recent_interval_runs",
                float(len(recent)),
                config.min_recent_interval_runs,
                ">=",
                len(recent) >= config.min_recent_interval_runs,
                "Number of retained interval runs in the recent window.",
            ),
            (
                "freshness",
                "error",
                "latest_interval_run_age_minutes",
                run_age,
                config.max_interval_run_age_minutes,
                "<=",
                run_age <= config.max_interval_run_age_minutes,
                f"Latest interval run was {latest_run.isoformat()}.",
            ),
            (
                "freshness",
                "error",
                "latest_interval_evaluation_age_minutes",
                evaluation_age,
                config.max_evaluation_age_minutes,
                "<=",
                evaluation_age <= config.max_evaluation_age_minutes,
                f"Latest retained evaluation ended at {latest_evaluation.isoformat()}.",
            ),
            (
                "calibration",
                "error",
                "minimum_recent_calibration_observation_count",
                minimum_recent_calibration,
                config.min_calibration_observation_count,
                ">=",
                minimum_recent_calibration
                >= config.min_calibration_observation_count,
                "Minimum causal calibration history across recent interval runs.",
            ),
            (
                "coverage",
                "error",
                "maximum_recent_coverage_shortfall_pct_points",
                coverage_shortfall,
                config.max_recent_coverage_shortfall_pct_points,
                "<=",
                coverage_shortfall
                <= config.max_recent_coverage_shortfall_pct_points,
                (
                    "Nominal coverage minus evaluation-row-weighted empirical "
                    "coverage in the recent window."
                ),
            ),
            (
                "history",
                "warning",
                "minimum_reference_interval_runs",
                float(len(reference)),
                config.min_reference_interval_runs,
                ">=",
                len(reference) >= config.min_reference_interval_runs,
                "Reference history required before calculating interval drift.",
            ),
        ]
        if len(reference) >= config.min_reference_interval_runs:
            reference_coverage = _weighted_mean(
                reference, "empirical_coverage_pct"
            )
            reference_width = _weighted_mean(
                reference, "average_interval_width_mw"
            )
            reference_calibration = float(
                reference["calibration_observation_count"].mean()
            )
            coverage_drop = reference_coverage - recent_coverage
            width_increase = _increase_pct(recent_width, reference_width)
            calibration_drop = _drop_pct(
                recent_calibration, reference_calibration
            )
            values.extend(
                [
                    (
                        "coverage",
                        "warning",
                        "maximum_interval_coverage_drop_pct_points",
                        coverage_drop,
                        config.max_coverage_drop_pct_points,
                        "<=",
                        coverage_drop <= config.max_coverage_drop_pct_points,
                        (
                            "Reference weighted empirical coverage minus recent "
                            "weighted empirical coverage."
                        ),
                    ),
                    (
                        "width",
                        "warning",
                        "maximum_average_interval_width_increase_pct",
                        width_increase,
                        config.max_average_interval_width_increase_pct,
                        "<=",
                        width_increase
                        <= config.max_average_interval_width_increase_pct,
                        (
                            "Percentage increase in evaluation-row-weighted "
                            "average interval width from reference to recent runs."
                        ),
                    ),
                    (
                        "calibration",
                        "warning",
                        "maximum_calibration_history_drop_pct",
                        calibration_drop,
                        config.max_calibration_history_drop_pct,
                        "<=",
                        calibration_drop
                        <= config.max_calibration_history_drop_pct,
                        (
                            "Percentage decrease in mean causal calibration rows "
                            "from reference to recent runs."
                        ),
                    ),
                ]
            )
        for (
            scope,
            severity,
            name,
            observed,
            threshold,
            comparator,
            passed,
            details,
        ) in values:
            checks.append(
                _check(
                    run_id=run_id,
                    run_timestamp=timestamp,
                    as_of=as_of,
                    scope=scope,
                    severity=severity,
                    name=name,
                    observed=observed,
                    threshold=threshold,
                    comparator=comparator,
                    passed=passed,
                    details=details,
                    identity=identity,
                    config=config,
                )
            )
    check_frame = pd.DataFrame(checks)
    if check_frame.empty:
        raise PredictionIntervalMonitoringError(
            "Prediction interval monitoring produced no checks."
        )
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
                "automatic_recalibration_allowed": False,
                "automatic_model_change_allowed": False,
                "automatic_schedule_change_allowed": False,
                "automatic_promotion_allowed": False,
                "check_count": int(len(check_frame)),
                "passed_check_count": int(check_frame["passed"].sum()),
                "failed_error_check_count": failed_errors,
                "failed_warning_check_count": failed_warnings,
                "monitored_interval_slice_count": int(
                    history.groupby(grouping, dropna=False).ngroups
                ),
                "retained_interval_run_count": int(
                    history["interval_run_id"].nunique()
                ),
                "policy_version": config.policy_version,
                "monitoring_contract_version": MONITORING_VERSION,
            }
        ]
    )
    return check_frame.reset_index(drop=True), summary

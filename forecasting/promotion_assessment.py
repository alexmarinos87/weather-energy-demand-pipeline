from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from math import isfinite, sqrt
from typing import Any
from uuid import uuid4

import pandas as pd


BASELINE_MODEL = "ridge_weather_lag"
CANDIDATE_MODEL = "ridge_target_weather"
POLICY_VERSION = "target-weather-promotion-policy-v1"
ASSESSMENT_VERSION = "target-weather-promotion-assessment-v1"
ELIGIBLE_STATUS = "eligible_for_human_review"
BLOCKED_STATUS = "blocked"
SUPPORTED_LEAD_BUCKETS = ("00-06h", "06-12h", "12-24h", "24-48h", "48h+")

COMPARISON_COLUMNS = {
    "run_id", "source_area", "resource_id", "city", "feature_timestamp_utc",
    "event_timestamp_utc", "requested_horizon_minutes", "split", "model_name",
    "actual_demand_mw", "predicted_demand_mw", "trained_through_utc",
    "forecast_weather_coverage_pct", "target_weather_provider",
    "target_weather_model", "target_weather_forecast_ingested_at_utc",
    "target_weather_forecast_valid_at_utc",
}
RECONCILIATION_COLUMNS = {
    "reconciliation_run_id", "source_area", "city", "forecast_provider",
    "forecast_model", "forecast_lead_time_bucket", "eligible_forecast_count",
    "matched_forecast_count", "forecast_observation_coverage_pct",
    "temperature_mae_c", "humidity_mae_pct",
}


class TargetWeatherPromotionError(ValueError):
    """Raised when promotion evidence is malformed or cannot be paired safely."""


@dataclass(frozen=True)
class TargetWeatherPromotionPolicy:
    min_model_observations: int = 24
    min_reconciliation_observations: int = 24
    min_model_forecast_coverage_pct: float = 90.0
    min_reconciliation_coverage_pct: float = 90.0
    max_temperature_mae_c: float = 2.5
    max_humidity_mae_pct: float = 15.0
    min_mae_improvement_pct: float = 0.0
    min_rmse_improvement_pct: float = 0.0
    max_absolute_bias_regression_mw: float = 0.0
    required_horizons: tuple[int, ...] = (30, 60)
    required_lead_buckets: tuple[str, ...] = ()
    require_test_split: bool = True
    policy_version: str = POLICY_VERSION

    def validate(self) -> None:
        for name in ("min_model_observations", "min_reconciliation_observations"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise TargetWeatherPromotionError(f"{name} must be a positive integer.")
        for name in ("min_model_forecast_coverage_pct", "min_reconciliation_coverage_pct"):
            if not 0 < float(getattr(self, name)) <= 100:
                raise TargetWeatherPromotionError(f"{name} must be in (0, 100].")
        for name in ("max_temperature_mae_c", "max_humidity_mae_pct", "max_absolute_bias_regression_mw"):
            value = float(getattr(self, name))
            if not isfinite(value) or value < 0:
                raise TargetWeatherPromotionError(f"{name} must be finite and non-negative.")
        for name in ("min_mae_improvement_pct", "min_rmse_improvement_pct"):
            if not isfinite(float(getattr(self, name))):
                raise TargetWeatherPromotionError(f"{name} must be finite.")
        if not self.required_horizons or any(
            isinstance(value, bool) or not isinstance(value, int) or value < 1
            for value in self.required_horizons
        ):
            raise TargetWeatherPromotionError("required_horizons must contain positive integers.")
        if len(set(self.required_horizons)) != len(self.required_horizons):
            raise TargetWeatherPromotionError("required_horizons must not contain duplicates.")
        unsupported = sorted(set(self.required_lead_buckets) - set(SUPPORTED_LEAD_BUCKETS))
        if unsupported:
            raise TargetWeatherPromotionError(
                "required_lead_buckets contains unsupported values: " + ", ".join(unsupported) + "."
            )
        if len(set(self.required_lead_buckets)) != len(self.required_lead_buckets):
            raise TargetWeatherPromotionError("required_lead_buckets must not contain duplicates.")
        if not isinstance(self.require_test_split, bool):
            raise TargetWeatherPromotionError("require_test_split must be boolean.")
        if not str(self.policy_version).strip():
            raise TargetWeatherPromotionError("policy_version must be non-empty.")


def _require(frame: pd.DataFrame, columns: set[str], label: str) -> None:
    missing = sorted(columns - set(frame.columns))
    if missing:
        raise TargetWeatherPromotionError(f"{label} is missing required columns: {', '.join(missing)}.")


def _text(frame: pd.DataFrame, column: str) -> pd.Series:
    values = frame[column].fillna("").astype(str).str.strip()
    if values.eq("").any():
        raise TargetWeatherPromotionError(f"{column} must contain non-empty values.")
    return values


def _utc(frame: pd.DataFrame, column: str) -> pd.Series:
    values = []
    for raw in frame[column]:
        stamp = pd.Timestamp(raw)
        if pd.isna(stamp) or stamp.tzinfo is None:
            raise TargetWeatherPromotionError(f"{column} must contain timezone-aware timestamps.")
        values.append(stamp.tz_convert("UTC"))
    return pd.Series(values, index=frame.index)


def _number(frame: pd.DataFrame, column: str, *, minimum: float | None = None) -> pd.Series:
    values = pd.to_numeric(frame[column], errors="coerce")
    if values.isna().any() or not values.map(lambda value: isfinite(float(value))).all():
        raise TargetWeatherPromotionError(f"{column} must contain finite numbers.")
    values = values.astype(float)
    if minimum is not None and (values < minimum).any():
        raise TargetWeatherPromotionError(f"{column} must be at least {minimum}.")
    return values


def _single(frame: pd.DataFrame, column: str, label: str) -> str:
    values = sorted(set(_text(frame, column)))
    if len(values) != 1:
        raise TargetWeatherPromotionError(f"{label} must contain exactly one {column}; found {len(values)}.")
    return values[0]


def _bucket(minutes: float) -> str:
    if minutes < 0:
        raise TargetWeatherPromotionError("Forecast pipeline lead must not be negative.")
    for limit, name in ((360, "00-06h"), (720, "06-12h"), (1440, "12-24h"), (2880, "24-48h")):
        if minutes < limit:
            return name
    return "48h+"


def prepare_comparison_predictions(frame: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    _require(frame, COMPARISON_COLUMNS, "Comparison predictions")
    result = frame.copy()
    run_id = _single(result, "run_id", "Comparison predictions")
    for column in ("source_area", "resource_id", "city", "split", "model_name", "target_weather_provider", "target_weather_model"):
        result[column] = _text(result, column)
    for column in ("feature_timestamp_utc", "event_timestamp_utc", "trained_through_utc", "target_weather_forecast_ingested_at_utc", "target_weather_forecast_valid_at_utc"):
        result[column] = _utc(result, column)
    for column in ("actual_demand_mw", "predicted_demand_mw", "forecast_weather_coverage_pct"):
        result[column] = _number(result, column)
    if not result["forecast_weather_coverage_pct"].between(0, 100).all():
        raise TargetWeatherPromotionError("forecast_weather_coverage_pct must be between 0 and 100.")
    result["requested_horizon_minutes"] = _number(result, "requested_horizon_minutes", minimum=1).astype(int)
    if set(result["model_name"]) != {BASELINE_MODEL, CANDIDATE_MODEL}:
        raise TargetWeatherPromotionError("Comparison predictions must contain exactly both weather ridge models.")
    if not (
        (result["trained_through_utc"] < result["feature_timestamp_utc"])
        & (result["feature_timestamp_utc"] < result["event_timestamp_utc"])
        & (result["target_weather_forecast_ingested_at_utc"] <= result["feature_timestamp_utc"])
        & (result["feature_timestamp_utc"] < result["target_weather_forecast_valid_at_utc"])
    ).all():
        raise TargetWeatherPromotionError("Comparison predictions violate a causal timestamp boundary.")
    result["forecast_pipeline_lead_minutes"] = (
        result["target_weather_forecast_valid_at_utc"]
        - result["target_weather_forecast_ingested_at_utc"]
    ).dt.total_seconds() / 60.0
    result["forecast_lead_time_bucket"] = result["forecast_pipeline_lead_minutes"].map(_bucket)
    if "origin_fold" not in result.columns:
        result["origin_fold"] = pd.NA

    slices = ["source_area", "resource_id", "city", "requested_horizon_minutes", "split", "origin_fold", "forecast_lead_time_bucket"]
    pairs = ["feature_timestamp_utc", "event_timestamp_utc", "target_weather_provider", "target_weather_model", "actual_demand_mw"]
    for _, group in result.groupby(slices, sort=True, dropna=False):
        model_rows, boundaries = [], []
        for model in (BASELINE_MODEL, CANDIDATE_MODEL):
            selected = group.loc[group["model_name"] == model]
            if selected.empty:
                raise TargetWeatherPromotionError("Every promotion slice must contain both weather ridge models.")
            model_rows.append(frozenset(selected[pairs].itertuples(index=False, name=None)))
            boundaries.append(frozenset(selected["trained_through_utc"]))
        if model_rows[0] != model_rows[1]:
            raise TargetWeatherPromotionError("Comparison models do not contain identical paired target rows.")
        if boundaries[0] != boundaries[1] or len(boundaries[0]) != 1:
            raise TargetWeatherPromotionError("Comparison models do not use one identical training boundary.")
    return result.reset_index(drop=True), run_id


def prepare_reconciliation_metrics(frame: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    _require(frame, RECONCILIATION_COLUMNS, "Reconciliation metrics")
    result = frame.copy()
    run_id = _single(result, "reconciliation_run_id", "Reconciliation metrics")
    for column in ("source_area", "city", "forecast_provider", "forecast_model", "forecast_lead_time_bucket"):
        result[column] = _text(result, column)
    unsupported = sorted(set(result["forecast_lead_time_bucket"]) - set(SUPPORTED_LEAD_BUCKETS))
    if unsupported:
        raise TargetWeatherPromotionError("Reconciliation metrics contain unsupported lead buckets: " + ", ".join(unsupported) + ".")
    for column in ("eligible_forecast_count", "matched_forecast_count", "forecast_observation_coverage_pct", "temperature_mae_c", "humidity_mae_pct"):
        result[column] = _number(result, column, minimum=0)
    if (result["eligible_forecast_count"] < 1).any() or (result["matched_forecast_count"] > result["eligible_forecast_count"]).any():
        raise TargetWeatherPromotionError("Reconciliation eligible and matched counts are invalid.")
    if not result["forecast_observation_coverage_pct"].between(0, 100).all():
        raise TargetWeatherPromotionError("forecast_observation_coverage_pct must be between 0 and 100.")
    if "forecast_issue_basis" not in result.columns:
        result["forecast_issue_basis"] = None
    return result.reset_index(drop=True), run_id


def _metrics(group: pd.DataFrame) -> dict[str, float]:
    errors = group["predicted_demand_mw"] - group["actual_demand_mw"]
    return {
        "n": float(len(group)),
        "mae": float(errors.abs().mean()),
        "rmse": float(sqrt((errors * errors).mean())),
        "bias": float(errors.mean()),
        "coverage": float(group["forecast_weather_coverage_pct"].min()),
    }


def _improvement(baseline: float, candidate: float) -> float:
    if abs(baseline) <= 1e-12:
        return 0.0 if abs(candidate) <= 1e-12 else -1.0e12
    return (baseline - candidate) / baseline * 100.0


def _weighted(frame: pd.DataFrame, value: str) -> float:
    weights = frame["matched_forecast_count"]
    total = float(weights.sum())
    return float((frame[value] * weights).sum() / total) if total > 0 else 0.0


def _check(assessment_id: str, timestamp: datetime, scope: str, name: str, observed: float, threshold: float, comparator: str, passed: bool, details: str, policy: TargetWeatherPromotionPolicy, **identity: Any) -> dict[str, Any]:
    row = {
        "assessment_id": assessment_id,
        "assessment_timestamp_utc": timestamp,
        "check_scope": scope,
        "check_name": name,
        "observed_value": float(observed),
        "threshold_value": float(threshold),
        "comparator": comparator,
        "passed": bool(passed),
        "details": details,
        "policy_version": policy.policy_version,
        "assessment_contract_version": ASSESSMENT_VERSION,
        "source_area": None, "resource_id": None, "city": None,
        "requested_horizon_minutes": None, "split": None, "origin_fold": None,
        "forecast_provider": None, "forecast_model": None,
        "forecast_lead_time_bucket": None,
    }
    row.update(identity)
    return row


def assess_target_weather_promotion(
    comparison_predictions: pd.DataFrame,
    reconciliation_metrics: pd.DataFrame,
    *,
    policy: TargetWeatherPromotionPolicy | None = None,
    assessment_id: str | None = None,
    assessment_timestamp: datetime | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return advisory checks and a human-review-only promotion summary."""
    policy = policy or TargetWeatherPromotionPolicy()
    policy.validate()
    comparison, comparison_run = prepare_comparison_predictions(comparison_predictions)
    reconciliation, reconciliation_run = prepare_reconciliation_metrics(reconciliation_metrics)
    assessment_id = assessment_id or str(uuid4())
    assessment_timestamp = assessment_timestamp or datetime.now(timezone.utc)
    if assessment_timestamp.tzinfo is None:
        raise TargetWeatherPromotionError("assessment_timestamp must be timezone-aware.")
    assessment_timestamp = assessment_timestamp.astimezone(timezone.utc)
    checks: list[dict[str, Any]] = []

    slice_columns = ["source_area", "resource_id", "city", "requested_horizon_minutes", "split", "origin_fold", "target_weather_provider", "target_weather_model", "forecast_lead_time_bucket"]
    for _, group in comparison.groupby(slice_columns, sort=True, dropna=False):
        first = group.iloc[0]
        origin = first["origin_fold"]
        identity = dict(
            source_area=first["source_area"], resource_id=first["resource_id"], city=first["city"],
            requested_horizon_minutes=int(first["requested_horizon_minutes"]), split=first["split"],
            origin_fold=None if pd.isna(origin) else int(origin), forecast_provider=first["target_weather_provider"],
            forecast_model=first["target_weather_model"], forecast_lead_time_bucket=first["forecast_lead_time_bucket"],
        )
        baseline = _metrics(group.loc[group["model_name"] == BASELINE_MODEL])
        candidate = _metrics(group.loc[group["model_name"] == CANDIDATE_MODEL])
        values = [
            ("minimum_paired_model_observations", min(baseline["n"], candidate["n"]), policy.min_model_observations, ">=", "Paired rows available to both models."),
            ("minimum_model_forecast_weather_coverage_pct", min(baseline["coverage"], candidate["coverage"]), policy.min_model_forecast_coverage_pct, ">=", "Minimum forecast-weather coverage in the paired cohort."),
            ("minimum_candidate_mae_improvement_pct", _improvement(baseline["mae"], candidate["mae"]), policy.min_mae_improvement_pct, ">=", f"Baseline MAE={baseline['mae']:.6g}; candidate MAE={candidate['mae']:.6g}."),
            ("minimum_candidate_rmse_improvement_pct", _improvement(baseline["rmse"], candidate["rmse"]), policy.min_rmse_improvement_pct, ">=", f"Baseline RMSE={baseline['rmse']:.6g}; candidate RMSE={candidate['rmse']:.6g}."),
            ("maximum_absolute_bias_regression_mw", abs(candidate["bias"]) - abs(baseline["bias"]), policy.max_absolute_bias_regression_mw, "<=", f"Baseline bias={baseline['bias']:.6g}; candidate bias={candidate['bias']:.6g}."),
        ]
        for name, observed, threshold, comparator, details in values:
            passed = observed >= threshold if comparator == ">=" else observed <= threshold
            checks.append(_check(assessment_id, assessment_timestamp, "model_slice", name, observed, threshold, comparator, passed, details, policy, **identity))

    candidates = comparison.loc[comparison["model_name"] == CANDIDATE_MODEL]
    for _, group in candidates.groupby(["source_area", "resource_id", "city"], sort=True):
        first = group.iloc[0]
        base_identity = dict(source_area=first["source_area"], resource_id=first["resource_id"], city=first["city"])
        present = set(group["requested_horizon_minutes"].astype(int))
        for horizon in policy.required_horizons:
            ok = horizon in present
            checks.append(_check(assessment_id, assessment_timestamp, "completeness", "required_horizon_present", float(ok), 1, "==", ok, f"Required demand horizon {horizon} minutes.", policy, **base_identity, requested_horizon_minutes=horizon))
        if policy.require_test_split:
            ok = "test" in set(group["split"])
            checks.append(_check(assessment_id, assessment_timestamp, "completeness", "required_test_split_present", float(ok), 1, "==", ok, "An untouched test split is required.", policy, **base_identity, split="test"))

    weather_slices = candidates[["source_area", "city", "target_weather_provider", "target_weather_model", "forecast_lead_time_bucket"]].drop_duplicates()
    if policy.required_lead_buckets:
        bases = weather_slices[["source_area", "city", "target_weather_provider", "target_weather_model"]].drop_duplicates()
        weather_slices = pd.DataFrame([
            {"source_area": row.source_area, "city": row.city, "target_weather_provider": row.target_weather_provider, "target_weather_model": row.target_weather_model, "forecast_lead_time_bucket": bucket}
            for row in bases.itertuples(index=False) for bucket in policy.required_lead_buckets
        ])
    for row in weather_slices.itertuples(index=False):
        mask = (
            reconciliation["source_area"].str.casefold().eq(str(row.source_area).casefold())
            & reconciliation["city"].str.casefold().eq(str(row.city).casefold())
            & reconciliation["forecast_provider"].str.casefold().eq(str(row.target_weather_provider).casefold())
            & reconciliation["forecast_model"].str.casefold().eq(str(row.target_weather_model).casefold())
            & reconciliation["forecast_lead_time_bucket"].eq(row.forecast_lead_time_bucket)
        )
        evidence = reconciliation.loc[mask]
        eligible = float(evidence["eligible_forecast_count"].sum()) if not evidence.empty else 0.0
        matched = float(evidence["matched_forecast_count"].sum()) if not evidence.empty else 0.0
        coverage = matched / eligible * 100.0 if eligible else 0.0
        temp = _weighted(evidence, "temperature_mae_c") if matched else policy.max_temperature_mae_c + 1
        humidity = _weighted(evidence, "humidity_mae_pct") if matched else policy.max_humidity_mae_pct + 1
        identity = dict(source_area=row.source_area, city=row.city, forecast_provider=row.target_weather_provider, forecast_model=row.target_weather_model, forecast_lead_time_bucket=row.forecast_lead_time_bucket)
        values = [
            ("minimum_reconciled_observations", matched, policy.min_reconciliation_observations, ">=", f"Matched mature forecasts out of {int(eligible)} eligible slots."),
            ("minimum_reconciliation_coverage_pct", coverage, policy.min_reconciliation_coverage_pct, ">=", "Weighted mature forecast coverage."),
            ("maximum_temperature_mae_c", temp, policy.max_temperature_mae_c, "<=", "Matched-count-weighted temperature MAE."),
            ("maximum_humidity_mae_pct", humidity, policy.max_humidity_mae_pct, "<=", "Matched-count-weighted humidity MAE."),
        ]
        for name, observed, threshold, comparator, details in values:
            passed = observed >= threshold if comparator == ">=" else observed <= threshold
            checks.append(_check(assessment_id, assessment_timestamp, "weather_quality", name, observed, threshold, comparator, passed, details, policy, **identity))

    check_frame = pd.DataFrame(checks)
    failed = int((~check_frame["passed"]).sum())
    summary = pd.DataFrame([{
        "assessment_id": assessment_id,
        "assessment_timestamp_utc": assessment_timestamp,
        "comparison_run_id": comparison_run,
        "reconciliation_run_id": reconciliation_run,
        "baseline_model": BASELINE_MODEL,
        "candidate_model": CANDIDATE_MODEL,
        "assessment_status": ELIGIBLE_STATUS if failed == 0 else BLOCKED_STATUS,
        "automatic_promotion_allowed": False,
        "check_count": int(len(check_frame)),
        "passed_check_count": int(check_frame["passed"].sum()),
        "failed_check_count": failed,
        "evaluated_model_slice_count": int(comparison.groupby(slice_columns, dropna=False).ngroups),
        "evaluated_weather_slice_count": int(len(weather_slices)),
        "required_horizons": ",".join(str(value) for value in policy.required_horizons),
        "required_lead_buckets": ",".join(policy.required_lead_buckets),
        "require_test_split": policy.require_test_split,
        "policy_version": policy.policy_version,
        "assessment_contract_version": ASSESSMENT_VERSION,
    }])
    return check_frame.reset_index(drop=True), summary

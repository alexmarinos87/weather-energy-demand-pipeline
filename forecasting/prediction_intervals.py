from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from math import ceil, isfinite
from uuid import uuid4

import pandas as pd


INTERVAL_CONTRACT_VERSION = "split-conformal-absolute-residual-v1"
CALIBRATION_METHOD = "absolute_residual_quantile"
GROUP_COLUMNS = ["source_area", "resource_id", "city"]
REQUIRED_COLUMNS = {
    "run_id",
    *GROUP_COLUMNS,
    "feature_timestamp_utc",
    "event_timestamp_utc",
    "requested_horizon_minutes",
    "split",
    "model_name",
    "actual_demand_mw",
    "predicted_demand_mw",
    "trained_through_utc",
    "feature_contract_version",
}


class PredictionIntervalError(ValueError):
    """Raised when point-prediction evidence cannot calibrate causal intervals."""


@dataclass(frozen=True)
class PredictionIntervalConfig:
    coverage_levels: tuple[float, ...] = (0.80, 0.90, 0.95)
    min_calibration_rows: int = 24
    contract_version: str = INTERVAL_CONTRACT_VERSION

    def validate(self) -> None:
        if not self.coverage_levels:
            raise PredictionIntervalError("At least one coverage level is required.")
        if len(set(self.coverage_levels)) != len(self.coverage_levels):
            raise PredictionIntervalError("coverage_levels must not contain duplicates.")
        for level in self.coverage_levels:
            if not isfinite(float(level)) or not 0 < float(level) < 1:
                raise PredictionIntervalError(
                    "coverage_levels must contain finite values between 0 and 1."
                )
        if (
            isinstance(self.min_calibration_rows, bool)
            or not isinstance(self.min_calibration_rows, int)
            or self.min_calibration_rows < 1
        ):
            raise PredictionIntervalError(
                "min_calibration_rows must be a positive integer."
            )
        if not isinstance(self.contract_version, str) or not self.contract_version.strip():
            raise PredictionIntervalError("contract_version must be non-empty.")

    @property
    def ordered_coverage_levels(self) -> tuple[float, ...]:
        return tuple(sorted(float(level) for level in self.coverage_levels))


def _text(frame: pd.DataFrame, column: str) -> pd.Series:
    values = frame[column].fillna("").astype(str).str.strip()
    if values.eq("").any():
        raise PredictionIntervalError(f"{column} must contain non-empty values.")
    return values


def _utc(frame: pd.DataFrame, column: str) -> pd.Series:
    values: list[pd.Timestamp] = []
    for raw in frame[column]:
        try:
            timestamp = pd.Timestamp(raw)
        except (TypeError, ValueError) as exc:
            raise PredictionIntervalError(
                f"{column} must contain valid timezone-aware timestamps."
            ) from exc
        if pd.isna(timestamp) or timestamp.tzinfo is None:
            raise PredictionIntervalError(
                f"{column} must contain timezone-aware timestamps."
            )
        values.append(timestamp.tz_convert("UTC"))
    return pd.Series(values, index=frame.index, dtype="datetime64[ns, UTC]")


def _number(frame: pd.DataFrame, column: str) -> pd.Series:
    values = pd.to_numeric(frame[column], errors="coerce")
    if values.isna().any() or not values.map(
        lambda value: isfinite(float(value))
    ).all():
        raise PredictionIntervalError(f"{column} must contain finite numbers.")
    return values.astype(float)


def prepare_point_prediction_evidence(
    frame: pd.DataFrame,
) -> tuple[pd.DataFrame, str]:
    """Normalize one point-prediction run without accepting training rows."""
    missing = sorted(REQUIRED_COLUMNS - set(frame.columns))
    if missing:
        raise PredictionIntervalError(
            "Point predictions are missing required columns: "
            + ", ".join(missing)
            + "."
        )
    prepared = frame.copy()
    for column in (
        "run_id",
        *GROUP_COLUMNS,
        "split",
        "model_name",
        "feature_contract_version",
    ):
        prepared[column] = _text(prepared, column)
    run_ids = sorted(set(prepared["run_id"]))
    if len(run_ids) != 1:
        raise PredictionIntervalError(
            f"Point predictions must contain exactly one run_id; found {len(run_ids)}."
        )
    if not set(prepared["split"]).issubset({"validation", "test"}):
        raise PredictionIntervalError(
            "Point prediction splits must be validation or test."
        )
    if set(prepared["split"]) != {"validation", "test"}:
        raise PredictionIntervalError(
            "Point predictions must contain both validation and test rows."
        )
    for column in (
        "feature_timestamp_utc",
        "event_timestamp_utc",
        "trained_through_utc",
    ):
        prepared[column] = _utc(prepared, column)
    for column in ("actual_demand_mw", "predicted_demand_mw"):
        prepared[column] = _number(prepared, column)
    horizons = pd.to_numeric(
        prepared["requested_horizon_minutes"], errors="coerce"
    )
    if horizons.isna().any() or (horizons <= 0).any():
        raise PredictionIntervalError(
            "requested_horizon_minutes must contain positive values."
        )
    prepared["requested_horizon_minutes"] = horizons.astype(int)
    if not (
        (prepared["trained_through_utc"] < prepared["feature_timestamp_utc"])
        & (
            prepared["feature_timestamp_utc"]
            < prepared["event_timestamp_utc"]
        )
    ).all():
        raise PredictionIntervalError(
            "Point predictions violate training, feature, or target ordering."
        )
    if "origin_fold" not in prepared.columns:
        prepared["origin_fold"] = pd.NA
    identity = [
        *GROUP_COLUMNS,
        "requested_horizon_minutes",
        "model_name",
        "split",
        "origin_fold",
        "feature_timestamp_utc",
        "event_timestamp_utc",
    ]
    if prepared.duplicated(subset=identity, keep=False).any():
        raise PredictionIntervalError(
            "Point predictions contain duplicate evaluation identities."
        )
    return prepared.sort_values(identity).reset_index(drop=True), run_ids[0]


def _finite_sample_radius(
    absolute_errors: pd.Series,
    coverage_level: float,
) -> tuple[float, int]:
    ordered = sorted(float(value) for value in absolute_errors)
    count = len(ordered)
    rank = min(count, max(1, ceil((count + 1) * coverage_level)))
    return float(ordered[rank - 1]), rank


def _metric_row(group: pd.DataFrame) -> dict[str, object]:
    first = group.iloc[0]
    return {
        "interval_run_id": first["interval_run_id"],
        "interval_run_timestamp_utc": first["interval_run_timestamp_utc"],
        "point_prediction_run_id": first["point_prediction_run_id"],
        "source_area": first["source_area"],
        "resource_id": first["resource_id"],
        "city": first["city"],
        "requested_horizon_minutes": int(first["requested_horizon_minutes"]),
        "model_name": first["model_name"],
        "feature_contract_version": first["feature_contract_version"],
        "evaluation_origin_fold": first["evaluation_origin_fold"],
        "target_coverage_level": float(first["target_coverage_level"]),
        "calibration_method": CALIBRATION_METHOD,
        "calibration_observation_count": int(
            first["calibration_observation_count"]
        ),
        "calibration_quantile_rank": int(first["calibration_quantile_rank"]),
        "calibration_radius_mw": float(first["calibration_radius_mw"]),
        "calibration_feature_start_utc": first[
            "calibration_feature_start_utc"
        ],
        "calibration_feature_end_utc": first[
            "calibration_feature_end_utc"
        ],
        "calibration_label_available_through_utc": first[
            "calibration_label_available_through_utc"
        ],
        "evaluation_feature_start_utc": group["feature_timestamp_utc"].min(),
        "evaluation_feature_end_utc": group["feature_timestamp_utc"].max(),
        "evaluation_start_utc": group["event_timestamp_utc"].min(),
        "evaluation_end_utc": group["event_timestamp_utc"].max(),
        "evaluation_observation_count": int(len(group)),
        "empirical_coverage_pct": float(group["interval_covered"].mean() * 100.0),
        "average_interval_width_mw": float(group["interval_width_mw"].mean()),
        "median_interval_width_mw": float(group["interval_width_mw"].median()),
        "minimum_interval_width_mw": float(group["interval_width_mw"].min()),
        "maximum_interval_width_mw": float(group["interval_width_mw"].max()),
        "interval_contract_version": first["interval_contract_version"],
    }


def calibrate_prediction_intervals(
    point_predictions: pd.DataFrame,
    *,
    config: PredictionIntervalConfig | None = None,
    interval_run_id: str | None = None,
    interval_run_timestamp: datetime | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Calibrate symmetric test intervals from causally available validation errors."""
    config = config or PredictionIntervalConfig()
    config.validate()
    prepared, point_run_id = prepare_point_prediction_evidence(point_predictions)
    interval_run_id = interval_run_id or str(uuid4())
    interval_run_timestamp = interval_run_timestamp or datetime.now(timezone.utc)
    if interval_run_timestamp.tzinfo is None:
        raise PredictionIntervalError(
            "interval_run_timestamp must be timezone-aware."
        )
    interval_run_timestamp = interval_run_timestamp.astimezone(timezone.utc)
    grouping = [
        *GROUP_COLUMNS,
        "requested_horizon_minutes",
        "model_name",
        "feature_contract_version",
    ]
    interval_rows: list[dict[str, object]] = []
    for _, model_group in prepared.groupby(grouping, sort=True, dropna=False):
        validation = model_group.loc[model_group["split"] == "validation"].copy()
        test = model_group.loc[model_group["split"] == "test"].copy()
        if validation.empty or test.empty:
            raise PredictionIntervalError(
                "Every interval group requires validation and test rows."
            )
        test_origins = test["origin_fold"].dropna().unique().tolist()
        if len(test_origins) > 1:
            raise PredictionIntervalError(
                "Each interval group may contain at most one test origin."
            )
        evaluation_origin = int(test_origins[0]) if test_origins else None
        test = test.sort_values("feature_timestamp_utc").reset_index(drop=True)
        evaluation_start = test["feature_timestamp_utc"].min()
        calibration = validation.loc[
            validation["event_timestamp_utc"] < evaluation_start
        ].copy()
        calibration = calibration.sort_values("feature_timestamp_utc")
        if len(calibration) < config.min_calibration_rows:
            first = model_group.iloc[0]
            raise PredictionIntervalError(
                f"Interval group {first['source_area']}/{first['resource_id']}/"
                f"{first['city']} horizon={int(first['requested_horizon_minutes'])} "
                f"model={first['model_name']} has {len(calibration)} causally "
                f"available calibration rows; minimum is "
                f"{config.min_calibration_rows}."
            )
        calibration_errors = (
            calibration["predicted_demand_mw"]
            - calibration["actual_demand_mw"]
        ).abs()
        calibration_label_through = calibration["event_timestamp_utc"].max()
        if not calibration_label_through < evaluation_start:
            raise PredictionIntervalError(
                "Calibration labels were not available before test feature time."
            )
        for coverage_level in config.ordered_coverage_levels:
            radius, rank = _finite_sample_radius(
                calibration_errors,
                coverage_level,
            )
            for row in test.itertuples(index=False):
                prediction = float(row.predicted_demand_mw)
                actual = float(row.actual_demand_mw)
                lower = prediction - radius
                upper = prediction + radius
                interval_rows.append(
                    {
                        "interval_run_id": interval_run_id,
                        "interval_run_timestamp_utc": interval_run_timestamp,
                        "point_prediction_run_id": point_run_id,
                        "source_area": row.source_area,
                        "resource_id": row.resource_id,
                        "city": row.city,
                        "feature_timestamp_utc": row.feature_timestamp_utc,
                        "event_timestamp_utc": row.event_timestamp_utc,
                        "requested_horizon_minutes": int(
                            row.requested_horizon_minutes
                        ),
                        "model_name": row.model_name,
                        "feature_contract_version": row.feature_contract_version,
                        "evaluation_origin_fold": evaluation_origin,
                        "actual_demand_mw": actual,
                        "point_prediction_mw": prediction,
                        "target_coverage_level": coverage_level,
                        "lower_prediction_mw": lower,
                        "upper_prediction_mw": upper,
                        "interval_width_mw": upper - lower,
                        "interval_covered": lower <= actual <= upper,
                        "calibration_method": CALIBRATION_METHOD,
                        "calibration_observation_count": int(len(calibration)),
                        "calibration_quantile_rank": rank,
                        "calibration_radius_mw": radius,
                        "calibration_feature_start_utc": calibration[
                            "feature_timestamp_utc"
                        ].min(),
                        "calibration_feature_end_utc": calibration[
                            "feature_timestamp_utc"
                        ].max(),
                        "calibration_label_available_through_utc": (
                            calibration_label_through
                        ),
                        "point_model_trained_through_utc": row.trained_through_utc,
                        "interval_contract_version": config.contract_version,
                    }
                )
    intervals = pd.DataFrame(interval_rows)
    if intervals.empty:
        raise PredictionIntervalError("No prediction intervals were produced.")
    if not (
        intervals["calibration_label_available_through_utc"]
        < intervals["feature_timestamp_utc"]
    ).all():
        raise PredictionIntervalError(
            "An interval used calibration labels unavailable at evaluation time."
        )
    if not (
        (intervals["lower_prediction_mw"] <= intervals["point_prediction_mw"])
        & (
            intervals["point_prediction_mw"]
            <= intervals["upper_prediction_mw"]
        )
        & (intervals["interval_width_mw"] >= 0)
    ).all():
        raise PredictionIntervalError("Prediction interval bounds are invalid.")
    metric_grouping = [
        *GROUP_COLUMNS,
        "requested_horizon_minutes",
        "model_name",
        "feature_contract_version",
        "evaluation_origin_fold",
        "target_coverage_level",
    ]
    metrics = pd.DataFrame(
        [
            _metric_row(group)
            for _, group in intervals.groupby(
                metric_grouping, sort=True, dropna=False
            )
        ]
    )
    return intervals.reset_index(drop=True), metrics.reset_index(drop=True)

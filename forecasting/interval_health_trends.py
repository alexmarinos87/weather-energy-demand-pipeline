from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from math import isfinite
from pathlib import Path
from typing import Any
from uuid import uuid4

import pandas as pd

from forecasting.interval_monitoring import (
    HEALTHY_STATUS,
    MONITORING_VERSION,
    POLICY_VERSION,
    PredictionIntervalMonitoringConfig,
)


TREND_CONTRACT_VERSION = "interval-health-trend-v1"
TREND_RUN_ID_PREFIX = "iht-"
SUPPORTED_STATUSES = {"healthy", "warning", "failed"}
SLICE_COLUMNS = [
    "source_area",
    "resource_id",
    "city",
    "requested_horizon_minutes",
    "model_name",
    "feature_contract_version",
    "target_coverage_level",
    "interval_contract_version",
]
REQUIRED_HISTORY_COLUMNS = {
    "scenario",
    "history_sequence",
    "interval_run_id",
    "interval_run_timestamp_utc",
    *SLICE_COLUMNS,
    "calibration_observation_count",
    "calibration_radius_mw",
    "evaluation_end_utc",
    "evaluation_observation_count",
    "empirical_coverage_pct",
    "average_interval_width_mw",
}
AUTHORITY_FIELDS = (
    "automatic_remediation_allowed",
    "automatic_recalibration_allowed",
    "automatic_model_change_allowed",
    "automatic_schedule_change_allowed",
    "automatic_promotion_allowed",
)
REQUIRED_SUMMARY_COLUMNS = {
    "scenario",
    "monitor_run_id",
    "monitor_status",
    "failed_error_check_count",
    "failed_warning_check_count",
    "policy_version",
    "monitoring_contract_version",
    *AUTHORITY_FIELDS,
}


class IntervalHealthTrendError(ValueError):
    """Raised when interval-health trend evidence is malformed or incomplete."""


@dataclass(frozen=True)
class IntervalHealthTrendConfig:
    recent_interval_run_count: int = 3
    reference_interval_run_count: int = 6
    min_recent_interval_runs: int = 2
    min_reference_interval_runs: int = 3
    contract_version: str = TREND_CONTRACT_VERSION

    @classmethod
    def from_monitoring_config(
        cls, config: PredictionIntervalMonitoringConfig | None = None
    ) -> "IntervalHealthTrendConfig":
        config = config or PredictionIntervalMonitoringConfig()
        config.validate()
        return cls(
            recent_interval_run_count=config.recent_interval_run_count,
            reference_interval_run_count=config.reference_interval_run_count,
            min_recent_interval_runs=config.min_recent_interval_runs,
            min_reference_interval_runs=config.min_reference_interval_runs,
        )

    def validate(self) -> None:
        for name in (
            "recent_interval_run_count",
            "reference_interval_run_count",
            "min_recent_interval_runs",
            "min_reference_interval_runs",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise IntervalHealthTrendError(f"{name} must be a positive integer.")
        if self.min_recent_interval_runs > self.recent_interval_run_count:
            raise IntervalHealthTrendError(
                "min_recent_interval_runs cannot exceed the recent window."
            )
        if self.min_reference_interval_runs > self.reference_interval_run_count:
            raise IntervalHealthTrendError(
                "min_reference_interval_runs cannot exceed the reference window."
            )
        if not str(self.contract_version).strip():
            raise IntervalHealthTrendError("contract_version must be non-empty.")


def _require(frame: pd.DataFrame, columns: set[str], label: str) -> None:
    missing = sorted(columns - set(frame.columns))
    if missing:
        raise IntervalHealthTrendError(
            f"{label} is missing required columns: {', '.join(missing)}."
        )


def _text(frame: pd.DataFrame, column: str) -> pd.Series:
    values = frame[column].fillna("").astype(str).str.strip()
    if values.eq("").any():
        raise IntervalHealthTrendError(f"{column} must contain non-empty values.")
    return values


def _utc(frame: pd.DataFrame, column: str) -> pd.Series:
    parsed: list[pd.Timestamp] = []
    for value in frame[column]:
        try:
            timestamp = pd.Timestamp(value)
        except (TypeError, ValueError) as exc:
            raise IntervalHealthTrendError(
                f"{column} must contain valid timezone-aware timestamps."
            ) from exc
        if pd.isna(timestamp) or timestamp.tzinfo is None:
            raise IntervalHealthTrendError(
                f"{column} must contain timezone-aware timestamps."
            )
        parsed.append(timestamp.tz_convert("UTC"))
    return pd.Series(parsed, index=frame.index, dtype="datetime64[ns, UTC]")


def _finite_number(
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
        raise IntervalHealthTrendError(f"{column} must contain finite values.")
    values = values.astype(float)
    if minimum is not None and (values < minimum).any():
        raise IntervalHealthTrendError(f"{column} must be at least {minimum}.")
    if maximum is not None and (values > maximum).any():
        raise IntervalHealthTrendError(f"{column} must be at most {maximum}.")
    return values


def _positive_integer(frame: pd.DataFrame, column: str) -> pd.Series:
    values = _finite_number(frame, column, minimum=1)
    if not (values % 1 == 0).all():
        raise IntervalHealthTrendError(f"{column} must contain positive integers.")
    return values.astype(int)


def _bool_false(frame: pd.DataFrame, column: str) -> None:
    values = frame[column]
    if values.dtype == bool:
        parsed = values
    else:
        parsed = values.astype(str).str.strip().str.lower().map(
            {"true": True, "false": False}
        )
    if parsed.isna().any() or parsed.any():
        raise IntervalHealthTrendError(
            f"{column} must contain only false authority values."
        )


def _normalised_slice_keys(frame: pd.DataFrame) -> pd.DataFrame:
    keys = frame[SLICE_COLUMNS].copy()
    for column in (
        "source_area",
        "resource_id",
        "city",
        "model_name",
        "feature_contract_version",
        "interval_contract_version",
    ):
        keys[column] = keys[column].astype(str).str.casefold()
    keys["requested_horizon_minutes"] = keys[
        "requested_horizon_minutes"
    ].astype(int)
    keys["target_coverage_level"] = keys["target_coverage_level"].astype(float)
    return keys


def prepare_interval_health_history(frame: pd.DataFrame) -> pd.DataFrame:
    """Validate one complete repeated interval-metric history."""
    _require(frame, REQUIRED_HISTORY_COLUMNS, "Interval-health history")
    prepared = frame.copy()
    for column in (
        "scenario",
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
    for column in (
        "history_sequence",
        "requested_horizon_minutes",
        "calibration_observation_count",
        "evaluation_observation_count",
    ):
        prepared[column] = _positive_integer(prepared, column)
    prepared["target_coverage_level"] = _finite_number(
        prepared, "target_coverage_level", minimum=0, maximum=1
    )
    if not prepared["target_coverage_level"].between(
        0, 1, inclusive="neither"
    ).all():
        raise IntervalHealthTrendError(
            "target_coverage_level must be strictly between 0 and 1."
        )
    prepared["calibration_radius_mw"] = _finite_number(
        prepared, "calibration_radius_mw", minimum=0
    )
    prepared["empirical_coverage_pct"] = _finite_number(
        prepared, "empirical_coverage_pct", minimum=0, maximum=100
    )
    prepared["average_interval_width_mw"] = _finite_number(
        prepared, "average_interval_width_mw", minimum=0
    )
    if not (
        prepared["evaluation_end_utc"]
        <= prepared["interval_run_timestamp_utc"]
    ).all():
        raise IntervalHealthTrendError(
            "Evaluation evidence cannot end after its interval run."
        )
    run_identity = prepared.groupby("interval_run_id", sort=False).agg(
        scenario_count=("scenario", "nunique"),
        sequence_count=("history_sequence", "nunique"),
        timestamp_count=("interval_run_timestamp_utc", "nunique"),
    )
    if (run_identity != 1).any(axis=None):
        raise IntervalHealthTrendError(
            "Each interval run must have one scenario, sequence, and timestamp."
        )
    duplicate_identity = ["scenario", "interval_run_id", *SLICE_COLUMNS]
    if prepared.duplicated(subset=duplicate_identity, keep=False).any():
        raise IntervalHealthTrendError(
            "Interval-health history contains duplicate run/slice identities."
        )

    slice_keys = _normalised_slice_keys(prepared)
    for column in SLICE_COLUMNS:
        prepared[f"_{column}_key"] = slice_keys[column]
    keyed_slice_columns = [f"_{column}_key" for column in SLICE_COLUMNS]

    expected_slice_set: set[tuple[Any, ...]] | None = None
    for scenario, scenario_frame in prepared.groupby("scenario", sort=True):
        run_meta = (
            scenario_frame[
                [
                    "interval_run_id",
                    "history_sequence",
                    "interval_run_timestamp_utc",
                ]
            ]
            .drop_duplicates()
            .sort_values(["history_sequence", "interval_run_timestamp_utc"])
        )
        expected_sequence = list(range(1, len(run_meta) + 1))
        if run_meta["history_sequence"].tolist() != expected_sequence:
            raise IntervalHealthTrendError(
                f"Scenario {scenario} history_sequence must be contiguous from 1."
            )
        current_expected: set[tuple[Any, ...]] | None = None
        for run_id, run_frame in scenario_frame.groupby(
            "interval_run_id", sort=False
        ):
            current = {
                tuple(row)
                for row in run_frame[keyed_slice_columns].itertuples(
                    index=False, name=None
                )
            }
            if current_expected is None:
                current_expected = current
            elif current != current_expected:
                raise IntervalHealthTrendError(
                    f"Scenario {scenario} run {run_id} has incomplete slices."
                )
        if expected_slice_set is None:
            expected_slice_set = current_expected
        elif current_expected != expected_slice_set:
            raise IntervalHealthTrendError(
                "Scenarios do not retain the same monitoring slice set."
            )
    return prepared.sort_values(
        [
            "scenario",
            "history_sequence",
            "interval_run_timestamp_utc",
            *keyed_slice_columns,
        ]
    ).reset_index(drop=True)


def prepare_interval_health_summaries(
    frame: pd.DataFrame, scenarios: set[str]
) -> pd.DataFrame:
    """Validate one retained monitoring summary per scenario."""
    _require(frame, REQUIRED_SUMMARY_COLUMNS, "Interval-health summaries")
    prepared = frame.copy()
    for column in (
        "scenario",
        "monitor_run_id",
        "monitor_status",
        "policy_version",
        "monitoring_contract_version",
    ):
        prepared[column] = _text(prepared, column)
    if prepared["scenario"].duplicated().any():
        raise IntervalHealthTrendError(
            "Interval-health summaries must contain one row per scenario."
        )
    if set(prepared["scenario"]) != scenarios:
        raise IntervalHealthTrendError(
            "Interval-health summaries do not match history scenarios."
        )
    if not set(prepared["monitor_status"]).issubset(SUPPORTED_STATUSES):
        raise IntervalHealthTrendError(
            "Interval-health summaries contain an unsupported status."
        )
    if set(prepared["policy_version"]) != {POLICY_VERSION}:
        raise IntervalHealthTrendError(
            "Interval-health summaries use an unexpected monitoring policy."
        )
    if set(prepared["monitoring_contract_version"]) != {MONITORING_VERSION}:
        raise IntervalHealthTrendError(
            "Interval-health summaries use an unexpected monitoring contract."
        )
    for column in (
        "failed_error_check_count",
        "failed_warning_check_count",
    ):
        prepared[column] = _finite_number(prepared, column, minimum=0).astype(int)
    for column in AUTHORITY_FIELDS:
        _bool_false(prepared, column)
    return prepared.reset_index(drop=True)


def _pct_change(current: pd.Series, previous: pd.Series) -> pd.Series:
    result = pd.Series(float("nan"), index=current.index, dtype=float)
    available = previous.notna()
    positive = available & (previous > 0)
    result.loc[positive] = (
        (current.loc[positive] - previous.loc[positive])
        / previous.loc[positive]
        * 100.0
    )
    zero = available & (previous == 0)
    result.loc[zero & (current == 0)] = 0.0
    result.loc[zero & (current > 0)] = 100.0
    return result


def _weighted_mean(frame: pd.DataFrame, column: str) -> float:
    weights = frame["evaluation_observation_count"].astype(float)
    total = float(weights.sum())
    if total <= 0:
        raise IntervalHealthTrendError(
            "Trend windows require positive evaluation observation counts."
        )
    return float((frame[column].astype(float) * weights).sum() / total)


def build_interval_health_trends(
    history: pd.DataFrame,
    summaries: pd.DataFrame,
    *,
    config: IntervalHealthTrendConfig | None = None,
    trend_run_id: str | None = None,
    trend_run_timestamp: datetime | pd.Timestamp | str | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Create run-level and exact-slice interval-health trend datasets."""
    config = config or IntervalHealthTrendConfig.from_monitoring_config()
    config.validate()
    prepared = prepare_interval_health_history(history)
    summary = prepare_interval_health_summaries(
        summaries, set(prepared["scenario"])
    )
    run_id = trend_run_id or TREND_RUN_ID_PREFIX + uuid4().hex[:24]
    if not (
        run_id.startswith(TREND_RUN_ID_PREFIX)
        and len(run_id) == len(TREND_RUN_ID_PREFIX) + 24
        and all(character in "0123456789abcdef" for character in run_id[4:])
    ):
        raise IntervalHealthTrendError(
            "trend_run_id must match iht- plus 24 lowercase hexadecimal characters."
        )
    timestamp = pd.Timestamp(trend_run_timestamp or datetime.now(timezone.utc))
    if pd.isna(timestamp) or timestamp.tzinfo is None:
        raise IntervalHealthTrendError(
            "trend_run_timestamp must be timezone-aware."
        )
    timestamp = timestamp.tz_convert("UTC")

    keyed_slice_columns = [f"_{column}_key" for column in SLICE_COLUMNS]
    grouping = ["scenario", *keyed_slice_columns]
    run_trends = prepared.copy()
    run_trends["nominal_coverage_pct"] = (
        run_trends["target_coverage_level"].astype(float) * 100.0
    )
    run_trends["coverage_shortfall_pct_points"] = (
        run_trends["nominal_coverage_pct"]
        - run_trends["empirical_coverage_pct"].astype(float)
    ).clip(lower=0)
    run_trends = run_trends.sort_values(
        [*grouping, "interval_run_timestamp_utc", "interval_run_id"]
    ).reset_index(drop=True)
    grouped = run_trends.groupby(grouping, sort=False, dropna=False)
    run_trends["slice_run_sequence"] = grouped.cumcount() + 1
    run_trends["slice_run_count"] = grouped["interval_run_id"].transform("size")
    reverse_rank = (
        run_trends["slice_run_count"] - run_trends["slice_run_sequence"] + 1
    )
    run_trends["monitoring_window"] = "older"
    run_trends.loc[
        reverse_rank <= config.reference_interval_run_count
        + config.recent_interval_run_count,
        "monitoring_window",
    ] = "reference"
    run_trends.loc[
        reverse_rank <= config.recent_interval_run_count,
        "monitoring_window",
    ] = "recent"
    run_trends["is_latest_interval_run"] = reverse_rank == 1
    for output, source in (
        ("previous_interval_run_id", "interval_run_id"),
        ("previous_interval_run_timestamp_utc", "interval_run_timestamp_utc"),
        ("previous_empirical_coverage_pct", "empirical_coverage_pct"),
        ("previous_average_interval_width_mw", "average_interval_width_mw"),
        (
            "previous_calibration_observation_count",
            "calibration_observation_count",
        ),
    ):
        run_trends[output] = grouped[source].shift(1)
    run_trends["coverage_change_from_previous_pct_points"] = (
        run_trends["empirical_coverage_pct"]
        - run_trends["previous_empirical_coverage_pct"]
    )
    run_trends["interval_width_change_from_previous_pct"] = _pct_change(
        run_trends["average_interval_width_mw"],
        run_trends["previous_average_interval_width_mw"],
    )
    run_trends["calibration_history_change_from_previous_pct"] = _pct_change(
        run_trends["calibration_observation_count"].astype(float),
        run_trends["previous_calibration_observation_count"].astype(float),
    )
    summary_columns = [
        "scenario",
        "monitor_run_id",
        "monitor_status",
        "failed_error_check_count",
        "failed_warning_check_count",
    ]
    run_trends = run_trends.merge(
        summary[summary_columns], on="scenario", how="left", validate="many_to_one"
    )
    run_trends.insert(0, "trend_run_id", run_id)
    run_trends.insert(1, "trend_run_timestamp_utc", timestamp)
    run_trends["trend_contract_version"] = config.contract_version

    slice_rows: list[dict[str, Any]] = []
    for _, group in run_trends.groupby(grouping, sort=True, dropna=False):
        group = group.sort_values(
            ["interval_run_timestamp_utc", "interval_run_id"]
        )
        recent = group.loc[group["monitoring_window"] == "recent"]
        reference = group.loc[group["monitoring_window"] == "reference"]
        if len(recent) < config.min_recent_interval_runs:
            raise IntervalHealthTrendError(
                "A trend slice has insufficient recent interval history."
            )
        latest = group.iloc[-1]
        reference_sufficient = (
            len(reference) >= config.min_reference_interval_runs
        )
        recent_coverage = _weighted_mean(recent, "empirical_coverage_pct")
        recent_width = _weighted_mean(recent, "average_interval_width_mw")
        recent_calibration = float(
            recent["calibration_observation_count"].mean()
        )
        row: dict[str, Any] = {
            "trend_run_id": run_id,
            "trend_run_timestamp_utc": timestamp,
            "scenario": latest["scenario"],
            **{column: latest[column] for column in SLICE_COLUMNS},
            "monitor_run_id": latest["monitor_run_id"],
            "monitor_status": latest["monitor_status"],
            "failed_error_check_count": int(
                latest["failed_error_check_count"]
            ),
            "failed_warning_check_count": int(
                latest["failed_warning_check_count"]
            ),
            "interval_run_count": int(len(group)),
            "recent_interval_run_count": int(len(recent)),
            "reference_interval_run_count": int(len(reference)),
            "reference_history_sufficient": bool(reference_sufficient),
            "latest_interval_run_id": latest["interval_run_id"],
            "latest_interval_run_timestamp_utc": latest[
                "interval_run_timestamp_utc"
            ],
            "latest_evaluation_end_utc": latest["evaluation_end_utc"],
            "latest_empirical_coverage_pct": float(
                latest["empirical_coverage_pct"]
            ),
            "latest_coverage_shortfall_pct_points": float(
                latest["coverage_shortfall_pct_points"]
            ),
            "latest_average_interval_width_mw": float(
                latest["average_interval_width_mw"]
            ),
            "latest_calibration_observation_count": int(
                latest["calibration_observation_count"]
            ),
            "recent_empirical_coverage_pct": recent_coverage,
            "recent_average_interval_width_mw": recent_width,
            "recent_mean_calibration_observation_count": recent_calibration,
            "recent_minimum_calibration_observation_count": int(
                recent["calibration_observation_count"].min()
            ),
            "reference_empirical_coverage_pct": None,
            "reference_average_interval_width_mw": None,
            "reference_mean_calibration_observation_count": None,
            "coverage_drop_pct_points": None,
            "average_interval_width_increase_pct": None,
            "calibration_history_drop_pct": None,
            "attention_required": bool(
                latest["monitor_status"] != HEALTHY_STATUS
            ),
            "trend_contract_version": config.contract_version,
        }
        if reference_sufficient:
            reference_coverage = _weighted_mean(
                reference, "empirical_coverage_pct"
            )
            reference_width = _weighted_mean(
                reference, "average_interval_width_mw"
            )
            reference_calibration = float(
                reference["calibration_observation_count"].mean()
            )
            row.update(
                {
                    "reference_empirical_coverage_pct": reference_coverage,
                    "reference_average_interval_width_mw": reference_width,
                    "reference_mean_calibration_observation_count": (
                        reference_calibration
                    ),
                    "coverage_drop_pct_points": (
                        reference_coverage - recent_coverage
                    ),
                    "average_interval_width_increase_pct": float(
                        _pct_change(
                            pd.Series([recent_width]),
                            pd.Series([reference_width]),
                        ).iloc[0]
                    ),
                    "calibration_history_drop_pct": (
                        0.0
                        if reference_calibration <= 0
                        else (
                            reference_calibration - recent_calibration
                        )
                        / reference_calibration
                        * 100.0
                    ),
                }
            )
        slice_rows.append(row)
    slice_trends = pd.DataFrame(slice_rows)
    if slice_trends.empty:
        raise IntervalHealthTrendError(
            "Interval-health trend generation produced no slices."
        )

    run_trends = run_trends.drop(columns=keyed_slice_columns)
    run_trends = run_trends.sort_values(
        [
            "scenario",
            "source_area",
            "requested_horizon_minutes",
            "model_name",
            "target_coverage_level",
            "interval_run_timestamp_utc",
        ]
    ).reset_index(drop=True)
    slice_trends = slice_trends.sort_values(
        [
            "scenario",
            "source_area",
            "requested_horizon_minutes",
            "model_name",
            "target_coverage_level",
        ]
    ).reset_index(drop=True)
    return run_trends, slice_trends


def read_frame(path: Path) -> pd.DataFrame:
    path = Path(path)
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    raise IntervalHealthTrendError(
        f"Unsupported tabular format for {path.name}; use CSV or Parquet."
    )


def write_frame_atomic(
    frame: pd.DataFrame, path: Path, output_format: str
) -> Path:
    path = Path(path)
    temporary = path.with_name(f".{path.name}.tmp")
    for candidate in (path, temporary):
        if candidate.exists():
            raise FileExistsError(f"Refusing to overwrite {candidate}.")
    try:
        if output_format == "csv":
            frame.to_csv(temporary, index=False)
        elif output_format == "parquet":
            frame.to_parquet(temporary, index=False)
        else:
            raise IntervalHealthTrendError(
                "output_format must be csv or parquet."
            )
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)
    return path

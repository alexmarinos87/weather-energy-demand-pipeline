from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from math import isfinite
from pathlib import Path
import re
from typing import Any, Iterable
from uuid import uuid4

import pandas as pd

from forecasting.interval_health_trends import TREND_CONTRACT_VERSION
from forecasting.interval_monitoring import (
    FAILED_STATUS,
    HEALTHY_STATUS,
    WARNING_STATUS,
    PredictionIntervalMonitoringConfig,
)

SENSITIVITY_CONTRACT_VERSION = "interval-policy-sensitivity-v1"
SENSITIVITY_RUN_ID_PATTERN = re.compile(r"^ips-[0-9a-f]{24}$")
POLICY_ID_PATTERN = re.compile(r"^[a-z][a-z0-9-]{2,63}$")
ACTIVE_POLICY_ID = "active-reference"
SUPPORTED_STATUSES = {HEALTHY_STATUS, WARNING_STATUS, FAILED_STATUS}
POLICY_ROLES = {"active_reference", "review_candidate"}
SLICE_IDENTITY_COLUMNS = [
    "scenario",
    "source_area",
    "resource_id",
    "city",
    "requested_horizon_minutes",
    "model_name",
    "feature_contract_version",
    "target_coverage_level",
    "interval_contract_version",
]
REQUIRED_TREND_COLUMNS = {
    "trend_run_id",
    "trend_run_timestamp_utc",
    "trend_contract_version",
    *SLICE_IDENTITY_COLUMNS,
    "monitor_status",
    "interval_run_count",
    "recent_interval_run_count",
    "reference_interval_run_count",
    "reference_history_sufficient",
    "latest_interval_run_id",
    "latest_interval_run_timestamp_utc",
    "latest_evaluation_end_utc",
    "latest_coverage_shortfall_pct_points",
    "recent_minimum_calibration_observation_count",
    "coverage_drop_pct_points",
    "average_interval_width_increase_pct",
    "calibration_history_drop_pct",
}
AUTHORITY_FIELDS = (
    "active_policy_updated",
    "retained_evidence_mutated",
    "interval_recalibration_performed",
    "model_change_performed",
    "schedule_change_performed",
    "promotion_change_performed",
    "alert_delivery_performed",
)


class IntervalPolicySensitivityError(ValueError):
    """Raised when retained trend evidence cannot be compared safely."""


@dataclass(frozen=True)
class IntervalMonitoringPolicyCandidate:
    policy_id: str
    policy_role: str
    policy_version: str
    rationale: str
    recent_interval_run_count: int
    reference_interval_run_count: int
    min_recent_interval_runs: int
    min_reference_interval_runs: int
    max_interval_run_age_minutes: int
    max_evaluation_age_minutes: int
    min_calibration_observation_count: int
    max_recent_coverage_shortfall_pct_points: float
    max_coverage_drop_pct_points: float
    max_average_interval_width_increase_pct: float
    max_calibration_history_drop_pct: float

    @classmethod
    def from_monitoring_config(
        cls,
        *,
        policy_id: str,
        policy_role: str,
        rationale: str,
        config: PredictionIntervalMonitoringConfig,
        policy_version: str | None = None,
    ) -> "IntervalMonitoringPolicyCandidate":
        config.validate()
        return cls(
            policy_id=policy_id,
            policy_role=policy_role,
            policy_version=policy_version or config.policy_version,
            rationale=rationale,
            recent_interval_run_count=config.recent_interval_run_count,
            reference_interval_run_count=config.reference_interval_run_count,
            min_recent_interval_runs=config.min_recent_interval_runs,
            min_reference_interval_runs=config.min_reference_interval_runs,
            max_interval_run_age_minutes=config.max_interval_run_age_minutes,
            max_evaluation_age_minutes=config.max_evaluation_age_minutes,
            min_calibration_observation_count=config.min_calibration_observation_count,
            max_recent_coverage_shortfall_pct_points=(
                config.max_recent_coverage_shortfall_pct_points
            ),
            max_coverage_drop_pct_points=config.max_coverage_drop_pct_points,
            max_average_interval_width_increase_pct=(
                config.max_average_interval_width_increase_pct
            ),
            max_calibration_history_drop_pct=(
                config.max_calibration_history_drop_pct
            ),
        )

    def validate(self) -> None:
        if not POLICY_ID_PATTERN.fullmatch(str(self.policy_id)):
            raise IntervalPolicySensitivityError(
                "policy_id must use lowercase letters, digits, and hyphens."
            )
        if self.policy_role not in POLICY_ROLES:
            raise IntervalPolicySensitivityError(
                "policy_role must be active_reference or review_candidate."
            )
        if not str(self.policy_version).strip() or not str(self.rationale).strip():
            raise IntervalPolicySensitivityError(
                "policy_version and rationale must be non-empty."
            )
        integers = (
            "recent_interval_run_count",
            "reference_interval_run_count",
            "min_recent_interval_runs",
            "min_reference_interval_runs",
            "max_interval_run_age_minutes",
            "max_evaluation_age_minutes",
            "min_calibration_observation_count",
        )
        for name in integers:
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise IntervalPolicySensitivityError(
                    f"{name} must be a positive integer."
                )
        if self.min_recent_interval_runs > self.recent_interval_run_count:
            raise IntervalPolicySensitivityError(
                "min_recent_interval_runs cannot exceed the recent window."
            )
        if self.min_reference_interval_runs > self.reference_interval_run_count:
            raise IntervalPolicySensitivityError(
                "min_reference_interval_runs cannot exceed the reference window."
            )
        for name in (
            "max_recent_coverage_shortfall_pct_points",
            "max_coverage_drop_pct_points",
            "max_average_interval_width_increase_pct",
            "max_calibration_history_drop_pct",
        ):
            value = float(getattr(self, name))
            if not isfinite(value) or value < 0:
                raise IntervalPolicySensitivityError(
                    f"{name} must be finite and non-negative."
                )

    def monitoring_config(self) -> PredictionIntervalMonitoringConfig:
        return PredictionIntervalMonitoringConfig(
            recent_interval_run_count=self.recent_interval_run_count,
            reference_interval_run_count=self.reference_interval_run_count,
            min_recent_interval_runs=self.min_recent_interval_runs,
            min_reference_interval_runs=self.min_reference_interval_runs,
            max_interval_run_age_minutes=self.max_interval_run_age_minutes,
            max_evaluation_age_minutes=self.max_evaluation_age_minutes,
            min_calibration_observation_count=self.min_calibration_observation_count,
            max_recent_coverage_shortfall_pct_points=(
                self.max_recent_coverage_shortfall_pct_points
            ),
            max_coverage_drop_pct_points=self.max_coverage_drop_pct_points,
            max_average_interval_width_increase_pct=(
                self.max_average_interval_width_increase_pct
            ),
            max_calibration_history_drop_pct=self.max_calibration_history_drop_pct,
            policy_version=self.policy_version,
        )


def default_policy_candidates() -> tuple[IntervalMonitoringPolicyCandidate, ...]:
    active = PredictionIntervalMonitoringConfig()
    configs = (
        (
            ACTIVE_POLICY_ID,
            "active_reference",
            "Exact checked-in active monitoring policy.",
            active,
        ),
        (
            "stricter-review",
            "review_candidate",
            "Counterfactual tighter thresholds for human robustness review.",
            PredictionIntervalMonitoringConfig(
                max_interval_run_age_minutes=5040,
                max_evaluation_age_minutes=10080,
                min_calibration_observation_count=30,
                max_recent_coverage_shortfall_pct_points=2.0,
                max_coverage_drop_pct_points=2.0,
                max_average_interval_width_increase_pct=15.0,
                max_calibration_history_drop_pct=15.0,
                policy_version=(
                    "prediction-interval-monitoring-policy-stricter-review-v1"
                ),
            ),
        ),
        (
            "tolerant-review",
            "review_candidate",
            "Counterfactual wider thresholds for human robustness review.",
            PredictionIntervalMonitoringConfig(
                max_interval_run_age_minutes=20160,
                max_evaluation_age_minutes=40320,
                min_calibration_observation_count=18,
                max_recent_coverage_shortfall_pct_points=12.0,
                max_coverage_drop_pct_points=12.0,
                max_average_interval_width_increase_pct=50.0,
                max_calibration_history_drop_pct=50.0,
                policy_version=(
                    "prediction-interval-monitoring-policy-tolerant-review-v1"
                ),
            ),
        ),
    )
    return tuple(
        IntervalMonitoringPolicyCandidate.from_monitoring_config(
            policy_id=policy_id,
            policy_role=role,
            rationale=rationale,
            config=config,
        )
        for policy_id, role, rationale, config in configs
    )


def candidates_from_records(
    records: Iterable[dict[str, Any]],
) -> tuple[IntervalMonitoringPolicyCandidate, ...]:
    try:
        candidates = tuple(
            IntervalMonitoringPolicyCandidate(**record) for record in records
        )
    except TypeError as exc:
        raise IntervalPolicySensitivityError(
            "Candidate records do not match the policy contract."
        ) from exc
    _validate_candidates(candidates)
    return candidates


def _validate_candidates(
    candidates: tuple[IntervalMonitoringPolicyCandidate, ...],
) -> None:
    if not 2 <= len(candidates) <= 5:
        raise IntervalPolicySensitivityError(
            "Sensitivity review requires between two and five policy candidates."
        )
    for candidate in candidates:
        candidate.validate()
    ids = [candidate.policy_id for candidate in candidates]
    if len(ids) != len(set(ids)):
        raise IntervalPolicySensitivityError("Policy candidate IDs must be unique.")
    active = [
        candidate
        for candidate in candidates
        if candidate.policy_role == "active_reference"
    ]
    if len(active) != 1 or active[0].policy_id != ACTIVE_POLICY_ID:
        raise IntervalPolicySensitivityError(
            "Exactly one active-reference candidate named active-reference is required."
        )
    expected = asdict(
        IntervalMonitoringPolicyCandidate.from_monitoring_config(
            policy_id=ACTIVE_POLICY_ID,
            policy_role="active_reference",
            rationale=active[0].rationale,
            config=PredictionIntervalMonitoringConfig(),
        )
    )
    if asdict(active[0]) != expected:
        raise IntervalPolicySensitivityError(
            "The active-reference candidate must exactly match the checked-in policy."
        )


def _require(frame: pd.DataFrame, columns: set[str], label: str) -> None:
    missing = sorted(columns - set(frame.columns))
    if missing:
        raise IntervalPolicySensitivityError(
            f"{label} is missing required columns: {', '.join(missing)}."
        )


def _utc_series(frame: pd.DataFrame, column: str) -> pd.Series:
    result: list[pd.Timestamp] = []
    for raw in frame[column]:
        try:
            value = pd.Timestamp(raw)
        except (TypeError, ValueError) as exc:
            raise IntervalPolicySensitivityError(
                f"{column} must contain timezone-aware timestamps."
            ) from exc
        if pd.isna(value) or value.tzinfo is None:
            raise IntervalPolicySensitivityError(
                f"{column} must contain timezone-aware timestamps."
            )
        result.append(value.tz_convert("UTC"))
    return pd.Series(result, index=frame.index, dtype="datetime64[ns, UTC]")


def _as_utc(value: Any, name: str) -> pd.Timestamp:
    try:
        timestamp = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise IntervalPolicySensitivityError(
            f"{name} must be a timezone-aware timestamp."
        ) from exc
    if pd.isna(timestamp) or timestamp.tzinfo is None:
        raise IntervalPolicySensitivityError(
            f"{name} must be a timezone-aware timestamp."
        )
    return timestamp.tz_convert("UTC")


def _boolean_series(frame: pd.DataFrame, column: str) -> pd.Series:
    values = frame[column]
    if values.dtype == bool:
        return values.astype(bool)
    parsed = values.astype(str).str.strip().str.lower().map(
        {"true": True, "false": False}
    )
    if parsed.isna().any():
        raise IntervalPolicySensitivityError(
            f"{column} must contain booleans."
        )
    return parsed.astype(bool)


def prepare_slice_trends(frame: pd.DataFrame) -> pd.DataFrame:
    _require(frame, REQUIRED_TREND_COLUMNS, "Interval-health slice trends")
    prepared = frame.copy()
    for column in (
        "trend_run_id",
        "trend_contract_version",
        "scenario",
        "source_area",
        "resource_id",
        "city",
        "model_name",
        "feature_contract_version",
        "interval_contract_version",
        "monitor_status",
        "latest_interval_run_id",
    ):
        prepared[column] = prepared[column].fillna("").astype(str).str.strip()
        if prepared[column].eq("").any():
            raise IntervalPolicySensitivityError(
                f"{column} must contain non-empty values."
            )
    if prepared["trend_run_id"].nunique() != 1:
        raise IntervalPolicySensitivityError(
            "Sensitivity input must bind to exactly one trend run."
        )
    if set(prepared["trend_contract_version"]) != {TREND_CONTRACT_VERSION}:
        raise IntervalPolicySensitivityError(
            "Sensitivity input uses an unsupported trend contract."
        )
    if not set(prepared["monitor_status"]).issubset(SUPPORTED_STATUSES):
        raise IntervalPolicySensitivityError(
            "Sensitivity input contains an unsupported retained status."
        )
    for scenario, group in prepared.groupby("scenario", sort=False):
        if group["monitor_status"].nunique() != 1:
            raise IntervalPolicySensitivityError(
                f"Scenario {scenario} has inconsistent retained status."
            )
    for column in (
        "trend_run_timestamp_utc",
        "latest_interval_run_timestamp_utc",
        "latest_evaluation_end_utc",
    ):
        prepared[column] = _utc_series(prepared, column)
    if not (
        prepared["latest_evaluation_end_utc"]
        <= prepared["latest_interval_run_timestamp_utc"]
    ).all():
        raise IntervalPolicySensitivityError(
            "Evaluation evidence cannot end after its interval run."
        )
    if not (
        prepared["latest_interval_run_timestamp_utc"]
        <= prepared["trend_run_timestamp_utc"]
    ).all():
        raise IntervalPolicySensitivityError(
            "Interval-run evidence cannot occur after the trend run."
        )
    for column in (
        "requested_horizon_minutes",
        "interval_run_count",
        "recent_interval_run_count",
        "reference_interval_run_count",
        "recent_minimum_calibration_observation_count",
    ):
        values = pd.to_numeric(prepared[column], errors="coerce")
        if (
            values.isna().any()
            or (values < 0).any()
            or not (values % 1 == 0).all()
        ):
            raise IntervalPolicySensitivityError(
                f"{column} must contain non-negative integers."
            )
        prepared[column] = values.astype(int)
    if (prepared["requested_horizon_minutes"] < 1).any() or (
        prepared["interval_run_count"] < 1
    ).any():
        raise IntervalPolicySensitivityError(
            "Horizon and interval-run counts must be positive."
        )
    for column in (
        "target_coverage_level",
        "latest_coverage_shortfall_pct_points",
    ):
        values = pd.to_numeric(prepared[column], errors="coerce")
        if values.isna().any() or not values.map(
            lambda item: isfinite(float(item))
        ).all():
            raise IntervalPolicySensitivityError(
                f"{column} must contain finite values."
            )
        prepared[column] = values.astype(float)
    if not prepared["target_coverage_level"].between(
        0, 1, inclusive="neither"
    ).all():
        raise IntervalPolicySensitivityError(
            "target_coverage_level must be strictly between zero and one."
        )
    if (prepared["latest_coverage_shortfall_pct_points"] < 0).any():
        raise IntervalPolicySensitivityError(
            "Coverage shortfall cannot be negative."
        )
    prepared["reference_history_sufficient"] = _boolean_series(
        prepared, "reference_history_sufficient"
    )
    drift_columns = [
        "coverage_drop_pct_points",
        "average_interval_width_increase_pct",
        "calibration_history_drop_pct",
    ]
    for column in drift_columns:
        prepared[column] = pd.to_numeric(prepared[column], errors="coerce")
    ready = prepared["reference_history_sufficient"]
    if prepared.loc[ready, drift_columns].isna().any(axis=None):
        raise IntervalPolicySensitivityError(
            "Reference-ready slices require complete drift evidence."
        )
    if prepared.loc[~ready, drift_columns].notna().any(axis=None):
        raise IntervalPolicySensitivityError(
            "Reference-insufficient slices must not fabricate drift evidence."
        )
    if prepared.duplicated(subset=SLICE_IDENTITY_COLUMNS, keep=False).any():
        raise IntervalPolicySensitivityError(
            "Sensitivity input contains duplicate exact-slice identities."
        )
    return prepared.reset_index(drop=True)


def _status(error_failures: int, warning_failures: int) -> str:
    if error_failures:
        return FAILED_STATUS
    if warning_failures:
        return WARNING_STATUS
    return HEALTHY_STATUS


def _slice_outcome(
    row: pd.Series,
    candidate: IntervalMonitoringPolicyCandidate,
    as_of: pd.Timestamp,
) -> dict[str, Any]:
    run_age = (
        as_of - row["latest_interval_run_timestamp_utc"]
    ).total_seconds() / 60.0
    evaluation_age = (
        as_of - row["latest_evaluation_end_utc"]
    ).total_seconds() / 60.0
    if run_age < 0 or evaluation_age < 0:
        raise IntervalPolicySensitivityError(
            "Sensitivity as-of time cannot precede retained interval evidence."
        )
    checks: dict[str, bool | None] = {
        "recent_history_passed": int(row["recent_interval_run_count"])
        >= candidate.min_recent_interval_runs,
        "interval_run_freshness_passed": run_age
        <= candidate.max_interval_run_age_minutes,
        "evaluation_freshness_passed": evaluation_age
        <= candidate.max_evaluation_age_minutes,
        "calibration_history_passed": int(
            row["recent_minimum_calibration_observation_count"]
        )
        >= candidate.min_calibration_observation_count,
        "coverage_shortfall_passed": float(
            row["latest_coverage_shortfall_pct_points"]
        )
        <= candidate.max_recent_coverage_shortfall_pct_points,
        "reference_history_passed": int(row["reference_interval_run_count"])
        >= candidate.min_reference_interval_runs,
        "coverage_drop_passed": None,
        "interval_width_increase_passed": None,
        "calibration_history_drop_passed": None,
    }
    reference_ready = bool(row["reference_history_sufficient"]) and bool(
        checks["reference_history_passed"]
    )
    if reference_ready:
        checks["coverage_drop_passed"] = (
            float(row["coverage_drop_pct_points"])
            <= candidate.max_coverage_drop_pct_points
        )
        checks["interval_width_increase_passed"] = (
            float(row["average_interval_width_increase_pct"])
            <= candidate.max_average_interval_width_increase_pct
        )
        checks["calibration_history_drop_passed"] = (
            float(row["calibration_history_drop_pct"])
            <= candidate.max_calibration_history_drop_pct
        )
    error_names = (
        "recent_history_passed",
        "interval_run_freshness_passed",
        "evaluation_freshness_passed",
        "calibration_history_passed",
        "coverage_shortfall_passed",
    )
    warning_names = (
        "reference_history_passed",
        "coverage_drop_passed",
        "interval_width_increase_passed",
        "calibration_history_drop_passed",
    )
    error_failures = sum(checks[name] is False for name in error_names)
    warning_failures = sum(checks[name] is False for name in warning_names)
    return {
        **checks,
        "candidate_slice_status": _status(error_failures, warning_failures),
        "candidate_error_failure_count": error_failures,
        "candidate_warning_failure_count": warning_failures,
        "observed_interval_run_age_minutes": run_age,
        "observed_evaluation_age_minutes": evaluation_age,
    }


def build_interval_policy_sensitivity(
    slice_trends: pd.DataFrame,
    *,
    candidates: tuple[IntervalMonitoringPolicyCandidate, ...] | None = None,
    sensitivity_run_id: str | None = None,
    sensitivity_run_timestamp: Any | None = None,
    as_of_utc: Any | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    """Compare reviewed policies without changing the active monitoring policy."""
    prepared = prepare_slice_trends(slice_trends)
    candidates = candidates or default_policy_candidates()
    _validate_candidates(candidates)
    run_id = sensitivity_run_id or "ips-" + uuid4().hex[:24]
    if not SENSITIVITY_RUN_ID_PATTERN.fullmatch(run_id):
        raise IntervalPolicySensitivityError(
            "sensitivity_run_id must match ips- plus 24 lowercase hexadecimal characters."
        )
    timestamp = _as_utc(
        sensitivity_run_timestamp or datetime.now(timezone.utc),
        "sensitivity_run_timestamp",
    )
    as_of = _as_utc(as_of_utc or timestamp, "as_of_utc")
    if as_of > timestamp:
        raise IntervalPolicySensitivityError(
            "as_of_utc cannot be later than sensitivity_run_timestamp."
        )
    rows: list[dict[str, Any]] = []
    candidate_fields = [
        name for name in asdict(candidates[0]) if name != "rationale"
    ]
    for candidate in candidates:
        candidate_data = asdict(candidate)
        for _, trend in prepared.iterrows():
            outcome = _slice_outcome(trend, candidate, as_of)
            rows.append(
                {
                    "sensitivity_run_id": run_id,
                    "sensitivity_run_timestamp_utc": timestamp,
                    "sensitivity_as_of_utc": as_of,
                    "trend_run_id": trend["trend_run_id"],
                    "trend_run_timestamp_utc": trend[
                        "trend_run_timestamp_utc"
                    ],
                    **{
                        name: candidate_data[name]
                        for name in candidate_fields
                    },
                    "policy_rationale": candidate.rationale,
                    **{
                        column: trend[column]
                        for column in SLICE_IDENTITY_COLUMNS
                    },
                    "retained_monitor_status": trend["monitor_status"],
                    "latest_interval_run_id": trend[
                        "latest_interval_run_id"
                    ],
                    "latest_interval_run_timestamp_utc": trend[
                        "latest_interval_run_timestamp_utc"
                    ],
                    "latest_evaluation_end_utc": trend[
                        "latest_evaluation_end_utc"
                    ],
                    "observed_recent_interval_run_count": int(
                        trend["recent_interval_run_count"]
                    ),
                    "observed_reference_interval_run_count": int(
                        trend["reference_interval_run_count"]
                    ),
                    "observed_minimum_calibration_observation_count": int(
                        trend[
                            "recent_minimum_calibration_observation_count"
                        ]
                    ),
                    "observed_coverage_shortfall_pct_points": float(
                        trend["latest_coverage_shortfall_pct_points"]
                    ),
                    "observed_coverage_drop_pct_points": None
                    if pd.isna(trend["coverage_drop_pct_points"])
                    else float(trend["coverage_drop_pct_points"]),
                    "observed_average_interval_width_increase_pct": None
                    if pd.isna(
                        trend["average_interval_width_increase_pct"]
                    )
                    else float(
                        trend["average_interval_width_increase_pct"]
                    ),
                    "observed_calibration_history_drop_pct": None
                    if pd.isna(trend["calibration_history_drop_pct"])
                    else float(trend["calibration_history_drop_pct"]),
                    **outcome,
                    **{field: False for field in AUTHORITY_FIELDS},
                    "sensitivity_contract_version": (
                        SENSITIVITY_CONTRACT_VERSION
                    ),
                }
            )
    slices = pd.DataFrame(rows)
    active = slices.loc[slices["policy_role"] == "active_reference"]
    active_statuses: dict[str, str] = {}
    retained_statuses: dict[str, str] = {}
    for scenario, group in active.groupby("scenario", sort=True):
        status = _status(
            int((group["candidate_slice_status"] == FAILED_STATUS).any()),
            int((group["candidate_slice_status"] == WARNING_STATUS).any()),
        )
        retained_values = set(group["retained_monitor_status"])
        if len(retained_values) != 1:
            raise IntervalPolicySensitivityError(
                f"Scenario {scenario} has inconsistent retained status."
            )
        retained = next(iter(retained_values))
        if status != retained:
            raise IntervalPolicySensitivityError(
                "Active-reference policy does not reproduce retained status "
                f"for {scenario}."
            )
        active_statuses[scenario] = status
        retained_statuses[scenario] = retained
    summary_rows: list[dict[str, Any]] = []
    for (scenario, policy_id), group in slices.groupby(
        ["scenario", "policy_id"], sort=True
    ):
        candidate_status = _status(
            int((group["candidate_slice_status"] == FAILED_STATUS).any()),
            int((group["candidate_slice_status"] == WARNING_STATUS).any()),
        )
        role = str(group.iloc[0]["policy_role"])
        active_status = active_statuses[scenario]
        changed = candidate_status != active_status
        summary_rows.append(
            {
                "sensitivity_run_id": run_id,
                "sensitivity_run_timestamp_utc": timestamp,
                "sensitivity_as_of_utc": as_of,
                "trend_run_id": str(group.iloc[0]["trend_run_id"]),
                "scenario": scenario,
                "policy_id": policy_id,
                "policy_role": role,
                "policy_version": str(group.iloc[0]["policy_version"]),
                "policy_rationale": str(group.iloc[0]["policy_rationale"]),
                "retained_monitor_status": retained_statuses[scenario],
                "active_reference_status": active_status,
                "candidate_status": candidate_status,
                "status_changed_from_active": changed,
                "sensitivity_class": "active_reference"
                if role == "active_reference"
                else "status_sensitive"
                if changed
                else "status_robust",
                "slice_count": int(len(group)),
                "healthy_slice_count": int(
                    (group["candidate_slice_status"] == HEALTHY_STATUS).sum()
                ),
                "warning_slice_count": int(
                    (group["candidate_slice_status"] == WARNING_STATUS).sum()
                ),
                "failed_slice_count": int(
                    (group["candidate_slice_status"] == FAILED_STATUS).sum()
                ),
                "changed_slice_count": int(
                    (group["candidate_slice_status"] != active_status).sum()
                ),
                "human_review_required": bool(
                    role != "active_reference" and changed
                ),
                **{field: False for field in AUTHORITY_FIELDS},
                "sensitivity_contract_version": (
                    SENSITIVITY_CONTRACT_VERSION
                ),
            }
        )
    summary = pd.DataFrame(summary_rows)
    slices = slices.sort_values(
        [
            "scenario",
            "policy_id",
            "source_area",
            "requested_horizon_minutes",
            "model_name",
            "target_coverage_level",
        ]
    ).reset_index(drop=True)
    summary = summary.sort_values(
        ["scenario", "policy_id"]
    ).reset_index(drop=True)
    return slices, summary, _markdown_report(summary)


def _markdown_report(summary: pd.DataFrame) -> str:
    lines = [
        "# Interval-monitoring policy sensitivity review",
        "",
        (
            "This report is counterfactual evidence for named human review. "
            "It does not update the active policy, mutate retained evidence, "
            "recalibrate an interval, change a model or schedule, promote a "
            "candidate, or deliver an alert."
        ),
        "",
        (
            "| Scenario | Candidate | Role | Retained status | Candidate "
            "status | Sensitivity | Changed slices |"
        ),
        "| --- | --- | --- | --- | --- | --- | ---: |",
    ]
    for row in summary.itertuples(index=False):
        lines.append(
            f"| {row.scenario} | {row.policy_id} | {row.policy_role} | "
            f"{row.retained_monitor_status} | {row.candidate_status} | "
            f"{row.sensitivity_class} | {int(row.changed_slice_count)} |"
        )
    lines.extend(
        [
            "",
            (
                "Empirical interval coverage is retrospective evidence, not "
                "an unconditional future guarantee under distribution shift."
            ),
            "",
            (
                "Any threshold decision requires a separate immutable named "
                "human review record; this report has no activation authority."
            ),
            "",
        ]
    )
    return "\n".join(lines)


def read_frame(path: Path) -> pd.DataFrame:
    path = Path(path)
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    if path.suffix.lower() in {".parquet", ".pq"}:
        return pd.read_parquet(path)
    raise IntervalPolicySensitivityError(
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
            raise IntervalPolicySensitivityError(
                "output_format must be csv or parquet."
            )
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)
    return path

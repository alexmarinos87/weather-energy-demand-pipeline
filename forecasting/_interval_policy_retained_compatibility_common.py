from __future__ import annotations

from dataclasses import asdict
import re
from typing import Any

import pandas as pd

from forecasting._interval_policy_candidate_revision_common import canonical, digest, utc_timestamp
from forecasting.interval_monitoring import PredictionIntervalMonitoringConfig
from forecasting.interval_policy_sensitivity import PolicyCandidate, STATUSES

COMPATIBILITY_CONTRACT_VERSION = "interval-policy-retained-compatibility-v2"
COMPATIBILITY_RUN_ID_PATTERN = re.compile(r"^ipca-[0-9a-f]{24}$")
PREVIOUS_POLICY_ID = "previous-five-point"
CURRENT_POLICY_ID = "reviewed-three-point"
PREVIOUS_POLICY_ROLE = "previous_reference"
CURRENT_POLICY_ROLE = "current_reference"
PREVIOUS_SHORTFALL_THRESHOLD = 5.0
CURRENT_SHORTFALL_THRESHOLD = 3.0
COMPATIBILITY_SAFETY_FIELDS = (
    "historical_statuses_rewritten", "retained_evidence_mutated",
    "monitoring_rerun_performed", "threshold_activation_performed",
    "interval_recalibration_performed", "model_change_performed",
    "fabric_execution_performed", "schedule_change_performed",
    "promotion_change_performed", "alert_delivery_performed",
    "deployment_performed", "external_publication_performed",
)
SUMMARY_REQUIRED_COLUMNS = {
    "compatibility_run_id", "compatibility_run_timestamp_utc", "trend_run_id",
    "scenario", "retained_monitor_status", "previous_policy_id", "previous_policy_status",
    "previous_shortfall_threshold_pct_points", "current_policy_id", "current_policy_status",
    "current_shortfall_threshold_pct_points", "slice_count", "changed_slice_count",
    "newly_failed_slice_count", "compatibility_classification", "retained_status_compatibility",
    "human_review_required", "compatibility_contract_version",
    "source_monitor_run_id", "source_monitor_as_of_utc", "source_health_checks_sha256",
    "comparison_engine_contract_version", *COMPATIBILITY_SAFETY_FIELDS,
}


class IntervalPolicyRetainedCompatibilityError(ValueError):
    """Raised when retained policy compatibility evidence is malformed or unsafe."""


def _candidate(candidate_id: str, role: str, version: str, rationale: str,
               config: PredictionIntervalMonitoringConfig) -> PolicyCandidate:
    return PolicyCandidate(
        candidate_id=candidate_id, candidate_role=role,
        candidate_version=version, rationale=rationale,
        min_recent_interval_runs=config.min_recent_interval_runs,
        min_reference_interval_runs=config.min_reference_interval_runs,
        max_interval_run_age_minutes=config.max_interval_run_age_minutes,
        max_evaluation_age_minutes=config.max_evaluation_age_minutes,
        min_calibration_observation_count=config.min_calibration_observation_count,
        max_recent_coverage_shortfall_pct_points=config.max_recent_coverage_shortfall_pct_points,
        max_coverage_drop_pct_points=config.max_coverage_drop_pct_points,
        max_average_interval_width_increase_pct=config.max_average_interval_width_increase_pct,
        max_calibration_history_drop_pct=config.max_calibration_history_drop_pct,
        source_policy_version=config.policy_version,
    )


def compatibility_policy_candidates() -> tuple[PolicyCandidate, PolicyCandidate]:
    current = PredictionIntervalMonitoringConfig()
    current.validate()
    if float(current.max_recent_coverage_shortfall_pct_points) != 3.0:
        raise IntervalPolicyRetainedCompatibilityError(
            "G38 requires the checked-in three-point recent coverage-shortfall default."
        )
    previous_values = asdict(current)
    previous_values["max_recent_coverage_shortfall_pct_points"] = 5.0
    previous = PredictionIntervalMonitoringConfig(**previous_values)
    previous.validate()
    candidates = (
        _candidate(PREVIOUS_POLICY_ID, "review_candidate", "pre-g36-five-point-policy-v1",
                   "Previous five-point hard limit retained as historical reference.", previous),
        _candidate(CURRENT_POLICY_ID, "active_reference", "reviewed-three-point-policy-v1",
                   "Checked-in three-point hard limit used by future monitoring runs.", current),
    )
    for candidate in candidates:
        candidate.validate()
    left, right = map(asdict, candidates)
    ignored = {"candidate_id", "candidate_role", "candidate_version", "rationale"}
    changed = {key for key in left if key not in ignored and canonical(left[key]) != canonical(right[key])}
    if changed != {"max_recent_coverage_shortfall_pct_points"}:
        raise IntervalPolicyRetainedCompatibilityError(
            "The compared policies must differ only in coverage shortfall."
        )
    return candidates


def _boolean(series: pd.Series, name: str) -> pd.Series:
    if series.dtype == bool:
        return series.astype(bool)
    values = series.astype(str).str.strip().str.lower().map({"true": True, "false": False})
    if values.isna().any():
        raise IntervalPolicyRetainedCompatibilityError(f"{name} must contain boolean values.")
    return values.astype(bool)


def prepare_compatibility_summary(frame: pd.DataFrame) -> pd.DataFrame:
    """Normalize structure and types; digest acceptance also checks relationships."""
    missing = sorted(SUMMARY_REQUIRED_COLUMNS - set(frame.columns))
    if missing:
        raise IntervalPolicyRetainedCompatibilityError(
            "Compatibility summary is missing: " + ", ".join(missing) + "."
        )
    if frame.empty:
        raise IntervalPolicyRetainedCompatibilityError("Compatibility summary must not be empty.")
    prepared = frame.copy()
    text_columns = (
        "compatibility_run_id", "trend_run_id", "scenario", "retained_monitor_status",
        "previous_policy_id", "previous_policy_status", "current_policy_id", "current_policy_status",
        "compatibility_classification", "retained_status_compatibility", "compatibility_contract_version",
        "source_monitor_run_id", "source_health_checks_sha256", "comparison_engine_contract_version",
    )
    for column in text_columns:
        prepared[column] = prepared[column].fillna("").astype(str).str.strip()
        if prepared[column].eq("").any():
            raise IntervalPolicyRetainedCompatibilityError(f"{column} must contain non-empty values.")
    for column in ("compatibility_run_timestamp_utc", "source_monitor_as_of_utc"):
        prepared[column] = prepared[column].map(lambda value, name=column: utc_timestamp(value, name))
    if prepared["compatibility_run_timestamp_utc"].nunique() != 1 or not (
        prepared["source_monitor_as_of_utc"] <= prepared["compatibility_run_timestamp_utc"]
    ).all():
        raise IntervalPolicyRetainedCompatibilityError("Summary historical timestamp bindings are invalid.")
    if not prepared["source_health_checks_sha256"].str.fullmatch(r"[0-9a-f]{64}").all() or (
        prepared["source_health_checks_sha256"].nunique() != 1
    ):
        raise IntervalPolicyRetainedCompatibilityError("Exactly one valid source_health_checks_sha256 is required.")
    if set(prepared["comparison_engine_contract_version"]) != {"interval-policy-compatibility-v1"}:
        raise IntervalPolicyRetainedCompatibilityError("Canonical comparison engine contract is invalid.")
    if prepared["source_monitor_run_id"].duplicated().any():
        raise IntervalPolicyRetainedCompatibilityError("Each scenario must bind one distinct monitor run.")
    for column in ("slice_count", "changed_slice_count", "newly_failed_slice_count"):
        # Reject before conversion, including when a caller normalizes before hashing.
        if prepared[column].map(pd.api.types.is_bool).any():
            raise IntervalPolicyRetainedCompatibilityError(
                f"{column} must contain counts, not booleans."
            )
        values = pd.to_numeric(prepared[column], errors="coerce")
        if values.isna().any() or (values < 0).any() or not (values % 1 == 0).all():
            raise IntervalPolicyRetainedCompatibilityError(f"{column} must contain non-negative integers.")
        prepared[column] = values.astype(int)
    if (prepared["slice_count"] < 1).any():
        raise IntervalPolicyRetainedCompatibilityError("slice_count must be positive.")
    for column, expected in (("previous_shortfall_threshold_pct_points", 5.0),
                             ("current_shortfall_threshold_pct_points", 3.0)):
        values = pd.to_numeric(prepared[column], errors="coerce").astype(float)
        if values.isna().any() or not (values == expected).all():
            raise IntervalPolicyRetainedCompatibilityError(f"{column} must remain {expected}.")
        prepared[column] = values
    for column in ("human_review_required", *COMPATIBILITY_SAFETY_FIELDS):
        prepared[column] = _boolean(prepared[column], column)
    if prepared[list(COMPATIBILITY_SAFETY_FIELDS)].any(axis=None):
        raise IntervalPolicyRetainedCompatibilityError("Compatibility evidence contains enabled side-effect fields.")
    if prepared["compatibility_run_id"].nunique() != 1 or not COMPATIBILITY_RUN_ID_PATTERN.fullmatch(
        prepared["compatibility_run_id"].iloc[0]
    ):
        raise IntervalPolicyRetainedCompatibilityError("Exactly one valid compatibility run is required.")
    if prepared["trend_run_id"].nunique() != 1:
        raise IntervalPolicyRetainedCompatibilityError("Exactly one retained trend run is required.")
    if prepared["scenario"].duplicated().any():
        raise IntervalPolicyRetainedCompatibilityError("Compatibility summary must contain one row per scenario.")
    for column in ("retained_monitor_status", "previous_policy_status", "current_policy_status"):
        if not set(prepared[column]).issubset(STATUSES):
            raise IntervalPolicyRetainedCompatibilityError(f"{column} contains an unsupported status.")
    if set(prepared["previous_policy_id"]) != {PREVIOUS_POLICY_ID}:
        raise IntervalPolicyRetainedCompatibilityError("previous_policy_id is invalid.")
    if set(prepared["current_policy_id"]) != {CURRENT_POLICY_ID}:
        raise IntervalPolicyRetainedCompatibilityError("current_policy_id is invalid.")
    if set(prepared["compatibility_contract_version"]) != {COMPATIBILITY_CONTRACT_VERSION}:
        raise IntervalPolicyRetainedCompatibilityError(
            "Compatibility contract version is invalid; legacy v1 evidence must not be relabelled as v2."
        )
    return prepared.sort_values("scenario").reset_index(drop=True)


def _validate_summary_relationships(prepared: pd.DataFrame) -> None:
    """Check consequences of the one fixed error-threshold tightening, not source observations."""
    for row in prepared.itertuples(index=False):
        def reject(reason: str) -> None:
            raise IntervalPolicyRetainedCompatibilityError(
                f"Compatibility summary digest is invalid for scenario {row.scenario!r}: {reason}"
            )

        total = int(row.slice_count)
        changed = int(row.changed_slice_count)
        newly_failed = int(row.newly_failed_slice_count)
        previous, current = row.previous_policy_status, row.current_policy_status
        retained = row.retained_monitor_status
        if not (0 <= changed == newly_failed <= total):
            reject("changed and newly-failed counts must agree and not exceed slice_count.")
        if changed == 0 and previous != current:
            reject("a scenario cannot change status without a changed slice.")
        if changed > 0 and current != "failed":
            reject("every changed slice becomes failed under the stricter error threshold.")
        if previous == "failed" and changed == total:
            reject("a previously failed scenario must retain at least one already-failed slice.")
        if retained not in {previous, current}:
            reject("the retained status must match one of the two source policy outcomes.")
        if previous == current:
            retained_label = "matches_both_policies"
        elif retained == previous:
            retained_label = "matches_previous_policy_only"
        else:
            retained_label = "matches_current_policy_only"
        if row.retained_status_compatibility != retained_label:
            reject("retained_status_compatibility contradicts the recorded statuses.")
        classification = "fully_compatible"
        if changed:
            classification = (
                "scenario_status_escalation" if previous != current
                else "slice_change_without_scenario_change"
            )
        if row.compatibility_classification != classification:
            reject("compatibility_classification contradicts slice or scenario changes.")
        if bool(row.human_review_required) != (changed > 0):
            reject("human_review_required must match whether slice conclusions change.")


def compatibility_summary_sha256(frame: pd.DataFrame) -> str:
    """Hash only structurally valid, internally consistent summary evidence."""
    prepared = prepare_compatibility_summary(frame)
    _validate_summary_relationships(prepared)
    return digest(prepared.to_dict(orient="records"))

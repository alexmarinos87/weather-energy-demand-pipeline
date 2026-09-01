from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping
from uuid import uuid4

import pandas as pd

from forecasting._interval_policy_candidate_revision_common import utc_timestamp
from forecasting._interval_policy_retained_compatibility_common import (
    COMPATIBILITY_CONTRACT_VERSION,
    COMPATIBILITY_RUN_ID_PATTERN,
    COMPATIBILITY_SAFETY_FIELDS,
    CURRENT_POLICY_ID,
    CURRENT_POLICY_ROLE,
    CURRENT_SHORTFALL_THRESHOLD,
    PREVIOUS_POLICY_ID,
    PREVIOUS_POLICY_ROLE,
    PREVIOUS_SHORTFALL_THRESHOLD,
    IntervalPolicyRetainedCompatibilityError,
    compatibility_policy_candidates,
)
from forecasting.interval_health_trends import SLICE_COLUMNS
from forecasting.interval_policy_sensitivity import (
    PolicyCandidate,
    STATUSES,
    STATUS_RANK,
    prepare_policy_sensitivity_input,
)


def _status(errors: int, warnings: int) -> str:
    return "failed" if errors else "warning" if warnings else "healthy"


def _evaluate(
    trend: Mapping[str, Any], candidate: PolicyCandidate, timestamp: pd.Timestamp
) -> dict[str, Any]:
    run_age = (
        timestamp - pd.Timestamp(trend["latest_interval_run_timestamp_utc"])
    ).total_seconds() / 60.0
    evaluation_age = (
        timestamp - pd.Timestamp(trend["latest_evaluation_end_utc"])
    ).total_seconds() / 60.0
    hard = {
        "recent_history_passed": (
            int(trend["recent_interval_run_count"])
            >= candidate.min_recent_interval_runs
        ),
        "run_age_passed": run_age <= candidate.max_interval_run_age_minutes,
        "evaluation_age_passed": evaluation_age <= candidate.max_evaluation_age_minutes,
        "calibration_history_passed": (
            float(trend["recent_minimum_calibration_observation_count"])
            >= candidate.min_calibration_observation_count
        ),
        "coverage_shortfall_passed": (
            float(trend["latest_coverage_shortfall_pct_points"])
            <= candidate.max_recent_coverage_shortfall_pct_points
        ),
    }
    reference_passed = (
        int(trend["reference_interval_run_count"])
        >= candidate.min_reference_interval_runs
    )
    evaluate_drift = bool(trend["reference_history_sufficient"] and reference_passed)
    drift = {
        "coverage_drop_passed": (
            float(trend["coverage_drop_pct_points"])
            <= candidate.max_coverage_drop_pct_points
            if evaluate_drift
            else None
        ),
        "interval_width_increase_passed": (
            float(trend["average_interval_width_increase_pct"])
            <= candidate.max_average_interval_width_increase_pct
            if evaluate_drift
            else None
        ),
        "calibration_history_drop_passed": (
            float(trend["calibration_history_drop_pct"])
            <= candidate.max_calibration_history_drop_pct
            if evaluate_drift
            else None
        ),
    }
    errors = sum(not value for value in hard.values())
    warnings = int(not reference_passed) + sum(
        not value for value in drift.values() if value is not None
    )
    evaluated = _status(errors, warnings)
    return {
        "evaluated_status": evaluated,
        "evaluated_status_rank": STATUS_RANK[evaluated],
        "error_failure_count": errors,
        "warning_failure_count": warnings,
        "latest_interval_run_age_minutes": run_age,
        "latest_evaluation_age_minutes": evaluation_age,
        "observed_recent_interval_run_count": int(trend["recent_interval_run_count"]),
        "observed_reference_interval_run_count": int(
            trend["reference_interval_run_count"]
        ),
        "observed_minimum_calibration_observation_count": float(
            trend["recent_minimum_calibration_observation_count"]
        ),
        "observed_coverage_shortfall_pct_points": float(
            trend["latest_coverage_shortfall_pct_points"]
        ),
        **hard,
        "reference_history_passed": reference_passed,
        "drift_rules_evaluated": evaluate_drift,
        **drift,
    }


def _retained_compatibility(retained: str, previous: str, current: str) -> str:
    if retained == previous == current:
        return "matches_both_policies"
    if retained == previous:
        return "matches_previous_policy_only"
    if retained == current:
        return "matches_current_policy_only"
    return "matches_neither_policy"


def evaluate_retained_policy_compatibility(
    slice_trends: pd.DataFrame,
    *,
    compatibility_run_id: str | None = None,
    compatibility_run_timestamp: Any | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    trends = prepare_policy_sensitivity_input(slice_trends)
    previous_candidate, current_candidate = compatibility_policy_candidates()
    run_id = compatibility_run_id or "ipca-" + uuid4().hex[:24]
    if not COMPATIBILITY_RUN_ID_PATTERN.fullmatch(run_id):
        raise IntervalPolicyRetainedCompatibilityError(
            "compatibility_run_id must be ipca- plus 24 lowercase hexadecimal characters."
        )
    timestamp = utc_timestamp(
        compatibility_run_timestamp or datetime.now(timezone.utc),
        "compatibility_run_timestamp",
    )
    if timestamp < trends["trend_run_timestamp_utc"].max():
        raise IntervalPolicyRetainedCompatibilityError(
            "Compatibility assessment cannot precede the retained trend run."
        )
    rows: list[dict[str, Any]] = []
    for trend in trends.to_dict(orient="records"):
        for candidate in (previous_candidate, current_candidate):
            is_previous = candidate.candidate_id == PREVIOUS_POLICY_ID
            rows.append(
                {
                    "compatibility_run_id": run_id,
                    "compatibility_run_timestamp_utc": timestamp,
                    "trend_run_id": trend["trend_run_id"],
                    "trend_run_timestamp_utc": trend["trend_run_timestamp_utc"],
                    "scenario": trend["scenario"],
                    **{column: trend[column] for column in SLICE_COLUMNS},
                    "policy_id": candidate.candidate_id,
                    "policy_role": PREVIOUS_POLICY_ROLE if is_previous else CURRENT_POLICY_ROLE,
                    "policy_candidate_version": candidate.candidate_version,
                    "source_policy_version": candidate.source_policy_version,
                    "max_recent_coverage_shortfall_pct_points": (
                        candidate.max_recent_coverage_shortfall_pct_points
                    ),
                    "retained_monitor_status": trend["monitor_status"],
                    **_evaluate(trend, candidate, timestamp),
                    "compatibility_contract_version": COMPATIBILITY_CONTRACT_VERSION,
                    **{field: False for field in COMPATIBILITY_SAFETY_FIELDS},
                }
            )
    slices = pd.DataFrame(rows)
    identity = ["scenario", *SLICE_COLUMNS]
    previous = slices.loc[
        slices["policy_id"] == PREVIOUS_POLICY_ID,
        identity + ["evaluated_status"],
    ].rename(columns={"evaluated_status": "previous_policy_slice_status"})
    current = slices.loc[
        slices["policy_id"] == CURRENT_POLICY_ID,
        identity + ["evaluated_status"],
    ].rename(columns={"evaluated_status": "current_policy_slice_status"})
    paired = previous.merge(current, on=identity, validate="one_to_one")
    paired["status_changed_from_previous"] = (
        paired["previous_policy_slice_status"]
        != paired["current_policy_slice_status"]
    )
    paired["newly_failed_under_current_policy"] = (
        (paired["previous_policy_slice_status"] != "failed")
        & (paired["current_policy_slice_status"] == "failed")
    )
    if paired.apply(
        lambda row: STATUS_RANK[row.current_policy_slice_status]
        < STATUS_RANK[row.previous_policy_slice_status],
        axis=1,
    ).any():
        raise IntervalPolicyRetainedCompatibilityError(
            "A tighter shortfall threshold cannot de-escalate a slice status."
        )
    slices = slices.merge(paired, on=identity, validate="many_to_one")
    slices["retained_status_matches_policy"] = (
        slices["retained_monitor_status"] == slices["evaluated_status"]
    )
    summary_rows: list[dict[str, Any]] = []
    for scenario, group in slices.groupby("scenario", sort=True):
        previous_group = group[group["policy_id"] == PREVIOUS_POLICY_ID]
        current_group = group[group["policy_id"] == CURRENT_POLICY_ID]
        previous_status = STATUSES[int(previous_group["evaluated_status_rank"].max())]
        current_status = STATUSES[int(current_group["evaluated_status_rank"].max())]
        retained = str(group["retained_monitor_status"].iloc[0])
        scenario_pairs = paired[paired["scenario"] == scenario]
        changed = int(scenario_pairs["status_changed_from_previous"].sum())
        newly_failed = int(
            scenario_pairs["newly_failed_under_current_policy"].sum()
        )
        classification = (
            "fully_compatible"
            if changed == 0
            else "slice_change_without_scenario_change"
            if previous_status == current_status
            else "scenario_status_escalation"
        )
        retained_compatibility = _retained_compatibility(
            retained, previous_status, current_status
        )
        summary_rows.append(
            {
                "compatibility_run_id": run_id,
                "compatibility_run_timestamp_utc": timestamp,
                "trend_run_id": str(group["trend_run_id"].iloc[0]),
                "scenario": scenario,
                "retained_monitor_status": retained,
                "previous_policy_id": PREVIOUS_POLICY_ID,
                "previous_policy_status": previous_status,
                "previous_shortfall_threshold_pct_points": PREVIOUS_SHORTFALL_THRESHOLD,
                "current_policy_id": CURRENT_POLICY_ID,
                "current_policy_status": current_status,
                "current_shortfall_threshold_pct_points": CURRENT_SHORTFALL_THRESHOLD,
                "slice_count": int(len(previous_group)),
                "changed_slice_count": changed,
                "newly_failed_slice_count": newly_failed,
                "compatibility_classification": classification,
                "retained_status_compatibility": retained_compatibility,
                "human_review_required": bool(
                    changed
                    or retained_compatibility
                    not in {"matches_both_policies", "matches_current_policy_only"}
                ),
                "compatibility_contract_version": COMPATIBILITY_CONTRACT_VERSION,
                **{field: False for field in COMPATIBILITY_SAFETY_FIELDS},
            }
        )
    summary = pd.DataFrame(summary_rows).sort_values("scenario").reset_index(drop=True)
    slices = slices.sort_values(
        ["scenario", "policy_role", "policy_id", *SLICE_COLUMNS]
    ).reset_index(drop=True)
    return slices, summary, _render_report(summary)


def _render_report(summary: pd.DataFrame) -> str:
    lines = [
        "# Retained interval-policy compatibility assessment",
        "",
        "This compares the previous five-point and reviewed three-point policies over identical retained trend slices.",
        "Historical monitor statuses are preserved and are not rewritten.",
        "",
        "| Scenario | Retained | Previous | Current | Classification | Changed | Newly failed |",
        "| --- | --- | --- | --- | --- | ---: | ---: |",
    ]
    for row in summary.itertuples(index=False):
        lines.append(
            f"| {row.scenario} | {row.retained_monitor_status} | "
            f"{row.previous_policy_status} | {row.current_policy_status} | "
            f"{row.compatibility_classification} | {row.changed_slice_count} | "
            f"{row.newly_failed_slice_count} |"
        )
    lines += [
        "",
        "No monitoring rerun, retained-evidence mutation, Fabric execution, ",
        "recalibration, model or schedule change, alert, deployment, or external ",
        "publication is performed.",
        "",
    ]
    return "\n".join(lines)

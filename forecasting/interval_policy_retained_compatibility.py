from __future__ import annotations

from datetime import datetime, timezone
from math import isclose, isfinite
from typing import Any
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
from forecasting.interval_policy_compatibility import (
    COMPATIBILITY_CONTRACT_VERSION as ENGINE_CONTRACT_VERSION,
    TARGET_CHECK,
    assess_retained_policy_compatibility,
    prepare_retained_health_checks,
)
from forecasting.interval_policy_sensitivity import STATUS_RANK, prepare_policy_sensitivity_input

# These are reporting aliases, not another implementation of monitoring rules.
CHECK_FIELDS = {
    "recent_history_passed": "minimum_recent_interval_runs",
    "run_age_passed": "latest_interval_run_age_minutes",
    "evaluation_age_passed": "latest_interval_evaluation_age_minutes",
    "calibration_history_passed": "minimum_recent_calibration_observation_count",
    "coverage_shortfall_passed": TARGET_CHECK,
    "reference_history_passed": "minimum_reference_interval_runs",
    "coverage_drop_passed": "maximum_interval_coverage_drop_pct_points",
    "interval_width_increase_passed": "maximum_average_interval_width_increase_pct",
    "calibration_history_drop_passed": "maximum_calibration_history_drop_pct",
}
DRIFT_FIELDS = ("coverage_drop_passed", "interval_width_increase_passed", "calibration_history_drop_passed")


def _close(actual: Any, expected: Any, label: str) -> None:
    try:
        left, right = float(actual), float(expected)
    except (TypeError, ValueError) as exc:
        raise IntervalPolicyRetainedCompatibilityError(f"Invalid {label} evidence.") from exc
    if not (isfinite(left) and isfinite(right) and isclose(left, right, rel_tol=0, abs_tol=1e-8)):
        raise IntervalPolicyRetainedCompatibilityError(
            f"Trend/check {label} evidence does not match."
        )


def _bind_trends(trends: pd.DataFrame, checks: pd.DataFrame) -> dict[tuple, pd.DataFrame]:
    required = {"monitor_run_id", "latest_interval_run_id", "recent_empirical_coverage_pct"}
    missing = sorted(required - set(trends))
    if missing:
        raise IntervalPolicyRetainedCompatibilityError("Trend bindings are missing: " + ", ".join(missing))
    for column in ("monitor_run_id", "latest_interval_run_id"):
        values = trends[column]
        if not values.map(lambda value: isinstance(value, str) and bool(value.strip())).all():
            raise IntervalPolicyRetainedCompatibilityError(f"{column} must contain non-empty text.")
    mapping = trends[["scenario", "monitor_run_id"]].drop_duplicates()
    if mapping["scenario"].duplicated().any() or mapping["monitor_run_id"].duplicated().any():
        raise IntervalPolicyRetainedCompatibilityError(
            "Each scenario must bind exactly one distinct monitor run."
        )
    identity = ["monitor_run_id", *SLICE_COLUMNS]
    groups = {key: group for key, group in checks.groupby(identity, sort=False, dropna=False)}
    trend_keys = set(trends[identity].itertuples(index=False, name=None))
    if trend_keys != set(groups):
        raise IntervalPolicyRetainedCompatibilityError(
            "Trend/check exact slice sets differ; no missing or extra slice is allowed."
        )
    for trend in trends.to_dict(orient="records"):
        group = groups[tuple(trend[column] for column in identity)]
        indexed = group.set_index("check_name")
        first = group.iloc[0]
        if trend["latest_interval_run_id"] != first["latest_interval_run_id"]:
            raise IntervalPolicyRetainedCompatibilityError("latest_interval_run_id binding differs.")
        as_of = first["monitor_as_of_utc"]
        if max(as_of, first["monitor_timestamp_utc"]) > trend["trend_run_timestamp_utc"]:
            raise IntervalPolicyRetainedCompatibilityError("Monitoring evidence cannot follow its trend run.")
        coverage = float(trend["recent_empirical_coverage_pct"])
        if not isfinite(coverage) or not 0 <= coverage <= 100:
            raise IntervalPolicyRetainedCompatibilityError("recent_empirical_coverage_pct must be within 0..100.")
        bindings = {
            "minimum_recent_interval_runs": trend["recent_interval_run_count"],
            "minimum_reference_interval_runs": trend["reference_interval_run_count"],
            "minimum_recent_calibration_observation_count": trend["recent_minimum_calibration_observation_count"],
            TARGET_CHECK: max(0.0, 100.0 * trend["target_coverage_level"] - coverage),
            "latest_interval_run_age_minutes": (
                as_of - trend["latest_interval_run_timestamp_utc"]
            ).total_seconds() / 60.0,
            "latest_interval_evaluation_age_minutes": (
                as_of - trend["latest_evaluation_end_utc"]
            ).total_seconds() / 60.0,
        }
        reference_passed = bool(indexed.loc["minimum_reference_interval_runs", "passed"])
        if bool(trend["reference_history_sufficient"]) != reference_passed:
            raise IntervalPolicyRetainedCompatibilityError("Reference-history sufficiency binding differs.")
        if reference_passed:
            bindings.update({
                "maximum_interval_coverage_drop_pct_points": trend["coverage_drop_pct_points"],
                "maximum_average_interval_width_increase_pct": trend["average_interval_width_increase_pct"],
                "maximum_calibration_history_drop_pct": trend["calibration_history_drop_pct"],
            })
        for name, value in bindings.items():
            _close(value, indexed.loc[name, "observed_value"], name)
    return groups


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
    retained_health_checks: pd.DataFrame | None = None,
    compatibility_run_id: str | None = None,
    compatibility_run_timestamp: Any | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    """Adapt scenario reports to the one retained-check comparison engine.

    Trend-only v1 inputs cannot establish the original monitoring reference time.
    Supply the matching original checks; no monitoring is rerun or re-aged here.
    """
    if retained_health_checks is None:
        raise IntervalPolicyRetainedCompatibilityError(
            "retained_health_checks is required. Supply the original checks with "
            "monitor_as_of_utc, or use forecasting.run_interval_policy_compatibility "
            "directly. Trend-only historical reconstruction is no longer supported."
        )
    if "monitor_as_of_utc" not in retained_health_checks:
        raise IntervalPolicyRetainedCompatibilityError("Retained checks require monitor_as_of_utc.")
    trends = prepare_policy_sensitivity_input(slice_trends)
    checks = prepare_retained_health_checks(retained_health_checks)
    groups = _bind_trends(trends, checks)
    previous_candidate, current_candidate = compatibility_policy_candidates()
    run_id = "ipca-" + uuid4().hex[:24] if compatibility_run_id is None else compatibility_run_id
    if not isinstance(run_id, str) or not COMPATIBILITY_RUN_ID_PATTERN.fullmatch(run_id):
        raise IntervalPolicyRetainedCompatibilityError(
            "compatibility_run_id must be ipca- plus 24 lowercase hexadecimal characters."
        )
    timestamp = utc_timestamp(
        datetime.now(timezone.utc) if compatibility_run_timestamp is None else compatibility_run_timestamp,
        "compatibility_run_timestamp",
    )
    if timestamp < trends["trend_run_timestamp_utc"].max():
        raise IntervalPolicyRetainedCompatibilityError("Compatibility assessment cannot precede the retained trend run.")
    canonical_slices, canonical_summary = assess_retained_policy_compatibility(
        checks, assessment_run_id="ipc-" + run_id[5:], assessment_timestamp_utc=timestamp,
    )
    identity = ["monitor_run_id", *SLICE_COLUMNS]
    paired = canonical_slices.rename(columns={"source_monitor_run_id": "monitor_run_id"})
    linked = trends.merge(paired, on=identity, how="inner", validate="one_to_one")
    rows: list[dict[str, Any]] = []
    for trend in linked.to_dict(orient="records"):
        source = groups[tuple(trend[column] for column in identity)].set_index("check_name")
        retained_threshold = float(source.loc[TARGET_CHECK, "threshold_value"])
        retained_slice_status = trend[
            "previous_policy_status" if retained_threshold == 5.0 else "current_policy_status"
        ]
        for candidate in (previous_candidate, current_candidate):
            is_previous = candidate.candidate_id == PREVIOUS_POLICY_ID
            status = trend["previous_policy_status" if is_previous else "current_policy_status"]
            results = {
                field: bool(source.loc[name, "passed"]) if name in source.index else None
                for field, name in CHECK_FIELDS.items()
            }
            # Canonical comparison alone decides whether the changed check flips.
            if TARGET_CHECK in trend["changed_check_names"] and (
                candidate.max_recent_coverage_shortfall_pct_points != retained_threshold
            ):
                results["coverage_shortfall_passed"] = not results["coverage_shortfall_passed"]
            errors = sum(results[field] is False for field in list(CHECK_FIELDS)[:5])
            warnings = sum(results[field] is False for field in list(CHECK_FIELDS)[5:])
            rows.append({
                "compatibility_run_id": run_id,
                "compatibility_run_timestamp_utc": timestamp,
                "trend_run_id": trend["trend_run_id"],
                "trend_run_timestamp_utc": trend["trend_run_timestamp_utc"],
                "scenario": trend["scenario"],
                **{column: trend[column] for column in SLICE_COLUMNS},
                "source_monitor_run_id": trend["monitor_run_id"],
                "source_monitor_as_of_utc": source.iloc[0]["monitor_as_of_utc"],
                "source_health_checks_sha256": trend["source_health_checks_sha256"],
                "comparison_engine_contract_version": ENGINE_CONTRACT_VERSION,
                "policy_id": candidate.candidate_id,
                "policy_role": PREVIOUS_POLICY_ROLE if is_previous else CURRENT_POLICY_ROLE,
                "policy_candidate_version": candidate.candidate_version,
                "source_policy_version": candidate.source_policy_version,
                "max_recent_coverage_shortfall_pct_points": candidate.max_recent_coverage_shortfall_pct_points,
                "retained_monitor_status": trend["monitor_status"],
                "retained_slice_status": retained_slice_status,
                "evaluated_status": status,
                "evaluated_status_rank": STATUS_RANK[status],
                "error_failure_count": errors,
                "warning_failure_count": warnings,
                "latest_interval_run_age_minutes": float(source.loc["latest_interval_run_age_minutes", "observed_value"]),
                "latest_evaluation_age_minutes": float(source.loc["latest_interval_evaluation_age_minutes", "observed_value"]),
                "observed_recent_interval_run_count": int(source.loc["minimum_recent_interval_runs", "observed_value"]),
                "observed_reference_interval_run_count": int(source.loc["minimum_reference_interval_runs", "observed_value"]),
                "observed_minimum_calibration_observation_count": float(source.loc["minimum_recent_calibration_observation_count", "observed_value"]),
                "observed_coverage_shortfall_pct_points": float(source.loc[TARGET_CHECK, "observed_value"]),
                **results,
                "drift_rules_evaluated": all(results[field] is not None for field in DRIFT_FIELDS),
                "previous_policy_slice_status": trend["previous_policy_status"],
                "current_policy_slice_status": trend["current_policy_status"],
                "status_changed_from_previous": bool(trend["status_changed"]),
                "newly_failed_under_current_policy": bool(
                    trend["previous_policy_status"] != "failed" and trend["current_policy_status"] == "failed"
                ),
                "retained_status_matches_policy": retained_slice_status == status,
                "compatibility_contract_version": COMPATIBILITY_CONTRACT_VERSION,
                **{field: False for field in COMPATIBILITY_SAFETY_FIELDS},
            })
    slices = pd.DataFrame(rows)
    summaries: list[dict[str, Any]] = []
    by_run = canonical_summary.set_index("source_monitor_run_id")
    for scenario, group in trends.groupby("scenario", sort=True):
        monitor_id = group["monitor_run_id"].iloc[0]
        direct = by_run.loc[monitor_id]
        monitor_checks = checks[checks["monitor_run_id"].eq(monitor_id)]
        threshold = float(monitor_checks.loc[monitor_checks["check_name"].eq(TARGET_CHECK), "threshold_value"].iloc[0])
        original = direct["previous_overall_status" if threshold == 5.0 else "current_overall_status"]
        retained = group["monitor_status"].iloc[0]
        if retained != original:
            raise IntervalPolicyRetainedCompatibilityError("Retained scenario status contradicts original health checks.")
        previous, current = direct["previous_overall_status"], direct["current_overall_status"]
        changed = int(direct["changed_monitoring_slice_count"])
        compatibility = _retained_compatibility(retained, previous, current)
        summaries.append({
            "compatibility_run_id": run_id,
            "compatibility_run_timestamp_utc": timestamp,
            "trend_run_id": group["trend_run_id"].iloc[0],
            "scenario": scenario,
            "source_monitor_run_id": monitor_id,
            "source_monitor_as_of_utc": monitor_checks["monitor_as_of_utc"].iloc[0],
            "source_health_checks_sha256": direct["source_health_checks_sha256"],
            "comparison_engine_contract_version": ENGINE_CONTRACT_VERSION,
            "retained_monitor_status": retained,
            "previous_policy_id": PREVIOUS_POLICY_ID,
            "previous_policy_status": previous,
            "previous_shortfall_threshold_pct_points": PREVIOUS_SHORTFALL_THRESHOLD,
            "current_policy_id": CURRENT_POLICY_ID,
            "current_policy_status": current,
            "current_shortfall_threshold_pct_points": CURRENT_SHORTFALL_THRESHOLD,
            "slice_count": int(direct["monitoring_slice_count"]),
            "changed_slice_count": changed,
            "newly_failed_slice_count": int(direct["newly_failed_slice_count"]),
            "compatibility_classification": (
                "fully_compatible" if not changed else "slice_change_without_scenario_change"
                if previous == current else "scenario_status_escalation"
            ),
            "retained_status_compatibility": compatibility,
            "human_review_required": bool(changed or compatibility not in {
                "matches_both_policies", "matches_current_policy_only"
            }),
            "compatibility_contract_version": COMPATIBILITY_CONTRACT_VERSION,
            **{field: False for field in COMPATIBILITY_SAFETY_FIELDS},
        })
    summary = pd.DataFrame(summaries).sort_values("scenario").reset_index(drop=True)
    slices = slices.sort_values(["scenario", "policy_role", "policy_id", *SLICE_COLUMNS]).reset_index(drop=True)
    return slices, summary, _render_report(summary)


def _render_report(summary: pd.DataFrame) -> str:
    lines = [
        "# Retained interval-policy compatibility assessment", "",
        "This scenario report delegates to the canonical retained-health-check comparison engine.",
        "Historical monitor statuses are preserved and are not rewritten.",
        "Freshness uses original check evidence; weighted recent coverage is never replaced by the latest row.", "",
        "| Scenario | Retained | Previous | Current | Classification | Changed | Newly failed |",
        "| --- | --- | --- | --- | --- | ---: | ---: |",
    ]
    for row in summary.itertuples(index=False):
        lines.append(
            f"| {row.scenario} | {row.retained_monitor_status} | {row.previous_policy_status} | "
            f"{row.current_policy_status} | {row.compatibility_classification} | "
            f"{row.changed_slice_count} | {row.newly_failed_slice_count} |"
        )
    lines += ["", "No monitoring rerun, retained-evidence mutation, Fabric execution, recalibration, "
              "model or schedule change, alert, deployment, or external publication is performed.", ""]
    return "\n".join(lines)

from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
from hashlib import sha256
import json
from math import isfinite
from pathlib import Path
from typing import Any, Iterable
from uuid import uuid4

import pandas as pd

from forecasting.interval_monitoring import (
    FAILED_STATUS,
    HEALTHY_STATUS,
    MONITORING_VERSION,
    POLICY_VERSION,
    WARNING_STATUS,
    PredictionIntervalMonitoringConfig,
)


COMPATIBILITY_CONTRACT_VERSION = "interval-policy-compatibility-v1"
PREVIOUS_POLICY_ID = "previous-five-point-policy"
CURRENT_POLICY_ID = "reviewed-three-point-policy"
PREVIOUS_COVERAGE_SHORTFALL_LIMIT = 5.0
TARGET_CHECK = "maximum_recent_coverage_shortfall_pct_points"

IDENTITY_COLUMNS = (
    "source_area",
    "resource_id",
    "city",
    "requested_horizon_minutes",
    "model_name",
    "feature_contract_version",
    "target_coverage_level",
    "interval_contract_version",
)
BASE_CHECKS = {
    "minimum_recent_interval_runs",
    "latest_interval_run_age_minutes",
    "latest_interval_evaluation_age_minutes",
    "minimum_recent_calibration_observation_count",
    TARGET_CHECK,
    "minimum_reference_interval_runs",
}
REQUIRED_COLUMNS = {
    "monitor_run_id",
    "monitor_timestamp_utc",
    "check_scope",
    "severity",
    "check_name",
    "observed_value",
    "threshold_value",
    "comparator",
    "passed",
    *IDENTITY_COLUMNS,
    "latest_interval_run_id",
    "policy_version",
    "monitoring_contract_version",
}
SAFETY_FIELDS = (
    "source_health_checks_mutated",
    "historical_statuses_rewritten",
    "monitoring_rerun_performed",
    "interval_recalibration_performed",
    "model_change_performed",
    "fabric_execution_performed",
    "schedule_change_performed",
    "alert_delivery_performed",
    "deployment_performed",
    "external_publication_performed",
)


class IntervalPolicyCompatibilityError(ValueError):
    """Raised when retained health evidence cannot be compared safely."""


def _as_utc(value: Any, name: str) -> pd.Timestamp:
    try:
        timestamp = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise IntervalPolicyCompatibilityError(
            f"{name} must be a valid timezone-aware timestamp."
        ) from exc
    if pd.isna(timestamp) or timestamp.tzinfo is None:
        raise IntervalPolicyCompatibilityError(f"{name} must be timezone-aware.")
    return timestamp.tz_convert("UTC")


def _bool(value: Any, name: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().casefold()
        if normalized == "true":
            return True
        if normalized == "false":
            return False
    raise IntervalPolicyCompatibilityError(f"{name} must contain booleans.")


def _finite(value: Any, name: str) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise IntervalPolicyCompatibilityError(
            f"{name} must contain finite numeric values."
        ) from exc
    if not isfinite(parsed):
        raise IntervalPolicyCompatibilityError(
            f"{name} must contain finite numeric values."
        )
    return parsed


def _text(value: Any, name: str) -> str:
    parsed = "" if value is None else str(value).strip()
    if not parsed:
        raise IntervalPolicyCompatibilityError(
            f"{name} must contain non-empty values."
        )
    return parsed


def _status(checks: Iterable[tuple[str, bool]]) -> str:
    values = list(checks)
    if any(severity == "error" and not passed for severity, passed in values):
        return FAILED_STATUS
    if any(severity == "warning" and not passed for severity, passed in values):
        return WARNING_STATUS
    return HEALTHY_STATUS


def _overall_status(statuses: Iterable[str]) -> str:
    values = set(statuses)
    if FAILED_STATUS in values:
        return FAILED_STATUS
    if WARNING_STATUS in values:
        return WARNING_STATUS
    return HEALTHY_STATUS


def _comparison_result(observed: float, threshold: float, comparator: str) -> bool:
    if comparator == "<=":
        return observed <= threshold
    if comparator == ">=":
        return observed >= threshold
    raise IntervalPolicyCompatibilityError(
        "Comparator must be '<=' or '>=' for retained health checks."
    )


def _json_value(value: Any) -> Any:
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, datetime):
        return _as_utc(value, "timestamp").isoformat()
    if isinstance(value, bool):
        return value
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        return value.item()
    return value


def _digest_rows(frame: pd.DataFrame, columns: list[str]) -> str:
    records = [
        {column: _json_value(row[column]) for column in columns}
        for _, row in frame.sort_values(columns, kind="mergesort").iterrows()
    ]
    encoded = json.dumps(
        records, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    return sha256(encoded).hexdigest()


def previous_policy_snapshot() -> dict[str, Any]:
    return asdict(
        PredictionIntervalMonitoringConfig(
            max_recent_coverage_shortfall_pct_points=(
                PREVIOUS_COVERAGE_SHORTFALL_LIMIT
            )
        )
    )


def current_policy_snapshot() -> dict[str, Any]:
    snapshot = asdict(PredictionIntervalMonitoringConfig())
    if snapshot["max_recent_coverage_shortfall_pct_points"] != 3.0:
        raise IntervalPolicyCompatibilityError(
            "The checked-in current policy is not the reviewed three-point policy."
        )
    return snapshot


def prepare_retained_health_checks(frame: pd.DataFrame) -> pd.DataFrame:
    """Validate immutable monitoring checks before counterfactual comparison."""
    missing = sorted(REQUIRED_COLUMNS - set(frame.columns))
    if missing:
        raise IntervalPolicyCompatibilityError(
            "Retained health checks are missing required columns: "
            + ", ".join(missing)
            + "."
        )
    if frame.empty:
        raise IntervalPolicyCompatibilityError(
            "Retained health checks must not be empty."
        )
    prepared = frame.copy()
    for column in (
        "monitor_run_id",
        "check_scope",
        "severity",
        "check_name",
        "latest_interval_run_id",
        "policy_version",
        "monitoring_contract_version",
        *IDENTITY_COLUMNS,
    ):
        prepared[column] = prepared[column].map(
            lambda value, name=column: _text(value, name)
        )
    prepared["monitor_timestamp_utc"] = prepared["monitor_timestamp_utc"].map(
        lambda value: _as_utc(value, "monitor_timestamp_utc")
    )
    prepared["observed_value"] = prepared["observed_value"].map(
        lambda value: _finite(value, "observed_value")
    )
    prepared["threshold_value"] = prepared["threshold_value"].map(
        lambda value: _finite(value, "threshold_value")
    )
    prepared["passed"] = prepared["passed"].map(
        lambda value: _bool(value, "passed")
    )
    prepared["requested_horizon_minutes"] = pd.to_numeric(
        prepared["requested_horizon_minutes"], errors="coerce"
    )
    if (
        prepared["requested_horizon_minutes"].isna().any()
        or (prepared["requested_horizon_minutes"] <= 0).any()
        or (prepared["requested_horizon_minutes"] % 1 != 0).any()
    ):
        raise IntervalPolicyCompatibilityError(
            "requested_horizon_minutes must contain positive integers."
        )
    prepared["requested_horizon_minutes"] = prepared[
        "requested_horizon_minutes"
    ].astype(int)
    prepared["target_coverage_level"] = pd.to_numeric(
        prepared["target_coverage_level"], errors="coerce"
    )
    if (
        prepared["target_coverage_level"].isna().any()
        or not prepared["target_coverage_level"].between(
            0, 1, inclusive="neither"
        ).all()
    ):
        raise IntervalPolicyCompatibilityError(
            "target_coverage_level must be strictly between zero and one."
        )
    if not prepared["severity"].isin({"error", "warning"}).all():
        raise IntervalPolicyCompatibilityError(
            "severity must be 'error' or 'warning'."
        )
    if not prepared["comparator"].isin({"<=", ">="}).all():
        raise IntervalPolicyCompatibilityError(
            "comparator must be '<=' or '>=' for every retained check."
        )
    if not prepared["policy_version"].eq(POLICY_VERSION).all():
        raise IntervalPolicyCompatibilityError(
            f"Retained checks must use policy_version={POLICY_VERSION}."
        )
    if not prepared["monitoring_contract_version"].eq(MONITORING_VERSION).all():
        raise IntervalPolicyCompatibilityError(
            "Retained checks use an unsupported monitoring contract version."
        )
    expected_passed = prepared.apply(
        lambda row: _comparison_result(
            float(row["observed_value"]),
            float(row["threshold_value"]),
            str(row["comparator"]),
        ),
        axis=1,
    )
    if not expected_passed.eq(prepared["passed"]).all():
        raise IntervalPolicyCompatibilityError(
            "Retained pass/fail evidence contradicts its observed value, threshold, "
            "or comparator."
        )
    timestamp_count = prepared.groupby("monitor_run_id", sort=False)[
        "monitor_timestamp_utc"
    ].nunique()
    if not timestamp_count.eq(1).all():
        raise IntervalPolicyCompatibilityError(
            "Each monitor_run_id must have exactly one monitor timestamp."
        )
    identity = ["monitor_run_id", *IDENTITY_COLUMNS, "check_name"]
    if prepared.duplicated(subset=identity, keep=False).any():
        raise IntervalPolicyCompatibilityError(
            "Retained health checks contain duplicate run/slice/check identities."
        )
    grouping = ["monitor_run_id", *IDENTITY_COLUMNS]
    for _, group in prepared.groupby(grouping, sort=False, dropna=False):
        names = set(group["check_name"])
        missing_checks = sorted(BASE_CHECKS - names)
        if missing_checks:
            raise IntervalPolicyCompatibilityError(
                "A retained monitoring slice is missing base checks: "
                + ", ".join(missing_checks)
                + "."
            )
        target = group.loc[group["check_name"] == TARGET_CHECK]
        if len(target) != 1:
            raise IntervalPolicyCompatibilityError(
                "Each retained monitoring slice must contain one coverage-shortfall "
                "check."
            )
        if float(target.iloc[0]["threshold_value"]) not in {3.0, 5.0}:
            raise IntervalPolicyCompatibilityError(
                "Coverage-shortfall evidence must come from the reviewed three-point "
                "or previous five-point policy."
            )
    return prepared.sort_values(identity, kind="mergesort").reset_index(drop=True)


def assess_retained_policy_compatibility(
    retained_health_checks: pd.DataFrame,
    *,
    assessment_run_id: str | None = None,
    assessment_timestamp_utc: Any | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Compare previous and current policies without mutating retained evidence."""
    prepared = prepare_retained_health_checks(retained_health_checks)
    current = current_policy_snapshot()
    previous = previous_policy_snapshot()
    run_id = _text(
        assessment_run_id or f"ipc-{uuid4().hex[:24]}", "assessment_run_id"
    )
    timestamp = _as_utc(
        assessment_timestamp_utc or datetime.now(timezone.utc),
        "assessment_timestamp_utc",
    )
    source_digest = _digest_rows(prepared, sorted(REQUIRED_COLUMNS))
    grouping = ["monitor_run_id", *IDENTITY_COLUMNS]
    rows: list[dict[str, Any]] = []
    for _, group in prepared.groupby(grouping, sort=True, dropna=False):
        first = group.iloc[0]
        previous_results: list[tuple[str, bool]] = []
        current_results: list[tuple[str, bool]] = []
        changed_checks: list[str] = []
        observed_shortfall = None
        for _, check in group.iterrows():
            name = str(check["check_name"])
            retained_passed = bool(check["passed"])
            if name == TARGET_CHECK:
                observed_shortfall = float(check["observed_value"])
                previous_passed = (
                    observed_shortfall <= PREVIOUS_COVERAGE_SHORTFALL_LIMIT
                )
                current_passed = observed_shortfall <= float(
                    current["max_recent_coverage_shortfall_pct_points"]
                )
            else:
                previous_passed = retained_passed
                current_passed = retained_passed
            previous_results.append((str(check["severity"]), previous_passed))
            current_results.append((str(check["severity"]), current_passed))
            if previous_passed != current_passed:
                changed_checks.append(name)
        if observed_shortfall is None:
            raise IntervalPolicyCompatibilityError(
                "Coverage-shortfall evidence was not available for a slice."
            )
        previous_status = _status(previous_results)
        current_status = _status(current_results)
        status_changed = previous_status != current_status
        rows.append(
            {
                "compatibility_run_id": run_id,
                "compatibility_timestamp_utc": timestamp,
                "source_health_checks_sha256": source_digest,
                "source_monitor_run_id": str(first["monitor_run_id"]),
                "source_monitor_timestamp_utc": first["monitor_timestamp_utc"],
                **{column: first[column] for column in IDENTITY_COLUMNS},
                "latest_interval_run_id": str(first["latest_interval_run_id"]),
                "previous_policy_id": PREVIOUS_POLICY_ID,
                "previous_policy_version": str(previous["policy_version"]),
                "previous_coverage_shortfall_limit_pct_points": (
                    PREVIOUS_COVERAGE_SHORTFALL_LIMIT
                ),
                "current_policy_id": CURRENT_POLICY_ID,
                "current_policy_version": str(current["policy_version"]),
                "current_coverage_shortfall_limit_pct_points": float(
                    current["max_recent_coverage_shortfall_pct_points"]
                ),
                "observed_recent_coverage_shortfall_pct_points": (
                    observed_shortfall
                ),
                "previous_policy_status": previous_status,
                "current_policy_status": current_status,
                "status_changed": status_changed,
                "status_transition": f"{previous_status}_to_{current_status}",
                "changed_check_count": len(changed_checks),
                "changed_check_names": sorted(changed_checks),
                "requires_human_review": status_changed,
                "compatibility_contract_version": (
                    COMPATIBILITY_CONTRACT_VERSION
                ),
                **{field: False for field in SAFETY_FIELDS},
            }
        )
    slice_frame = pd.DataFrame(rows)
    summaries: list[dict[str, Any]] = []
    for monitor_run_id, group in slice_frame.groupby(
        "source_monitor_run_id", sort=True
    ):
        previous_status = _overall_status(group["previous_policy_status"])
        current_status = _overall_status(group["current_policy_status"])
        changed_count = int(group["status_changed"].sum())
        summaries.append(
            {
                "compatibility_run_id": run_id,
                "compatibility_timestamp_utc": timestamp,
                "source_health_checks_sha256": source_digest,
                "source_monitor_run_id": monitor_run_id,
                "source_monitor_timestamp_utc": group.iloc[0][
                    "source_monitor_timestamp_utc"
                ],
                "previous_policy_id": PREVIOUS_POLICY_ID,
                "previous_policy_version": str(previous["policy_version"]),
                "previous_coverage_shortfall_limit_pct_points": (
                    PREVIOUS_COVERAGE_SHORTFALL_LIMIT
                ),
                "current_policy_id": CURRENT_POLICY_ID,
                "current_policy_version": str(current["policy_version"]),
                "current_coverage_shortfall_limit_pct_points": float(
                    current["max_recent_coverage_shortfall_pct_points"]
                ),
                "previous_overall_status": previous_status,
                "current_overall_status": current_status,
                "overall_status_changed": previous_status != current_status,
                "monitoring_slice_count": int(len(group)),
                "changed_monitoring_slice_count": changed_count,
                "unchanged_monitoring_slice_count": int(len(group) - changed_count),
                "newly_failed_slice_count": int(
                    (
                        (group["previous_policy_status"] != FAILED_STATUS)
                        & (group["current_policy_status"] == FAILED_STATUS)
                    ).sum()
                ),
                "compatibility_classification": (
                    "policy_change_affects_retained_conclusions"
                    if changed_count
                    else "retained_conclusions_unchanged"
                ),
                "requires_human_review": bool(changed_count),
                "compatibility_contract_version": (
                    COMPATIBILITY_CONTRACT_VERSION
                ),
                **{field: False for field in SAFETY_FIELDS},
            }
        )
    summary_frame = pd.DataFrame(summaries)
    return (
        slice_frame.sort_values(
            ["source_monitor_run_id", *IDENTITY_COLUMNS], kind="mergesort"
        ).reset_index(drop=True),
        summary_frame.sort_values(
            "source_monitor_run_id", kind="mergesort"
        ).reset_index(drop=True),
    )


def render_compatibility_report(
    slices: pd.DataFrame, summary: pd.DataFrame
) -> str:
    if slices.empty or summary.empty:
        raise IntervalPolicyCompatibilityError(
            "Compatibility report requires slice and summary evidence."
        )
    lines = [
        "# Retained interval-policy compatibility assessment",
        "",
        f"Compatibility run: `{summary.iloc[0]['compatibility_run_id']}`",
        "",
        "This report compares the previous five-point and reviewed three-point ",
        "recent coverage-shortfall policies over the same immutable retained ",
        "health-check rows. It does not rerun monitoring or rewrite history.",
        "",
        "| Monitor run | Previous status | Current status | Slices | Changed | Newly failed |",
        "| --- | --- | --- | ---: | ---: | ---: |",
    ]
    for _, row in summary.iterrows():
        lines.append(
            "| {source_monitor_run_id} | {previous_overall_status} | "
            "{current_overall_status} | {monitoring_slice_count} | "
            "{changed_monitoring_slice_count} | {newly_failed_slice_count} |".format(
                **row.to_dict()
            )
        )
    lines.extend(
        [
            "",
            "## Human authority boundary",
            "",
            "The comparison is counterfactual retained evidence only. It does not ",
            "change prior health rows, activate a policy, recalibrate an interval, ",
            "execute Fabric, alter a schedule or model, deliver an alert, deploy, ",
            "or publish externally.",
            "",
        ]
    )
    return "\n".join(lines)


def _write_frame(frame: pd.DataFrame, path: Path, output_format: str) -> None:
    if path.exists():
        raise FileExistsError(f"Refusing to overwrite existing evidence: {path}")
    if output_format == "csv":
        frame.to_csv(path, index=False)
    elif output_format == "parquet":
        frame.to_parquet(path, index=False)
    else:
        raise IntervalPolicyCompatibilityError(
            "output_format must be 'csv' or 'parquet'."
        )


def write_compatibility_assessment(
    output_dir: str | Path,
    slices: pd.DataFrame,
    summary: pd.DataFrame,
    *,
    output_format: str = "parquet",
) -> dict[str, Path]:
    if slices.empty or summary.empty:
        raise IntervalPolicyCompatibilityError(
            "Cannot write empty compatibility evidence."
        )
    run_ids = set(slices["compatibility_run_id"]) | set(
        summary["compatibility_run_id"]
    )
    if len(run_ids) != 1:
        raise IntervalPolicyCompatibilityError(
            "Compatibility outputs must contain exactly one run ID."
        )
    run_id = next(iter(run_ids))
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    extension = "csv" if output_format == "csv" else "parquet"
    paths = {
        "slices": root
        / f"interval_policy_compatibility_slices_{run_id}.{extension}",
        "summary": root
        / f"interval_policy_compatibility_summary_{run_id}.{extension}",
        "report": root / f"interval_policy_compatibility_report_{run_id}.md",
    }
    if paths["report"].exists():
        raise FileExistsError(
            f"Refusing to overwrite existing evidence: {paths['report']}"
        )
    _write_frame(slices, paths["slices"], output_format)
    _write_frame(summary, paths["summary"], output_format)
    paths["report"].write_text(
        render_compatibility_report(slices, summary), encoding="utf-8"
    )
    return paths

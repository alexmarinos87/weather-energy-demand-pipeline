from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
from hashlib import sha256
import json
from math import isfinite
from pathlib import Path
import re
from tempfile import TemporaryDirectory
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
RUN_ID_PATTERN = re.compile(r"ipc-[0-9a-f]{24}")

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
# Scope, severity, comparator and configuration field from the canonical monitor.
CHECK_CONTRACTS = {
    "minimum_recent_interval_runs": (
        "history", "error", ">=", "min_recent_interval_runs"
    ),
    "latest_interval_run_age_minutes": (
        "freshness", "error", "<=", "max_interval_run_age_minutes"
    ),
    "latest_interval_evaluation_age_minutes": (
        "freshness", "error", "<=", "max_evaluation_age_minutes"
    ),
    "minimum_recent_calibration_observation_count": (
        "calibration", "error", ">=", "min_calibration_observation_count"
    ),
    TARGET_CHECK: (
        "coverage", "error", "<=", "max_recent_coverage_shortfall_pct_points"
    ),
    "minimum_reference_interval_runs": (
        "history", "warning", ">=", "min_reference_interval_runs"
    ),
    "maximum_interval_coverage_drop_pct_points": (
        "coverage", "warning", "<=", "max_coverage_drop_pct_points"
    ),
    "maximum_average_interval_width_increase_pct": (
        "width", "warning", "<=", "max_average_interval_width_increase_pct"
    ),
    "maximum_calibration_history_drop_pct": (
        "calibration", "warning", "<=", "max_calibration_history_drop_pct"
    ),
}
DRIFT_CHECKS = set(CHECK_CONTRACTS) - BASE_CHECKS
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
    if pd.api.types.is_bool(value):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().casefold()
        if normalized == "true":
            return True
        if normalized == "false":
            return False
    raise IntervalPolicyCompatibilityError(f"{name} must contain booleans.")


def _finite(value: Any, name: str) -> float:
    if pd.api.types.is_bool(value):
        raise IntervalPolicyCompatibilityError(
            f"{name} must contain finite numeric values, not booleans."
        )
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
    if not pd.api.types.is_scalar(value) or pd.isna(value):
        raise IntervalPolicyCompatibilityError(
            f"{name} must contain non-empty values."
        )
    parsed = str(value).strip()
    if not parsed:
        raise IntervalPolicyCompatibilityError(
            f"{name} must contain non-empty values."
        )
    return parsed


def _run_id(value: Any) -> str:
    if not isinstance(value, str) or not RUN_ID_PATTERN.fullmatch(value):
        raise IntervalPolicyCompatibilityError(
            "assessment_run_id must be ipc- plus 24 lowercase hexadecimal characters."
        )
    return value


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
    config = PredictionIntervalMonitoringConfig(
        max_recent_coverage_shortfall_pct_points=PREVIOUS_COVERAGE_SHORTFALL_LIMIT
    )
    config.validate()
    return asdict(config)


def current_policy_snapshot() -> dict[str, Any]:
    config = PredictionIntervalMonitoringConfig()
    config.validate()
    snapshot = asdict(config)
    if snapshot["max_recent_coverage_shortfall_pct_points"] != 3.0:
        raise IntervalPolicyCompatibilityError(
            "The checked-in current policy is not the reviewed three-point policy."
        )
    return snapshot


def _validate_check_contract(group: pd.DataFrame, current: dict[str, Any]) -> None:
    names = set(group["check_name"])
    missing_checks = sorted(BASE_CHECKS - names)
    if missing_checks:
        raise IntervalPolicyCompatibilityError(
            "A retained monitoring slice is missing base checks: "
            + ", ".join(missing_checks) + "."
        )
    if names - set(CHECK_CONTRACTS):
        raise IntervalPolicyCompatibilityError(
            "Retained evidence contains a check outside the canonical monitor contract."
        )
    if group["latest_interval_run_id"].nunique() != 1:
        raise IntervalPolicyCompatibilityError(
            "Each retained slice must have one latest_interval_run_id."
        )
    for check in group.itertuples(index=False):
        scope, severity, comparator, field = CHECK_CONTRACTS[check.check_name]
        if (check.check_scope, check.severity, check.comparator) != (
            scope, severity, comparator
        ):
            raise IntervalPolicyCompatibilityError(
                f"{check.check_name} contradicts canonical scope/severity/comparator semantics."
            )
        allowed = {3.0, 5.0} if check.check_name == TARGET_CHECK else {float(current[field])}
        if float(check.threshold_value) not in allowed:
            raise IntervalPolicyCompatibilityError(
                "Retained threshold must match the reviewed three-point or previous "
                "five-point policy; non-target thresholds must remain unchanged."
            )
        if check.check_name in BASE_CHECKS and check.observed_value < 0:
            raise IntervalPolicyCompatibilityError(
                f"{check.check_name} must have a non-negative observed value."
            )
    reference_passed = bool(group.loc[
        group["check_name"].eq("minimum_reference_interval_runs"), "passed"
    ].iloc[0])
    expected_drift = DRIFT_CHECKS if reference_passed else set()
    if names & DRIFT_CHECKS != expected_drift:
        raise IntervalPolicyCompatibilityError(
            "Retained drift checks must be complete exactly when reference history passes."
        )


def prepare_retained_health_checks(frame: pd.DataFrame) -> pd.DataFrame:
    """Validate immutable monitoring checks before counterfactual comparison."""
    missing = sorted(REQUIRED_COLUMNS - set(frame.columns))
    if missing:
        raise IntervalPolicyCompatibilityError(
            "Retained health checks are missing required columns: "
            + ", ".join(missing) + "."
        )
    if frame.empty:
        raise IntervalPolicyCompatibilityError("Retained health checks must not be empty.")
    prepared = frame.copy()
    for column in (
        "monitor_run_id", "check_scope", "severity", "check_name",
        "latest_interval_run_id", "policy_version", "monitoring_contract_version",
        *IDENTITY_COLUMNS,
    ):
        prepared[column] = prepared[column].map(
            lambda value, name=column: _text(value, name)
        )
    prepared["monitor_timestamp_utc"] = prepared["monitor_timestamp_utc"].map(
        lambda value: _as_utc(value, "monitor_timestamp_utc")
    )
    if "monitor_as_of_utc" in prepared:
        prepared["monitor_as_of_utc"] = prepared["monitor_as_of_utc"].map(
            lambda value: _as_utc(value, "monitor_as_of_utc")
        )
        if not prepared.groupby("monitor_run_id")["monitor_as_of_utc"].nunique().eq(1).all():
            raise IntervalPolicyCompatibilityError(
                "Each monitor_run_id must have exactly one monitor_as_of_utc."
            )
    for column in ("observed_value", "threshold_value"):
        prepared[column] = prepared[column].map(
            lambda value, name=column: _finite(value, name)
        )
    prepared["passed"] = prepared["passed"].map(lambda value: _bool(value, "passed"))
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
    prepared["requested_horizon_minutes"] = prepared["requested_horizon_minutes"].astype(int)
    prepared["target_coverage_level"] = pd.to_numeric(
        prepared["target_coverage_level"], errors="coerce"
    )
    if (
        prepared["target_coverage_level"].isna().any()
        or not prepared["target_coverage_level"].between(0, 1, inclusive="neither").all()
    ):
        raise IntervalPolicyCompatibilityError(
            "target_coverage_level must be strictly between zero and one."
        )
    if not prepared["severity"].isin({"error", "warning"}).all():
        raise IntervalPolicyCompatibilityError("severity must be 'error' or 'warning'.")
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
            float(row["observed_value"]), float(row["threshold_value"]), str(row["comparator"])
        ), axis=1,
    )
    if not expected_passed.eq(prepared["passed"]).all():
        raise IntervalPolicyCompatibilityError(
            "Retained pass/fail evidence contradicts its observed value, threshold, or comparator."
        )
    timestamp_count = prepared.groupby("monitor_run_id", sort=False)["monitor_timestamp_utc"].nunique()
    if not timestamp_count.eq(1).all():
        raise IntervalPolicyCompatibilityError(
            "Each monitor_run_id must have exactly one monitor timestamp."
        )
    identity = ["monitor_run_id", *IDENTITY_COLUMNS, "check_name"]
    if prepared.duplicated(subset=identity, keep=False).any():
        raise IntervalPolicyCompatibilityError(
            "Retained health checks contain duplicate run/slice/check identities."
        )
    current = current_policy_snapshot()
    grouping = ["monitor_run_id", *IDENTITY_COLUMNS]
    for _, group in prepared.groupby(grouping, sort=False, dropna=False):
        _validate_check_contract(group, current)
    target_thresholds = prepared.loc[prepared["check_name"].eq(TARGET_CHECK)].groupby(
        "monitor_run_id"
    )["threshold_value"].nunique()
    if not target_thresholds.eq(1).all():
        raise IntervalPolicyCompatibilityError(
            "Each monitor run must retain one coverage-shortfall threshold."
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
    run_id = _run_id(
        f"ipc-{uuid4().hex[:24]}" if assessment_run_id is None else assessment_run_id
    )
    timestamp = _as_utc(
        datetime.now(timezone.utc) if assessment_timestamp_utc is None else assessment_timestamp_utc,
        "assessment_timestamp_utc",
    )
    time_columns = ["monitor_timestamp_utc"]
    if "monitor_as_of_utc" in prepared:
        time_columns.append("monitor_as_of_utc")
    if any(timestamp < prepared[column].max() for column in time_columns):
        raise IntervalPolicyCompatibilityError(
            "The assessment cannot precede retained monitoring evidence."
        )
    digest_columns = set(REQUIRED_COLUMNS) | set(time_columns)
    source_digest = _digest_rows(prepared, sorted(digest_columns))
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
                previous_passed = observed_shortfall <= PREVIOUS_COVERAGE_SHORTFALL_LIMIT
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
        rows.append({
            "compatibility_run_id": run_id,
            "compatibility_timestamp_utc": timestamp,
            "source_health_checks_sha256": source_digest,
            "source_monitor_run_id": str(first["monitor_run_id"]),
            "source_monitor_timestamp_utc": first["monitor_timestamp_utc"],
            **{column: first[column] for column in IDENTITY_COLUMNS},
            "latest_interval_run_id": str(first["latest_interval_run_id"]),
            "previous_policy_id": PREVIOUS_POLICY_ID,
            "previous_policy_version": str(previous["policy_version"]),
            "previous_coverage_shortfall_limit_pct_points": PREVIOUS_COVERAGE_SHORTFALL_LIMIT,
            "current_policy_id": CURRENT_POLICY_ID,
            "current_policy_version": str(current["policy_version"]),
            "current_coverage_shortfall_limit_pct_points": float(
                current["max_recent_coverage_shortfall_pct_points"]
            ),
            "observed_recent_coverage_shortfall_pct_points": observed_shortfall,
            "previous_policy_status": previous_status,
            "current_policy_status": current_status,
            "status_changed": status_changed,
            "status_transition": f"{previous_status}_to_{current_status}",
            "changed_check_count": len(changed_checks),
            "changed_check_names": sorted(changed_checks),
            "requires_human_review": status_changed,
            "compatibility_contract_version": COMPATIBILITY_CONTRACT_VERSION,
            **{field: False for field in SAFETY_FIELDS},
        })
    slice_frame = pd.DataFrame(rows)
    summaries: list[dict[str, Any]] = []
    for monitor_run_id, group in slice_frame.groupby("source_monitor_run_id", sort=True):
        previous_status = _overall_status(group["previous_policy_status"])
        current_status = _overall_status(group["current_policy_status"])
        changed_count = int(group["status_changed"].sum())
        summaries.append({
            "compatibility_run_id": run_id,
            "compatibility_timestamp_utc": timestamp,
            "source_health_checks_sha256": source_digest,
            "source_monitor_run_id": monitor_run_id,
            "source_monitor_timestamp_utc": group.iloc[0]["source_monitor_timestamp_utc"],
            "previous_policy_id": PREVIOUS_POLICY_ID,
            "previous_policy_version": str(previous["policy_version"]),
            "previous_coverage_shortfall_limit_pct_points": PREVIOUS_COVERAGE_SHORTFALL_LIMIT,
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
            "newly_failed_slice_count": int((
                (group["previous_policy_status"] != FAILED_STATUS)
                & (group["current_policy_status"] == FAILED_STATUS)
            ).sum()),
            "compatibility_classification": (
                "policy_change_affects_retained_conclusions"
                if changed_count else "retained_conclusions_unchanged"
            ),
            "requires_human_review": bool(changed_count),
            "compatibility_contract_version": COMPATIBILITY_CONTRACT_VERSION,
            **{field: False for field in SAFETY_FIELDS},
        })
    summary_frame = pd.DataFrame(summaries)
    return (
        slice_frame.sort_values(
            ["source_monitor_run_id", *IDENTITY_COLUMNS], kind="mergesort"
        ).reset_index(drop=True),
        summary_frame.sort_values("source_monitor_run_id", kind="mergesort").reset_index(drop=True),
    )


def render_compatibility_report(slices: pd.DataFrame, summary: pd.DataFrame) -> str:
    if slices.empty or summary.empty:
        raise IntervalPolicyCompatibilityError(
            "Compatibility report requires slice and summary evidence."
        )
    lines = [
        "# Retained interval-policy compatibility assessment", "",
        f"Compatibility run: `{summary.iloc[0]['compatibility_run_id']}`", "",
        "This report compares the previous five-point and reviewed three-point ",
        "recent coverage-shortfall policies over the same immutable retained ",
        "health-check rows. It does not rerun monitoring or rewrite history.", "",
        "| Monitor run | Previous status | Current status | Slices | Changed | Newly failed |",
        "| --- | --- | --- | ---: | ---: | ---: |",
    ]
    for _, row in summary.iterrows():
        lines.append(
            "| {source_monitor_run_id} | {previous_overall_status} | "
            "{current_overall_status} | {monitoring_slice_count} | "
            "{changed_monitoring_slice_count} | {newly_failed_slice_count} |".format(**row.to_dict())
        )
    lines.extend([
        "", "## Human authority boundary", "",
        "The comparison is counterfactual retained evidence only. It does not ",
        "change prior health rows, activate a policy, recalibrate an interval, ",
        "execute Fabric, alter a schedule or model, deliver an alert, deploy, ",
        "or publish externally.", "",
    ])
    return "\n".join(lines)


def _write_frame(frame: pd.DataFrame, path: Path, output_format: str) -> None:
    if output_format not in {"csv", "parquet"}:
        raise IntervalPolicyCompatibilityError("output_format must be 'csv' or 'parquet'.")
    try:
        handle = path.open("xb")
    except FileExistsError as exc:
        raise FileExistsError(f"Refusing to overwrite existing evidence: {path}") from exc
    with handle:
        if output_format == "csv":
            frame.to_csv(handle, index=False, encoding="utf-8")
        else:
            frame.to_parquet(handle, index=False)


def write_compatibility_assessment(
    output_dir: str | Path,
    slices: pd.DataFrame,
    summary: pd.DataFrame,
    *,
    output_format: str = "parquet",
) -> dict[str, Path]:
    """Stage a complete assessment and publish each file without replacement.

    Ordinary failures roll back only files published by this invocation. This
    is not a crash-atomic transaction across all three files.
    """
    if output_format not in {"csv", "parquet"}:
        raise IntervalPolicyCompatibilityError("output_format must be 'csv' or 'parquet'.")
    if slices.empty or summary.empty:
        raise IntervalPolicyCompatibilityError("Cannot write empty compatibility evidence.")
    run_ids = set(slices["compatibility_run_id"]) | set(summary["compatibility_run_id"])
    if len(run_ids) != 1:
        raise IntervalPolicyCompatibilityError(
            "Compatibility outputs must contain exactly one run ID."
        )
    run_id = _run_id(next(iter(run_ids)))
    report = render_compatibility_report(slices, summary)
    root = Path(output_dir)
    paths = {
        "slices": root / f"interval_policy_compatibility_slices_{run_id}.{output_format}",
        "summary": root / f"interval_policy_compatibility_summary_{run_id}.{output_format}",
        "report": root / f"interval_policy_compatibility_report_{run_id}.md",
    }
    for path in paths.values():
        if path.exists() or path.is_symlink():
            raise FileExistsError(f"Refusing to overwrite existing evidence: {path}")
    root.mkdir(parents=True, exist_ok=True)
    # Same-filesystem staging permits atomic no-replace hard-link publication.
    with TemporaryDirectory(prefix=".ipc-stage-", dir=root) as temporary:
        staged = {role: Path(temporary) / path.name for role, path in paths.items()}
        _write_frame(slices, staged["slices"], output_format)
        _write_frame(summary, staged["summary"], output_format)
        with staged["report"].open("x", encoding="utf-8") as handle:
            handle.write(report)
        published: list[tuple[Path, Path]] = []
        try:
            for role, destination in paths.items():
                try:
                    destination.hardlink_to(staged[role])
                except FileExistsError as exc:
                    raise FileExistsError(
                        f"Refusing to overwrite existing evidence: {destination}"
                    ) from exc
                published.append((destination, staged[role]))
        except BaseException:
            for destination, source in reversed(published):
                try:
                    actual, expected = destination.lstat(), source.stat()
                    if (actual.st_dev, actual.st_ino) == (expected.st_dev, expected.st_ino):
                        destination.unlink()
                except FileNotFoundError:
                    pass
            raise
    return paths

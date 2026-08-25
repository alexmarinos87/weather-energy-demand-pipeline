from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from math import isfinite
from pathlib import Path
from typing import Any, Iterable
from uuid import uuid4
import json
import re

import pandas as pd

from forecasting.interval_health_trends import SLICE_COLUMNS, TREND_CONTRACT_VERSION
from forecasting.interval_monitoring import POLICY_VERSION, PredictionIntervalMonitoringConfig

SENSITIVITY_CONTRACT_VERSION = "interval-policy-sensitivity-v1"
RUN_ID_PATTERN = re.compile(r"^ips-[0-9a-f]{24}$")
STATUSES = ("healthy", "warning", "failed")
STATUS_RANK = {status: rank for rank, status in enumerate(STATUSES)}
AUTHORITY_FIELDS = (
    "active_policy_updated",
    "candidate_thresholds_activated",
    "retained_evidence_mutated",
    "interval_recalibration_performed",
    "model_change_performed",
    "schedule_change_performed",
    "promotion_change_performed",
    "alert_delivery_performed",
)
REQUIRED = {
    "trend_run_id", "trend_run_timestamp_utc", "scenario", *SLICE_COLUMNS,
    "monitor_status", "trend_contract_version", "recent_interval_run_count",
    "reference_interval_run_count", "reference_history_sufficient",
    "latest_interval_run_timestamp_utc", "latest_evaluation_end_utc",
    "recent_minimum_calibration_observation_count",
    "latest_coverage_shortfall_pct_points", "coverage_drop_pct_points",
    "average_interval_width_increase_pct", "calibration_history_drop_pct",
}


class IntervalPolicySensitivityError(ValueError):
    """Raised when retained sensitivity evidence is malformed or unsafe."""


@dataclass(frozen=True)
class PolicyCandidate:
    candidate_id: str
    candidate_role: str
    candidate_version: str
    rationale: str
    min_recent_interval_runs: int
    min_reference_interval_runs: int
    max_interval_run_age_minutes: int
    max_evaluation_age_minutes: int
    min_calibration_observation_count: int
    max_recent_coverage_shortfall_pct_points: float
    max_coverage_drop_pct_points: float
    max_average_interval_width_increase_pct: float
    max_calibration_history_drop_pct: float
    source_policy_version: str = POLICY_VERSION

    def validate(self) -> None:
        for name in ("candidate_id", "candidate_role", "candidate_version", "rationale", "source_policy_version"):
            if not str(getattr(self, name)).strip():
                raise IntervalPolicySensitivityError(f"{name} must be non-empty.")
        if self.candidate_role not in {"active_reference", "review_candidate"}:
            raise IntervalPolicySensitivityError("candidate_role is invalid.")
        for name in (
            "min_recent_interval_runs", "min_reference_interval_runs",
            "max_interval_run_age_minutes", "max_evaluation_age_minutes",
            "min_calibration_observation_count",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise IntervalPolicySensitivityError(f"{name} must be a positive integer.")
        for name in (
            "max_recent_coverage_shortfall_pct_points", "max_coverage_drop_pct_points",
            "max_average_interval_width_increase_pct", "max_calibration_history_drop_pct",
        ):
            value = float(getattr(self, name))
            if not isfinite(value) or value < 0:
                raise IntervalPolicySensitivityError(f"{name} must be finite and non-negative.")


def _from_config(candidate_id: str, role: str, rationale: str, config: PredictionIntervalMonitoringConfig) -> PolicyCandidate:
    return PolicyCandidate(
        candidate_id, role, "interval-monitoring-review-candidate-v1", rationale,
        config.min_recent_interval_runs, config.min_reference_interval_runs,
        config.max_interval_run_age_minutes, config.max_evaluation_age_minutes,
        config.min_calibration_observation_count,
        config.max_recent_coverage_shortfall_pct_points,
        config.max_coverage_drop_pct_points,
        config.max_average_interval_width_increase_pct,
        config.max_calibration_history_drop_pct,
        config.policy_version,
    )


def default_policy_candidates() -> tuple[PolicyCandidate, ...]:
    active_config = PredictionIntervalMonitoringConfig()
    active_config.validate()
    return (
        _from_config(
            "active-reference", "active_reference",
            "Exact checked-in monitoring policy used only as the comparison anchor.",
            active_config,
        ),
        PolicyCandidate(
            "stricter-review", "review_candidate", "interval-monitoring-review-candidate-v1",
            "Tighter reviewed limits used to expose escalation sensitivity.",
            3, 6, 5040, 10080, 30, 2.0, 2.0, 15.0, 15.0,
        ),
        PolicyCandidate(
            "tolerant-review", "review_candidate", "interval-monitoring-review-candidate-v1",
            "Wider reviewed limits used to expose de-escalation sensitivity.",
            1, 1, 20160, 40320, 18, 12.0, 12.0, 50.0, 50.0,
        ),
    )


def load_policy_candidates(path: Path) -> tuple[PolicyCandidate, ...]:
    document = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(document, list):
        raise IntervalPolicySensitivityError("Candidate policy document must be a JSON list.")
    try:
        return tuple(PolicyCandidate(**item) for item in document)
    except TypeError as exc:
        raise IntervalPolicySensitivityError("Candidate policy document contains invalid fields.") from exc


def _utc(series: pd.Series, name: str) -> pd.Series:
    values = []
    for value in series:
        timestamp = pd.Timestamp(value)
        if pd.isna(timestamp) or timestamp.tzinfo is None:
            raise IntervalPolicySensitivityError(f"{name} must contain timezone-aware timestamps.")
        values.append(timestamp.tz_convert("UTC"))
    return pd.Series(values, index=series.index, dtype="datetime64[ns, UTC]")


def _number(series: pd.Series, name: str, *, minimum: float | None = None, maximum: float | None = None, nullable: bool = False) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")
    if not nullable and values.isna().any():
        raise IntervalPolicySensitivityError(f"{name} must contain finite values.")
    finite = values.dropna()
    if not finite.map(lambda value: isfinite(float(value))).all():
        raise IntervalPolicySensitivityError(f"{name} must contain finite values.")
    if minimum is not None and (finite < minimum).any():
        raise IntervalPolicySensitivityError(f"{name} must be at least {minimum}.")
    if maximum is not None and (finite > maximum).any():
        raise IntervalPolicySensitivityError(f"{name} must be at most {maximum}.")
    return values


def _boolean(series: pd.Series, name: str) -> pd.Series:
    if series.dtype == bool:
        return series.astype(bool)
    parsed = series.astype(str).str.strip().str.lower().map({"true": True, "false": False})
    if parsed.isna().any():
        raise IntervalPolicySensitivityError(f"{name} must contain boolean values.")
    return parsed.astype(bool)


def prepare_policy_sensitivity_input(frame: pd.DataFrame) -> pd.DataFrame:
    missing = sorted(REQUIRED - set(frame.columns))
    if missing:
        raise IntervalPolicySensitivityError("Slice trends are missing: " + ", ".join(missing) + ".")
    prepared = frame.copy()
    text_columns = ["trend_run_id", "scenario", *SLICE_COLUMNS, "monitor_status", "trend_contract_version"]
    for column in text_columns:
        if column in {"requested_horizon_minutes", "target_coverage_level"}:
            continue
        prepared[column] = prepared[column].fillna("").astype(str).str.strip()
        if prepared[column].eq("").any():
            raise IntervalPolicySensitivityError(f"{column} must be non-empty.")
    if prepared["trend_run_id"].nunique() != 1:
        raise IntervalPolicySensitivityError("Exactly one retained trend run is required.")
    if set(prepared["trend_contract_version"]) != {TREND_CONTRACT_VERSION}:
        raise IntervalPolicySensitivityError("interval-health-trend-v1 evidence is required.")
    if not set(prepared["monitor_status"]).issubset(STATUSES):
        raise IntervalPolicySensitivityError("Retained monitor status is invalid.")
    for column in ("trend_run_timestamp_utc", "latest_interval_run_timestamp_utc", "latest_evaluation_end_utc"):
        prepared[column] = _utc(prepared[column], column)
    for column, minimum in (
        ("requested_horizon_minutes", 1), ("recent_interval_run_count", 1),
        ("reference_interval_run_count", 0),
        ("recent_minimum_calibration_observation_count", 1),
    ):
        values = _number(prepared[column], column, minimum=minimum)
        if not (values % 1 == 0).all():
            raise IntervalPolicySensitivityError(f"{column} must contain integers.")
        prepared[column] = values.astype(int)
    prepared["target_coverage_level"] = _number(prepared["target_coverage_level"], "target_coverage_level", minimum=0, maximum=1)
    if not prepared["target_coverage_level"].between(0, 1, inclusive="neither").all():
        raise IntervalPolicySensitivityError("target_coverage_level must be strictly between zero and one.")
    prepared["latest_coverage_shortfall_pct_points"] = _number(
        prepared["latest_coverage_shortfall_pct_points"], "latest_coverage_shortfall_pct_points", minimum=0, maximum=100
    )
    drift_columns = ["coverage_drop_pct_points", "average_interval_width_increase_pct", "calibration_history_drop_pct"]
    for column in drift_columns:
        prepared[column] = _number(prepared[column], column, nullable=True)
    prepared["reference_history_sufficient"] = _boolean(prepared["reference_history_sufficient"], "reference_history_sufficient")
    if not (prepared["latest_evaluation_end_utc"] <= prepared["latest_interval_run_timestamp_utc"]).all():
        raise IntervalPolicySensitivityError("Evaluation evidence cannot end after its interval run.")
    if not (prepared["latest_interval_run_timestamp_utc"] <= prepared["trend_run_timestamp_utc"]).all():
        raise IntervalPolicySensitivityError("Interval runs cannot follow the retained trend run.")
    sufficient = prepared["reference_history_sufficient"]
    if prepared.loc[sufficient, drift_columns].isna().any(axis=None):
        raise IntervalPolicySensitivityError("Reference-sufficient slices require complete drift values.")
    if prepared.loc[~sufficient, drift_columns].notna().any(axis=None):
        raise IntervalPolicySensitivityError("Reference-insufficient slices must leave drift values null.")
    identity = ["scenario", *SLICE_COLUMNS]
    normalized = prepared[identity].copy()
    for column in normalized.columns:
        if column not in {"requested_horizon_minutes", "target_coverage_level"}:
            normalized[column] = normalized[column].astype(str).str.casefold()
    if normalized.duplicated(keep=False).any():
        raise IntervalPolicySensitivityError("duplicate exact-slice identities are not allowed.")
    if (prepared.groupby("scenario")["monitor_status"].nunique() != 1).any():
        raise IntervalPolicySensitivityError("Each scenario must retain one canonical status.")
    return prepared.sort_values(identity).reset_index(drop=True)


def _validate_candidates(candidates: Iterable[PolicyCandidate]) -> tuple[PolicyCandidate, ...]:
    result = tuple(candidates)
    if not 2 <= len(result) <= 5:
        raise IntervalPolicySensitivityError("Between two and five candidates are required.")
    for candidate in result:
        candidate.validate()
    if len({candidate.candidate_id for candidate in result}) != len(result):
        raise IntervalPolicySensitivityError("Candidate IDs must be unique.")
    active = [candidate for candidate in result if candidate.candidate_role == "active_reference"]
    if len(active) != 1:
        raise IntervalPolicySensitivityError("Exactly one active reference is required.")
    config = PredictionIntervalMonitoringConfig()
    expected = _from_config(active[0].candidate_id, "active_reference", active[0].rationale, config)
    comparable = [field for field in asdict(expected) if field not in {"candidate_version", "rationale"}]
    if any(getattr(active[0], field) != getattr(expected, field) for field in comparable):
        raise IntervalPolicySensitivityError("The active reference must exactly match the checked-in monitoring policy.")
    return result


def _candidate_status(error_failures: int, warning_failures: int) -> str:
    return "failed" if error_failures else "warning" if warning_failures else "healthy"


def evaluate_policy_sensitivity(
    slice_trends: pd.DataFrame,
    *,
    candidates: Iterable[PolicyCandidate] | None = None,
    sensitivity_run_id: str | None = None,
    sensitivity_run_timestamp: Any | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    trends = prepare_policy_sensitivity_input(slice_trends)
    policies = _validate_candidates(candidates or default_policy_candidates())
    run_id = sensitivity_run_id or "ips-" + uuid4().hex[:24]
    if not RUN_ID_PATTERN.fullmatch(run_id):
        raise IntervalPolicySensitivityError("sensitivity_run_id must be ips- plus 24 lowercase hex characters.")
    timestamp = pd.Timestamp(sensitivity_run_timestamp or datetime.now(timezone.utc))
    if pd.isna(timestamp) or timestamp.tzinfo is None:
        raise IntervalPolicySensitivityError("sensitivity_run_timestamp must be timezone-aware.")
    timestamp = timestamp.tz_convert("UTC")
    if timestamp < trends["trend_run_timestamp_utc"].max():
        raise IntervalPolicySensitivityError("Sensitivity review cannot precede the trend run.")

    rows: list[dict[str, Any]] = []
    for trend in trends.to_dict(orient="records"):
        run_age = (timestamp - trend["latest_interval_run_timestamp_utc"]).total_seconds() / 60.0
        evaluation_age = (timestamp - trend["latest_evaluation_end_utc"]).total_seconds() / 60.0
        for candidate in policies:
            hard = {
                "recent_history_passed": trend["recent_interval_run_count"] >= candidate.min_recent_interval_runs,
                "run_age_passed": run_age <= candidate.max_interval_run_age_minutes,
                "evaluation_age_passed": evaluation_age <= candidate.max_evaluation_age_minutes,
                "calibration_history_passed": trend["recent_minimum_calibration_observation_count"] >= candidate.min_calibration_observation_count,
                "coverage_shortfall_passed": trend["latest_coverage_shortfall_pct_points"] <= candidate.max_recent_coverage_shortfall_pct_points,
            }
            reference_passed = trend["reference_interval_run_count"] >= candidate.min_reference_interval_runs
            evaluate_drift = bool(trend["reference_history_sufficient"] and reference_passed)
            drift = {
                "coverage_drop_passed": trend["coverage_drop_pct_points"] <= candidate.max_coverage_drop_pct_points if evaluate_drift else None,
                "interval_width_increase_passed": trend["average_interval_width_increase_pct"] <= candidate.max_average_interval_width_increase_pct if evaluate_drift else None,
                "calibration_history_drop_passed": trend["calibration_history_drop_pct"] <= candidate.max_calibration_history_drop_pct if evaluate_drift else None,
            }
            error_failures = sum(not value for value in hard.values())
            warning_failures = int(not reference_passed) + sum(
                not value for value in drift.values() if value is not None
            )
            status = _candidate_status(error_failures, warning_failures)
            rows.append({
                "sensitivity_run_id": run_id,
                "sensitivity_run_timestamp_utc": timestamp,
                "trend_run_id": trend["trend_run_id"],
                "trend_run_timestamp_utc": trend["trend_run_timestamp_utc"],
                "scenario": trend["scenario"],
                **{column: trend[column] for column in SLICE_COLUMNS},
                **asdict(candidate),
                "retained_monitor_status": trend["monitor_status"],
                "candidate_slice_status": status,
                "candidate_status_rank": STATUS_RANK[status],
                "error_failure_count": error_failures,
                "warning_failure_count": warning_failures,
                "latest_interval_run_age_minutes": run_age,
                "latest_evaluation_age_minutes": evaluation_age,
                "observed_recent_interval_run_count": trend["recent_interval_run_count"],
                "observed_reference_interval_run_count": trend["reference_interval_run_count"],
                "observed_minimum_calibration_observation_count": trend["recent_minimum_calibration_observation_count"],
                "observed_coverage_shortfall_pct_points": trend["latest_coverage_shortfall_pct_points"],
                "observed_coverage_drop_pct_points": trend["coverage_drop_pct_points"],
                "observed_interval_width_increase_pct": trend["average_interval_width_increase_pct"],
                "observed_calibration_history_drop_pct": trend["calibration_history_drop_pct"],
                **hard,
                "reference_history_passed": reference_passed,
                "drift_rules_evaluated": evaluate_drift,
                **drift,
                "sensitivity_contract_version": SENSITIVITY_CONTRACT_VERSION,
                **{field: False for field in AUTHORITY_FIELDS},
            })
    slices = pd.DataFrame(rows)
    identity = ["scenario", *SLICE_COLUMNS]
    active_id = next(candidate.candidate_id for candidate in policies if candidate.candidate_role == "active_reference")
    active = slices.loc[slices["candidate_id"] == active_id, identity + ["candidate_slice_status"]].rename(
        columns={"candidate_slice_status": "active_reference_slice_status"}
    )
    slices = slices.merge(active, on=identity, how="left", validate="many_to_one")
    slices["status_changed_from_active_slice"] = slices["candidate_slice_status"] != slices["active_reference_slice_status"]

    summaries = []
    for (scenario, candidate_id), group in slices.groupby(["scenario", "candidate_id"], sort=True):
        first = group.iloc[0]
        status = STATUSES[int(group["candidate_status_rank"].max())]
        summaries.append({
            "sensitivity_run_id": run_id,
            "sensitivity_run_timestamp_utc": timestamp,
            "trend_run_id": first["trend_run_id"],
            "scenario": scenario,
            "candidate_id": candidate_id,
            "candidate_role": first["candidate_role"],
            "candidate_version": first["candidate_version"],
            "candidate_rationale": first["rationale"],
            "retained_monitor_status": first["retained_monitor_status"],
            "candidate_status": status,
            "candidate_status_rank": STATUS_RANK[status],
            "slice_count": len(group),
            "healthy_slice_count": int((group["candidate_slice_status"] == "healthy").sum()),
            "warning_slice_count": int((group["candidate_slice_status"] == "warning").sum()),
            "failed_slice_count": int((group["candidate_slice_status"] == "failed").sum()),
            "changed_slice_count": int(group["status_changed_from_active_slice"].sum()),
            "sensitivity_contract_version": SENSITIVITY_CONTRACT_VERSION,
            **{field: False for field in AUTHORITY_FIELDS},
        })
    summary = pd.DataFrame(summaries)
    active_summary = summary.loc[summary["candidate_role"] == "active_reference", ["scenario", "candidate_status"]].rename(
        columns={"candidate_status": "active_reference_status"}
    )
    summary = summary.merge(active_summary, on="scenario", how="left", validate="many_to_one")
    active_rows = summary["candidate_role"] == "active_reference"
    if not (summary.loc[active_rows, "candidate_status"].to_numpy() == summary.loc[active_rows, "retained_monitor_status"].to_numpy()).all():
        raise IntervalPolicySensitivityError("The active reference does not reproduce retained canonical monitor status.")
    summary["status_changed_from_active"] = summary["candidate_status"] != summary["active_reference_status"]
    summary["sensitivity_classification"] = summary.apply(
        lambda row: "active_reference" if row["candidate_role"] == "active_reference" else "status_sensitive" if row["status_changed_from_active"] else "status_robust",
        axis=1,
    )
    summary["human_review_required"] = (summary["candidate_role"] == "review_candidate") & summary["status_changed_from_active"]
    slices = slices.sort_values(["scenario", "candidate_role", "candidate_id", *SLICE_COLUMNS]).reset_index(drop=True)
    summary = summary.sort_values(["scenario", "candidate_role", "candidate_id"]).reset_index(drop=True)
    report = _render_report(summary)
    return slices, summary, report


def _render_report(summary: pd.DataFrame) -> str:
    lines = [
        "# Interval-monitoring policy sensitivity review", "",
        "Candidate outcomes are counterfactual human-review evidence only.",
        "The active policy is not updated and automatic threshold activation is not authorised.", "",
        "| Scenario | Candidate | Role | Retained | Candidate | Classification | Changed slices |",
        "| --- | --- | --- | --- | --- | --- | ---: |",
    ]
    for row in summary.itertuples(index=False):
        lines.append(
            f"| {row.scenario} | {row.candidate_id} | {row.candidate_role} | {row.retained_monitor_status} | "
            f"{row.candidate_status} | {row.sensitivity_classification} | {int(row.changed_slice_count)} |"
        )
    lines += [
        "", "Empirical coverage is retrospective evidence, not an unconditional future guarantee.",
        "No recalibration, model, schedule, promotion, alert, or evidence mutation is performed.", "",
    ]
    return "\n".join(lines)


def read_frame(path: Path) -> pd.DataFrame:
    suffix = Path(path).suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix in {".parquet", ".pq"}:
        return pd.read_parquet(path)
    raise IntervalPolicySensitivityError("Use a CSV or Parquet input.")


def write_frame_atomic(frame: pd.DataFrame, path: Path, output_format: str) -> Path:
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
            raise IntervalPolicySensitivityError("output_format must be csv or parquet.")
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)
    return path


def write_text_atomic(content: str, path: Path) -> Path:
    path = Path(path)
    temporary = path.with_name(f".{path.name}.tmp")
    for candidate in (path, temporary):
        if candidate.exists():
            raise FileExistsError(f"Refusing to overwrite {candidate}.")
    try:
        temporary.write_text(content, encoding="utf-8")
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)
    return path

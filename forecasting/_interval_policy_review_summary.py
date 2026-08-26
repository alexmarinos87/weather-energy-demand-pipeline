from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd


DECISION_CONTRACT_VERSION = "interval-policy-review-decision-v1"
SENSITIVITY_CONTRACT_VERSION = "interval-policy-sensitivity-v1"
DECISION_ID_PATTERN = re.compile(r"^ipd-[0-9a-f]{24}$")
SENSITIVITY_RUN_ID_PATTERN = re.compile(r"^ips-[0-9a-f]{24}$")
TREND_RUN_ID_PATTERN = re.compile(r"^iht-[0-9a-f]{24}$")
ALLOWED_DECISIONS = {
    "retain_active_policy",
    "reject_candidate",
    "request_candidate_revision",
}
DECISION_EFFECTS = {
    "retain_active_policy": "active_policy_retained",
    "reject_candidate": "candidate_rejected",
    "request_candidate_revision": "candidate_revision_required",
}
STATUSES = {"healthy", "warning", "failed"}
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
DECISION_SAFETY_FIELDS = (
    "threshold_activation_authorized",
    "active_policy_updated",
    "candidate_thresholds_activated",
    "retained_evidence_mutated",
    "interval_recalibration_performed",
    "model_change_performed",
    "schedule_change_performed",
    "promotion_change_performed",
    "alert_delivery_performed",
    "deployment_performed",
    "external_publication_performed",
)
REQUIRED_SUMMARY_COLUMNS = {
    "sensitivity_run_id",
    "sensitivity_run_timestamp_utc",
    "trend_run_id",
    "scenario",
    "candidate_id",
    "candidate_role",
    "candidate_version",
    "retained_monitor_status",
    "active_reference_status",
    "candidate_status",
    "status_changed_from_active",
    "sensitivity_classification",
    "slice_count",
    "changed_slice_count",
    "human_review_required",
    "sensitivity_contract_version",
    *AUTHORITY_FIELDS,
}


class IntervalPolicyReviewDecisionError(ValueError):
    """Raised when a policy-review decision is malformed or unsafe."""


def _required_text(value: Any, name: str) -> str:
    if value is None:
        raise IntervalPolicyReviewDecisionError(f"{name} must be non-empty.")
    text = str(value).strip()
    if not text:
        raise IntervalPolicyReviewDecisionError(f"{name} must be non-empty.")
    return text


def _utc_timestamp(value: Any, name: str) -> pd.Timestamp:
    try:
        timestamp = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise IntervalPolicyReviewDecisionError(
            f"{name} must be a valid timezone-aware timestamp."
        ) from exc
    if pd.isna(timestamp) or timestamp.tzinfo is None:
        raise IntervalPolicyReviewDecisionError(
            f"{name} must be timezone-aware."
        )
    return timestamp.tz_convert("UTC")


def _boolean(series: pd.Series, name: str) -> pd.Series:
    if series.dtype == bool:
        return series.astype(bool)
    parsed = series.astype(str).str.strip().str.lower().map(
        {"true": True, "false": False}
    )
    if parsed.isna().any():
        raise IntervalPolicyReviewDecisionError(
            f"{name} must contain boolean values."
        )
    return parsed.astype(bool)


def _non_negative_integer(series: pd.Series, name: str) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")
    if values.isna().any() or (values < 0).any() or not (values % 1 == 0).all():
        raise IntervalPolicyReviewDecisionError(
            f"{name} must contain non-negative integers."
        )
    return values.astype(int)


def _canonical(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _canonical(value[key]) for key in sorted(value)}
    if isinstance(value, (list, tuple)):
        return [_canonical(item) for item in value]
    if isinstance(value, (datetime, pd.Timestamp)):
        return _utc_timestamp(value, "timestamp").isoformat()
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(value, "item"):
        return value.item()
    return value


def _digest(value: Any) -> str:
    encoded = json.dumps(
        _canonical(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _unique_texts(
    values: Iterable[Any], name: str, *, required: bool
) -> list[str]:
    if isinstance(values, (str, bytes)) or not isinstance(values, Iterable):
        raise IntervalPolicyReviewDecisionError(
            f"{name} must be a list of strings."
        )
    result = [_required_text(value, name) for value in values]
    if required and not result:
        raise IntervalPolicyReviewDecisionError(
            f"{name} must contain at least one item."
        )
    if len(set(result)) != len(result):
        raise IntervalPolicyReviewDecisionError(
            f"{name} must not contain duplicates."
        )
    return result


def prepare_sensitivity_summary(frame: pd.DataFrame) -> pd.DataFrame:
    """Validate one complete retained G26 sensitivity summary."""
    missing = sorted(REQUIRED_SUMMARY_COLUMNS - set(frame.columns))
    if missing:
        raise IntervalPolicyReviewDecisionError(
            "Sensitivity summary is missing required columns: "
            + ", ".join(missing)
            + "."
        )
    prepared = frame.copy()
    text_columns = (
        "sensitivity_run_id",
        "trend_run_id",
        "scenario",
        "candidate_id",
        "candidate_role",
        "candidate_version",
        "retained_monitor_status",
        "active_reference_status",
        "candidate_status",
        "sensitivity_classification",
        "sensitivity_contract_version",
    )
    for column in text_columns:
        prepared[column] = prepared[column].map(
            lambda value, name=column: _required_text(value, name)
        )
    prepared["sensitivity_run_timestamp_utc"] = prepared[
        "sensitivity_run_timestamp_utc"
    ].map(lambda value: _utc_timestamp(value, "sensitivity_run_timestamp_utc"))
    for column in (
        "status_changed_from_active",
        "human_review_required",
        *AUTHORITY_FIELDS,
    ):
        prepared[column] = _boolean(prepared[column], column)
    prepared["slice_count"] = _non_negative_integer(
        prepared["slice_count"], "slice_count"
    )
    prepared["changed_slice_count"] = _non_negative_integer(
        prepared["changed_slice_count"], "changed_slice_count"
    )
    if (prepared["slice_count"] < 1).any():
        raise IntervalPolicyReviewDecisionError(
            "slice_count must contain positive integers."
        )
    if (prepared["changed_slice_count"] > prepared["slice_count"]).any():
        raise IntervalPolicyReviewDecisionError(
            "changed_slice_count cannot exceed slice_count."
        )
    if prepared["sensitivity_run_id"].nunique() != 1:
        raise IntervalPolicyReviewDecisionError(
            "Exactly one sensitivity_run_id is required."
        )
    sensitivity_run_id = prepared["sensitivity_run_id"].iloc[0]
    if not SENSITIVITY_RUN_ID_PATTERN.fullmatch(sensitivity_run_id):
        raise IntervalPolicyReviewDecisionError(
            "sensitivity_run_id is malformed."
        )
    if prepared["sensitivity_run_timestamp_utc"].nunique() != 1:
        raise IntervalPolicyReviewDecisionError(
            "Exactly one sensitivity run timestamp is required."
        )
    if prepared["trend_run_id"].nunique() != 1:
        raise IntervalPolicyReviewDecisionError(
            "Exactly one trend_run_id is required."
        )
    if not TREND_RUN_ID_PATTERN.fullmatch(prepared["trend_run_id"].iloc[0]):
        raise IntervalPolicyReviewDecisionError("trend_run_id is malformed.")
    if set(prepared["sensitivity_contract_version"]) != {
        SENSITIVITY_CONTRACT_VERSION
    }:
        raise IntervalPolicyReviewDecisionError(
            "interval-policy-sensitivity-v1 evidence is required."
        )
    if not set(prepared["candidate_role"]).issubset(
        {"active_reference", "review_candidate"}
    ):
        raise IntervalPolicyReviewDecisionError("candidate_role is invalid.")
    for column in (
        "retained_monitor_status",
        "active_reference_status",
        "candidate_status",
    ):
        if not set(prepared[column]).issubset(STATUSES):
            raise IntervalPolicyReviewDecisionError(f"{column} is invalid.")
    if prepared[list(AUTHORITY_FIELDS)].any(axis=None):
        raise IntervalPolicyReviewDecisionError(
            "Sensitivity evidence must not contain activated authority fields."
        )
    identity = ["scenario", "candidate_id"]
    if prepared.duplicated(subset=identity, keep=False).any():
        raise IntervalPolicyReviewDecisionError(
            "Sensitivity summary contains duplicate scenario/candidate rows."
        )
    active = prepared.loc[prepared["candidate_role"] == "active_reference"]
    if active["candidate_id"].nunique() != 1 or set(active["candidate_id"]) != {
        "active-reference"
    }:
        raise IntervalPolicyReviewDecisionError(
            "Exactly one active-reference candidate is required."
        )
    scenario_set = set(prepared["scenario"])
    for candidate_id, candidate_rows in prepared.groupby(
        "candidate_id", sort=False
    ):
        if set(candidate_rows["scenario"]) != scenario_set:
            raise IntervalPolicyReviewDecisionError(
                f"Candidate {candidate_id} does not cover every retained scenario."
            )
        if candidate_rows["candidate_role"].nunique() != 1:
            raise IntervalPolicyReviewDecisionError(
                f"Candidate {candidate_id} has inconsistent roles."
            )
        if candidate_rows["candidate_version"].nunique() != 1:
            raise IntervalPolicyReviewDecisionError(
                f"Candidate {candidate_id} has inconsistent versions."
            )
    if not (
        (active["candidate_status"] == active["active_reference_status"])
        & (active["active_reference_status"] == active["retained_monitor_status"])
        & (~active["status_changed_from_active"])
        & (active["changed_slice_count"] == 0)
        & (active["sensitivity_classification"] == "active_reference")
    ).all():
        raise IntervalPolicyReviewDecisionError(
            "Active-reference evidence does not reproduce retained monitoring status."
        )
    review = prepared.loc[prepared["candidate_role"] == "review_candidate"]
    expected_changed = review["candidate_status"] != review[
        "active_reference_status"
    ]
    if not (
        review["status_changed_from_active"] == expected_changed
    ).all():
        raise IntervalPolicyReviewDecisionError(
            "Review-candidate status-change evidence is inconsistent."
        )
    expected_classification = expected_changed.map(
        {True: "status_sensitive", False: "status_robust"}
    )
    if not (
        review["sensitivity_classification"] == expected_classification
    ).all():
        raise IntervalPolicyReviewDecisionError(
            "Review-candidate sensitivity classification is inconsistent."
        )
    return prepared.sort_values(identity).reset_index(drop=True)


def sensitivity_summary_sha256(frame: pd.DataFrame) -> str:
    """Return a stable digest of one validated sensitivity summary."""
    prepared = prepare_sensitivity_summary(frame)
    return _digest(prepared.to_dict(orient="records"))



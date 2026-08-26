from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime
from typing import Any, Iterable, Mapping

import pandas as pd


REVIEW_CONTRACT_VERSION = "interval-policy-candidate-revision-review-v1"
REVIEW_ID_PATTERN = re.compile(r"^irv-[0-9a-f]{24}$")
ALLOWED_REVIEW_DECISIONS = {
    "accept_for_sensitivity_review",
    "reject_revision_package",
    "request_package_revision",
}
REVIEW_EFFECTS = {
    "accept_for_sensitivity_review": "separate_sensitivity_review_eligible",
    "reject_revision_package": "revision_package_rejected",
    "request_package_revision": "revision_package_revision_required",
}
NEXT_ACTIONS = {
    "accept_for_sensitivity_review": "separate_sensitivity_review_request_required",
    "reject_revision_package": "no_further_action_recorded",
    "request_package_revision": "new_revision_package_required",
}
REVIEW_SAFETY_FIELDS = (
    "sensitivity_review_executed",
    "automatic_sensitivity_review_allowed",
    "sensitivity_review_execution_authorized",
    "threshold_activation_authorized",
    "candidate_thresholds_activated",
    "active_policy_updated",
    "source_package_mutated",
    "source_decision_mutated",
    "source_sensitivity_evidence_mutated",
    "interval_recalibration_performed",
    "model_change_performed",
    "schedule_change_performed",
    "promotion_change_performed",
    "alert_delivery_performed",
    "deployment_performed",
    "external_publication_performed",
)


class IntervalPolicyCandidateRevisionReviewError(ValueError):
    """Raised when revision-package review evidence is malformed or unsafe."""


def required_text(value: Any, name: str, *, minimum_length: int = 1) -> str:
    if value is None:
        raise IntervalPolicyCandidateRevisionReviewError(
            f"{name} must be non-empty."
        )
    text = str(value).strip()
    if len(text) < minimum_length:
        raise IntervalPolicyCandidateRevisionReviewError(
            f"{name} must contain at least {minimum_length} characters."
        )
    return text


def unique_texts(
    values: Iterable[Any],
    name: str,
    *,
    required: bool,
    minimum_length: int = 1,
) -> list[str]:
    if isinstance(values, (str, bytes)) or not isinstance(values, Iterable):
        raise IntervalPolicyCandidateRevisionReviewError(
            f"{name} must be a list of strings."
        )
    result = [
        required_text(value, name, minimum_length=minimum_length)
        for value in values
    ]
    if required and not result:
        raise IntervalPolicyCandidateRevisionReviewError(
            f"{name} must contain at least one item."
        )
    if len(set(result)) != len(result):
        raise IntervalPolicyCandidateRevisionReviewError(
            f"{name} must not contain duplicates."
        )
    return result


def utc_timestamp(value: Any, name: str) -> pd.Timestamp:
    try:
        timestamp = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise IntervalPolicyCandidateRevisionReviewError(
            f"{name} must be a valid timezone-aware timestamp."
        ) from exc
    if pd.isna(timestamp) or timestamp.tzinfo is None:
        raise IntervalPolicyCandidateRevisionReviewError(
            f"{name} must be timezone-aware."
        )
    return timestamp.tz_convert("UTC")


def canonical(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): canonical(value[key]) for key in sorted(value)}
    if isinstance(value, (list, tuple)):
        return [canonical(item) for item in value]
    if isinstance(value, (datetime, pd.Timestamp)):
        return utc_timestamp(value, "timestamp").isoformat()
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


def digest(value: Any) -> str:
    encoded = json.dumps(
        canonical(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping

import pandas as pd


REVISION_SENSITIVITY_CONTRACT_VERSION = (
    "interval-policy-revision-sensitivity-v1"
)
REVISION_SENSITIVITY_SAFETY_FIELDS = (
    "automatic_sensitivity_rerun_used",
    "active_policy_updated",
    "candidate_thresholds_activated",
    "source_review_mutated",
    "source_package_mutated",
    "source_decision_mutated",
    "retained_evidence_mutated",
    "interval_recalibration_performed",
    "model_change_performed",
    "schedule_change_performed",
    "promotion_change_performed",
    "alert_delivery_performed",
    "deployment_performed",
    "external_publication_performed",
)


class IntervalPolicyRevisionSensitivityError(ValueError):
    """Raised when revised-candidate sensitivity evidence is unsafe."""


def utc_timestamp(value: Any, name: str) -> pd.Timestamp:
    try:
        timestamp = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise IntervalPolicyRevisionSensitivityError(
            f"{name} must be a valid timezone-aware timestamp."
        ) from exc
    if pd.isna(timestamp) or timestamp.tzinfo is None:
        raise IntervalPolicyRevisionSensitivityError(
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


def file_sha256(path: Path) -> str:
    value = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()

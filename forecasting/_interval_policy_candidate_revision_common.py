from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict
from datetime import datetime
from math import isfinite
from typing import Any, Iterable, Mapping

import pandas as pd

from forecasting.interval_monitoring import (
    POLICY_VERSION,
    PredictionIntervalMonitoringConfig,
    PredictionIntervalMonitoringError,
)


REVISION_PACKAGE_CONTRACT_VERSION = "interval-policy-candidate-revision-v1"
REVISION_PACKAGE_ID_PATTERN = re.compile(r"^ipr-[0-9a-f]{24}$")
CANDIDATE_ROLE = "review_candidate"
ACTIVE_CANDIDATE_ID = "active-reference"

PACKAGE_SAFETY_FIELDS = (
    "threshold_activation_authorized",
    "candidate_thresholds_activated",
    "active_policy_updated",
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

CONFIG_FIELDS = tuple(asdict(PredictionIntervalMonitoringConfig()).keys())
THRESHOLD_FIELDS = tuple(field for field in CONFIG_FIELDS if field != "policy_version")
CANDIDATE_FIELDS = (
    "candidate_id",
    "candidate_role",
    "candidate_version",
    "rationale",
    *CONFIG_FIELDS,
)


class IntervalPolicyCandidateRevisionError(ValueError):
    """Raised when candidate-revision evidence is malformed or unsafe."""


def required_text(value: Any, name: str, *, minimum_length: int = 1) -> str:
    if value is None:
        raise IntervalPolicyCandidateRevisionError(f"{name} must be non-empty.")
    text = str(value).strip()
    if len(text) < minimum_length:
        raise IntervalPolicyCandidateRevisionError(
            f"{name} must contain at least {minimum_length} characters."
        )
    return text


def utc_timestamp(value: Any, name: str) -> pd.Timestamp:
    try:
        timestamp = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise IntervalPolicyCandidateRevisionError(
            f"{name} must be a valid timezone-aware timestamp."
        ) from exc
    if pd.isna(timestamp) or timestamp.tzinfo is None:
        raise IntervalPolicyCandidateRevisionError(
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


def unique_texts(
    values: Iterable[Any],
    name: str,
    *,
    required: bool,
    minimum_length: int = 1,
) -> list[str]:
    if isinstance(values, (str, bytes)) or not isinstance(values, Iterable):
        raise IntervalPolicyCandidateRevisionError(
            f"{name} must be a list of strings."
        )
    result = [
        required_text(value, name, minimum_length=minimum_length)
        for value in values
    ]
    if required and not result:
        raise IntervalPolicyCandidateRevisionError(
            f"{name} must contain at least one item."
        )
    if len(set(result)) != len(result):
        raise IntervalPolicyCandidateRevisionError(
            f"{name} must not contain duplicates."
        )
    return result


def active_policy_snapshot() -> dict[str, Any]:
    config = PredictionIntervalMonitoringConfig()
    config.validate()
    return canonical(asdict(config))


def prepare_candidate_snapshot(
    value: Mapping[str, Any],
    *,
    label: str,
) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise IntervalPolicyCandidateRevisionError(
            f"{label} must be a JSON object."
        )
    missing = sorted(set(CANDIDATE_FIELDS) - set(value))
    unexpected = sorted(set(value) - set(CANDIDATE_FIELDS))
    if missing or unexpected:
        details = []
        if missing:
            details.append("missing " + ", ".join(missing))
        if unexpected:
            details.append("unexpected " + ", ".join(unexpected))
        raise IntervalPolicyCandidateRevisionError(
            f"{label} fields are invalid: {'; '.join(details)}."
        )
    candidate = dict(value)
    candidate["candidate_id"] = required_text(
        candidate["candidate_id"], f"{label}.candidate_id"
    )
    candidate["candidate_role"] = required_text(
        candidate["candidate_role"], f"{label}.candidate_role"
    )
    candidate["candidate_version"] = required_text(
        candidate["candidate_version"], f"{label}.candidate_version"
    )
    candidate["rationale"] = required_text(
        candidate["rationale"], f"{label}.rationale", minimum_length=20
    )
    if candidate["candidate_role"] != CANDIDATE_ROLE:
        raise IntervalPolicyCandidateRevisionError(
            f"{label}.candidate_role must be review_candidate."
        )
    if candidate["candidate_id"] == ACTIVE_CANDIDATE_ID:
        raise IntervalPolicyCandidateRevisionError(
            f"{label}.candidate_id cannot be active-reference."
        )

    config_values: dict[str, Any] = {}
    integer_fields = {
        "recent_interval_run_count",
        "reference_interval_run_count",
        "min_recent_interval_runs",
        "min_reference_interval_runs",
        "max_interval_run_age_minutes",
        "max_evaluation_age_minutes",
        "min_calibration_observation_count",
    }
    for field in CONFIG_FIELDS:
        raw = candidate[field]
        if field in integer_fields:
            if isinstance(raw, bool):
                raise IntervalPolicyCandidateRevisionError(
                    f"{label}.{field} must be a positive integer."
                )
            try:
                numeric = float(raw)
            except (TypeError, ValueError) as exc:
                raise IntervalPolicyCandidateRevisionError(
                    f"{label}.{field} must be a positive integer."
                ) from exc
            if not isfinite(numeric) or numeric < 1 or numeric % 1:
                raise IntervalPolicyCandidateRevisionError(
                    f"{label}.{field} must be a positive integer."
                )
            config_values[field] = int(numeric)
        elif field == "policy_version":
            config_values[field] = required_text(
                raw, f"{label}.policy_version"
            )
        else:
            try:
                numeric = float(raw)
            except (TypeError, ValueError) as exc:
                raise IntervalPolicyCandidateRevisionError(
                    f"{label}.{field} must be finite and non-negative."
                ) from exc
            if not isfinite(numeric) or numeric < 0:
                raise IntervalPolicyCandidateRevisionError(
                    f"{label}.{field} must be finite and non-negative."
                )
            config_values[field] = numeric
    try:
        PredictionIntervalMonitoringConfig(**config_values).validate()
    except PredictionIntervalMonitoringError as exc:
        raise IntervalPolicyCandidateRevisionError(
            f"{label} monitoring configuration is invalid: {exc}"
        ) from exc
    if config_values["policy_version"] != POLICY_VERSION:
        raise IntervalPolicyCandidateRevisionError(
            f"{label}.policy_version must remain {POLICY_VERSION}."
        )
    return canonical(
        {
            "candidate_id": candidate["candidate_id"],
            "candidate_role": candidate["candidate_role"],
            "candidate_version": candidate["candidate_version"],
            "rationale": candidate["rationale"],
            **config_values,
        }
    )


def threshold_changes(
    source: Mapping[str, Any], revised: Mapping[str, Any]
) -> list[dict[str, Any]]:
    changes = []
    for field in THRESHOLD_FIELDS:
        if source[field] != revised[field]:
            changes.append(
                {
                    "field": field,
                    "source_value": source[field],
                    "revised_value": revised[field],
                }
            )
    return changes


def prepare_change_responses(
    values: Iterable[Mapping[str, Any]],
    *,
    requested_changes: Iterable[str],
    changed_fields: Iterable[str],
) -> list[dict[str, Any]]:
    if isinstance(values, (str, bytes)) or not isinstance(values, Iterable):
        raise IntervalPolicyCandidateRevisionError(
            "requested_change_responses must be a list of objects."
        )
    expected_requests = list(requested_changes)
    expected_fields = set(changed_fields)
    responses: list[dict[str, Any]] = []
    for index, item in enumerate(values):
        if not isinstance(item, Mapping):
            raise IntervalPolicyCandidateRevisionError(
                "Each requested-change response must be an object."
            )
        allowed = {"requested_change", "response", "changed_threshold_fields"}
        missing = sorted(allowed - set(item))
        unexpected = sorted(set(item) - allowed)
        if missing or unexpected:
            raise IntervalPolicyCandidateRevisionError(
                f"requested_change_responses[{index}] fields are invalid."
            )
        request = required_text(
            item["requested_change"],
            "requested_change",
            minimum_length=10,
        )
        response = required_text(
            item["response"], "response", minimum_length=20
        )
        fields = unique_texts(
            item["changed_threshold_fields"],
            "changed_threshold_fields",
            required=True,
        )
        unknown = sorted(set(fields) - expected_fields)
        if unknown:
            raise IntervalPolicyCandidateRevisionError(
                "requested-change responses reference unchanged thresholds: "
                + ", ".join(unknown)
                + "."
            )
        responses.append(
            {
                "requested_change": request,
                "response": response,
                "changed_threshold_fields": sorted(fields),
            }
        )
    if [item["requested_change"] for item in responses] != expected_requests:
        raise IntervalPolicyCandidateRevisionError(
            "requested_change_responses must cover the decision requests exactly and in order."
        )
    covered = {
        field
        for item in responses
        for field in item["changed_threshold_fields"]
    }
    if covered != expected_fields:
        missing = sorted(expected_fields - covered)
        raise IntervalPolicyCandidateRevisionError(
            "Every changed threshold must be addressed by the revision responses: "
            + ", ".join(missing)
            + "."
        )
    return responses

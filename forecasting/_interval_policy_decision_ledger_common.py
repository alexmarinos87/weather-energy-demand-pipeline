from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from math import isfinite
from typing import Any, Iterable

import pandas as pd

from forecasting.interval_policy_review_decision import (
    DECISION_CONTRACT_VERSION,
    DECISION_SAFETY_FIELDS,
    IntervalPolicyReviewDecisionError,
    verify_policy_review_decision,
)


LEDGER_CONTRACT_VERSION = "interval-policy-decision-ledger-v1"
LEDGER_RUN_ID_PATTERN = re.compile(r"^ipl-[0-9a-f]{24}$")
DECISION_ID_PATTERN = re.compile(r"^ipd-[0-9a-f]{24}$")
LEDGER_SAFETY_FIELDS = (
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
ENTRY_COLUMNS = {
    "ledger_run_id",
    "ledger_run_timestamp_utc",
    "decision_sequence",
    "decision_id",
    "decision_revision",
    "decision_sha256",
    "decision_timestamp_utc",
    "sensitivity_run_id",
    "trend_run_id",
    "sensitivity_summary_sha256",
    "decision",
    "decision_effect",
    "target_candidate_id",
    "target_candidate_role",
    "target_candidate_version",
    "reviewer_name",
    "reviewer_role",
    "review_ticket",
    "rationale",
    "requested_change_count",
    "scenario_count",
    "changed_slice_count_total",
    "follow_up_human_action_required",
    "decision_contract_version",
    "ledger_contract_version",
    *LEDGER_SAFETY_FIELDS,
}
SUMMARY_COLUMNS = {
    "ledger_run_id",
    "ledger_run_timestamp_utc",
    "decision_count",
    "sensitivity_run_count",
    "target_candidate_count",
    "retain_active_policy_count",
    "reject_candidate_count",
    "request_candidate_revision_count",
    "first_decision_timestamp_utc",
    "last_decision_timestamp_utc",
    "conflict_count",
    "human_review_required",
    "ledger_contract_version",
    *LEDGER_SAFETY_FIELDS,
}


class IntervalPolicyDecisionLedgerError(ValueError):
    """Raised when G27 decision evidence cannot form one safe ledger."""


def _utc(value: Any, name: str) -> pd.Timestamp:
    try:
        timestamp = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise IntervalPolicyDecisionLedgerError(
            f"{name} must be a valid timezone-aware timestamp."
        ) from exc
    if pd.isna(timestamp) or timestamp.tzinfo is None:
        raise IntervalPolicyDecisionLedgerError(
            f"{name} must be timezone-aware."
        )
    return timestamp.tz_convert("UTC")


def _text(value: Any, name: str) -> str:
    if value is None:
        raise IntervalPolicyDecisionLedgerError(f"{name} must be non-empty.")
    text = str(value).strip()
    if not text:
        raise IntervalPolicyDecisionLedgerError(f"{name} must be non-empty.")
    return text


def _canonical(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _canonical(value[key]) for key in sorted(value)}
    if isinstance(value, (list, tuple)):
        return [_canonical(item) for item in value]
    if isinstance(value, (datetime, pd.Timestamp)):
        return _utc(value, "timestamp").isoformat()
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


def _false_authority(document: dict[str, Any], fields: Iterable[str]) -> None:
    for field in fields:
        if document.get(field) is not False:
            raise IntervalPolicyDecisionLedgerError(
                f"Authority field {field} must be false."
            )


def _decision_row(
    decision: dict[str, Any],
    *,
    ledger_run_id: str,
    ledger_run_timestamp: pd.Timestamp,
) -> dict[str, Any]:
    _false_authority(decision, DECISION_SAFETY_FIELDS)
    scenarios = decision.get("scenario_evidence")
    if not isinstance(scenarios, list) or not scenarios:
        raise IntervalPolicyDecisionLedgerError(
            "Each decision must contain retained scenario evidence."
        )
    changed_total = 0
    for scenario in scenarios:
        if not isinstance(scenario, dict):
            raise IntervalPolicyDecisionLedgerError(
                "Scenario evidence must contain JSON objects."
            )
        changed = scenario.get("changed_slice_count")
        if isinstance(changed, bool) or not isinstance(changed, int) or changed < 0:
            raise IntervalPolicyDecisionLedgerError(
                "changed_slice_count must contain non-negative integers."
            )
        changed_total += changed
    decision_timestamp = _utc(
        decision.get("decision_timestamp_utc"), "decision_timestamp_utc"
    )
    if decision_timestamp > ledger_run_timestamp:
        raise IntervalPolicyDecisionLedgerError(
            "A decision cannot occur after its ledger run timestamp."
        )
    requested_changes = decision.get("requested_changes", [])
    if not isinstance(requested_changes, list):
        raise IntervalPolicyDecisionLedgerError(
            "requested_changes must be a list."
        )
    row = {
        "ledger_run_id": ledger_run_id,
        "ledger_run_timestamp_utc": ledger_run_timestamp,
        "decision_sequence": 0,
        "decision_id": _text(decision.get("decision_id"), "decision_id"),
        "decision_revision": int(decision.get("decision_revision", 0)),
        "decision_sha256": _text(
            decision.get("decision_sha256"), "decision_sha256"
        ),
        "decision_timestamp_utc": decision_timestamp,
        "sensitivity_run_id": _text(
            decision.get("sensitivity_run_id"), "sensitivity_run_id"
        ),
        "trend_run_id": _text(decision.get("trend_run_id"), "trend_run_id"),
        "sensitivity_summary_sha256": _text(
            decision.get("sensitivity_summary_sha256"),
            "sensitivity_summary_sha256",
        ),
        "decision": _text(decision.get("decision"), "decision"),
        "decision_effect": _text(
            decision.get("decision_effect"), "decision_effect"
        ),
        "target_candidate_id": _text(
            decision.get("target_candidate_id"), "target_candidate_id"
        ),
        "target_candidate_role": _text(
            decision.get("target_candidate_role"), "target_candidate_role"
        ),
        "target_candidate_version": _text(
            decision.get("target_candidate_version"),
            "target_candidate_version",
        ),
        "reviewer_name": _text(
            decision.get("reviewer_name"), "reviewer_name"
        ),
        "reviewer_role": _text(
            decision.get("reviewer_role"), "reviewer_role"
        ),
        "review_ticket": _text(
            decision.get("review_ticket"), "review_ticket"
        ),
        "rationale": _text(decision.get("rationale"), "rationale"),
        "requested_change_count": len(requested_changes),
        "scenario_count": len(scenarios),
        "changed_slice_count_total": changed_total,
        "follow_up_human_action_required": bool(
            decision.get("follow_up_human_action_required")
        ),
        "decision_contract_version": _text(
            decision.get("decision_contract_version"),
            "decision_contract_version",
        ),
        "ledger_contract_version": LEDGER_CONTRACT_VERSION,
        **{field: False for field in LEDGER_SAFETY_FIELDS},
    }
    if row["decision_contract_version"] != DECISION_CONTRACT_VERSION:
        raise IntervalPolicyDecisionLedgerError(
            "Ledger input uses an unsupported decision contract."
        )
    if not DECISION_ID_PATTERN.fullmatch(row["decision_id"]):
        raise IntervalPolicyDecisionLedgerError("decision_id is malformed.")
    return row

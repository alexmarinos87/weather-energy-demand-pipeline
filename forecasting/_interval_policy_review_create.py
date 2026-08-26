from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from forecasting._interval_policy_review_summary import (
    ALLOWED_DECISIONS,
    AUTHORITY_FIELDS,
    DECISION_CONTRACT_VERSION,
    DECISION_EFFECTS,
    DECISION_ID_PATTERN,
    DECISION_SAFETY_FIELDS,
    IntervalPolicyReviewDecisionError,
    _canonical,
    _digest,
    _required_text,
    _unique_texts,
    _utc_timestamp,
    prepare_sensitivity_summary,
    sensitivity_summary_sha256,
)


def create_policy_review_decision(
    sensitivity_summary: pd.DataFrame,
    *,
    decision: str,
    target_candidate_id: str,
    reviewer_name: str,
    reviewer_role: str,
    review_ticket: str,
    rationale: str,
    requested_changes: Iterable[Any] = (),
    decision_timestamp_utc: Any | None = None,
) -> dict[str, Any]:
    """Create one immutable, named, non-activating policy-review decision."""
    prepared = prepare_sensitivity_summary(sensitivity_summary)
    decision = _required_text(decision, "decision")
    if decision not in ALLOWED_DECISIONS:
        raise IntervalPolicyReviewDecisionError(
            "decision must be retain_active_policy, reject_candidate, or "
            "request_candidate_revision."
        )
    target_candidate_id = _required_text(
        target_candidate_id, "target_candidate_id"
    )
    target = prepared.loc[prepared["candidate_id"] == target_candidate_id]
    if target.empty:
        raise IntervalPolicyReviewDecisionError(
            "target_candidate_id is not present in the sensitivity summary."
        )
    target_role = target["candidate_role"].iloc[0]
    target_version = target["candidate_version"].iloc[0]
    changes = _unique_texts(
        requested_changes,
        "requested_changes",
        required=decision == "request_candidate_revision",
    )
    if decision == "retain_active_policy":
        if target_role != "active_reference" or target_candidate_id != "active-reference":
            raise IntervalPolicyReviewDecisionError(
                "retain_active_policy must target active-reference."
            )
        if changes:
            raise IntervalPolicyReviewDecisionError(
                "retain_active_policy cannot contain requested changes."
            )
    elif decision == "reject_candidate":
        if target_role != "review_candidate":
            raise IntervalPolicyReviewDecisionError(
                "reject_candidate must target a review candidate."
            )
        if changes:
            raise IntervalPolicyReviewDecisionError(
                "reject_candidate cannot contain requested changes."
            )
    else:
        if target_role != "review_candidate":
            raise IntervalPolicyReviewDecisionError(
                "request_candidate_revision must target a review candidate."
            )
        if any(len(change) < 10 for change in changes):
            raise IntervalPolicyReviewDecisionError(
                "Each requested change must contain at least 10 characters."
            )
    timestamp = _utc_timestamp(
        decision_timestamp_utc or datetime.now(timezone.utc),
        "decision_timestamp_utc",
    )
    sensitivity_timestamp = prepared["sensitivity_run_timestamp_utc"].iloc[0]
    if timestamp < sensitivity_timestamp:
        raise IntervalPolicyReviewDecisionError(
            "decision_timestamp_utc cannot precede sensitivity evidence."
        )
    scenario_evidence = []
    for row in target.sort_values("scenario").itertuples(index=False):
        scenario_evidence.append(
            {
                "scenario": row.scenario,
                "retained_monitor_status": row.retained_monitor_status,
                "active_reference_status": row.active_reference_status,
                "target_candidate_status": row.candidate_status,
                "sensitivity_classification": row.sensitivity_classification,
                "status_changed_from_active": bool(
                    row.status_changed_from_active
                ),
                "changed_slice_count": int(row.changed_slice_count),
                "human_review_required": bool(row.human_review_required),
            }
        )
    core = {
        "sensitivity_run_id": prepared["sensitivity_run_id"].iloc[0],
        "sensitivity_run_timestamp_utc": sensitivity_timestamp.isoformat(),
        "trend_run_id": prepared["trend_run_id"].iloc[0],
        "sensitivity_summary_sha256": sensitivity_summary_sha256(prepared),
        "decision": decision,
        "decision_effect": DECISION_EFFECTS[decision],
        "target_candidate_id": target_candidate_id,
        "target_candidate_role": target_role,
        "target_candidate_version": target_version,
        "reviewer_name": _required_text(reviewer_name, "reviewer_name"),
        "reviewer_role": _required_text(reviewer_role, "reviewer_role"),
        "review_ticket": _required_text(review_ticket, "review_ticket"),
        "rationale": _required_text(rationale, "rationale"),
        "requested_changes": changes,
        "decision_timestamp_utc": timestamp.isoformat(),
        "scenario_evidence": scenario_evidence,
    }
    document = {
        "decision_id": "ipd-" + _digest(core)[:24],
        "decision_revision": 1,
        **core,
        "named_human_review_confirmed": True,
        "follow_up_human_action_required": decision
        == "request_candidate_revision",
        **{field: False for field in DECISION_SAFETY_FIELDS},
        "decision_contract_version": DECISION_CONTRACT_VERSION,
    }
    document["decision_sha256"] = _digest(document)
    from forecasting._interval_policy_review_verify import verify_policy_review_decision
    verify_policy_review_decision(document, prepared)
    return document



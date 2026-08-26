from __future__ import annotations

from typing import Any

import pandas as pd

from forecasting._interval_policy_review_summary import (
    ALLOWED_DECISIONS,
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


def verify_policy_review_decision(
    decision: dict[str, Any],
    sensitivity_summary: pd.DataFrame,
) -> None:
    """Verify a decision and its binding to retained G26 evidence."""
    if not isinstance(decision, dict):
        raise IntervalPolicyReviewDecisionError(
            "Policy review decision must be a JSON object."
        )
    prepared = prepare_sensitivity_summary(sensitivity_summary)
    if decision.get("decision_contract_version") != DECISION_CONTRACT_VERSION:
        raise IntervalPolicyReviewDecisionError(
            "Unsupported policy review decision contract."
        )
    decision_id = _required_text(decision.get("decision_id"), "decision_id")
    if not DECISION_ID_PATTERN.fullmatch(decision_id):
        raise IntervalPolicyReviewDecisionError("decision_id is malformed.")
    if decision.get("decision_revision") != 1:
        raise IntervalPolicyReviewDecisionError(
            "decision_revision must be 1."
        )
    expected_hash = _digest(
        {key: value for key, value in decision.items() if key != "decision_sha256"}
    )
    if decision.get("decision_sha256") != expected_hash:
        raise IntervalPolicyReviewDecisionError(
            "Policy review decision hash is invalid."
        )
    if decision.get("sensitivity_summary_sha256") != sensitivity_summary_sha256(
        prepared
    ):
        raise IntervalPolicyReviewDecisionError(
            "Policy review decision does not match the retained sensitivity summary."
        )
    if decision.get("sensitivity_run_id") != prepared[
        "sensitivity_run_id"
    ].iloc[0]:
        raise IntervalPolicyReviewDecisionError(
            "Policy review decision sensitivity_run_id is inconsistent."
        )
    if decision.get("trend_run_id") != prepared["trend_run_id"].iloc[0]:
        raise IntervalPolicyReviewDecisionError(
            "Policy review decision trend_run_id is inconsistent."
        )
    decision_name = decision.get("decision")
    if decision_name not in ALLOWED_DECISIONS:
        raise IntervalPolicyReviewDecisionError("decision is invalid.")
    if decision.get("decision_effect") != DECISION_EFFECTS[decision_name]:
        raise IntervalPolicyReviewDecisionError(
            "decision_effect is inconsistent."
        )
    target_id = _required_text(
        decision.get("target_candidate_id"), "target_candidate_id"
    )
    target = prepared.loc[prepared["candidate_id"] == target_id]
    if target.empty:
        raise IntervalPolicyReviewDecisionError(
            "Decision target is absent from sensitivity evidence."
        )
    if decision.get("target_candidate_role") != target[
        "candidate_role"
    ].iloc[0]:
        raise IntervalPolicyReviewDecisionError(
            "Decision target role is inconsistent."
        )
    if decision.get("target_candidate_version") != target[
        "candidate_version"
    ].iloc[0]:
        raise IntervalPolicyReviewDecisionError(
            "Decision target version is inconsistent."
        )
    changes = _unique_texts(
        decision.get("requested_changes", ()),
        "requested_changes",
        required=decision_name == "request_candidate_revision",
    )
    if decision_name == "retain_active_policy":
        if (
            target_id != "active-reference"
            or target["candidate_role"].iloc[0] != "active_reference"
            or changes
        ):
            raise IntervalPolicyReviewDecisionError(
                "Retain decision fields are inconsistent."
            )
    elif decision_name == "reject_candidate":
        if target["candidate_role"].iloc[0] != "review_candidate" or changes:
            raise IntervalPolicyReviewDecisionError(
                "Reject decision fields are inconsistent."
            )
    elif target["candidate_role"].iloc[0] != "review_candidate" or not changes:
        raise IntervalPolicyReviewDecisionError(
            "Revision decision fields are inconsistent."
        )
    _required_text(decision.get("reviewer_name"), "reviewer_name")
    _required_text(decision.get("reviewer_role"), "reviewer_role")
    _required_text(decision.get("review_ticket"), "review_ticket")
    _required_text(decision.get("rationale"), "rationale")
    decision_timestamp = _utc_timestamp(
        decision.get("decision_timestamp_utc"), "decision_timestamp_utc"
    )
    if decision_timestamp < prepared["sensitivity_run_timestamp_utc"].iloc[0]:
        raise IntervalPolicyReviewDecisionError(
            "Decision timestamp precedes sensitivity evidence."
        )
    expected_scenarios = []
    for row in target.sort_values("scenario").itertuples(index=False):
        expected_scenarios.append(
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
    if _canonical(decision.get("scenario_evidence")) != _canonical(
        expected_scenarios
    ):
        raise IntervalPolicyReviewDecisionError(
            "Decision scenario evidence is inconsistent."
        )
    if decision.get("named_human_review_confirmed") is not True:
        raise IntervalPolicyReviewDecisionError(
            "Named human review must be confirmed."
        )
    expected_follow_up = decision_name == "request_candidate_revision"
    if decision.get("follow_up_human_action_required") is not expected_follow_up:
        raise IntervalPolicyReviewDecisionError(
            "follow_up_human_action_required is inconsistent."
        )
    for field in DECISION_SAFETY_FIELDS:
        if decision.get(field) is not False:
            raise IntervalPolicyReviewDecisionError(
                f"Decision safety field {field} must be false."
            )

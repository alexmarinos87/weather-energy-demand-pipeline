from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import re
from typing import Any, Iterable, Mapping
from uuid import uuid4

import pandas as pd

from forecasting._interval_policy_candidate_revision_common import (
    canonical,
    digest,
    utc_timestamp,
)
from forecasting._interval_policy_retained_compatibility_common import (
    CURRENT_POLICY_ID,
    CURRENT_SHORTFALL_THRESHOLD,
    PREVIOUS_POLICY_ID,
    PREVIOUS_SHORTFALL_THRESHOLD,
    IntervalPolicyRetainedCompatibilityError,
    compatibility_summary_sha256,
    prepare_compatibility_summary,
)
from forecasting.interval_policy_retained_compatibility_manifest import (
    verify_compatibility_manifest,
)


REVIEW_CONTRACT_VERSION = "interval-policy-retained-compatibility-review-v1"
REVIEW_ID_PATTERN = re.compile(r"^ipcr-[0-9a-f]{24}$")
ALLOWED_DECISIONS = {
    "accept_non_retroactive_transition",
    "request_historical_annotation",
    "request_compatibility_reassessment",
}
DECISION_EFFECTS = {
    "accept_non_retroactive_transition": "non_retroactive_transition_accepted",
    "request_historical_annotation": (
        "separate_historical_annotation_proposal_required"
    ),
    "request_compatibility_reassessment": (
        "new_compatibility_assessment_required"
    ),
}
REVIEW_SAFETY_FIELDS = (
    "historical_statuses_rewritten",
    "historical_annotation_applied",
    "retained_evidence_mutated",
    "monitoring_rerun_performed",
    "policy_version_updated",
    "threshold_activation_performed",
    "interval_recalibration_performed",
    "model_change_performed",
    "fabric_execution_performed",
    "schedule_change_performed",
    "promotion_change_performed",
    "alert_delivery_performed",
    "deployment_performed",
    "external_publication_performed",
)


class IntervalPolicyRetainedCompatibilityReviewError(ValueError):
    """Raised when a named G39 review is malformed or unsafe."""


def _required_text(value: Any, name: str, *, minimum_length: int = 1) -> str:
    text = "" if value is None else str(value).strip()
    if len(text) < minimum_length:
        raise IntervalPolicyRetainedCompatibilityReviewError(
            f"{name} must contain at least {minimum_length} characters."
        )
    return text


def _unique_texts(
    values: Iterable[Any], name: str, *, required: bool
) -> list[str]:
    if isinstance(values, (str, bytes)):
        raise IntervalPolicyRetainedCompatibilityReviewError(
            f"{name} must be a list of strings."
        )
    result = [
        _required_text(value, name, minimum_length=10) for value in values
    ]
    if required and not result:
        raise IntervalPolicyRetainedCompatibilityReviewError(
            f"{name} must contain at least one item."
        )
    if len(set(result)) != len(result):
        raise IntervalPolicyRetainedCompatibilityReviewError(
            f"{name} must not contain duplicates."
        )
    return result


def _scenario_evidence(summary: pd.DataFrame) -> list[dict[str, Any]]:
    return [
        {
            "scenario": row.scenario,
            "retained_monitor_status": row.retained_monitor_status,
            "previous_policy_status": row.previous_policy_status,
            "current_policy_status": row.current_policy_status,
            "compatibility_classification": row.compatibility_classification,
            "retained_status_compatibility": row.retained_status_compatibility,
            "changed_slice_count": int(row.changed_slice_count),
            "newly_failed_slice_count": int(row.newly_failed_slice_count),
            "human_review_required": bool(row.human_review_required),
        }
        for row in summary.sort_values("scenario").itertuples(index=False)
    ]


def create_retained_compatibility_review(
    summary: pd.DataFrame,
    manifest: Mapping[str, Any],
    *,
    artifact_directory: Path,
    decision: str,
    reviewer_name: str,
    reviewer_role: str,
    review_ticket: str,
    rationale: str,
    requested_actions: Iterable[Any] = (),
    reviewed_at_utc: Any | None = None,
    review_id: str | None = None,
) -> dict[str, Any]:
    """Create one immutable named review over exact G38 evidence."""
    prepared = prepare_compatibility_summary(summary)
    try:
        verify_compatibility_manifest(
            manifest,
            prepared,
            artifact_directory=artifact_directory,
        )
    except IntervalPolicyRetainedCompatibilityError as exc:
        raise IntervalPolicyRetainedCompatibilityReviewError(str(exc)) from exc

    decision = _required_text(decision, "decision")
    if decision not in ALLOWED_DECISIONS:
        raise IntervalPolicyRetainedCompatibilityReviewError(
            "Unsupported compatibility review decision."
        )
    requires_actions = decision != "accept_non_retroactive_transition"
    actions = _unique_texts(
        requested_actions,
        "requested_actions",
        required=requires_actions,
    )
    if not requires_actions and actions:
        raise IntervalPolicyRetainedCompatibilityReviewError(
            "An accepted non-retroactive transition cannot contain requested_actions."
        )

    reviewed_at = utc_timestamp(
        reviewed_at_utc or datetime.now(timezone.utc),
        "reviewed_at_utc",
    )
    source_timestamp = prepared["compatibility_run_timestamp_utc"].iloc[0]
    if reviewed_at < source_timestamp:
        raise IntervalPolicyRetainedCompatibilityReviewError(
            "The review cannot precede the compatibility assessment."
        )
    identifier = review_id or "ipcr-" + uuid4().hex[:24]
    if not REVIEW_ID_PATTERN.fullmatch(identifier):
        raise IntervalPolicyRetainedCompatibilityReviewError(
            "review_id must be ipcr- plus 24 lowercase hexadecimal characters."
        )

    document: dict[str, Any] = {
        "review_id": identifier,
        "review_revision": 1,
        "compatibility_run_id": prepared["compatibility_run_id"].iloc[0],
        "compatibility_run_timestamp_utc": source_timestamp,
        "trend_run_id": prepared["trend_run_id"].iloc[0],
        "compatibility_summary_sha256": compatibility_summary_sha256(prepared),
        "compatibility_manifest_sha256": manifest.get("manifest_sha256"),
        "previous_policy_id": PREVIOUS_POLICY_ID,
        "previous_shortfall_threshold_pct_points": PREVIOUS_SHORTFALL_THRESHOLD,
        "current_policy_id": CURRENT_POLICY_ID,
        "current_shortfall_threshold_pct_points": CURRENT_SHORTFALL_THRESHOLD,
        "decision": decision,
        "decision_effect": DECISION_EFFECTS[decision],
        "reviewer_name": _required_text(reviewer_name, "reviewer_name"),
        "reviewer_role": _required_text(reviewer_role, "reviewer_role"),
        "review_ticket": _required_text(review_ticket, "review_ticket"),
        "rationale": _required_text(
            rationale, "rationale", minimum_length=20
        ),
        "requested_actions": actions,
        "reviewed_at_utc": reviewed_at,
        "scenario_evidence": _scenario_evidence(prepared),
        "named_human_review_confirmed": True,
        "follow_up_human_action_required": requires_actions,
        **{field: False for field in REVIEW_SAFETY_FIELDS},
        "review_contract_version": REVIEW_CONTRACT_VERSION,
    }
    document["review_sha256"] = digest(document)
    return canonical(document)


def verify_retained_compatibility_review(
    review: Mapping[str, Any],
    summary: pd.DataFrame,
    manifest: Mapping[str, Any],
    *,
    artifact_directory: Path,
) -> None:
    """Verify one G39 review and all retained G38 bindings."""
    document = dict(review)
    prepared = prepare_compatibility_summary(summary)
    try:
        verify_compatibility_manifest(
            manifest,
            prepared,
            artifact_directory=artifact_directory,
        )
    except IntervalPolicyRetainedCompatibilityError as exc:
        raise IntervalPolicyRetainedCompatibilityReviewError(str(exc)) from exc

    expected_hash = digest(
        {key: value for key, value in document.items() if key != "review_sha256"}
    )
    if document.get("review_sha256") != expected_hash:
        raise IntervalPolicyRetainedCompatibilityReviewError(
            "Compatibility review hash is invalid."
        )
    if not REVIEW_ID_PATTERN.fullmatch(str(document.get("review_id", ""))):
        raise IntervalPolicyRetainedCompatibilityReviewError(
            "Compatibility review ID is invalid."
        )
    if document.get("review_revision") != 1:
        raise IntervalPolicyRetainedCompatibilityReviewError(
            "Compatibility review revision is invalid."
        )
    if document.get("review_contract_version") != REVIEW_CONTRACT_VERSION:
        raise IntervalPolicyRetainedCompatibilityReviewError(
            "Compatibility review contract is invalid."
        )
    decision = document.get("decision")
    if decision not in ALLOWED_DECISIONS:
        raise IntervalPolicyRetainedCompatibilityReviewError(
            "Compatibility review decision is invalid."
        )
    if document.get("decision_effect") != DECISION_EFFECTS[decision]:
        raise IntervalPolicyRetainedCompatibilityReviewError(
            "Compatibility review decision effect is invalid."
        )
    actions = _unique_texts(
        document.get("requested_actions", ()),
        "requested_actions",
        required=decision != "accept_non_retroactive_transition",
    )
    if decision == "accept_non_retroactive_transition" and actions:
        raise IntervalPolicyRetainedCompatibilityReviewError(
            "An accepted non-retroactive transition cannot contain requested_actions."
        )
    bindings = {
        "compatibility_run_id": prepared["compatibility_run_id"].iloc[0],
        "trend_run_id": prepared["trend_run_id"].iloc[0],
        "compatibility_summary_sha256": compatibility_summary_sha256(prepared),
        "compatibility_manifest_sha256": manifest.get("manifest_sha256"),
        "previous_policy_id": PREVIOUS_POLICY_ID,
        "previous_shortfall_threshold_pct_points": PREVIOUS_SHORTFALL_THRESHOLD,
        "current_policy_id": CURRENT_POLICY_ID,
        "current_shortfall_threshold_pct_points": CURRENT_SHORTFALL_THRESHOLD,
    }
    for field, expected in bindings.items():
        if document.get(field) != expected:
            raise IntervalPolicyRetainedCompatibilityReviewError(
                f"Compatibility review {field} binding is invalid."
            )
    if canonical(document.get("scenario_evidence")) != canonical(
        _scenario_evidence(prepared)
    ):
        raise IntervalPolicyRetainedCompatibilityReviewError(
            "Compatibility review scenario evidence is invalid."
        )
    reviewed_at = utc_timestamp(document.get("reviewed_at_utc"), "reviewed_at_utc")
    if reviewed_at < prepared["compatibility_run_timestamp_utc"].iloc[0]:
        raise IntervalPolicyRetainedCompatibilityReviewError(
            "The review cannot precede the compatibility assessment."
        )
    if document.get("named_human_review_confirmed") is not True:
        raise IntervalPolicyRetainedCompatibilityReviewError(
            "Named human review confirmation is required."
        )
    if document.get("follow_up_human_action_required") is not (
        decision != "accept_non_retroactive_transition"
    ):
        raise IntervalPolicyRetainedCompatibilityReviewError(
            "Follow-up human action evidence is inconsistent."
        )
    for field in REVIEW_SAFETY_FIELDS:
        if document.get(field) is not False:
            raise IntervalPolicyRetainedCompatibilityReviewError(
                f"Compatibility review safety field {field} must be false."
            )


def _render_review(review: Mapping[str, Any]) -> str:
    lines = [
        "# Retained interval-policy compatibility review",
        "",
        f"- Review ID: `{review['review_id']}`",
        f"- Decision: `{review['decision']}`",
        f"- Effect: `{review['decision_effect']}`",
        f"- Reviewer: {review['reviewer_name']} ({review['reviewer_role']})",
        f"- Ticket: `{review['review_ticket']}`",
        f"- Reviewed at: `{review['reviewed_at_utc']}`",
        "",
        review["rationale"],
        "",
        "| Scenario | Retained | Previous | Current | Classification | Newly failed |",
        "| --- | --- | --- | --- | --- | ---: |",
    ]
    for item in review["scenario_evidence"]:
        lines.append(
            f"| {item['scenario']} | {item['retained_monitor_status']} | "
            f"{item['previous_policy_status']} | {item['current_policy_status']} | "
            f"{item['compatibility_classification']} | "
            f"{item['newly_failed_slice_count']} |"
        )
    if review["requested_actions"]:
        lines += ["", "## Requested actions", ""]
        lines.extend(f"- {item}" for item in review["requested_actions"])
    lines += [
        "",
        "This review does not rewrite historical statuses, apply annotations, ",
        "rerun monitoring, update the policy version, execute Fabric, activate ",
        "thresholds or schedules, recalibrate intervals, alter models, deliver ",
        "alerts, deploy, or publish externally.",
        "",
    ]
    return "\n".join(lines)


def write_retained_compatibility_review(
    output_directory: Path,
    review: Mapping[str, Any],
    summary: pd.DataFrame,
    manifest: Mapping[str, Any],
    *,
    artifact_directory: Path,
) -> dict[str, Path]:
    verify_retained_compatibility_review(
        review,
        summary,
        manifest,
        artifact_directory=artifact_directory,
    )
    directory = Path(output_directory)
    directory.mkdir(parents=True, exist_ok=True)
    identifier = review["review_id"]
    json_path = directory / f"interval_policy_retained_compatibility_review_{identifier}.json"
    markdown_path = directory / f"interval_policy_retained_compatibility_review_{identifier}.md"
    temporary_json = json_path.with_name(f".{json_path.name}.tmp")
    temporary_markdown = markdown_path.with_name(f".{markdown_path.name}.tmp")
    for path in (json_path, markdown_path, temporary_json, temporary_markdown):
        if path.exists():
            raise FileExistsError(f"Refusing to overwrite {path}.")
    try:
        temporary_json.write_text(
            json.dumps(canonical(review), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        temporary_markdown.write_text(_render_review(review), encoding="utf-8")
        temporary_json.replace(json_path)
        temporary_markdown.replace(markdown_path)
    finally:
        temporary_json.unlink(missing_ok=True)
        temporary_markdown.unlink(missing_ok=True)
    return {"json": json_path, "markdown": markdown_path}

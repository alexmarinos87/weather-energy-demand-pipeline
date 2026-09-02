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
from forecasting.interval_policy_historical_annotation_proposal import (
    PROPOSAL_CONTRACT_VERSION,
    PROPOSAL_SAFETY_FIELDS,
    IntervalPolicyHistoricalAnnotationProposalError,
    verify_historical_annotation_proposal,
)


REVIEW_CONTRACT_VERSION = (
    "interval-policy-historical-annotation-proposal-review-v1"
)
REVIEW_ID_PATTERN = re.compile(r"^iphapr-[0-9a-f]{24}$")
ALLOWED_DECISIONS = {
    "accept_for_separate_annotation_storage_change",
    "reject_historical_annotation_proposal",
    "request_historical_annotation_proposal_revision",
}
DECISION_EFFECTS = {
    "accept_for_separate_annotation_storage_change": (
        "separate_annotation_storage_change_request_required"
    ),
    "reject_historical_annotation_proposal": "no_further_action_recorded",
    "request_historical_annotation_proposal_revision": (
        "revised_historical_annotation_proposal_required"
    ),
}
REVIEW_SAFETY_FIELDS = (
    "annotation_storage_change_authorized",
    "annotation_storage_change_created",
    "historical_annotation_applied",
    "historical_statuses_rewritten",
    "source_proposal_mutated",
    "source_review_mutated",
    "source_compatibility_evidence_mutated",
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


class IntervalPolicyHistoricalAnnotationProposalReviewError(ValueError):
    """Raised when a named G42 proposal review is malformed or unsafe."""


def _required_text(value: Any, name: str, *, minimum_length: int = 1) -> str:
    text = "" if value is None else str(value).strip()
    if len(text) < minimum_length:
        raise IntervalPolicyHistoricalAnnotationProposalReviewError(
            f"{name} must contain at least {minimum_length} characters."
        )
    return text


def _utc(value: Any, name: str) -> pd.Timestamp:
    try:
        return utc_timestamp(value, name)
    except Exception as exc:
        raise IntervalPolicyHistoricalAnnotationProposalReviewError(str(exc)) from exc


def _unique_updates(
    values: Iterable[Any],
    *,
    required: bool,
) -> list[str]:
    if isinstance(values, (str, bytes)) or not isinstance(values, Iterable):
        raise IntervalPolicyHistoricalAnnotationProposalReviewError(
            "requested_updates must be a list of strings."
        )
    updates = [
        _required_text(value, "requested_updates", minimum_length=10)
        for value in values
    ]
    if required and not updates:
        raise IntervalPolicyHistoricalAnnotationProposalReviewError(
            "requested_updates must contain at least one item."
        )
    if len(set(updates)) != len(updates):
        raise IntervalPolicyHistoricalAnnotationProposalReviewError(
            "requested_updates must not contain duplicates."
        )
    return updates


def _verify_source(
    proposal: Mapping[str, Any],
    source_review: Mapping[str, Any],
    summary: pd.DataFrame,
    manifest: Mapping[str, Any],
    artifact_directory: Path,
) -> None:
    try:
        verify_historical_annotation_proposal(
            proposal,
            source_review,
            summary,
            manifest,
            artifact_directory=artifact_directory,
        )
    except IntervalPolicyHistoricalAnnotationProposalError as exc:
        raise IntervalPolicyHistoricalAnnotationProposalReviewError(str(exc)) from exc


def create_historical_annotation_proposal_review(
    proposal: Mapping[str, Any],
    source_review: Mapping[str, Any],
    summary: pd.DataFrame,
    manifest: Mapping[str, Any],
    *,
    artifact_directory: Path,
    decision: str,
    reviewer_name: str,
    reviewer_role: str,
    review_ticket: str,
    rationale: str,
    requested_updates: Iterable[Any] = (),
    reviewed_at_utc: Any | None = None,
    review_id: str | None = None,
) -> dict[str, Any]:
    """Create one immutable named review without applying annotations."""
    proposal_document = dict(proposal)
    source_document = dict(source_review)
    _verify_source(
        proposal_document,
        source_document,
        summary,
        manifest,
        Path(artifact_directory),
    )
    if proposal_document.get("proposal_contract_version") != PROPOSAL_CONTRACT_VERSION:
        raise IntervalPolicyHistoricalAnnotationProposalReviewError(
            "Source proposal contract version is invalid."
        )
    if proposal_document.get("follow_up_human_review_required") is not True:
        raise IntervalPolicyHistoricalAnnotationProposalReviewError(
            "The source proposal must require a separate human review."
        )
    if proposal_document.get("next_action") != (
        "named_historical_annotation_proposal_review_required"
    ):
        raise IntervalPolicyHistoricalAnnotationProposalReviewError(
            "The source proposal next action is invalid."
        )
    for field in PROPOSAL_SAFETY_FIELDS:
        if proposal_document.get(field) is not False:
            raise IntervalPolicyHistoricalAnnotationProposalReviewError(
                f"Source proposal safety field {field} must be false."
            )

    decision = _required_text(decision, "decision")
    if decision not in ALLOWED_DECISIONS:
        raise IntervalPolicyHistoricalAnnotationProposalReviewError(
            "Unsupported historical annotation proposal review decision."
        )
    revision_requested = decision == (
        "request_historical_annotation_proposal_revision"
    )
    updates = _unique_updates(requested_updates, required=revision_requested)
    if not revision_requested and updates:
        raise IntervalPolicyHistoricalAnnotationProposalReviewError(
            "Only a revision-request decision may contain requested_updates."
        )
    reviewed_at = _utc(
        reviewed_at_utc or datetime.now(timezone.utc),
        "reviewed_at_utc",
    )
    proposed_at = _utc(
        proposal_document.get("proposed_at_utc"), "proposed_at_utc"
    )
    if reviewed_at < proposed_at:
        raise IntervalPolicyHistoricalAnnotationProposalReviewError(
            "The review cannot precede the annotation proposal."
        )
    identifier = review_id or "iphapr-" + uuid4().hex[:24]
    if not REVIEW_ID_PATTERN.fullmatch(identifier):
        raise IntervalPolicyHistoricalAnnotationProposalReviewError(
            "review_id must be iphapr- plus 24 lowercase hexadecimal characters."
        )

    follow_up = decision != "reject_historical_annotation_proposal"
    document: dict[str, Any] = {
        "review_id": identifier,
        "review_revision": 1,
        "source_proposal_id": _required_text(
            proposal_document.get("proposal_id"), "source_proposal_id"
        ),
        "source_proposal_sha256": _required_text(
            proposal_document.get("proposal_sha256"), "source_proposal_sha256"
        ),
        "source_review_id": _required_text(
            proposal_document.get("source_review_id"), "source_review_id"
        ),
        "source_review_sha256": _required_text(
            proposal_document.get("source_review_sha256"), "source_review_sha256"
        ),
        "compatibility_run_id": _required_text(
            proposal_document.get("compatibility_run_id"), "compatibility_run_id"
        ),
        "trend_run_id": _required_text(
            proposal_document.get("trend_run_id"), "trend_run_id"
        ),
        "compatibility_summary_sha256": _required_text(
            proposal_document.get("compatibility_summary_sha256"),
            "compatibility_summary_sha256",
        ),
        "compatibility_manifest_sha256": _required_text(
            proposal_document.get("compatibility_manifest_sha256"),
            "compatibility_manifest_sha256",
        ),
        "annotation_count": int(proposal_document.get("annotation_count", 0)),
        "annotations_sha256": digest(proposal_document.get("annotations")),
        "requested_actions_sha256": digest(
            proposal_document.get("requested_actions")
        ),
        "requested_action_responses_sha256": digest(
            proposal_document.get("requested_action_responses")
        ),
        "decision": decision,
        "decision_effect": DECISION_EFFECTS[decision],
        "reviewer_name": _required_text(reviewer_name, "reviewer_name"),
        "reviewer_role": _required_text(reviewer_role, "reviewer_role"),
        "review_ticket": _required_text(review_ticket, "review_ticket"),
        "rationale": _required_text(
            rationale, "rationale", minimum_length=20
        ),
        "requested_updates": updates,
        "reviewed_at_utc": reviewed_at,
        "named_human_review_confirmed": True,
        "follow_up_human_action_required": follow_up,
        "next_action": DECISION_EFFECTS[decision],
        **{field: False for field in REVIEW_SAFETY_FIELDS},
        "review_contract_version": REVIEW_CONTRACT_VERSION,
    }
    document["review_sha256"] = digest(document)
    return canonical(document)


def verify_historical_annotation_proposal_review(
    review: Mapping[str, Any],
    proposal: Mapping[str, Any],
    source_review: Mapping[str, Any],
    summary: pd.DataFrame,
    manifest: Mapping[str, Any],
    *,
    artifact_directory: Path,
) -> None:
    """Verify a G42 receipt and every retained G41/G39/G38 binding."""
    document = dict(review)
    proposal_document = dict(proposal)
    _verify_source(
        proposal_document,
        source_review,
        summary,
        manifest,
        Path(artifact_directory),
    )
    expected_hash = digest(
        {key: value for key, value in document.items() if key != "review_sha256"}
    )
    if document.get("review_sha256") != expected_hash:
        raise IntervalPolicyHistoricalAnnotationProposalReviewError(
            "Historical annotation proposal review hash is invalid."
        )
    if not REVIEW_ID_PATTERN.fullmatch(str(document.get("review_id", ""))):
        raise IntervalPolicyHistoricalAnnotationProposalReviewError(
            "Historical annotation proposal review ID is invalid."
        )
    if document.get("review_revision") != 1:
        raise IntervalPolicyHistoricalAnnotationProposalReviewError(
            "Historical annotation proposal review revision is invalid."
        )
    if document.get("review_contract_version") != REVIEW_CONTRACT_VERSION:
        raise IntervalPolicyHistoricalAnnotationProposalReviewError(
            "Historical annotation proposal review contract is invalid."
        )
    decision = document.get("decision")
    if decision not in ALLOWED_DECISIONS:
        raise IntervalPolicyHistoricalAnnotationProposalReviewError(
            "Historical annotation proposal review decision is invalid."
        )
    if document.get("decision_effect") != DECISION_EFFECTS[decision]:
        raise IntervalPolicyHistoricalAnnotationProposalReviewError(
            "Historical annotation proposal review decision effect is invalid."
        )
    updates = _unique_updates(
        document.get("requested_updates", ()),
        required=decision == "request_historical_annotation_proposal_revision",
    )
    if decision != "request_historical_annotation_proposal_revision" and updates:
        raise IntervalPolicyHistoricalAnnotationProposalReviewError(
            "Only a revision-request decision may contain requested_updates."
        )
    bindings = {
        "source_proposal_id": proposal_document.get("proposal_id"),
        "source_proposal_sha256": proposal_document.get("proposal_sha256"),
        "source_review_id": proposal_document.get("source_review_id"),
        "source_review_sha256": proposal_document.get("source_review_sha256"),
        "compatibility_run_id": proposal_document.get("compatibility_run_id"),
        "trend_run_id": proposal_document.get("trend_run_id"),
        "compatibility_summary_sha256": proposal_document.get(
            "compatibility_summary_sha256"
        ),
        "compatibility_manifest_sha256": proposal_document.get(
            "compatibility_manifest_sha256"
        ),
        "annotation_count": int(proposal_document.get("annotation_count", 0)),
        "annotations_sha256": digest(proposal_document.get("annotations")),
        "requested_actions_sha256": digest(
            proposal_document.get("requested_actions")
        ),
        "requested_action_responses_sha256": digest(
            proposal_document.get("requested_action_responses")
        ),
    }
    for field, expected in bindings.items():
        if canonical(document.get(field)) != canonical(expected):
            raise IntervalPolicyHistoricalAnnotationProposalReviewError(
                f"Historical annotation proposal review {field} binding is invalid."
            )
    if _utc(document.get("reviewed_at_utc"), "reviewed_at_utc") < _utc(
        proposal_document.get("proposed_at_utc"), "proposed_at_utc"
    ):
        raise IntervalPolicyHistoricalAnnotationProposalReviewError(
            "The review cannot precede the annotation proposal."
        )
    for field in ("reviewer_name", "reviewer_role", "review_ticket"):
        _required_text(document.get(field), field)
    _required_text(document.get("rationale"), "rationale", minimum_length=20)
    if document.get("named_human_review_confirmed") is not True:
        raise IntervalPolicyHistoricalAnnotationProposalReviewError(
            "Named human review confirmation is required."
        )
    expected_follow_up = decision != "reject_historical_annotation_proposal"
    if document.get("follow_up_human_action_required") is not expected_follow_up:
        raise IntervalPolicyHistoricalAnnotationProposalReviewError(
            "Follow-up human action evidence is inconsistent."
        )
    if document.get("next_action") != DECISION_EFFECTS[decision]:
        raise IntervalPolicyHistoricalAnnotationProposalReviewError(
            "Historical annotation proposal review next action is invalid."
        )
    for field in REVIEW_SAFETY_FIELDS:
        if document.get(field) is not False:
            raise IntervalPolicyHistoricalAnnotationProposalReviewError(
                f"Historical annotation proposal review safety field {field} must be false."
            )


def _render_review(review: Mapping[str, Any]) -> str:
    lines = [
        "# Historical interval-policy annotation proposal review",
        "",
        f"- Review ID: `{review['review_id']}`",
        f"- Proposal: `{review['source_proposal_id']}`",
        f"- Decision: `{review['decision']}`",
        f"- Effect: `{review['decision_effect']}`",
        f"- Reviewer: {review['reviewer_name']} ({review['reviewer_role']})",
        f"- Ticket: `{review['review_ticket']}`",
        f"- Reviewed at: `{review['reviewed_at_utc']}`",
        f"- Proposed annotations: {review['annotation_count']}",
        "",
        review["rationale"],
    ]
    if review["requested_updates"]:
        lines += ["", "## Requested updates", ""]
        lines.extend(f"- {item}" for item in review["requested_updates"])
    lines += [
        "",
        "This review does not create or authorize an annotation-storage change, "
        "apply annotations, rewrite historical statuses, rerun monitoring, update "
        "or activate policy thresholds, execute Fabric, change models or schedules, "
        "deliver alerts, deploy, or publish externally.",
        "",
    ]
    return "\n".join(lines)


def write_historical_annotation_proposal_review(
    output_directory: Path,
    review: Mapping[str, Any],
    proposal: Mapping[str, Any],
    source_review: Mapping[str, Any],
    summary: pd.DataFrame,
    manifest: Mapping[str, Any],
    *,
    artifact_directory: Path,
) -> dict[str, Path]:
    verify_historical_annotation_proposal_review(
        review,
        proposal,
        source_review,
        summary,
        manifest,
        artifact_directory=artifact_directory,
    )
    directory = Path(output_directory)
    directory.mkdir(parents=True, exist_ok=True)
    identifier = review["review_id"]
    outputs = {
        "json": directory
        / f"interval_policy_historical_annotation_proposal_review_{identifier}.json",
        "markdown": directory
        / f"interval_policy_historical_annotation_proposal_review_{identifier}.md",
    }
    temporary = {
        key: path.with_name(f".{path.name}.tmp") for key, path in outputs.items()
    }
    for path in (*outputs.values(), *temporary.values()):
        if path.exists():
            raise FileExistsError(f"Refusing to overwrite {path}.")
    try:
        temporary["json"].write_text(
            json.dumps(canonical(review), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        temporary["markdown"].write_text(
            _render_review(review), encoding="utf-8"
        )
        temporary["json"].replace(outputs["json"])
        temporary["markdown"].replace(outputs["markdown"])
    finally:
        for path in temporary.values():
            path.unlink(missing_ok=True)
    return outputs


__all__ = [
    "ALLOWED_DECISIONS",
    "DECISION_EFFECTS",
    "REVIEW_CONTRACT_VERSION",
    "REVIEW_SAFETY_FIELDS",
    "IntervalPolicyHistoricalAnnotationProposalReviewError",
    "create_historical_annotation_proposal_review",
    "verify_historical_annotation_proposal_review",
    "write_historical_annotation_proposal_review",
]

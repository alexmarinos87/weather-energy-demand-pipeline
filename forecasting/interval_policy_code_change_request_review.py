from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

import pandas as pd

from forecasting._interval_policy_candidate_revision_common import (
    canonical,
    digest,
    required_text,
    utc_timestamp,
)
from forecasting.interval_policy_code_change_request import (
    read_frame,
    read_json,
    verify_interval_policy_code_change_request,
)


CODE_CHANGE_REQUEST_REVIEW_CONTRACT_VERSION = (
    "interval-policy-code-change-request-review-v1"
)
CODE_CHANGE_REQUEST_REVIEW_ID_PATTERN = re.compile(r"^ipccrr-[0-9a-f]{24}$")
ALLOWED_REVIEW_DECISIONS = {
    "accept_for_separate_policy_defaults_pr",
    "reject_code_change_request",
    "request_code_change_request_revision",
}
REVIEW_EFFECTS = {
    "accept_for_separate_policy_defaults_pr": "separate_policy_defaults_pr_required",
    "reject_code_change_request": "no_further_action_recorded",
    "request_code_change_request_revision": "revised_code_change_request_required",
}
CODE_CHANGE_REQUEST_REVIEW_SAFETY_FIELDS = (
    "policy_defaults_pr_authorized",
    "policy_defaults_pr_created",
    "code_change_authorized",
    "code_change_created",
    "branch_created",
    "pull_request_created",
    "implementation_authorized",
    "implementation_applied",
    "patch_applied",
    "source_file_mutated",
    "threshold_activation_authorized",
    "active_policy_updated",
    "candidate_thresholds_activated",
    "source_request_mutated",
    "source_review_mutated",
    "source_proposal_mutated",
    "retained_evidence_mutated",
    "interval_recalibration_performed",
    "model_change_performed",
    "schedule_change_performed",
    "promotion_change_performed",
    "alert_delivery_performed",
    "deployment_performed",
    "external_publication_performed",
)


class IntervalPolicyCodeChangeRequestReviewError(ValueError):
    """Raised when a named G35b review is malformed or unsafe."""


def _unique_texts(
    values: Iterable[Any],
    name: str,
    *,
    required: bool,
) -> list[str]:
    if isinstance(values, (str, bytes)):
        raise IntervalPolicyCodeChangeRequestReviewError(
            f"{name} must be a list of strings."
        )
    result = [required_text(value, name) for value in values]
    if required and not result:
        raise IntervalPolicyCodeChangeRequestReviewError(
            f"{name} must contain at least one item."
        )
    if len(set(result)) != len(result):
        raise IntervalPolicyCodeChangeRequestReviewError(
            f"{name} must not contain duplicates."
        )
    if any(len(value) < 10 for value in result):
        raise IntervalPolicyCodeChangeRequestReviewError(
            f"Each {name} item must contain at least 10 characters."
        )
    return result


def create_interval_policy_code_change_request_review(
    request: Mapping[str, Any],
    dry_run_review: Mapping[str, Any],
    proposal: Mapping[str, Any],
    disposition: Mapping[str, Any],
    revision_summary: pd.DataFrame,
    revision_manifest: Mapping[str, Any],
    revision_package: Mapping[str, Any],
    source_decision: Mapping[str, Any],
    source_sensitivity_summary: pd.DataFrame,
    *,
    current_repository_commit: str,
    current_repository_tree: str,
    policy_source_text: str,
    review_decision: str,
    reviewer_name: str,
    reviewer_role: str,
    review_ticket: str,
    rationale: str,
    requested_updates: Iterable[Any] = (),
    reviewed_at_utc: Any | None = None,
) -> dict[str, Any]:
    """Create one immutable review without creating or authorising a code PR."""
    request_document = dict(request)
    dry_run_review_document = dict(dry_run_review)
    proposal_document = dict(proposal)
    disposition_document = dict(disposition)
    verify_interval_policy_code_change_request(
        request_document,
        dry_run_review_document,
        proposal_document,
        disposition_document,
        revision_summary,
        revision_manifest,
        revision_package,
        source_decision,
        source_sensitivity_summary,
        current_repository_commit=current_repository_commit,
        current_repository_tree=current_repository_tree,
        policy_source_text=policy_source_text,
    )
    if request_document.get("next_action") != (
        "named_code_change_request_review_required"
    ):
        raise IntervalPolicyCodeChangeRequestReviewError(
            "The source request does not require a named G35b review."
        )
    decision = required_text(review_decision, "review_decision")
    if decision not in ALLOWED_REVIEW_DECISIONS:
        raise IntervalPolicyCodeChangeRequestReviewError(
            "Unsupported code-change request review decision."
        )
    updates = _unique_texts(
        requested_updates,
        "requested_updates",
        required=decision == "request_code_change_request_revision",
    )
    if decision != "request_code_change_request_revision" and updates:
        raise IntervalPolicyCodeChangeRequestReviewError(
            "Only request_code_change_request_revision may contain updates."
        )
    reviewed_at = utc_timestamp(
        reviewed_at_utc or datetime.now(timezone.utc), "reviewed_at_utc"
    )
    requested_at = utc_timestamp(
        request_document.get("requested_at_utc"), "requested_at_utc"
    )
    if reviewed_at < requested_at:
        raise IntervalPolicyCodeChangeRequestReviewError(
            "The review cannot predate the G35a request."
        )
    core = {
        "source_code_change_request_id": request_document[
            "code_change_request_id"
        ],
        "source_code_change_request_sha256": request_document[
            "code_change_request_sha256"
        ],
        "source_implementation_dry_run_review_id": request_document[
            "source_implementation_dry_run_review_id"
        ],
        "source_implementation_dry_run_review_sha256": request_document[
            "source_implementation_dry_run_review_sha256"
        ],
        "source_implementation_dry_run_id": request_document[
            "source_implementation_dry_run_id"
        ],
        "source_implementation_dry_run_sha256": request_document[
            "source_implementation_dry_run_sha256"
        ],
        "current_repository_commit": request_document[
            "current_repository_commit"
        ],
        "current_repository_tree": request_document["current_repository_tree"],
        "policy_source_path": request_document["policy_source_path"],
        "current_policy_source_sha256": request_document[
            "current_policy_source_sha256"
        ],
        "current_policy_source_git_blob_sha1": request_document[
            "current_policy_source_git_blob_sha1"
        ],
        "active_policy_sha256": request_document["active_policy_sha256"],
        "proposed_policy_sha256": request_document["proposed_policy_sha256"],
        "revised_candidate_id": request_document["revised_candidate_id"],
        "revised_candidate_version": request_document[
            "revised_candidate_version"
        ],
        "changed_threshold_fields": list(
            request_document["changed_threshold_fields"]
        ),
        "changed_threshold_count": request_document[
            "changed_threshold_count"
        ],
        "changed_threshold_digest": request_document[
            "changed_threshold_digest"
        ],
        "reviewed_patch_sha256": request_document["reviewed_patch_sha256"],
        "intended_paths": list(request_document["intended_paths"]),
        "validation_commands": list(request_document["validation_commands"]),
        "requested_branch_name": request_document["requested_branch_name"],
        "requested_pr_title": request_document["requested_pr_title"],
        "review_decision": decision,
        "review_effect": REVIEW_EFFECTS[decision],
        "requested_updates": updates,
        "reviewer_name": required_text(reviewer_name, "reviewer_name"),
        "reviewer_role": required_text(reviewer_role, "reviewer_role"),
        "review_ticket": required_text(review_ticket, "review_ticket"),
        "rationale": required_text(rationale, "rationale", minimum_length=30),
        "reviewed_at_utc": reviewed_at.isoformat(),
    }
    document = {
        "code_change_request_review_id": "ipccrr-" + digest(core)[:24],
        "code_change_request_review_revision": 1,
        **core,
        "named_human_review_confirmed": True,
        "follow_up_human_action_required": decision in {
            "accept_for_separate_policy_defaults_pr",
            "request_code_change_request_revision",
        },
        "next_action": REVIEW_EFFECTS[decision],
        **{
            field: False
            for field in CODE_CHANGE_REQUEST_REVIEW_SAFETY_FIELDS
        },
        "code_change_request_review_contract_version": (
            CODE_CHANGE_REQUEST_REVIEW_CONTRACT_VERSION
        ),
    }
    document["code_change_request_review_sha256"] = digest(document)
    verify_interval_policy_code_change_request_review(
        document,
        request_document,
        dry_run_review_document,
        proposal_document,
        disposition_document,
        revision_summary,
        revision_manifest,
        revision_package,
        source_decision,
        source_sensitivity_summary,
        current_repository_commit=current_repository_commit,
        current_repository_tree=current_repository_tree,
        policy_source_text=policy_source_text,
    )
    return document


def verify_interval_policy_code_change_request_review(
    review: Mapping[str, Any],
    request: Mapping[str, Any],
    dry_run_review: Mapping[str, Any],
    proposal: Mapping[str, Any],
    disposition: Mapping[str, Any],
    revision_summary: pd.DataFrame,
    revision_manifest: Mapping[str, Any],
    revision_package: Mapping[str, Any],
    source_decision: Mapping[str, Any],
    source_sensitivity_summary: pd.DataFrame,
    *,
    current_repository_commit: str,
    current_repository_tree: str,
    policy_source_text: str,
) -> None:
    """Verify G35b integrity against the complete request and evidence chain."""
    document = dict(review)
    request_document = dict(request)
    verify_interval_policy_code_change_request(
        request_document,
        dry_run_review,
        proposal,
        disposition,
        revision_summary,
        revision_manifest,
        revision_package,
        source_decision,
        source_sensitivity_summary,
        current_repository_commit=current_repository_commit,
        current_repository_tree=current_repository_tree,
        policy_source_text=policy_source_text,
    )
    if document.get("code_change_request_review_contract_version") != (
        CODE_CHANGE_REQUEST_REVIEW_CONTRACT_VERSION
    ):
        raise IntervalPolicyCodeChangeRequestReviewError(
            "Code-change request review contract is invalid."
        )
    review_id = required_text(
        document.get("code_change_request_review_id"),
        "code_change_request_review_id",
    )
    if not CODE_CHANGE_REQUEST_REVIEW_ID_PATTERN.fullmatch(review_id):
        raise IntervalPolicyCodeChangeRequestReviewError(
            "code_change_request_review_id is malformed."
        )
    if document.get("code_change_request_review_revision") != 1:
        raise IntervalPolicyCodeChangeRequestReviewError(
            "code_change_request_review_revision must be 1."
        )
    expected_hash = digest(
        {
            key: value
            for key, value in document.items()
            if key != "code_change_request_review_sha256"
        }
    )
    if document.get("code_change_request_review_sha256") != expected_hash:
        raise IntervalPolicyCodeChangeRequestReviewError(
            "Code-change request review hash is invalid."
        )
    bindings = {
        "source_code_change_request_id": request_document[
            "code_change_request_id"
        ],
        "source_code_change_request_sha256": request_document[
            "code_change_request_sha256"
        ],
        "source_implementation_dry_run_review_id": request_document[
            "source_implementation_dry_run_review_id"
        ],
        "source_implementation_dry_run_review_sha256": request_document[
            "source_implementation_dry_run_review_sha256"
        ],
        "source_implementation_dry_run_id": request_document[
            "source_implementation_dry_run_id"
        ],
        "source_implementation_dry_run_sha256": request_document[
            "source_implementation_dry_run_sha256"
        ],
        "current_repository_commit": request_document[
            "current_repository_commit"
        ],
        "current_repository_tree": request_document["current_repository_tree"],
        "policy_source_path": request_document["policy_source_path"],
        "current_policy_source_sha256": request_document[
            "current_policy_source_sha256"
        ],
        "current_policy_source_git_blob_sha1": request_document[
            "current_policy_source_git_blob_sha1"
        ],
        "active_policy_sha256": request_document["active_policy_sha256"],
        "proposed_policy_sha256": request_document["proposed_policy_sha256"],
        "revised_candidate_id": request_document["revised_candidate_id"],
        "revised_candidate_version": request_document[
            "revised_candidate_version"
        ],
        "changed_threshold_fields": request_document[
            "changed_threshold_fields"
        ],
        "changed_threshold_count": request_document[
            "changed_threshold_count"
        ],
        "changed_threshold_digest": request_document[
            "changed_threshold_digest"
        ],
        "reviewed_patch_sha256": request_document["reviewed_patch_sha256"],
        "intended_paths": request_document["intended_paths"],
        "validation_commands": request_document["validation_commands"],
        "requested_branch_name": request_document["requested_branch_name"],
        "requested_pr_title": request_document["requested_pr_title"],
    }
    for field, expected in bindings.items():
        if canonical(document.get(field)) != canonical(expected):
            raise IntervalPolicyCodeChangeRequestReviewError(
                f"Code-change request review {field} is inconsistent."
            )
    decision = document.get("review_decision")
    if decision not in ALLOWED_REVIEW_DECISIONS:
        raise IntervalPolicyCodeChangeRequestReviewError(
            "Review decision is invalid."
        )
    if document.get("review_effect") != REVIEW_EFFECTS[decision]:
        raise IntervalPolicyCodeChangeRequestReviewError(
            "Review effect is inconsistent."
        )
    if document.get("next_action") != REVIEW_EFFECTS[decision]:
        raise IntervalPolicyCodeChangeRequestReviewError(
            "Next action is inconsistent."
        )
    updates = _unique_texts(
        document.get("requested_updates", ()),
        "requested_updates",
        required=decision == "request_code_change_request_revision",
    )
    if decision != "request_code_change_request_revision" and updates:
        raise IntervalPolicyCodeChangeRequestReviewError(
            "Requested updates are inconsistent."
        )
    required_text(document.get("reviewer_name"), "reviewer_name")
    required_text(document.get("reviewer_role"), "reviewer_role")
    required_text(document.get("review_ticket"), "review_ticket")
    required_text(document.get("rationale"), "rationale", minimum_length=30)
    reviewed_at = utc_timestamp(
        document.get("reviewed_at_utc"), "reviewed_at_utc"
    )
    if reviewed_at < utc_timestamp(
        request_document.get("requested_at_utc"), "requested_at_utc"
    ):
        raise IntervalPolicyCodeChangeRequestReviewError(
            "Code-change request review predates the request."
        )
    if document.get("named_human_review_confirmed") is not True:
        raise IntervalPolicyCodeChangeRequestReviewError(
            "Named human review must be confirmed."
        )
    expected_follow_up = decision in {
        "accept_for_separate_policy_defaults_pr",
        "request_code_change_request_revision",
    }
    if document.get("follow_up_human_action_required") is not expected_follow_up:
        raise IntervalPolicyCodeChangeRequestReviewError(
            "Follow-up human-action evidence is inconsistent."
        )
    for field in CODE_CHANGE_REQUEST_REVIEW_SAFETY_FIELDS:
        if document.get(field) is not False:
            raise IntervalPolicyCodeChangeRequestReviewError(
                f"Code-change request review safety field {field} must be false."
            )


def render_interval_policy_code_change_request_review(
    review: Mapping[str, Any],
) -> str:
    document = dict(review)
    lines = [
        "# Interval-policy code-change request review",
        "",
        f"- Review ID: `{document['code_change_request_review_id']}`",
        f"- Request: `{document['source_code_change_request_id']}`",
        f"- Decision: `{document['review_decision']}`",
        f"- Effect: `{document['review_effect']}`",
        f"- Repository commit: `{document['current_repository_commit']}`",
        f"- Reviewer: {document['reviewer_name']} ({document['reviewer_role']})",
        f"- Review ticket: `{document['review_ticket']}`",
        f"- Reviewed at: `{document['reviewed_at_utc']}`",
        "",
        "## Rationale",
        "",
        document["rationale"],
        "",
    ]
    if document["requested_updates"]:
        lines.extend(["## Requested updates", ""])
        lines.extend(f"- {value}" for value in document["requested_updates"])
        lines.append("")
    lines.extend(
        [
            "This is immutable human review evidence only.",
            "Acceptance requires a separate policy-defaults implementation PR. "
            "It does not authorize or create that PR, apply the reviewed patch, "
            "activate thresholds, recalibrate intervals, change models or schedules, "
            "deliver alerts, deploy, or publish externally.",
            "",
        ]
    )
    return "\n".join(lines)


def write_interval_policy_code_change_request_review(
    output_directory: Path,
    review: Mapping[str, Any],
    request: Mapping[str, Any],
    dry_run_review: Mapping[str, Any],
    proposal: Mapping[str, Any],
    disposition: Mapping[str, Any],
    revision_summary: pd.DataFrame,
    revision_manifest: Mapping[str, Any],
    revision_package: Mapping[str, Any],
    source_decision: Mapping[str, Any],
    source_sensitivity_summary: pd.DataFrame,
    *,
    current_repository_commit: str,
    current_repository_tree: str,
    policy_source_text: str,
) -> tuple[Path, Path]:
    document = dict(review)
    verify_interval_policy_code_change_request_review(
        document,
        request,
        dry_run_review,
        proposal,
        disposition,
        revision_summary,
        revision_manifest,
        revision_package,
        source_decision,
        source_sensitivity_summary,
        current_repository_commit=current_repository_commit,
        current_repository_tree=current_repository_tree,
        policy_source_text=policy_source_text,
    )
    output_directory = Path(output_directory)
    output_directory.mkdir(parents=True, exist_ok=True)
    review_id = document["code_change_request_review_id"]
    json_path = output_directory / f"interval_policy_code_change_request_review_{review_id}.json"
    markdown_path = output_directory / f"interval_policy_code_change_request_review_{review_id}.md"
    temporary_paths = [
        json_path.with_name(f".{json_path.name}.tmp"),
        markdown_path.with_name(f".{markdown_path.name}.tmp"),
    ]
    for candidate in (json_path, markdown_path, *temporary_paths):
        if candidate.exists():
            raise FileExistsError(f"Refusing to overwrite {candidate}.")
    try:
        temporary_paths[0].write_text(
            json.dumps(canonical(document), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        temporary_paths[1].write_text(
            render_interval_policy_code_change_request_review(document),
            encoding="utf-8",
        )
        temporary_paths[0].replace(json_path)
        temporary_paths[1].replace(markdown_path)
    finally:
        for temporary in temporary_paths:
            temporary.unlink(missing_ok=True)
    return json_path, markdown_path


__all__ = [
    "ALLOWED_REVIEW_DECISIONS",
    "CODE_CHANGE_REQUEST_REVIEW_CONTRACT_VERSION",
    "CODE_CHANGE_REQUEST_REVIEW_SAFETY_FIELDS",
    "IntervalPolicyCodeChangeRequestReviewError",
    "create_interval_policy_code_change_request_review",
    "read_frame",
    "read_json",
    "render_interval_policy_code_change_request_review",
    "verify_interval_policy_code_change_request_review",
    "write_interval_policy_code_change_request_review",
]

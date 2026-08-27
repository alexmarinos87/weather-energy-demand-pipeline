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
from forecasting.interval_policy_implementation_dry_run import (
    IMPLEMENTATION_DRY_RUN_SAFETY_FIELDS,
    read_frame,
    read_json,
    verify_interval_policy_implementation_dry_run,
)


DRY_RUN_REVIEW_CONTRACT_VERSION = (
    "interval-policy-implementation-dry-run-review-v1"
)
DRY_RUN_REVIEW_ID_PATTERN = re.compile(r"^ipidr-[0-9a-f]{24}$")
ALLOWED_REVIEW_DECISIONS = {
    "accept_for_separate_code_change_pr",
    "reject_implementation_dry_run",
    "request_dry_run_revision",
}
REVIEW_EFFECTS = {
    "accept_for_separate_code_change_pr": "separate_code_change_pr_required",
    "reject_implementation_dry_run": "no_further_action_recorded",
    "request_dry_run_revision": "revised_dry_run_required",
}
DRY_RUN_REVIEW_SAFETY_FIELDS = (
    "code_change_authorized",
    "code_change_created",
    "implementation_authorized",
    "implementation_applied",
    "source_file_mutated",
    "threshold_activation_authorized",
    "active_policy_updated",
    "candidate_thresholds_activated",
    "source_proposal_mutated",
    "source_disposition_mutated",
    "retained_evidence_mutated",
    "interval_recalibration_performed",
    "model_change_performed",
    "schedule_change_performed",
    "promotion_change_performed",
    "alert_delivery_performed",
    "deployment_performed",
    "external_publication_performed",
)


class IntervalPolicyImplementationDryRunReviewError(ValueError):
    """Raised when a named dry-run review is malformed or unsafe."""


def _unique_texts(
    values: Iterable[Any],
    name: str,
    *,
    required: bool,
) -> list[str]:
    if isinstance(values, (str, bytes)):
        raise IntervalPolicyImplementationDryRunReviewError(
            f"{name} must be a list of strings."
        )
    result = [required_text(item, name) for item in values]
    if required and not result:
        raise IntervalPolicyImplementationDryRunReviewError(
            f"{name} must contain at least one item."
        )
    if len(set(result)) != len(result):
        raise IntervalPolicyImplementationDryRunReviewError(
            f"{name} must not contain duplicates."
        )
    if any(len(item) < 10 for item in result):
        raise IntervalPolicyImplementationDryRunReviewError(
            f"Each {name} item must contain at least 10 characters."
        )
    return result


def create_interval_policy_implementation_dry_run_review(
    proposal: Mapping[str, Any],
    disposition: Mapping[str, Any],
    revision_summary: pd.DataFrame,
    revision_manifest: Mapping[str, Any],
    revision_package: Mapping[str, Any],
    source_decision: Mapping[str, Any],
    source_sensitivity_summary: pd.DataFrame,
    *,
    repository_base_commit: str,
    repository_base_tree: str,
    policy_source_text: str,
    review_decision: str,
    reviewer_name: str,
    reviewer_role: str,
    review_ticket: str,
    rationale: str,
    requested_updates: Iterable[Any] = (),
    reviewed_at_utc: Any | None = None,
) -> dict[str, Any]:
    """Create one immutable named review without authorising implementation."""
    proposal_document = dict(proposal)
    disposition_document = dict(disposition)
    verify_interval_policy_implementation_dry_run(
        proposal_document,
        disposition_document,
        revision_summary,
        revision_manifest,
        revision_package,
        source_decision,
        source_sensitivity_summary,
        repository_base_commit=repository_base_commit,
        repository_base_tree=repository_base_tree,
        policy_source_text=policy_source_text,
    )
    if proposal_document.get("next_review_action") != (
        "named_implementation_dry_run_review_required"
    ):
        raise IntervalPolicyImplementationDryRunReviewError(
            "The source proposal does not require a named review."
        )
    decision_name = required_text(review_decision, "review_decision")
    if decision_name not in ALLOWED_REVIEW_DECISIONS:
        raise IntervalPolicyImplementationDryRunReviewError(
            "Unsupported implementation dry-run review decision."
        )
    updates = _unique_texts(
        requested_updates,
        "requested_updates",
        required=decision_name == "request_dry_run_revision",
    )
    if decision_name != "request_dry_run_revision" and updates:
        raise IntervalPolicyImplementationDryRunReviewError(
            "Only request_dry_run_revision may contain requested_updates."
        )
    reviewed_at = utc_timestamp(
        reviewed_at_utc or datetime.now(timezone.utc), "reviewed_at_utc"
    )
    prepared_at = utc_timestamp(
        proposal_document.get("prepared_at_utc"), "prepared_at_utc"
    )
    if reviewed_at < prepared_at:
        raise IntervalPolicyImplementationDryRunReviewError(
            "Dry-run review cannot precede the proposal."
        )
    changed_fields = [
        item["field"]
        for item in proposal_document["active_to_proposed_changes"]
    ]
    core = {
        "source_implementation_dry_run_id": proposal_document[
            "implementation_dry_run_id"
        ],
        "source_implementation_dry_run_sha256": proposal_document[
            "implementation_dry_run_sha256"
        ],
        "source_disposition_id": disposition_document["disposition_id"],
        "source_disposition_sha256": disposition_document[
            "disposition_sha256"
        ],
        "repository_base_commit": proposal_document[
            "repository_base_commit"
        ],
        "repository_base_tree": proposal_document["repository_base_tree"],
        "policy_source_path": proposal_document["policy_source_path"],
        "policy_source_sha256": proposal_document["policy_source_sha256"],
        "policy_source_git_blob_sha1": proposal_document[
            "policy_source_git_blob_sha1"
        ],
        "active_policy_sha256": proposal_document["active_policy_sha256"],
        "proposed_policy_sha256": proposal_document[
            "proposed_policy_sha256"
        ],
        "revised_candidate_id": proposal_document["revised_candidate_id"],
        "revised_candidate_version": proposal_document[
            "revised_candidate_version"
        ],
        "changed_threshold_fields": changed_fields,
        "changed_threshold_count": len(changed_fields),
        "intended_paths": list(proposal_document["intended_paths"]),
        "validation_commands": list(
            proposal_document["validation_commands"]
        ),
        "review_decision": decision_name,
        "review_effect": REVIEW_EFFECTS[decision_name],
        "requested_updates": updates,
        "reviewer_name": required_text(reviewer_name, "reviewer_name"),
        "reviewer_role": required_text(reviewer_role, "reviewer_role"),
        "review_ticket": required_text(review_ticket, "review_ticket"),
        "rationale": required_text(rationale, "rationale", minimum_length=30),
        "reviewed_at_utc": reviewed_at.isoformat(),
    }
    document = {
        "implementation_dry_run_review_id": "ipidr-" + digest(core)[:24],
        "implementation_dry_run_review_revision": 1,
        **core,
        "named_human_review_confirmed": True,
        "follow_up_human_action_required": decision_name in {
            "accept_for_separate_code_change_pr",
            "request_dry_run_revision",
        },
        "next_action": REVIEW_EFFECTS[decision_name],
        **{field: False for field in DRY_RUN_REVIEW_SAFETY_FIELDS},
        "implementation_dry_run_review_contract_version": (
            DRY_RUN_REVIEW_CONTRACT_VERSION
        ),
    }
    document["implementation_dry_run_review_sha256"] = digest(document)
    verify_interval_policy_implementation_dry_run_review(
        document,
        proposal_document,
        disposition_document,
        revision_summary,
        revision_manifest,
        revision_package,
        source_decision,
        source_sensitivity_summary,
        repository_base_commit=repository_base_commit,
        repository_base_tree=repository_base_tree,
        policy_source_text=policy_source_text,
    )
    return document


def verify_interval_policy_implementation_dry_run_review(
    review: Mapping[str, Any],
    proposal: Mapping[str, Any],
    disposition: Mapping[str, Any],
    revision_summary: pd.DataFrame,
    revision_manifest: Mapping[str, Any],
    revision_package: Mapping[str, Any],
    source_decision: Mapping[str, Any],
    source_sensitivity_summary: pd.DataFrame,
    *,
    repository_base_commit: str,
    repository_base_tree: str,
    policy_source_text: str,
) -> None:
    """Verify review integrity and its complete repository/evidence binding."""
    document = dict(review)
    proposal_document = dict(proposal)
    disposition_document = dict(disposition)
    verify_interval_policy_implementation_dry_run(
        proposal_document,
        disposition_document,
        revision_summary,
        revision_manifest,
        revision_package,
        source_decision,
        source_sensitivity_summary,
        repository_base_commit=repository_base_commit,
        repository_base_tree=repository_base_tree,
        policy_source_text=policy_source_text,
    )
    if document.get("implementation_dry_run_review_contract_version") != (
        DRY_RUN_REVIEW_CONTRACT_VERSION
    ):
        raise IntervalPolicyImplementationDryRunReviewError(
            "Implementation dry-run review contract is invalid."
        )
    review_id = required_text(
        document.get("implementation_dry_run_review_id"),
        "implementation_dry_run_review_id",
    )
    if not DRY_RUN_REVIEW_ID_PATTERN.fullmatch(review_id):
        raise IntervalPolicyImplementationDryRunReviewError(
            "implementation_dry_run_review_id is malformed."
        )
    if document.get("implementation_dry_run_review_revision") != 1:
        raise IntervalPolicyImplementationDryRunReviewError(
            "implementation_dry_run_review_revision must be 1."
        )
    expected_hash = digest(
        {
            key: value
            for key, value in document.items()
            if key != "implementation_dry_run_review_sha256"
        }
    )
    if document.get("implementation_dry_run_review_sha256") != expected_hash:
        raise IntervalPolicyImplementationDryRunReviewError(
            "Implementation dry-run review hash is invalid."
        )
    bindings = {
        "source_implementation_dry_run_id": proposal_document[
            "implementation_dry_run_id"
        ],
        "source_implementation_dry_run_sha256": proposal_document[
            "implementation_dry_run_sha256"
        ],
        "source_disposition_id": disposition_document["disposition_id"],
        "source_disposition_sha256": disposition_document[
            "disposition_sha256"
        ],
        "repository_base_commit": proposal_document[
            "repository_base_commit"
        ],
        "repository_base_tree": proposal_document["repository_base_tree"],
        "policy_source_path": proposal_document["policy_source_path"],
        "policy_source_sha256": proposal_document["policy_source_sha256"],
        "policy_source_git_blob_sha1": proposal_document[
            "policy_source_git_blob_sha1"
        ],
        "active_policy_sha256": proposal_document["active_policy_sha256"],
        "proposed_policy_sha256": proposal_document[
            "proposed_policy_sha256"
        ],
        "revised_candidate_id": proposal_document["revised_candidate_id"],
        "revised_candidate_version": proposal_document[
            "revised_candidate_version"
        ],
        "intended_paths": proposal_document["intended_paths"],
        "validation_commands": proposal_document["validation_commands"],
    }
    for field, expected in bindings.items():
        if canonical(document.get(field)) != canonical(expected):
            raise IntervalPolicyImplementationDryRunReviewError(
                f"Implementation dry-run review {field} is inconsistent."
            )
    expected_fields = [
        item["field"]
        for item in proposal_document["active_to_proposed_changes"]
    ]
    if document.get("changed_threshold_fields") != expected_fields:
        raise IntervalPolicyImplementationDryRunReviewError(
            "changed_threshold_fields are inconsistent."
        )
    if document.get("changed_threshold_count") != len(expected_fields):
        raise IntervalPolicyImplementationDryRunReviewError(
            "changed_threshold_count is inconsistent."
        )
    decision_name = document.get("review_decision")
    if decision_name not in ALLOWED_REVIEW_DECISIONS:
        raise IntervalPolicyImplementationDryRunReviewError(
            "Review decision is invalid."
        )
    if document.get("review_effect") != REVIEW_EFFECTS[decision_name]:
        raise IntervalPolicyImplementationDryRunReviewError(
            "Review effect is inconsistent."
        )
    if document.get("next_action") != REVIEW_EFFECTS[decision_name]:
        raise IntervalPolicyImplementationDryRunReviewError(
            "Next action is inconsistent."
        )
    updates = _unique_texts(
        document.get("requested_updates", ()),
        "requested_updates",
        required=decision_name == "request_dry_run_revision",
    )
    if decision_name != "request_dry_run_revision" and updates:
        raise IntervalPolicyImplementationDryRunReviewError(
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
        proposal_document.get("prepared_at_utc"), "prepared_at_utc"
    ):
        raise IntervalPolicyImplementationDryRunReviewError(
            "Review timestamp precedes the proposal."
        )
    if document.get("named_human_review_confirmed") is not True:
        raise IntervalPolicyImplementationDryRunReviewError(
            "Named human review must be confirmed."
        )
    expected_follow_up = decision_name in {
        "accept_for_separate_code_change_pr",
        "request_dry_run_revision",
    }
    if document.get("follow_up_human_action_required") is not expected_follow_up:
        raise IntervalPolicyImplementationDryRunReviewError(
            "Follow-up human-action evidence is inconsistent."
        )
    for field in DRY_RUN_REVIEW_SAFETY_FIELDS:
        if document.get(field) is not False:
            raise IntervalPolicyImplementationDryRunReviewError(
                f"Dry-run review safety field {field} must be false."
            )


def render_interval_policy_implementation_dry_run_review(
    review: Mapping[str, Any],
) -> str:
    document = dict(review)
    lines = [
        "# Interval-policy implementation dry-run review",
        "",
        f"- Review ID: `{document['implementation_dry_run_review_id']}`",
        f"- Proposal: `{document['source_implementation_dry_run_id']}`",
        f"- Decision: `{document['review_decision']}`",
        f"- Effect: `{document['review_effect']}`",
        f"- Repository base: `{document['repository_base_commit']}`",
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
        lines.extend(f"- {item}" for item in document["requested_updates"])
        lines.append("")
    lines.extend(
        [
            "This is immutable human review evidence only.",
            "Acceptance requires a separate code-change PR. It does not authorize "
            "or create that PR, apply the patch, update or activate the policy, "
            "recalibrate intervals, change models or schedules, deliver alerts, "
            "deploy, or publish externally.",
            "",
        ]
    )
    return "\n".join(lines)


def write_interval_policy_implementation_dry_run_review(
    output_directory: Path,
    review: Mapping[str, Any],
    proposal: Mapping[str, Any],
    disposition: Mapping[str, Any],
    revision_summary: pd.DataFrame,
    revision_manifest: Mapping[str, Any],
    revision_package: Mapping[str, Any],
    source_decision: Mapping[str, Any],
    source_sensitivity_summary: pd.DataFrame,
    *,
    repository_base_commit: str,
    repository_base_tree: str,
    policy_source_text: str,
) -> tuple[Path, Path]:
    document = dict(review)
    verify_interval_policy_implementation_dry_run_review(
        document,
        proposal,
        disposition,
        revision_summary,
        revision_manifest,
        revision_package,
        source_decision,
        source_sensitivity_summary,
        repository_base_commit=repository_base_commit,
        repository_base_tree=repository_base_tree,
        policy_source_text=policy_source_text,
    )
    output_directory = Path(output_directory)
    output_directory.mkdir(parents=True, exist_ok=True)
    review_id = document["implementation_dry_run_review_id"]
    json_path = output_directory / (
        f"interval_policy_implementation_dry_run_review_{review_id}.json"
    )
    markdown_path = output_directory / (
        f"interval_policy_implementation_dry_run_review_{review_id}.md"
    )
    temporary_json = json_path.with_name(f".{json_path.name}.tmp")
    temporary_markdown = markdown_path.with_name(f".{markdown_path.name}.tmp")
    for path in (
        json_path,
        markdown_path,
        temporary_json,
        temporary_markdown,
    ):
        if path.exists():
            raise FileExistsError(f"Refusing to overwrite {path}.")
    try:
        temporary_json.write_text(
            json.dumps(canonical(document), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        temporary_markdown.write_text(
            render_interval_policy_implementation_dry_run_review(document),
            encoding="utf-8",
        )
        temporary_json.replace(json_path)
        temporary_markdown.replace(markdown_path)
    finally:
        temporary_json.unlink(missing_ok=True)
        temporary_markdown.unlink(missing_ok=True)
    return json_path, markdown_path


__all__ = [
    "ALLOWED_REVIEW_DECISIONS",
    "DRY_RUN_REVIEW_CONTRACT_VERSION",
    "DRY_RUN_REVIEW_SAFETY_FIELDS",
    "IntervalPolicyImplementationDryRunReviewError",
    "create_interval_policy_implementation_dry_run_review",
    "read_frame",
    "read_json",
    "render_interval_policy_implementation_dry_run_review",
    "verify_interval_policy_implementation_dry_run_review",
    "write_interval_policy_implementation_dry_run_review",
]

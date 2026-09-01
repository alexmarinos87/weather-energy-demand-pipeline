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
    POLICY_SOURCE_PATH,
    read_frame,
    read_json,
    source_git_blob_sha1,
    source_sha256,
)
from forecasting.interval_policy_implementation_dry_run_review import (
    verify_interval_policy_implementation_dry_run_review,
)


CODE_CHANGE_REQUEST_CONTRACT_VERSION = "interval-policy-code-change-request-v1"
CODE_CHANGE_REQUEST_ID_PATTERN = re.compile(r"^ipccr-[0-9a-f]{24}$")
BRANCH_NAME_PATTERN = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9._/-]{2,99}$")
CODE_CHANGE_REQUEST_SAFETY_FIELDS = (
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
    "source_review_mutated",
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


class IntervalPolicyCodeChangeRequestError(ValueError):
    """Raised when a repository-bound code-change request is malformed or unsafe."""


def _full_sha(value: Any, name: str) -> str:
    text = required_text(value, name)
    if not re.fullmatch(r"[0-9a-f]{40}", text):
        raise IntervalPolicyCodeChangeRequestError(
            f"{name} must be a lowercase 40-character Git SHA."
        )
    return text


def _branch_name(value: Any) -> str:
    name = required_text(value, "requested_branch_name")
    if not BRANCH_NAME_PATTERN.fullmatch(name):
        raise IntervalPolicyCodeChangeRequestError(
            "requested_branch_name contains unsupported characters."
        )
    if name in {"main", "master"} or name.startswith("refs/"):
        raise IntervalPolicyCodeChangeRequestError(
            "requested_branch_name must identify a separate feature branch."
        )
    return name


def _unique_texts(
    values: Iterable[Any],
    name: str,
    *,
    required: bool,
    minimum_length: int = 1,
) -> list[str]:
    if isinstance(values, (str, bytes)):
        raise IntervalPolicyCodeChangeRequestError(
            f"{name} must be a list of strings."
        )
    result = [required_text(item, name) for item in values]
    if required and not result:
        raise IntervalPolicyCodeChangeRequestError(
            f"{name} must contain at least one item."
        )
    if len(set(result)) != len(result):
        raise IntervalPolicyCodeChangeRequestError(
            f"{name} must not contain duplicates."
        )
    if any(len(item) < minimum_length for item in result):
        raise IntervalPolicyCodeChangeRequestError(
            f"Each {name} item must contain at least {minimum_length} characters."
        )
    return result


def create_interval_policy_code_change_request(
    review: Mapping[str, Any],
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
    requested_branch_name: str,
    requested_pr_title: str,
    requested_by: str,
    requester_role: str,
    request_ticket: str,
    rationale: str,
    requested_at_utc: Any | None = None,
) -> dict[str, Any]:
    """Create one immutable request without creating a branch, PR, or source edit."""
    review_document = dict(review)
    proposal_document = dict(proposal)
    disposition_document = dict(disposition)
    verify_interval_policy_implementation_dry_run_review(
        review_document,
        proposal_document,
        disposition_document,
        revision_summary,
        revision_manifest,
        revision_package,
        source_decision,
        source_sensitivity_summary,
        repository_base_commit=proposal_document["repository_base_commit"],
        repository_base_tree=proposal_document["repository_base_tree"],
        policy_source_text=policy_source_text,
    )
    if review_document.get("review_decision") != (
        "accept_for_separate_code_change_pr"
    ):
        raise IntervalPolicyCodeChangeRequestError(
            "G35a requires an accept_for_separate_code_change_pr G34 review."
        )
    if review_document.get("review_effect") != "separate_code_change_pr_required":
        raise IntervalPolicyCodeChangeRequestError(
            "The G34 review effect does not require a separate code-change PR."
        )
    if review_document.get("follow_up_human_action_required") is not True:
        raise IntervalPolicyCodeChangeRequestError(
            "The G34 review must require separate human follow-up."
        )

    current_commit = _full_sha(
        current_repository_commit, "current_repository_commit"
    )
    current_tree = _full_sha(current_repository_tree, "current_repository_tree")
    if not isinstance(policy_source_text, str) or not policy_source_text.strip():
        raise IntervalPolicyCodeChangeRequestError(
            "policy_source_text must be non-empty text."
        )
    if proposal_document.get("policy_source_path") != POLICY_SOURCE_PATH:
        raise IntervalPolicyCodeChangeRequestError(
            "The reviewed proposal uses an unsupported policy source path."
        )
    current_source_sha = source_sha256(policy_source_text)
    current_blob_sha = source_git_blob_sha1(policy_source_text)
    if current_source_sha != proposal_document.get("policy_source_sha256"):
        raise IntervalPolicyCodeChangeRequestError(
            "The current policy source no longer matches the reviewed proposal."
        )
    if current_blob_sha != proposal_document.get("policy_source_git_blob_sha1"):
        raise IntervalPolicyCodeChangeRequestError(
            "The current policy Git blob no longer matches the reviewed proposal."
        )

    intended_paths = _unique_texts(
        proposal_document.get("intended_paths", ()),
        "intended_paths",
        required=True,
        minimum_length=3,
    )
    commands = _unique_texts(
        proposal_document.get("validation_commands", ()),
        "validation_commands",
        required=True,
        minimum_length=8,
    )
    changed_fields = [
        required_text(item.get("field"), "changed_threshold_field")
        for item in proposal_document.get("active_to_proposed_changes", ())
    ]
    if not changed_fields or len(set(changed_fields)) != len(changed_fields):
        raise IntervalPolicyCodeChangeRequestError(
            "The reviewed proposal must contain unique changed thresholds."
        )

    requested_at = utc_timestamp(
        requested_at_utc or datetime.now(timezone.utc), "requested_at_utc"
    )
    reviewed_at = utc_timestamp(
        review_document.get("reviewed_at_utc"), "reviewed_at_utc"
    )
    if requested_at < reviewed_at:
        raise IntervalPolicyCodeChangeRequestError(
            "The code-change request cannot predate the G34 review."
        )

    branch_name = _branch_name(requested_branch_name)
    pr_title = required_text(
        requested_pr_title, "requested_pr_title", minimum_length=10
    )
    core = {
        "source_implementation_dry_run_review_id": review_document[
            "implementation_dry_run_review_id"
        ],
        "source_implementation_dry_run_review_sha256": review_document[
            "implementation_dry_run_review_sha256"
        ],
        "source_implementation_dry_run_id": proposal_document[
            "implementation_dry_run_id"
        ],
        "source_implementation_dry_run_sha256": proposal_document[
            "implementation_dry_run_sha256"
        ],
        "source_disposition_id": disposition_document["disposition_id"],
        "source_disposition_sha256": disposition_document["disposition_sha256"],
        "reviewed_repository_base_commit": proposal_document[
            "repository_base_commit"
        ],
        "reviewed_repository_base_tree": proposal_document[
            "repository_base_tree"
        ],
        "current_repository_commit": current_commit,
        "current_repository_tree": current_tree,
        "policy_source_path": POLICY_SOURCE_PATH,
        "current_policy_source_sha256": current_source_sha,
        "current_policy_source_git_blob_sha1": current_blob_sha,
        "active_policy_sha256": proposal_document["active_policy_sha256"],
        "proposed_policy_sha256": proposal_document["proposed_policy_sha256"],
        "revised_candidate_id": proposal_document["revised_candidate_id"],
        "revised_candidate_version": proposal_document[
            "revised_candidate_version"
        ],
        "changed_threshold_fields": changed_fields,
        "changed_threshold_count": len(changed_fields),
        "changed_threshold_digest": digest(
            proposal_document["active_to_proposed_changes"]
        ),
        "reviewed_patch_sha256": digest(proposal_document["dry_run_patch"]),
        "intended_paths": intended_paths,
        "validation_commands": commands,
        "requested_branch_name": branch_name,
        "requested_pr_title": pr_title,
        "requested_by": required_text(requested_by, "requested_by"),
        "requester_role": required_text(requester_role, "requester_role"),
        "request_ticket": required_text(request_ticket, "request_ticket"),
        "rationale": required_text(rationale, "rationale", minimum_length=30),
        "requested_at_utc": requested_at.isoformat(),
        "request_status": "ready_for_named_code_change_request_review",
        "next_action": "named_code_change_request_review_required",
        "exact_current_base_required": True,
    }
    document = {
        "code_change_request_id": "ipccr-" + digest(core)[:24],
        "code_change_request_revision": 1,
        **core,
        "human_request_confirmed": True,
        **{field: False for field in CODE_CHANGE_REQUEST_SAFETY_FIELDS},
        "code_change_request_contract_version": (
            CODE_CHANGE_REQUEST_CONTRACT_VERSION
        ),
    }
    document["code_change_request_sha256"] = digest(document)
    verify_interval_policy_code_change_request(
        document,
        review_document,
        proposal_document,
        disposition_document,
        revision_summary,
        revision_manifest,
        revision_package,
        source_decision,
        source_sensitivity_summary,
        current_repository_commit=current_commit,
        current_repository_tree=current_tree,
        policy_source_text=policy_source_text,
    )
    return document


def verify_interval_policy_code_change_request(
    request: Mapping[str, Any],
    review: Mapping[str, Any],
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
    """Verify one request against its reviewed proposal and current repository."""
    document = dict(request)
    review_document = dict(review)
    proposal_document = dict(proposal)
    disposition_document = dict(disposition)
    verify_interval_policy_implementation_dry_run_review(
        review_document,
        proposal_document,
        disposition_document,
        revision_summary,
        revision_manifest,
        revision_package,
        source_decision,
        source_sensitivity_summary,
        repository_base_commit=proposal_document["repository_base_commit"],
        repository_base_tree=proposal_document["repository_base_tree"],
        policy_source_text=policy_source_text,
    )
    if review_document.get("review_decision") != (
        "accept_for_separate_code_change_pr"
    ):
        raise IntervalPolicyCodeChangeRequestError(
            "The source G34 review does not accept a separate code-change PR."
        )
    if document.get("code_change_request_contract_version") != (
        CODE_CHANGE_REQUEST_CONTRACT_VERSION
    ):
        raise IntervalPolicyCodeChangeRequestError(
            "Code-change request contract version is invalid."
        )
    request_id = required_text(
        document.get("code_change_request_id"), "code_change_request_id"
    )
    if not CODE_CHANGE_REQUEST_ID_PATTERN.fullmatch(request_id):
        raise IntervalPolicyCodeChangeRequestError(
            "code_change_request_id is malformed."
        )
    if document.get("code_change_request_revision") != 1:
        raise IntervalPolicyCodeChangeRequestError(
            "code_change_request_revision must be 1."
        )
    expected_hash = digest(
        {
            key: value
            for key, value in document.items()
            if key != "code_change_request_sha256"
        }
    )
    if document.get("code_change_request_sha256") != expected_hash:
        raise IntervalPolicyCodeChangeRequestError(
            "Code-change request hash is invalid."
        )

    bindings = {
        "source_implementation_dry_run_review_id": review_document[
            "implementation_dry_run_review_id"
        ],
        "source_implementation_dry_run_review_sha256": review_document[
            "implementation_dry_run_review_sha256"
        ],
        "source_implementation_dry_run_id": proposal_document[
            "implementation_dry_run_id"
        ],
        "source_implementation_dry_run_sha256": proposal_document[
            "implementation_dry_run_sha256"
        ],
        "source_disposition_id": disposition_document["disposition_id"],
        "source_disposition_sha256": disposition_document["disposition_sha256"],
        "reviewed_repository_base_commit": proposal_document[
            "repository_base_commit"
        ],
        "reviewed_repository_base_tree": proposal_document[
            "repository_base_tree"
        ],
        "policy_source_path": POLICY_SOURCE_PATH,
        "active_policy_sha256": proposal_document["active_policy_sha256"],
        "proposed_policy_sha256": proposal_document["proposed_policy_sha256"],
        "revised_candidate_id": proposal_document["revised_candidate_id"],
        "revised_candidate_version": proposal_document[
            "revised_candidate_version"
        ],
        "intended_paths": proposal_document["intended_paths"],
        "validation_commands": proposal_document["validation_commands"],
    }
    for field, expected in bindings.items():
        if canonical(document.get(field)) != canonical(expected):
            raise IntervalPolicyCodeChangeRequestError(
                f"Code-change request {field} is inconsistent."
            )

    current_commit = _full_sha(
        current_repository_commit, "current_repository_commit"
    )
    current_tree = _full_sha(current_repository_tree, "current_repository_tree")
    if document.get("current_repository_commit") != current_commit:
        raise IntervalPolicyCodeChangeRequestError(
            "Current repository commit has changed."
        )
    if document.get("current_repository_tree") != current_tree:
        raise IntervalPolicyCodeChangeRequestError(
            "Current repository tree has changed."
        )
    if not isinstance(policy_source_text, str) or not policy_source_text.strip():
        raise IntervalPolicyCodeChangeRequestError(
            "policy_source_text must be non-empty text."
        )
    current_source_sha = source_sha256(policy_source_text)
    current_blob_sha = source_git_blob_sha1(policy_source_text)
    if document.get("current_policy_source_sha256") != current_source_sha:
        raise IntervalPolicyCodeChangeRequestError(
            "Current policy source SHA-256 has changed."
        )
    if document.get("current_policy_source_git_blob_sha1") != current_blob_sha:
        raise IntervalPolicyCodeChangeRequestError(
            "Current policy source Git blob has changed."
        )
    if current_source_sha != proposal_document["policy_source_sha256"]:
        raise IntervalPolicyCodeChangeRequestError(
            "Current policy source no longer matches the reviewed proposal."
        )
    if current_blob_sha != proposal_document["policy_source_git_blob_sha1"]:
        raise IntervalPolicyCodeChangeRequestError(
            "Current policy Git blob no longer matches the reviewed proposal."
        )

    expected_fields = [
        item["field"]
        for item in proposal_document["active_to_proposed_changes"]
    ]
    if document.get("changed_threshold_fields") != expected_fields:
        raise IntervalPolicyCodeChangeRequestError(
            "changed_threshold_fields are inconsistent."
        )
    if document.get("changed_threshold_count") != len(expected_fields):
        raise IntervalPolicyCodeChangeRequestError(
            "changed_threshold_count is inconsistent."
        )
    if document.get("changed_threshold_digest") != digest(
        proposal_document["active_to_proposed_changes"]
    ):
        raise IntervalPolicyCodeChangeRequestError(
            "changed_threshold_digest is invalid."
        )
    if document.get("reviewed_patch_sha256") != digest(
        proposal_document["dry_run_patch"]
    ):
        raise IntervalPolicyCodeChangeRequestError(
            "reviewed_patch_sha256 is invalid."
        )
    _branch_name(document.get("requested_branch_name"))
    required_text(
        document.get("requested_pr_title"),
        "requested_pr_title",
        minimum_length=10,
    )
    required_text(document.get("requested_by"), "requested_by")
    required_text(document.get("requester_role"), "requester_role")
    required_text(document.get("request_ticket"), "request_ticket")
    required_text(document.get("rationale"), "rationale", minimum_length=30)
    requested_at = utc_timestamp(
        document.get("requested_at_utc"), "requested_at_utc"
    )
    if requested_at < utc_timestamp(
        review_document.get("reviewed_at_utc"), "reviewed_at_utc"
    ):
        raise IntervalPolicyCodeChangeRequestError(
            "Code-change request predates the G34 review."
        )
    if document.get("request_status") != (
        "ready_for_named_code_change_request_review"
    ):
        raise IntervalPolicyCodeChangeRequestError(
            "request_status is invalid."
        )
    if document.get("next_action") != "named_code_change_request_review_required":
        raise IntervalPolicyCodeChangeRequestError(
            "next_action is invalid."
        )
    if document.get("exact_current_base_required") is not True:
        raise IntervalPolicyCodeChangeRequestError(
            "Exact-current-base evidence must be required."
        )
    if document.get("human_request_confirmed") is not True:
        raise IntervalPolicyCodeChangeRequestError(
            "Human request must be confirmed."
        )
    for field in CODE_CHANGE_REQUEST_SAFETY_FIELDS:
        if document.get(field) is not False:
            raise IntervalPolicyCodeChangeRequestError(
                f"Code-change request safety field {field} must be false."
            )


def render_interval_policy_code_change_request(
    request: Mapping[str, Any],
) -> str:
    document = dict(request)
    lines = [
        "# Interval-policy code-change request",
        "",
        f"- Request ID: `{document['code_change_request_id']}`",
        f"- Source G34 review: `{document['source_implementation_dry_run_review_id']}`",
        f"- Current repository commit: `{document['current_repository_commit']}`",
        f"- Requested branch: `{document['requested_branch_name']}`",
        f"- Requested PR title: {document['requested_pr_title']}",
        f"- Requested by: {document['requested_by']} ({document['requester_role']})",
        f"- Request ticket: `{document['request_ticket']}`",
        f"- Requested at: `{document['requested_at_utc']}`",
        "",
        "## Rationale",
        "",
        document["rationale"],
        "",
        "## Changed thresholds",
        "",
    ]
    lines.extend(
        f"- `{field}`" for field in document["changed_threshold_fields"]
    )
    lines.extend(
        [
            "",
            "This request records a human intention to prepare a separate reviewed "
            "implementation branch and pull request.",
            "It does not create that branch or PR, apply the reviewed patch, update "
            "or activate the monitoring policy, recalibrate intervals, change models "
            "or schedules, deliver alerts, deploy, or publish externally.",
            "",
        ]
    )
    return "\n".join(lines)


def write_interval_policy_code_change_request(
    output_directory: Path,
    request: Mapping[str, Any],
    review: Mapping[str, Any],
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
    document = dict(request)
    verify_interval_policy_code_change_request(
        document,
        review,
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
    request_id = document["code_change_request_id"]
    json_path = output_directory / f"interval_policy_code_change_request_{request_id}.json"
    markdown_path = output_directory / f"interval_policy_code_change_request_{request_id}.md"
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
            render_interval_policy_code_change_request(document),
            encoding="utf-8",
        )
        temporary_paths[0].replace(json_path)
        temporary_paths[1].replace(markdown_path)
    finally:
        for temporary in temporary_paths:
            temporary.unlink(missing_ok=True)
    return json_path, markdown_path


__all__ = [
    "CODE_CHANGE_REQUEST_CONTRACT_VERSION",
    "CODE_CHANGE_REQUEST_SAFETY_FIELDS",
    "IntervalPolicyCodeChangeRequestError",
    "create_interval_policy_code_change_request",
    "read_frame",
    "read_json",
    "render_interval_policy_code_change_request",
    "verify_interval_policy_code_change_request",
    "write_interval_policy_code_change_request",
]

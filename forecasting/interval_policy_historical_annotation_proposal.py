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
from forecasting.interval_policy_retained_compatibility_review import (
    REVIEW_CONTRACT_VERSION,
    REVIEW_SAFETY_FIELDS,
    IntervalPolicyRetainedCompatibilityReviewError,
    verify_retained_compatibility_review,
)


PROPOSAL_CONTRACT_VERSION = "interval-policy-historical-annotation-proposal-v1"
PROPOSAL_ID_PATTERN = re.compile(r"^iphap-[0-9a-f]{24}$")
ANNOTATION_ID_PATTERN = re.compile(r"^[a-z][a-z0-9_-]{2,63}$")
ALLOWED_SCOPES = {"compatibility_run", "scenario"}
PROPOSAL_SAFETY_FIELDS = (
    "historical_statuses_rewritten",
    "historical_annotation_applied",
    "annotation_storage_updated",
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


class IntervalPolicyHistoricalAnnotationProposalError(ValueError):
    """Raised when a G41 historical-annotation proposal is invalid or unsafe."""


def _required_text(value: Any, name: str, *, minimum_length: int = 1) -> str:
    text = "" if value is None else str(value).strip()
    if len(text) < minimum_length:
        raise IntervalPolicyHistoricalAnnotationProposalError(
            f"{name} must contain at least {minimum_length} characters."
        )
    return text


def _utc(value: Any, name: str) -> pd.Timestamp:
    try:
        return utc_timestamp(value, name)
    except Exception as exc:
        raise IntervalPolicyHistoricalAnnotationProposalError(str(exc)) from exc


def _verify_source_review(
    review: Mapping[str, Any],
    summary: pd.DataFrame,
    manifest: Mapping[str, Any],
    artifact_directory: Path,
) -> None:
    try:
        verify_retained_compatibility_review(
            review,
            summary,
            manifest,
            artifact_directory=artifact_directory,
        )
    except IntervalPolicyRetainedCompatibilityReviewError as exc:
        raise IntervalPolicyHistoricalAnnotationProposalError(str(exc)) from exc


def _prepare_annotations(
    values: Iterable[Mapping[str, Any]],
    *,
    allowed_scenarios: set[str],
) -> list[dict[str, Any]]:
    if isinstance(values, (str, bytes)) or not isinstance(values, Iterable):
        raise IntervalPolicyHistoricalAnnotationProposalError(
            "annotations must be a list of objects."
        )
    annotations: list[dict[str, Any]] = []
    seen: set[str] = set()
    allowed = {
        "annotation_id",
        "scope",
        "scenario",
        "annotation_text",
        "justification",
    }
    for index, item in enumerate(values):
        if not isinstance(item, Mapping):
            raise IntervalPolicyHistoricalAnnotationProposalError(
                "Each annotation must be a JSON object."
            )
        missing = sorted(allowed - set(item))
        unexpected = sorted(set(item) - allowed)
        if missing or unexpected:
            raise IntervalPolicyHistoricalAnnotationProposalError(
                f"annotations[{index}] fields are invalid."
            )
        annotation_id = _required_text(
            item["annotation_id"], "annotation_id"
        )
        if not ANNOTATION_ID_PATTERN.fullmatch(annotation_id):
            raise IntervalPolicyHistoricalAnnotationProposalError(
                "annotation_id must use lowercase letters, numbers, underscores, "
                "or hyphens and begin with a letter."
            )
        if annotation_id in seen:
            raise IntervalPolicyHistoricalAnnotationProposalError(
                "annotation_id values must be unique."
            )
        seen.add(annotation_id)
        scope = _required_text(item["scope"], "scope")
        if scope not in ALLOWED_SCOPES:
            raise IntervalPolicyHistoricalAnnotationProposalError(
                "annotation scope must be compatibility_run or scenario."
            )
        raw_scenario = item.get("scenario")
        if scope == "compatibility_run":
            if raw_scenario not in (None, ""):
                raise IntervalPolicyHistoricalAnnotationProposalError(
                    "A compatibility_run annotation cannot name a scenario."
                )
            scenario = None
        else:
            scenario = _required_text(raw_scenario, "scenario")
            if scenario not in allowed_scenarios:
                raise IntervalPolicyHistoricalAnnotationProposalError(
                    f"Unknown compatibility scenario: {scenario}."
                )
        annotations.append(
            {
                "annotation_id": annotation_id,
                "scope": scope,
                "scenario": scenario,
                "annotation_text": _required_text(
                    item["annotation_text"],
                    "annotation_text",
                    minimum_length=20,
                ),
                "justification": _required_text(
                    item["justification"],
                    "justification",
                    minimum_length=20,
                ),
            }
        )
    if not annotations:
        raise IntervalPolicyHistoricalAnnotationProposalError(
            "At least one historical annotation is required."
        )
    return annotations


def _prepare_action_responses(
    values: Iterable[Mapping[str, Any]],
    *,
    requested_actions: list[str],
    annotation_ids: set[str],
) -> list[dict[str, Any]]:
    if isinstance(values, (str, bytes)) or not isinstance(values, Iterable):
        raise IntervalPolicyHistoricalAnnotationProposalError(
            "requested_action_responses must be a list of objects."
        )
    responses: list[dict[str, Any]] = []
    expected_fields = {"requested_action", "response", "annotation_ids"}
    for index, item in enumerate(values):
        if not isinstance(item, Mapping):
            raise IntervalPolicyHistoricalAnnotationProposalError(
                "Each requested-action response must be a JSON object."
            )
        if set(item) != expected_fields:
            raise IntervalPolicyHistoricalAnnotationProposalError(
                f"requested_action_responses[{index}] fields are invalid."
            )
        raw_ids = item["annotation_ids"]
        if isinstance(raw_ids, (str, bytes)) or not isinstance(raw_ids, Iterable):
            raise IntervalPolicyHistoricalAnnotationProposalError(
                "annotation_ids must be a list."
            )
        ids = [_required_text(value, "annotation_ids") for value in raw_ids]
        if not ids or len(set(ids)) != len(ids):
            raise IntervalPolicyHistoricalAnnotationProposalError(
                "annotation_ids must be non-empty and unique."
            )
        unknown = sorted(set(ids) - annotation_ids)
        if unknown:
            raise IntervalPolicyHistoricalAnnotationProposalError(
                "Requested-action responses reference unknown annotations: "
                + ", ".join(unknown)
                + "."
            )
        responses.append(
            {
                "requested_action": _required_text(
                    item["requested_action"],
                    "requested_action",
                    minimum_length=10,
                ),
                "response": _required_text(
                    item["response"],
                    "response",
                    minimum_length=20,
                ),
                "annotation_ids": ids,
            }
        )
    if [item["requested_action"] for item in responses] != requested_actions:
        raise IntervalPolicyHistoricalAnnotationProposalError(
            "requested_action_responses must cover source requested actions "
            "exactly and in order."
        )
    covered = {
        annotation_id
        for item in responses
        for annotation_id in item["annotation_ids"]
    }
    if covered != annotation_ids:
        missing = sorted(annotation_ids - covered)
        raise IntervalPolicyHistoricalAnnotationProposalError(
            "Every proposed annotation must address a requested action: "
            + ", ".join(missing)
            + "."
        )
    return responses


def create_historical_annotation_proposal(
    review: Mapping[str, Any],
    summary: pd.DataFrame,
    manifest: Mapping[str, Any],
    *,
    artifact_directory: Path,
    annotations: Iterable[Mapping[str, Any]],
    requested_action_responses: Iterable[Mapping[str, Any]],
    proposed_by: str,
    proposer_role: str,
    proposal_ticket: str,
    rationale: str,
    proposed_at_utc: Any | None = None,
    proposal_id: str | None = None,
) -> dict[str, Any]:
    """Create a non-applying annotation proposal over one verified G39 review."""
    document = dict(review)
    _verify_source_review(document, summary, manifest, Path(artifact_directory))
    if document.get("decision") != "request_historical_annotation":
        raise IntervalPolicyHistoricalAnnotationProposalError(
            "G41 requires a request_historical_annotation review."
        )
    if document.get("decision_effect") != (
        "separate_historical_annotation_proposal_required"
    ):
        raise IntervalPolicyHistoricalAnnotationProposalError(
            "The source review decision effect is invalid."
        )
    if document.get("follow_up_human_action_required") is not True:
        raise IntervalPolicyHistoricalAnnotationProposalError(
            "The source review must require human follow-up."
        )
    if document.get("review_contract_version") != REVIEW_CONTRACT_VERSION:
        raise IntervalPolicyHistoricalAnnotationProposalError(
            "The source review contract is invalid."
        )
    for field in REVIEW_SAFETY_FIELDS:
        if document.get(field) is not False:
            raise IntervalPolicyHistoricalAnnotationProposalError(
                f"Source review safety field {field} must be false."
            )
    requested_actions = document.get("requested_actions")
    if not isinstance(requested_actions, list) or not requested_actions:
        raise IntervalPolicyHistoricalAnnotationProposalError(
            "The source review must retain requested actions."
        )
    requested_actions = [
        _required_text(action, "requested_actions", minimum_length=10)
        for action in requested_actions
    ]
    scenarios = document.get("scenario_evidence")
    if not isinstance(scenarios, list) or not scenarios:
        raise IntervalPolicyHistoricalAnnotationProposalError(
            "The source review must retain scenario evidence."
        )
    allowed_scenarios = {
        _required_text(item.get("scenario"), "scenario") for item in scenarios
    }
    prepared_annotations = _prepare_annotations(
        annotations,
        allowed_scenarios=allowed_scenarios,
    )
    responses = _prepare_action_responses(
        requested_action_responses,
        requested_actions=requested_actions,
        annotation_ids={
            item["annotation_id"] for item in prepared_annotations
        },
    )
    proposed_at = _utc(
        proposed_at_utc or datetime.now(timezone.utc),
        "proposed_at_utc",
    )
    reviewed_at = _utc(document.get("reviewed_at_utc"), "reviewed_at_utc")
    if proposed_at < reviewed_at:
        raise IntervalPolicyHistoricalAnnotationProposalError(
            "The annotation proposal cannot precede the source review."
        )
    identifier = proposal_id or "iphap-" + uuid4().hex[:24]
    if not PROPOSAL_ID_PATTERN.fullmatch(identifier):
        raise IntervalPolicyHistoricalAnnotationProposalError(
            "proposal_id must be iphap- plus 24 lowercase hexadecimal characters."
        )

    proposal: dict[str, Any] = {
        "proposal_id": identifier,
        "proposal_revision": 1,
        "source_review_id": _required_text(
            document.get("review_id"), "source_review_id"
        ),
        "source_review_sha256": _required_text(
            document.get("review_sha256"), "source_review_sha256"
        ),
        "compatibility_run_id": _required_text(
            document.get("compatibility_run_id"), "compatibility_run_id"
        ),
        "trend_run_id": _required_text(
            document.get("trend_run_id"), "trend_run_id"
        ),
        "compatibility_summary_sha256": _required_text(
            document.get("compatibility_summary_sha256"),
            "compatibility_summary_sha256",
        ),
        "compatibility_manifest_sha256": _required_text(
            document.get("compatibility_manifest_sha256"),
            "compatibility_manifest_sha256",
        ),
        "previous_policy_id": document.get("previous_policy_id"),
        "previous_shortfall_threshold_pct_points": document.get(
            "previous_shortfall_threshold_pct_points"
        ),
        "current_policy_id": document.get("current_policy_id"),
        "current_shortfall_threshold_pct_points": document.get(
            "current_shortfall_threshold_pct_points"
        ),
        "requested_actions": requested_actions,
        "requested_action_responses": responses,
        "annotations": prepared_annotations,
        "annotation_count": len(prepared_annotations),
        "proposed_by": _required_text(proposed_by, "proposed_by"),
        "proposer_role": _required_text(proposer_role, "proposer_role"),
        "proposal_ticket": _required_text(proposal_ticket, "proposal_ticket"),
        "rationale": _required_text(
            rationale, "rationale", minimum_length=20
        ),
        "proposed_at_utc": proposed_at,
        "named_human_proposal_confirmed": True,
        "follow_up_human_review_required": True,
        "next_action": "named_historical_annotation_proposal_review_required",
        **{field: False for field in PROPOSAL_SAFETY_FIELDS},
        "proposal_contract_version": PROPOSAL_CONTRACT_VERSION,
    }
    proposal["proposal_sha256"] = digest(proposal)
    return canonical(proposal)


def verify_historical_annotation_proposal(
    proposal: Mapping[str, Any],
    review: Mapping[str, Any],
    summary: pd.DataFrame,
    manifest: Mapping[str, Any],
    *,
    artifact_directory: Path,
) -> None:
    """Verify proposal integrity, review binding, and no-application boundary."""
    document = dict(proposal)
    source = dict(review)
    _verify_source_review(source, summary, manifest, Path(artifact_directory))
    expected_hash = digest(
        {
            key: value
            for key, value in document.items()
            if key != "proposal_sha256"
        }
    )
    if document.get("proposal_sha256") != expected_hash:
        raise IntervalPolicyHistoricalAnnotationProposalError(
            "Historical annotation proposal hash is invalid."
        )
    if not PROPOSAL_ID_PATTERN.fullmatch(str(document.get("proposal_id", ""))):
        raise IntervalPolicyHistoricalAnnotationProposalError(
            "Historical annotation proposal ID is invalid."
        )
    if document.get("proposal_revision") != 1:
        raise IntervalPolicyHistoricalAnnotationProposalError(
            "Historical annotation proposal revision is invalid."
        )
    if document.get("proposal_contract_version") != PROPOSAL_CONTRACT_VERSION:
        raise IntervalPolicyHistoricalAnnotationProposalError(
            "Historical annotation proposal contract is invalid."
        )
    if source.get("decision") != "request_historical_annotation":
        raise IntervalPolicyHistoricalAnnotationProposalError(
            "G41 requires a request_historical_annotation review."
        )
    bindings = {
        "source_review_id": source.get("review_id"),
        "source_review_sha256": source.get("review_sha256"),
        "compatibility_run_id": source.get("compatibility_run_id"),
        "trend_run_id": source.get("trend_run_id"),
        "compatibility_summary_sha256": source.get(
            "compatibility_summary_sha256"
        ),
        "compatibility_manifest_sha256": source.get(
            "compatibility_manifest_sha256"
        ),
        "previous_policy_id": source.get("previous_policy_id"),
        "previous_shortfall_threshold_pct_points": source.get(
            "previous_shortfall_threshold_pct_points"
        ),
        "current_policy_id": source.get("current_policy_id"),
        "current_shortfall_threshold_pct_points": source.get(
            "current_shortfall_threshold_pct_points"
        ),
        "requested_actions": source.get("requested_actions"),
    }
    for field, expected in bindings.items():
        if canonical(document.get(field)) != canonical(expected):
            raise IntervalPolicyHistoricalAnnotationProposalError(
                f"Historical annotation proposal {field} binding is invalid."
            )
    scenarios = {
        _required_text(item.get("scenario"), "scenario")
        for item in source.get("scenario_evidence", [])
    }
    annotations = _prepare_annotations(
        document.get("annotations", ()),
        allowed_scenarios=scenarios,
    )
    responses = _prepare_action_responses(
        document.get("requested_action_responses", ()),
        requested_actions=list(source["requested_actions"]),
        annotation_ids={item["annotation_id"] for item in annotations},
    )
    if canonical(annotations) != canonical(document.get("annotations")):
        raise IntervalPolicyHistoricalAnnotationProposalError(
            "Historical annotation proposal annotations are invalid."
        )
    if canonical(responses) != canonical(
        document.get("requested_action_responses")
    ):
        raise IntervalPolicyHistoricalAnnotationProposalError(
            "Historical annotation proposal action responses are invalid."
        )
    if int(document.get("annotation_count", -1)) != len(annotations):
        raise IntervalPolicyHistoricalAnnotationProposalError(
            "Historical annotation proposal count is invalid."
        )
    if _utc(document.get("proposed_at_utc"), "proposed_at_utc") < _utc(
        source.get("reviewed_at_utc"), "reviewed_at_utc"
    ):
        raise IntervalPolicyHistoricalAnnotationProposalError(
            "The annotation proposal cannot precede the source review."
        )
    for field in (
        "proposed_by",
        "proposer_role",
        "proposal_ticket",
    ):
        _required_text(document.get(field), field)
    _required_text(document.get("rationale"), "rationale", minimum_length=20)
    if document.get("named_human_proposal_confirmed") is not True:
        raise IntervalPolicyHistoricalAnnotationProposalError(
            "Named human proposal confirmation is required."
        )
    if document.get("follow_up_human_review_required") is not True:
        raise IntervalPolicyHistoricalAnnotationProposalError(
            "A separate human review must remain required."
        )
    if document.get("next_action") != (
        "named_historical_annotation_proposal_review_required"
    ):
        raise IntervalPolicyHistoricalAnnotationProposalError(
            "Historical annotation proposal next action is invalid."
        )
    for field in PROPOSAL_SAFETY_FIELDS:
        if document.get(field) is not False:
            raise IntervalPolicyHistoricalAnnotationProposalError(
                f"Historical annotation proposal safety field {field} must be false."
            )


def _render_proposal(proposal: Mapping[str, Any]) -> str:
    lines = [
        "# Historical interval-policy annotation proposal",
        "",
        f"- Proposal ID: `{proposal['proposal_id']}`",
        f"- Source review: `{proposal['source_review_id']}`",
        f"- Compatibility run: `{proposal['compatibility_run_id']}`",
        f"- Proposed by: {proposal['proposed_by']} ({proposal['proposer_role']})",
        f"- Ticket: `{proposal['proposal_ticket']}`",
        f"- Proposed at: `{proposal['proposed_at_utc']}`",
        f"- Annotation count: {proposal['annotation_count']}",
        "",
        proposal["rationale"],
        "",
        "| Annotation | Scope | Scenario | Text |",
        "| --- | --- | --- | --- |",
    ]
    for item in proposal["annotations"]:
        lines.append(
            f"| {item['annotation_id']} | {item['scope']} | "
            f"{item['scenario'] or '-'} | {item['annotation_text']} |"
        )
    lines += ["", "## Requested-action responses", ""]
    for item in proposal["requested_action_responses"]:
        lines.append(f"- **{item['requested_action']}** — {item['response']}")
    lines += [
        "",
        "This proposal does not apply annotations or rewrite historical statuses. "
        "It requires a separate named review before any annotation-storage change "
        "could be considered.",
        "",
    ]
    return "\n".join(lines)


def write_historical_annotation_proposal(
    output_directory: Path,
    proposal: Mapping[str, Any],
    review: Mapping[str, Any],
    summary: pd.DataFrame,
    manifest: Mapping[str, Any],
    *,
    artifact_directory: Path,
) -> dict[str, Path]:
    verify_historical_annotation_proposal(
        proposal,
        review,
        summary,
        manifest,
        artifact_directory=artifact_directory,
    )
    directory = Path(output_directory)
    directory.mkdir(parents=True, exist_ok=True)
    identifier = proposal["proposal_id"]
    outputs = {
        "json": directory
        / f"interval_policy_historical_annotation_proposal_{identifier}.json",
        "markdown": directory
        / f"interval_policy_historical_annotation_proposal_{identifier}.md",
    }
    temporary = {
        key: path.with_name(f".{path.name}.tmp") for key, path in outputs.items()
    }
    for path in (*outputs.values(), *temporary.values()):
        if path.exists():
            raise FileExistsError(f"Refusing to overwrite {path}.")
    try:
        temporary["json"].write_text(
            json.dumps(canonical(proposal), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        temporary["markdown"].write_text(
            _render_proposal(proposal),
            encoding="utf-8",
        )
        temporary["json"].replace(outputs["json"])
        temporary["markdown"].replace(outputs["markdown"])
    finally:
        for path in temporary.values():
            path.unlink(missing_ok=True)
    return outputs


__all__ = [
    "ANNOTATION_ID_PATTERN",
    "PROPOSAL_CONTRACT_VERSION",
    "PROPOSAL_SAFETY_FIELDS",
    "IntervalPolicyHistoricalAnnotationProposalError",
    "create_historical_annotation_proposal",
    "verify_historical_annotation_proposal",
    "write_historical_annotation_proposal",
]

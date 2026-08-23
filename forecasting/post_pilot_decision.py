from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from forecasting.fabric_pilot_receipt import (
    ELIGIBLE_OUTCOME,
    FabricPilotReceiptError,
    verify_fabric_pilot_run_assessment,
    verify_fabric_pilot_run_receipt,
)
from forecasting.model_registry import (
    ModelCandidateRegistryError,
    verify_manifest,
)


DECISION_CONTRACT_VERSION = "fabric-post-pilot-human-decision-v1"
DECISION_ID_PATTERN = re.compile(r"^fpd-[0-9a-f]{24}$")
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
ALLOWED_DECISIONS = {
    "continue_evidence_collection",
    "revise_candidate",
    "retire_candidate",
}
DECISION_EFFECTS = {
    "continue_evidence_collection": "new_pilot_plan_required",
    "revise_candidate": "new_candidate_required",
    "retire_candidate": "registry_retirement_required",
}


class PostPilotDecisionError(ValueError):
    """Raised when a post-pilot human decision is unsafe or inconsistent."""


def _required_text(value: Any, name: str) -> str:
    if value is None:
        raise PostPilotDecisionError(f"{name} must be non-empty.")
    text = str(value).strip()
    if not text:
        raise PostPilotDecisionError(f"{name} must be non-empty.")
    return text


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    text = str(value).strip()
    return text or None


def _unique_texts(values: Iterable[Any], name: str, *, allow_empty: bool) -> list[str]:
    if isinstance(values, (str, bytes)) or not isinstance(values, Iterable):
        raise PostPilotDecisionError(f"{name} must be a list of strings.")
    result = [_required_text(value, name) for value in values]
    if not allow_empty and not result:
        raise PostPilotDecisionError(f"{name} must contain at least one item.")
    if len(set(result)) != len(result):
        raise PostPilotDecisionError(f"{name} must not contain duplicates.")
    return result


def _utc_timestamp(value: Any, name: str) -> pd.Timestamp:
    try:
        timestamp = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise PostPilotDecisionError(
            f"{name} must be a valid timezone-aware timestamp."
        ) from exc
    if pd.isna(timestamp) or timestamp.tzinfo is None:
        raise PostPilotDecisionError(f"{name} must be timezone-aware.")
    return timestamp.tz_convert("UTC")


def _canonical(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _canonical(value[key]) for key in sorted(value)}
    if isinstance(value, (list, tuple)):
        return [_canonical(item) for item in value]
    if isinstance(value, (datetime, pd.Timestamp)):
        return _utc_timestamp(value, "timestamp").isoformat()
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


def _digest(document: dict[str, Any], *, exclude: Iterable[str] = ()) -> str:
    excluded = set(exclude)
    material = {key: value for key, value in document.items() if key not in excluded}
    encoded = json.dumps(
        _canonical(material),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _verify_chain(
    candidate_manifest: dict[str, Any],
    receipt: dict[str, Any],
    assessment: dict[str, Any],
) -> None:
    try:
        verify_manifest(candidate_manifest)
        verify_fabric_pilot_run_receipt(receipt)
        verify_fabric_pilot_run_assessment(assessment)
    except (
        ModelCandidateRegistryError,
        FabricPilotReceiptError,
    ) as exc:
        raise PostPilotDecisionError(f"Post-pilot evidence failed verification: {exc}") from exc

    if candidate_manifest.get("candidate_state") != "approved":
        raise PostPilotDecisionError(
            "Post-pilot decisions require the exact approved candidate history."
        )
    if receipt.get("candidate_id") != candidate_manifest.get("candidate_id"):
        raise PostPilotDecisionError(
            "Pilot receipt candidate_id does not match the approved candidate."
        )
    bindings = (
        ("pilot_id", assessment.get("pilot_id"), receipt.get("pilot_id")),
        ("receipt_id", assessment.get("receipt_id"), receipt.get("receipt_id")),
        ("receipt_hash", assessment.get("receipt_hash"), receipt.get("receipt_hash")),
        (
            "external_run_id",
            assessment.get("external_run_id"),
            receipt.get("external_run_id"),
        ),
        ("run_status", assessment.get("run_status"), receipt.get("run_status")),
    )
    for name, assessed, recorded in bindings:
        if assessed != recorded:
            raise PostPilotDecisionError(
                f"Pilot assessment {name} does not match the receipt."
            )
    if assessment.get("post_pilot_human_decision_required") is not True:
        raise PostPilotDecisionError(
            "Pilot assessment must explicitly require a human post-pilot decision."
        )
    if assessment.get("automatic_model_activation_allowed") is not False:
        raise PostPilotDecisionError(
            "Pilot assessment must prohibit automatic model activation."
        )
    if assessment.get("deployment_authorized") is not False:
        raise PostPilotDecisionError(
            "Pilot assessment cannot authorize deployment."
        )


def create_post_pilot_decision(
    candidate_manifest: dict[str, Any],
    receipt: dict[str, Any],
    assessment: dict[str, Any],
    *,
    decision: str,
    decided_by: str,
    decision_role: str,
    review_ticket: str,
    reason: str,
    follow_up_actions: Iterable[Any],
    revision_requirements: Iterable[Any] = (),
    retirement_reason: str | None = None,
    decided_at_utc: Any | None = None,
) -> dict[str, Any]:
    """Create one immutable human disposition without performing its next action."""
    _verify_chain(candidate_manifest, receipt, assessment)
    decision = _required_text(decision, "decision")
    if decision not in ALLOWED_DECISIONS:
        raise PostPilotDecisionError(
            "decision must be continue_evidence_collection, revise_candidate, "
            "or retire_candidate."
        )
    actor = _required_text(decided_by, "decided_by")
    role = _required_text(decision_role, "decision_role")
    ticket = _required_text(review_ticket, "review_ticket")
    rationale = _required_text(reason, "reason")
    actions = _unique_texts(follow_up_actions, "follow_up_actions", allow_empty=False)
    revisions = _unique_texts(
        revision_requirements, "revision_requirements", allow_empty=True
    )
    retirement = _optional_text(retirement_reason)

    if decision == "continue_evidence_collection":
        eligible = (
            assessment.get("assessment_outcome") == ELIGIBLE_OUTCOME
            and assessment.get("run_status") == "completed"
            and int(assessment.get("failed_check_count", -1)) == 0
            and assessment.get("rollback_required") is False
            and assessment.get("active_model_unchanged") is True
        )
        if not eligible:
            raise PostPilotDecisionError(
                "continue_evidence_collection requires a fully eligible completed "
                "pilot assessment with the control model unchanged."
            )
        if revisions or retirement is not None:
            raise PostPilotDecisionError(
                "Continuation cannot include revision requirements or a retirement reason."
            )
    elif decision == "revise_candidate":
        if not revisions:
            raise PostPilotDecisionError(
                "revise_candidate requires at least one revision requirement."
            )
        if retirement is not None:
            raise PostPilotDecisionError(
                "revise_candidate cannot include a retirement reason."
            )
    else:
        if retirement is None:
            raise PostPilotDecisionError(
                "retire_candidate requires a non-empty retirement reason."
            )
        if revisions:
            raise PostPilotDecisionError(
                "retire_candidate cannot include revision requirements."
            )

    decided_at = _utc_timestamp(
        decided_at_utc or datetime.now(timezone.utc), "decided_at_utc"
    )
    assessment_time = _utc_timestamp(
        assessment.get("assessed_at_utc"), "assessed_at_utc"
    )
    candidate_time = _utc_timestamp(
        candidate_manifest.get("updated_at_utc"), "candidate updated_at_utc"
    )
    if decided_at < max(assessment_time, candidate_time):
        raise PostPilotDecisionError(
            "decided_at_utc cannot precede the pilot assessment or candidate approval."
        )

    core = {
        "pilot_id": receipt["pilot_id"],
        "candidate_id": candidate_manifest["candidate_id"],
        "candidate_revision": int(candidate_manifest["candidate_revision"]),
        "candidate_manifest_hash": candidate_manifest["manifest_hash"],
        "candidate_state_at_decision": candidate_manifest["candidate_state"],
        "receipt_id": receipt["receipt_id"],
        "receipt_hash": receipt["receipt_hash"],
        "assessment_id": assessment["assessment_id"],
        "assessment_hash": assessment["assessment_hash"],
        "external_run_id": receipt["external_run_id"],
        "assessment_outcome": assessment["assessment_outcome"],
        "run_status": assessment["run_status"],
        "decision": decision,
        "decision_effect": DECISION_EFFECTS[decision],
        "decided_by": actor,
        "decision_role": role,
        "review_ticket": ticket,
        "decision_reason": rationale,
        "follow_up_actions": actions,
        "revision_requirements": revisions,
        "retirement_reason": retirement,
        "decided_at_utc": decided_at.isoformat(),
    }
    result = {
        "decision_id": "fpd-" + _digest(core)[:24],
        "decision_revision": 1,
        **core,
        "human_decision_confirmed": True,
        "automatic_decision_used": False,
        "registry_mutation_performed": False,
        "new_pilot_authorized": False,
        "schedule_activation_allowed": False,
        "deployment_authorized": False,
        "model_activation_authorized": False,
        "automatic_model_activation_allowed": False,
        "active_model_unchanged": True,
        "source_evidence_mutated": False,
        "follow_up_human_action_required": True,
        "decision_contract_version": DECISION_CONTRACT_VERSION,
    }
    result["decision_hash"] = _digest(result, exclude=("decision_hash",))
    verify_post_pilot_decision(result)
    return result


def verify_post_pilot_decision(decision: dict[str, Any]) -> None:
    """Verify one standalone immutable post-pilot decision document."""
    if not isinstance(decision, dict):
        raise PostPilotDecisionError("Post-pilot decision must be a JSON object.")
    if decision.get("decision_contract_version") != DECISION_CONTRACT_VERSION:
        raise PostPilotDecisionError("Unsupported post-pilot decision contract.")
    if decision.get("decision_hash") != _digest(
        decision, exclude=("decision_hash",)
    ):
        raise PostPilotDecisionError("Post-pilot decision hash is invalid.")
    if not DECISION_ID_PATTERN.fullmatch(
        _required_text(decision.get("decision_id"), "decision_id")
    ):
        raise PostPilotDecisionError("Post-pilot decision ID is malformed.")
    if decision.get("decision_revision") != 1:
        raise PostPilotDecisionError("Post-pilot decision revision must be 1.")
    outcome = decision.get("decision")
    if outcome not in ALLOWED_DECISIONS:
        raise PostPilotDecisionError("Post-pilot decision outcome is invalid.")
    if decision.get("decision_effect") != DECISION_EFFECTS[outcome]:
        raise PostPilotDecisionError("Post-pilot decision effect is inconsistent.")
    if decision.get("candidate_state_at_decision") != "approved":
        raise PostPilotDecisionError("Candidate state at decision must be approved.")
    for field in ("candidate_manifest_hash", "receipt_hash", "assessment_hash", "decision_hash"):
        if not SHA256_PATTERN.fullmatch(
            _required_text(decision.get(field), field)
        ):
            raise PostPilotDecisionError(f"{field} is malformed.")
    _utc_timestamp(decision.get("decided_at_utc"), "decided_at_utc")
    _required_text(decision.get("decided_by"), "decided_by")
    _required_text(decision.get("decision_role"), "decision_role")
    _required_text(decision.get("review_ticket"), "review_ticket")
    _required_text(decision.get("decision_reason"), "decision_reason")
    actions = _unique_texts(
        decision.get("follow_up_actions", ()),
        "follow_up_actions",
        allow_empty=False,
    )
    revisions = _unique_texts(
        decision.get("revision_requirements", ()),
        "revision_requirements",
        allow_empty=True,
    )
    retirement = _optional_text(decision.get("retirement_reason"))
    if not actions:
        raise PostPilotDecisionError("Post-pilot follow-up actions are required.")
    if outcome == "continue_evidence_collection":
        if decision.get("assessment_outcome") != ELIGIBLE_OUTCOME:
            raise PostPilotDecisionError(
                "Continuation requires an eligible post-pilot assessment."
            )
        if decision.get("run_status") != "completed" or revisions or retirement is not None:
            raise PostPilotDecisionError(
                "Continuation decision fields are inconsistent."
            )
    elif outcome == "revise_candidate":
        if not revisions or retirement is not None:
            raise PostPilotDecisionError("Revision decision fields are inconsistent.")
    elif retirement is None or revisions:
        raise PostPilotDecisionError("Retirement decision fields are inconsistent.")

    safety = {
        "human_decision_confirmed": True,
        "automatic_decision_used": False,
        "registry_mutation_performed": False,
        "new_pilot_authorized": False,
        "schedule_activation_allowed": False,
        "deployment_authorized": False,
        "model_activation_authorized": False,
        "automatic_model_activation_allowed": False,
        "active_model_unchanged": True,
        "source_evidence_mutated": False,
        "follow_up_human_action_required": True,
    }
    for field, expected in safety.items():
        if decision.get(field) is not expected:
            raise PostPilotDecisionError(
                f"Post-pilot safety flag {field} is invalid."
            )


def write_post_pilot_decision(
    output_directory: Path,
    decision: dict[str, Any],
) -> Path:
    """Write one assessment-bound decision without overwriting prior evidence."""
    verify_post_pilot_decision(decision)
    output_directory = Path(output_directory)
    output_directory.mkdir(parents=True, exist_ok=True)
    path = output_directory / f"post_pilot_decision_{decision['assessment_id']}.json"
    temporary = path.with_suffix(".tmp")
    for candidate in (path, temporary):
        if candidate.exists():
            raise FileExistsError(f"Refusing to overwrite {candidate}.")
    try:
        temporary.write_text(
            json.dumps(_canonical(decision), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)
    return path

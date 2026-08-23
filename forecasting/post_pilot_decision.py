from __future__ import annotations

import hashlib
import json
import re
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from forecasting.fabric_pilot import FabricPilotError, verify_fabric_pilot_plan
from forecasting.fabric_pilot_authorization import (
    FabricPilotAuthorizationError,
    verify_fabric_pilot_authorization,
)
from forecasting.fabric_pilot_receipt import (
    FabricPilotReceiptError,
    verify_fabric_pilot_run_assessment,
    verify_fabric_pilot_run_receipt,
)


DECISION_CONTRACT_VERSION = "post-pilot-human-decision-v1"
DECISION_ID_PATTERN = re.compile(r"^fpd-[0-9a-f]{24}$")
AUTOMATION_IDENTITY_PATTERN = re.compile(
    r"(^|[-_.])(bot|automation|github-actions|ci|runner|service-account)([-_.]|$)",
    re.IGNORECASE,
)
ALLOWED_DECISIONS = {
    "continue_evidence_collection",
    "revise_candidate",
    "retire_candidate",
}
ELIGIBLE_ASSESSMENT = "eligible_for_post_pilot_review"
FAILED_ASSESSMENT = "pilot_failed"
DECISION_EFFECTS = {
    "continue_evidence_collection": "request_new_pilot_cycle",
    "revise_candidate": "request_candidate_revision",
    "retire_candidate": "request_candidate_retirement",
}


class PostPilotDecisionError(ValueError):
    """Raised when a post-pilot decision is unsafe or inconsistent."""


def _required_text(value: Any, name: str) -> str:
    if value is None:
        raise PostPilotDecisionError(f"{name} must be non-empty.")
    text = str(value).strip()
    if not text:
        raise PostPilotDecisionError(f"{name} must be non-empty.")
    return text


def _human_identity(value: Any, name: str) -> str:
    identity = _required_text(value, name)
    if AUTOMATION_IDENTITY_PATTERN.search(identity):
        raise PostPilotDecisionError(
            f"{name} must identify a human reviewer, not automation."
        )
    return identity


def _utc_iso(value: Any, name: str) -> str:
    try:
        timestamp = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise PostPilotDecisionError(
            f"{name} must be a valid timezone-aware timestamp."
        ) from exc
    if pd.isna(timestamp) or timestamp.tzinfo is None:
        raise PostPilotDecisionError(f"{name} must be timezone-aware.")
    return timestamp.tz_convert("UTC").isoformat()


def _canonical(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _canonical(value[key]) for key in sorted(value)}
    if isinstance(value, (list, tuple)):
        return [_canonical(item) for item in value]
    if isinstance(value, (datetime, pd.Timestamp)):
        return _utc_iso(value, "timestamp")
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
    material = {
        key: value for key, value in document.items() if key not in excluded
    }
    encoded = json.dumps(
        _canonical(material),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _unique_action_items(values: Iterable[Any]) -> list[str]:
    items = [_required_text(value, "action_item") for value in values]
    if not items:
        raise PostPilotDecisionError(
            "action_items must contain at least one item."
        )
    if len(set(items)) != len(items):
        raise PostPilotDecisionError("action_items must not contain duplicates.")
    return items


def _verify_chain(
    plan: dict[str, Any],
    authorization: dict[str, Any],
    receipt: dict[str, Any],
    assessment: dict[str, Any],
) -> None:
    try:
        verify_fabric_pilot_plan(plan)
    except FabricPilotError as exc:
        raise PostPilotDecisionError(
            f"Pilot plan failed verification: {exc}"
        ) from exc
    try:
        verify_fabric_pilot_authorization(authorization)
    except FabricPilotAuthorizationError as exc:
        raise PostPilotDecisionError(
            f"Pilot authorization failed verification: {exc}"
        ) from exc
    try:
        verify_fabric_pilot_run_receipt(receipt)
        verify_fabric_pilot_run_assessment(assessment)
    except FabricPilotReceiptError as exc:
        raise PostPilotDecisionError(
            f"Pilot receipt or assessment failed verification: {exc}"
        ) from exc

    if receipt.get("pilot_id") != plan.get("pilot_id") or receipt.get(
        "plan_hash"
    ) != plan.get("plan_hash"):
        raise PostPilotDecisionError("Receipt does not match the pilot plan.")
    if authorization.get("pilot_id") != plan.get("pilot_id") or authorization.get(
        "plan_hash"
    ) != plan.get("plan_hash"):
        raise PostPilotDecisionError(
            "Authorization does not match the pilot plan."
        )
    if receipt.get("authorization_id") != authorization.get(
        "authorization_id"
    ) or receipt.get("authorization_hash") != authorization.get(
        "authorization_hash"
    ):
        raise PostPilotDecisionError(
            "Receipt does not match the pilot authorization."
        )
    if assessment.get("pilot_id") != plan.get("pilot_id"):
        raise PostPilotDecisionError("Assessment does not match the pilot plan.")
    if assessment.get("receipt_id") != receipt.get(
        "receipt_id"
    ) or assessment.get("receipt_hash") != receipt.get("receipt_hash"):
        raise PostPilotDecisionError("Assessment does not match the run receipt.")
    if assessment.get("authorization_id") != authorization.get(
        "authorization_id"
    ):
        raise PostPilotDecisionError(
            "Assessment does not match the pilot authorization."
        )
    if assessment.get("external_run_id") != receipt.get("external_run_id"):
        raise PostPilotDecisionError(
            "Assessment does not match the external pilot run."
        )


def _decision_flags(decision: str) -> dict[str, Any]:
    return {
        "decision_effect": DECISION_EFFECTS[decision],
        "new_pilot_cycle_requested": decision
        == "continue_evidence_collection",
        "candidate_revision_requested": decision == "revise_candidate",
        "candidate_retirement_requested": decision == "retire_candidate",
    }


def create_post_pilot_decision(
    plan: dict[str, Any],
    authorization: dict[str, Any],
    receipt: dict[str, Any],
    assessment: dict[str, Any],
    *,
    confirm_pilot_id: str,
    confirm_receipt_id: str,
    confirm_assessment_id: str,
    decision: str,
    decision_maker: str,
    review_ticket: str,
    reason: str,
    action_items: Iterable[Any],
    decided_at_utc: Any | None = None,
) -> dict[str, Any]:
    """Record one immutable human decision without implementing it."""
    _verify_chain(plan, authorization, receipt, assessment)
    if _required_text(confirm_pilot_id, "confirm_pilot_id") != plan["pilot_id"]:
        raise PostPilotDecisionError(
            "confirm_pilot_id must exactly match the pilot plan."
        )
    if _required_text(confirm_receipt_id, "confirm_receipt_id") != receipt[
        "receipt_id"
    ]:
        raise PostPilotDecisionError(
            "confirm_receipt_id must exactly match the run receipt."
        )
    if _required_text(confirm_assessment_id, "confirm_assessment_id") != assessment[
        "assessment_id"
    ]:
        raise PostPilotDecisionError(
            "confirm_assessment_id must exactly match the run assessment."
        )

    normalized_decision = _required_text(decision, "decision").lower()
    if normalized_decision not in ALLOWED_DECISIONS:
        raise PostPilotDecisionError(
            "decision must be one of: "
            + ", ".join(sorted(ALLOWED_DECISIONS))
            + "."
        )
    assessment_outcome = _required_text(
        assessment.get("assessment_outcome"), "assessment_outcome"
    )
    if assessment_outcome not in {ELIGIBLE_ASSESSMENT, FAILED_ASSESSMENT}:
        raise PostPilotDecisionError("Assessment outcome is unsupported.")
    if (
        normalized_decision == "continue_evidence_collection"
        and assessment_outcome != ELIGIBLE_ASSESSMENT
    ):
        raise PostPilotDecisionError(
            "A failed pilot cannot continue unchanged; revise or retire the candidate."
        )

    human = _human_identity(decision_maker, "decision_maker")
    ticket = _required_text(review_ticket, "review_ticket")
    rationale = _required_text(reason, "reason")
    if len(rationale) < 20:
        raise PostPilotDecisionError("reason must contain at least 20 characters.")
    actions = _unique_action_items(action_items)
    decided_at = _utc_iso(
        decided_at_utc or datetime.now(timezone.utc), "decided_at_utc"
    )
    recorded_at = pd.Timestamp(decided_at)
    assessed_at = pd.Timestamp(assessment.get("assessed_at_utc"))
    if assessed_at.tzinfo is None:
        raise PostPilotDecisionError(
            "assessment assessed_at_utc must be timezone-aware."
        )
    if recorded_at < assessed_at.tz_convert("UTC"):
        raise PostPilotDecisionError(
            "decided_at_utc cannot precede the run assessment."
        )

    core = {
        "pilot_id": plan["pilot_id"],
        "plan_hash": plan["plan_hash"],
        "authorization_id": authorization["authorization_id"],
        "authorization_hash": authorization["authorization_hash"],
        "receipt_id": receipt["receipt_id"],
        "receipt_hash": receipt["receipt_hash"],
        "assessment_id": assessment["assessment_id"],
        "assessment_hash": assessment["assessment_hash"],
        "candidate_id": plan["candidate_id"],
        "candidate_version": plan["candidate_version"],
        "external_run_id": receipt["external_run_id"],
        "assessment_outcome": assessment_outcome,
        "run_status": receipt["run_status"],
        "decision": normalized_decision,
        "decision_maker": human,
        "review_ticket": ticket,
        "reason": rationale,
        "action_items": actions,
        "decided_at_utc": decided_at,
    }
    decision_id = "fpd-" + _digest(core)[:24]
    record = {
        "decision_id": decision_id,
        "decision_revision": 1,
        "decision_state": "recorded",
        **core,
        **_decision_flags(normalized_decision),
        "human_decision_recorded": True,
        "automatic_decision_allowed": False,
        "model_registry_mutation_allowed": False,
        "authorization_reuse_allowed": False,
        "pilot_reexecution_authorized": False,
        "schedule_activation_allowed": False,
        "deployment_authorized": False,
        "model_activation_authorized": False,
        "active_model_before": "ridge_weather_lag",
        "active_model_expected_after": "ridge_weather_lag",
        "active_model_unchanged": True,
        "source_evidence_mutated": False,
        "decision_contract_version": DECISION_CONTRACT_VERSION,
    }
    record["decision_hash"] = _digest(record, exclude=("decision_hash",))
    verify_post_pilot_decision(record)
    return record


def verify_post_pilot_decision(record: dict[str, Any]) -> None:
    """Verify the record hash, decision mapping, and non-mutating boundary."""
    if not isinstance(record, dict):
        raise PostPilotDecisionError("Decision record must be a JSON object.")
    if record.get("decision_contract_version") != DECISION_CONTRACT_VERSION:
        raise PostPilotDecisionError("Unsupported post-pilot decision contract.")
    if record.get("decision_hash") != _digest(
        record, exclude=("decision_hash",)
    ):
        raise PostPilotDecisionError("Post-pilot decision hash is invalid.")
    if not DECISION_ID_PATTERN.fullmatch(
        _required_text(record.get("decision_id"), "decision_id")
    ):
        raise PostPilotDecisionError("Decision ID is malformed.")
    if record.get("decision_revision") != 1 or record.get(
        "decision_state"
    ) != "recorded":
        raise PostPilotDecisionError("Decision revision or state is invalid.")

    decision = _required_text(record.get("decision"), "decision")
    if decision not in ALLOWED_DECISIONS:
        raise PostPilotDecisionError("Decision value is unsupported.")
    outcome = _required_text(
        record.get("assessment_outcome"), "assessment_outcome"
    )
    if outcome not in {ELIGIBLE_ASSESSMENT, FAILED_ASSESSMENT}:
        raise PostPilotDecisionError("Assessment outcome is unsupported.")
    if decision == "continue_evidence_collection" and outcome != ELIGIBLE_ASSESSMENT:
        raise PostPilotDecisionError(
            "A failed pilot cannot request an unchanged continuation."
        )

    expected_flags = _decision_flags(decision)
    for field, expected in expected_flags.items():
        if record.get(field) != expected:
            raise PostPilotDecisionError(
                f"Decision mapping field {field} is invalid."
            )
    _human_identity(record.get("decision_maker"), "decision_maker")
    _required_text(record.get("review_ticket"), "review_ticket")
    reason = _required_text(record.get("reason"), "reason")
    if len(reason) < 20:
        raise PostPilotDecisionError("reason must contain at least 20 characters.")
    action_items = record.get("action_items")
    if not isinstance(action_items, list):
        raise PostPilotDecisionError("action_items must be a list.")
    _unique_action_items(action_items)
    _utc_iso(record.get("decided_at_utc"), "decided_at_utc")

    safety = {
        "human_decision_recorded": True,
        "automatic_decision_allowed": False,
        "model_registry_mutation_allowed": False,
        "authorization_reuse_allowed": False,
        "pilot_reexecution_authorized": False,
        "schedule_activation_allowed": False,
        "deployment_authorized": False,
        "model_activation_authorized": False,
        "active_model_unchanged": True,
        "source_evidence_mutated": False,
    }
    for field, expected in safety.items():
        if record.get(field) is not expected:
            raise PostPilotDecisionError(
                f"Decision safety flag {field} is invalid."
            )
    if record.get("active_model_before") != "ridge_weather_lag" or record.get(
        "active_model_expected_after"
    ) != "ridge_weather_lag":
        raise PostPilotDecisionError(
            "Post-pilot decisions cannot change the active model."
        )


def _write_json_exclusive(path: Path, document: dict[str, Any]) -> Path:
    temporary = path.with_suffix(".tmp")
    for candidate in (path, temporary):
        if candidate.exists() or candidate.is_symlink():
            raise FileExistsError(f"Refusing to overwrite {candidate}.")
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with temporary.open("x", encoding="utf-8") as handle:
            json.dump(_canonical(document), handle, indent=2, sort_keys=True)
            handle.write("\n")
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)
    return path


def write_post_pilot_decision(
    output_root: Path, record: dict[str, Any]
) -> Path:
    """Write one immutable decision record."""
    verify_post_pilot_decision(record)
    path = (
        Path(output_root)
        / record["pilot_id"]
        / f"post_pilot_decision_{record['decision_id']}.json"
    )
    return _write_json_exclusive(path, record)


def load_post_pilot_decision(path: Path) -> dict[str, Any]:
    source = Path(path)
    if source.is_symlink() or not source.is_file():
        raise PostPilotDecisionError(
            "Decision path must be a regular non-symlink file."
        )
    record = json.loads(source.read_text(encoding="utf-8"))
    verify_post_pilot_decision(record)
    return deepcopy(record)

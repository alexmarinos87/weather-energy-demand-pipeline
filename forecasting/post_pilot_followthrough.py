from __future__ import annotations

import hashlib
import json
import re
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from forecasting.post_pilot_closure import (
    PostPilotClosureError,
    verify_post_pilot_closure_bundle,
    verify_recovered_post_pilot_closure,
)
from forecasting.post_pilot_decision import (
    PostPilotDecisionError,
    verify_post_pilot_decision,
)


FOLLOWTHROUGH_CONTRACT_VERSION = "post-pilot-followthrough-request-v1"
REQUEST_ID_PATTERN = re.compile(r"^pfr-[0-9a-f]{24}$")
AUTOMATION_IDENTITY_PATTERN = re.compile(
    r"(^|[-_.])(bot|automation|github-actions|ci|runner|service-account)([-_.]|$)",
    re.IGNORECASE,
)
DECISION_MAPPING = {
    "continue_evidence_collection": {
        "followthrough_type": "prepare_new_pilot_cycle",
        "new_pilot_cycle_requested": True,
        "candidate_revision_requested": False,
        "candidate_retirement_requested": False,
        "new_pilot_plan_required": True,
        "new_preflight_required": True,
        "new_authorization_required": True,
        "new_candidate_version_required": False,
        "new_evidence_cycle_required": True,
        "registry_retirement_review_required": False,
    },
    "revise_candidate": {
        "followthrough_type": "prepare_candidate_revision",
        "new_pilot_cycle_requested": False,
        "candidate_revision_requested": True,
        "candidate_retirement_requested": False,
        "new_pilot_plan_required": False,
        "new_preflight_required": False,
        "new_authorization_required": False,
        "new_candidate_version_required": True,
        "new_evidence_cycle_required": True,
        "registry_retirement_review_required": False,
    },
    "retire_candidate": {
        "followthrough_type": "prepare_registry_retirement_review",
        "new_pilot_cycle_requested": False,
        "candidate_revision_requested": False,
        "candidate_retirement_requested": True,
        "new_pilot_plan_required": False,
        "new_preflight_required": False,
        "new_authorization_required": False,
        "new_candidate_version_required": False,
        "new_evidence_cycle_required": False,
        "registry_retirement_review_required": True,
    },
}


class PostPilotFollowthroughError(ValueError):
    """Raised when a follow-through request is unsafe or inconsistent."""


def _required_text(value: Any, name: str) -> str:
    if value is None:
        raise PostPilotFollowthroughError(f"{name} must be non-empty.")
    text = str(value).strip()
    if not text:
        raise PostPilotFollowthroughError(f"{name} must be non-empty.")
    return text


def _human_identity(value: Any, name: str) -> str:
    identity = _required_text(value, name)
    if AUTOMATION_IDENTITY_PATTERN.search(identity):
        raise PostPilotFollowthroughError(
            f"{name} must identify a human requester, not automation."
        )
    return identity


def _utc_iso(value: Any, name: str) -> str:
    try:
        timestamp = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise PostPilotFollowthroughError(
            f"{name} must be a valid timezone-aware timestamp."
        ) from exc
    if pd.isna(timestamp) or timestamp.tzinfo is None:
        raise PostPilotFollowthroughError(f"{name} must be timezone-aware.")
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
    payload = json.dumps(
        _canonical(material),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _unique_action_items(values: Iterable[Any]) -> list[str]:
    items = [_required_text(value, "action_item") for value in values]
    if not items:
        raise PostPilotFollowthroughError(
            "action_items must contain at least one item."
        )
    if len(set(items)) != len(items):
        raise PostPilotFollowthroughError(
            "action_items must not contain duplicates."
        )
    return items


def _verify_sources(
    decision_record: dict[str, Any],
    closure_bundle: Path,
    recovered_directory: Path,
    *,
    verified_at_utc: Any | None = None,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    try:
        verify_post_pilot_decision(decision_record)
    except PostPilotDecisionError as exc:
        raise PostPilotFollowthroughError(
            f"Post-pilot decision failed verification: {exc}"
        ) from exc
    try:
        manifest, archive_verification = verify_post_pilot_closure_bundle(
            closure_bundle, verified_at_utc=verified_at_utc
        )
        recovered_manifest, recovery_verification = (
            verify_recovered_post_pilot_closure(
                recovered_directory, verified_at_utc=verified_at_utc
            )
        )
    except PostPilotClosureError as exc:
        raise PostPilotFollowthroughError(
            f"Post-pilot closure failed verification: {exc}"
        ) from exc
    if recovered_manifest != manifest:
        raise PostPilotFollowthroughError(
            "Recovered closure does not match the verified closure archive."
        )
    expected = {
        "pilot_id": decision_record.get("pilot_id"),
        "candidate_id": decision_record.get("candidate_id"),
        "candidate_version": decision_record.get("candidate_version"),
        "decision_id": decision_record.get("decision_id"),
        "decision_hash": decision_record.get("decision_hash"),
        "decision": decision_record.get("decision"),
        "external_run_id": decision_record.get("external_run_id"),
    }
    for field, value in expected.items():
        if manifest.get(field) != value:
            raise PostPilotFollowthroughError(
                f"Closure field {field} does not match the decision record."
            )
    if archive_verification.get("manifest_hash") != manifest.get(
        "manifest_hash"
    ) or recovery_verification.get("manifest_hash") != manifest.get(
        "manifest_hash"
    ):
        raise PostPilotFollowthroughError(
            "Closure verification records do not match the manifest."
        )
    return manifest, archive_verification, recovery_verification


def create_post_pilot_followthrough_request(
    decision_record: dict[str, Any],
    closure_bundle: Path,
    recovered_directory: Path,
    *,
    confirm_decision_id: str,
    confirm_closure_id: str,
    requested_by: str,
    owner: str,
    review_ticket: str,
    reason: str,
    action_items: Iterable[Any],
    requested_at_utc: Any | None = None,
) -> dict[str, Any]:
    """Derive one immutable request without implementing the recorded decision."""
    requested_at = _utc_iso(
        requested_at_utc or datetime.now(timezone.utc), "requested_at_utc"
    )
    manifest, archive_verification, _ = _verify_sources(
        decision_record,
        closure_bundle,
        recovered_directory,
        verified_at_utc=requested_at,
    )
    if _required_text(confirm_decision_id, "confirm_decision_id") != decision_record[
        "decision_id"
    ]:
        raise PostPilotFollowthroughError(
            "confirm_decision_id must exactly match the decision record."
        )
    if _required_text(confirm_closure_id, "confirm_closure_id") != manifest[
        "closure_id"
    ]:
        raise PostPilotFollowthroughError(
            "confirm_closure_id must exactly match the closure manifest."
        )
    decision = _required_text(decision_record.get("decision"), "decision")
    mapping = DECISION_MAPPING.get(decision)
    if mapping is None:
        raise PostPilotFollowthroughError("Decision value is unsupported.")
    requester = _human_identity(requested_by, "requested_by")
    request_owner = _required_text(owner, "owner")
    ticket = _required_text(review_ticket, "review_ticket")
    rationale = _required_text(reason, "reason")
    if len(rationale) < 20:
        raise PostPilotFollowthroughError(
            "reason must contain at least 20 characters."
        )
    actions = _unique_action_items(action_items)
    requested_timestamp = pd.Timestamp(requested_at)
    for source_name, source_time in (
        ("decision", decision_record.get("decided_at_utc")),
        ("closure", manifest.get("created_at_utc")),
    ):
        source_timestamp = pd.Timestamp(source_time)
        if source_timestamp.tzinfo is None:
            raise PostPilotFollowthroughError(
                f"{source_name} timestamp must be timezone-aware."
            )
        if requested_timestamp < source_timestamp.tz_convert("UTC"):
            raise PostPilotFollowthroughError(
                f"requested_at_utc cannot precede the {source_name}."
            )

    core = {
        "pilot_id": manifest["pilot_id"],
        "closure_id": manifest["closure_id"],
        "closure_manifest_hash": manifest["manifest_hash"],
        "closure_archive_sha256": archive_verification["archive_sha256"],
        "closure_recovery_verified": True,
        "decision_id": decision_record["decision_id"],
        "decision_hash": decision_record["decision_hash"],
        "decision": decision,
        "candidate_id": decision_record["candidate_id"],
        "candidate_version": decision_record["candidate_version"],
        "external_run_id": decision_record["external_run_id"],
        "requested_by": requester,
        "owner": request_owner,
        "review_ticket": ticket,
        "reason": rationale,
        "action_items": actions,
        "requested_at_utc": requested_at,
        **mapping,
    }
    request = {
        "request_id": "pfr-" + _digest(core)[:24],
        "request_revision": 1,
        "request_state": "requested",
        **core,
        "automatic_execution_allowed": False,
        "model_registry_mutation_allowed": False,
        "pilot_execution_authorized": False,
        "authorization_reuse_allowed": False,
        "schedule_activation_allowed": False,
        "deployment_authorized": False,
        "model_activation_authorized": False,
        "active_model_before": "ridge_weather_lag",
        "active_model_expected_after": "ridge_weather_lag",
        "active_model_unchanged": True,
        "closure_mutation_allowed": False,
        "source_evidence_mutated": False,
        "permanent_deletion_allowed": False,
        "followthrough_contract_version": FOLLOWTHROUGH_CONTRACT_VERSION,
    }
    request["request_hash"] = _digest(request, exclude=("request_hash",))
    verify_post_pilot_followthrough_request(request)
    return request


def verify_post_pilot_followthrough_request(request: dict[str, Any]) -> None:
    """Verify request hash, mapping, human ownership, and no-execution boundary."""
    if not isinstance(request, dict):
        raise PostPilotFollowthroughError(
            "Follow-through request must be a JSON object."
        )
    if request.get("followthrough_contract_version") != FOLLOWTHROUGH_CONTRACT_VERSION:
        raise PostPilotFollowthroughError(
            "Unsupported follow-through request contract."
        )
    if request.get("request_hash") != _digest(
        request, exclude=("request_hash",)
    ):
        raise PostPilotFollowthroughError(
            "Follow-through request hash is invalid."
        )
    request_id = _required_text(request.get("request_id"), "request_id")
    if not REQUEST_ID_PATTERN.fullmatch(request_id):
        raise PostPilotFollowthroughError("Request ID is malformed.")
    if request.get("request_revision") != 1 or request.get(
        "request_state"
    ) != "requested":
        raise PostPilotFollowthroughError(
            "Request revision or state is invalid."
        )
    decision = _required_text(request.get("decision"), "decision")
    mapping = DECISION_MAPPING.get(decision)
    if mapping is None:
        raise PostPilotFollowthroughError("Decision value is unsupported.")
    for field, expected in mapping.items():
        if request.get(field) != expected:
            raise PostPilotFollowthroughError(
                f"Follow-through mapping field {field} is invalid."
            )
    _human_identity(request.get("requested_by"), "requested_by")
    _required_text(request.get("owner"), "owner")
    _required_text(request.get("review_ticket"), "review_ticket")
    reason = _required_text(request.get("reason"), "reason")
    if len(reason) < 20:
        raise PostPilotFollowthroughError(
            "reason must contain at least 20 characters."
        )
    action_items = request.get("action_items")
    if not isinstance(action_items, list):
        raise PostPilotFollowthroughError("action_items must be a list.")
    _unique_action_items(action_items)
    _utc_iso(request.get("requested_at_utc"), "requested_at_utc")
    if request.get("closure_recovery_verified") is not True:
        raise PostPilotFollowthroughError(
            "A clean closure recovery must be verified."
        )

    safety = {
        "automatic_execution_allowed": False,
        "model_registry_mutation_allowed": False,
        "pilot_execution_authorized": False,
        "authorization_reuse_allowed": False,
        "schedule_activation_allowed": False,
        "deployment_authorized": False,
        "model_activation_authorized": False,
        "active_model_unchanged": True,
        "closure_mutation_allowed": False,
        "source_evidence_mutated": False,
        "permanent_deletion_allowed": False,
    }
    for field, expected in safety.items():
        if request.get(field) is not expected:
            raise PostPilotFollowthroughError(
                f"Follow-through safety flag {field} is invalid."
            )
    if request.get("active_model_before") != "ridge_weather_lag" or request.get(
        "active_model_expected_after"
    ) != "ridge_weather_lag":
        raise PostPilotFollowthroughError(
            "A follow-through request cannot change the active model."
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


def write_post_pilot_followthrough_request(
    output_root: Path, request: dict[str, Any]
) -> Path:
    """Write one immutable follow-through request."""
    verify_post_pilot_followthrough_request(request)
    path = (
        Path(output_root)
        / request["pilot_id"]
        / f"post_pilot_followthrough_{request['request_id']}.json"
    )
    return _write_json_exclusive(path, request)


def load_post_pilot_followthrough_request(path: Path) -> dict[str, Any]:
    source = Path(path)
    if source.is_symlink() or not source.is_file():
        raise PostPilotFollowthroughError(
            "Request path must be a regular non-symlink file."
        )
    request = json.loads(source.read_text(encoding="utf-8"))
    verify_post_pilot_followthrough_request(request)
    return deepcopy(request)

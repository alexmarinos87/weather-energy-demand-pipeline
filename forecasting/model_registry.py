from __future__ import annotations

import hashlib
import json
import re
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd


BASELINE_MODEL = "ridge_weather_lag"
CANDIDATE_MODEL = "ridge_target_weather"
MANIFEST_CONTRACT_VERSION = "target-weather-model-candidate-v1"
EVENT_CONTRACT_VERSION = "model-candidate-review-event-v1"
SHA_PATTERN = re.compile(r"^[0-9a-f]{40}$")
VERSION_PATTERN = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+(?:[-+][A-Za-z0-9.-]+)?$")
REPOSITORY_PATTERN = re.compile(r"^[^/\s]+/[^/\s]+$")

STATES = {"draft", "review_requested", "approved", "rejected", "retired"}
TRANSITIONS = {
    (None, "registered"): "draft",
    ("draft", "review_requested"): "review_requested",
    ("review_requested", "approved"): "approved",
    ("review_requested", "rejected"): "rejected",
    ("draft", "retired"): "retired",
    ("review_requested", "retired"): "retired",
    ("approved", "retired"): "retired",
    ("rejected", "retired"): "retired",
}

PROMOTION_REQUIRED = {
    "assessment_id",
    "assessment_timestamp_utc",
    "comparison_run_id",
    "reconciliation_run_id",
    "baseline_model",
    "candidate_model",
    "assessment_status",
    "automatic_promotion_allowed",
    "failed_check_count",
}
HEALTH_REQUIRED = {
    "monitor_run_id",
    "monitor_timestamp_utc",
    "monitor_status",
    "automatic_remediation_allowed",
    "failed_error_check_count",
    "failed_warning_check_count",
}


class ModelCandidateRegistryError(ValueError):
    """Raised when candidate evidence or review history is invalid."""


def _required_text(value: Any, name: str) -> str:
    if value is None:
        raise ModelCandidateRegistryError(f"{name} must be non-empty.")
    text = str(value).strip()
    if not text:
        raise ModelCandidateRegistryError(f"{name} must be non-empty.")
    return text


def _utc_iso(value: Any, name: str) -> str:
    try:
        timestamp = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise ModelCandidateRegistryError(
            f"{name} must be a valid timezone-aware timestamp."
        ) from exc
    if pd.isna(timestamp) or timestamp.tzinfo is None:
        raise ModelCandidateRegistryError(
            f"{name} must be a timezone-aware timestamp."
        )
    return timestamp.tz_convert("UTC").isoformat()


def _boolean(value: Any, name: str) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"true", "1"}:
        return True
    if text in {"false", "0"}:
        return False
    raise ModelCandidateRegistryError(f"{name} must be boolean.")


def _non_negative_int(value: Any, name: str) -> int:
    if isinstance(value, bool):
        raise ModelCandidateRegistryError(f"{name} must be a non-negative integer.")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ModelCandidateRegistryError(
            f"{name} must be a non-negative integer."
        ) from exc
    if parsed < 0:
        raise ModelCandidateRegistryError(f"{name} must be a non-negative integer.")
    return parsed


def _single_row(frame: pd.DataFrame, required: set[str], label: str) -> dict[str, Any]:
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ModelCandidateRegistryError(
            f"{label} is missing required columns: {', '.join(missing)}."
        )
    if len(frame) != 1:
        raise ModelCandidateRegistryError(
            f"{label} must contain exactly one row; found {len(frame)}."
        )
    return frame.iloc[0].to_dict()


def _canonical(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _canonical(value[key]) for key in sorted(value)}
    if isinstance(value, (list, tuple)):
        return [_canonical(item) for item in value]
    if isinstance(value, (datetime, pd.Timestamp)):
        return _utc_iso(value, "timestamp")
    if pd.isna(value):
        return None
    return value


def _digest(payload: dict[str, Any], excluded: Iterable[str] = ()) -> str:
    excluded_set = set(excluded)
    material = {key: value for key, value in payload.items() if key not in excluded_set}
    encoded = json.dumps(
        _canonical(material),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _event_hash(event: dict[str, Any]) -> str:
    return _digest(event, excluded=("event_hash",))


def _manifest_hash(manifest: dict[str, Any]) -> str:
    return _digest(manifest, excluded=("manifest_hash",))


def _feature_versions(values: Iterable[Any]) -> list[str]:
    versions = sorted({_required_text(value, "feature_contract_version") for value in values})
    if not versions:
        raise ModelCandidateRegistryError(
            "feature_contract_versions must contain at least one version."
        )
    return versions


def _optional_text(row: dict[str, Any], column: str) -> str | None:
    value = row.get(column)
    if value is None or pd.isna(value):
        return None
    return _required_text(value, column)


def register_candidate(
    promotion_summary: pd.DataFrame,
    provider_health_summary: pd.DataFrame,
    *,
    repository: str,
    code_commit_sha: str,
    code_tree_sha: str,
    candidate_version: str,
    training_data_boundary_utc: Any,
    feature_contract_versions: Iterable[Any],
    forecast_weather_contract_version: str,
    actor: str,
    reason: str,
    created_at_utc: Any | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Create deterministic draft candidate evidence and its registration event."""
    promotion = _single_row(
        promotion_summary, PROMOTION_REQUIRED, "Promotion summary"
    )
    health = _single_row(
        provider_health_summary, HEALTH_REQUIRED, "Provider-health summary"
    )
    repository = _required_text(repository, "repository")
    commit_sha = _required_text(code_commit_sha, "code_commit_sha").lower()
    tree_sha = _required_text(code_tree_sha, "code_tree_sha").lower()
    version = _required_text(candidate_version, "candidate_version")
    if not REPOSITORY_PATTERN.fullmatch(repository):
        raise ModelCandidateRegistryError("repository must use owner/name form.")
    if not SHA_PATTERN.fullmatch(commit_sha) or not SHA_PATTERN.fullmatch(tree_sha):
        raise ModelCandidateRegistryError(
            "code_commit_sha and code_tree_sha must be lowercase 40-character hexadecimal SHAs."
        )
    if not VERSION_PATTERN.fullmatch(version):
        raise ModelCandidateRegistryError(
            "candidate_version must be a semantic version such as 0.1.0."
        )
    baseline_model = _required_text(promotion["baseline_model"], "baseline_model")
    candidate_model = _required_text(promotion["candidate_model"], "candidate_model")
    if baseline_model != BASELINE_MODEL or candidate_model != CANDIDATE_MODEL:
        raise ModelCandidateRegistryError(
            "Promotion summary must compare ridge_weather_lag with ridge_target_weather."
        )
    automatic_promotion = _boolean(
        promotion["automatic_promotion_allowed"], "automatic_promotion_allowed"
    )
    automatic_remediation = _boolean(
        health["automatic_remediation_allowed"], "automatic_remediation_allowed"
    )
    if automatic_promotion or automatic_remediation:
        raise ModelCandidateRegistryError(
            "Candidate evidence must prohibit automatic promotion and remediation."
        )
    promotion_status = _required_text(
        promotion["assessment_status"], "assessment_status"
    )
    if promotion_status not in {"blocked", "eligible_for_human_review"}:
        raise ModelCandidateRegistryError("Unsupported promotion assessment status.")
    health_status = _required_text(health["monitor_status"], "monitor_status")
    if health_status not in {"healthy", "warning", "failed"}:
        raise ModelCandidateRegistryError("Unsupported provider-health status.")
    boundary = _utc_iso(training_data_boundary_utc, "training_data_boundary_utc")
    created_at = _utc_iso(
        created_at_utc or datetime.now(timezone.utc), "created_at_utc"
    )
    actor = _required_text(actor, "actor")
    reason = _required_text(reason, "reason")
    contract_versions = _feature_versions(feature_contract_versions)
    forecast_contract = _required_text(
        forecast_weather_contract_version, "forecast_weather_contract_version"
    )
    core = {
        "repository": repository,
        "code_commit_sha": commit_sha,
        "code_tree_sha": tree_sha,
        "candidate_version": version,
        "baseline_model": baseline_model,
        "candidate_model": candidate_model,
        "training_data_boundary_utc": boundary,
        "feature_contract_versions": contract_versions,
        "forecast_weather_contract_version": forecast_contract,
        "promotion_assessment_id": _required_text(
            promotion["assessment_id"], "assessment_id"
        ),
        "comparison_run_id": _required_text(
            promotion["comparison_run_id"], "comparison_run_id"
        ),
        "reconciliation_run_id": _required_text(
            promotion["reconciliation_run_id"], "reconciliation_run_id"
        ),
        "provider_health_monitor_run_id": _required_text(
            health["monitor_run_id"], "monitor_run_id"
        ),
    }
    candidate_id = "twc-" + _digest(core)[:24]
    event = {
        "candidate_id": candidate_id,
        "event_sequence": 1,
        "action": "registered",
        "from_state": None,
        "to_state": "draft",
        "actor": actor,
        "reason": reason,
        "event_timestamp_utc": created_at,
        "review_ticket": None,
        "previous_event_hash": None,
        "event_contract_version": EVENT_CONTRACT_VERSION,
    }
    event["event_hash"] = _event_hash(event)
    manifest = {
        "candidate_id": candidate_id,
        "candidate_revision": 1,
        "candidate_state": "draft",
        **core,
        "promotion_assessment_status": promotion_status,
        "promotion_failed_check_count": _non_negative_int(
            promotion["failed_check_count"], "failed_check_count"
        ),
        "promotion_policy_version": _optional_text(
            promotion, "policy_version"
        ),
        "promotion_assessment_contract_version": _optional_text(
            promotion, "assessment_contract_version"
        ),
        "provider_health_status": health_status,
        "provider_failed_error_check_count": _non_negative_int(
            health["failed_error_check_count"], "failed_error_check_count"
        ),
        "provider_failed_warning_check_count": _non_negative_int(
            health["failed_warning_check_count"], "failed_warning_check_count"
        ),
        "provider_health_policy_version": _optional_text(
            health, "policy_version"
        ),
        "provider_health_contract_version": _optional_text(
            health, "monitoring_contract_version"
        ),
        "automatic_promotion_allowed": False,
        "automatic_remediation_allowed": False,
        "deployment_authorized": False,
        "active_model_unchanged": True,
        "created_at_utc": created_at,
        "updated_at_utc": created_at,
        "created_by": actor,
        "registration_reason": reason,
        "review_requested_by": None,
        "review_requested_at_utc": None,
        "review_decision": None,
        "reviewed_by": None,
        "reviewed_at_utc": None,
        "review_ticket": None,
        "review_reason": None,
        "retired_by": None,
        "retired_at_utc": None,
        "retirement_reason": None,
        "latest_event_hash": event["event_hash"],
        "previous_manifest_hash": None,
        "manifest_contract_version": MANIFEST_CONTRACT_VERSION,
    }
    manifest["manifest_hash"] = _manifest_hash(manifest)
    return manifest, event


def verify_manifest(manifest: dict[str, Any]) -> None:
    if manifest.get("manifest_contract_version") != MANIFEST_CONTRACT_VERSION:
        raise ModelCandidateRegistryError("Unsupported candidate manifest contract.")
    state = manifest.get("candidate_state")
    if state not in STATES:
        raise ModelCandidateRegistryError("Candidate manifest contains an invalid state.")
    if manifest.get("automatic_promotion_allowed") is not False:
        raise ModelCandidateRegistryError("Automatic promotion must remain disabled.")
    if manifest.get("automatic_remediation_allowed") is not False:
        raise ModelCandidateRegistryError("Automatic remediation must remain disabled.")
    if manifest.get("deployment_authorized") is not False:
        raise ModelCandidateRegistryError("Candidate manifests cannot authorize deployment.")
    if manifest.get("active_model_unchanged") is not True:
        raise ModelCandidateRegistryError("Candidate manifests must keep the active model unchanged.")
    if manifest.get("manifest_hash") != _manifest_hash(manifest):
        raise ModelCandidateRegistryError("Candidate manifest hash is invalid.")


def _review_eligible(manifest: dict[str, Any]) -> None:
    if manifest["promotion_assessment_status"] != "eligible_for_human_review":
        raise ModelCandidateRegistryError(
            "A blocked promotion assessment cannot enter review."
        )
    if int(manifest["promotion_failed_check_count"]) != 0:
        raise ModelCandidateRegistryError(
            "Promotion evidence contains failed checks."
        )
    if manifest["provider_health_status"] != "healthy":
        raise ModelCandidateRegistryError(
            "Provider health must be healthy before review is requested."
        )
    if int(manifest["provider_failed_error_check_count"]) != 0:
        raise ModelCandidateRegistryError(
            "Provider-health evidence contains failed error checks."
        )


def transition_candidate(
    manifest: dict[str, Any],
    *,
    action: str,
    actor: str,
    reason: str,
    event_timestamp_utc: Any | None = None,
    review_ticket: str | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Create the next immutable manifest and event revision."""
    verify_manifest(manifest)
    current_state = manifest["candidate_state"]
    action = _required_text(action, "action")
    next_state = TRANSITIONS.get((current_state, action))
    if next_state is None:
        raise ModelCandidateRegistryError(
            f"Action {action!r} is not allowed from state {current_state!r}."
        )
    actor = _required_text(actor, "actor")
    reason = _required_text(reason, "reason")
    timestamp = _utc_iso(
        event_timestamp_utc or datetime.now(timezone.utc), "event_timestamp_utc"
    )
    if pd.Timestamp(timestamp) < pd.Timestamp(manifest["updated_at_utc"]):
        raise ModelCandidateRegistryError(
            "Review events cannot precede the latest manifest timestamp."
        )
    ticket = None if review_ticket is None else _required_text(review_ticket, "review_ticket")
    if action in {"approved", "rejected", "retired"} and ticket is None:
        raise ModelCandidateRegistryError(
            f"{action} requires a non-empty review_ticket."
        )
    if action == "review_requested":
        _review_eligible(manifest)
    event_sequence = int(manifest["candidate_revision"]) + 1
    event = {
        "candidate_id": manifest["candidate_id"],
        "event_sequence": event_sequence,
        "action": action,
        "from_state": current_state,
        "to_state": next_state,
        "actor": actor,
        "reason": reason,
        "event_timestamp_utc": timestamp,
        "review_ticket": ticket,
        "previous_event_hash": manifest["latest_event_hash"],
        "event_contract_version": EVENT_CONTRACT_VERSION,
    }
    event["event_hash"] = _event_hash(event)
    updated = deepcopy(manifest)
    previous_manifest_hash = updated.pop("manifest_hash")
    updated.update(
        {
            "candidate_revision": event_sequence,
            "candidate_state": next_state,
            "updated_at_utc": timestamp,
            "latest_event_hash": event["event_hash"],
            "previous_manifest_hash": previous_manifest_hash,
            "deployment_authorized": False,
            "active_model_unchanged": True,
            "automatic_promotion_allowed": False,
            "automatic_remediation_allowed": False,
        }
    )
    if action == "review_requested":
        updated["review_requested_by"] = actor
        updated["review_requested_at_utc"] = timestamp
    elif action in {"approved", "rejected"}:
        updated["review_decision"] = action
        updated["reviewed_by"] = actor
        updated["reviewed_at_utc"] = timestamp
        updated["review_ticket"] = ticket
        updated["review_reason"] = reason
    elif action == "retired":
        updated["retired_by"] = actor
        updated["retired_at_utc"] = timestamp
        updated["retirement_reason"] = reason
        updated["review_ticket"] = ticket
    updated["manifest_hash"] = _manifest_hash(updated)
    return updated, event


def verify_candidate_history(
    manifests: Iterable[dict[str, Any]], events: Iterable[dict[str, Any]]
) -> dict[str, Any]:
    """Verify revision ordering, state transitions, and both hash chains."""
    manifest_list = sorted(manifests, key=lambda item: int(item["candidate_revision"]))
    event_list = sorted(events, key=lambda item: int(item["event_sequence"]))
    if not manifest_list or len(manifest_list) != len(event_list):
        raise ModelCandidateRegistryError(
            "Candidate history must contain one event for every manifest revision."
        )
    candidate_ids = {
        str(item.get("candidate_id")) for item in [*manifest_list, *event_list]
    }
    if len(candidate_ids) != 1:
        raise ModelCandidateRegistryError("Candidate history mixes candidate IDs.")
    previous_manifest_hash: str | None = None
    previous_event_hash: str | None = None
    expected_state: str | None = None
    for index, (manifest, event) in enumerate(
        zip(manifest_list, event_list), start=1
    ):
        verify_manifest(manifest)
        if int(manifest["candidate_revision"]) != index:
            raise ModelCandidateRegistryError("Manifest revisions are not contiguous.")
        if int(event.get("event_sequence", 0)) != index:
            raise ModelCandidateRegistryError("Event sequences are not contiguous.")
        if event.get("event_contract_version") != EVENT_CONTRACT_VERSION:
            raise ModelCandidateRegistryError("Unsupported candidate event contract.")
        if event.get("event_hash") != _event_hash(event):
            raise ModelCandidateRegistryError("Candidate event hash is invalid.")
        if event.get("previous_event_hash") != previous_event_hash:
            raise ModelCandidateRegistryError("Candidate event hash chain is broken.")
        if manifest.get("previous_manifest_hash") != previous_manifest_hash:
            raise ModelCandidateRegistryError("Candidate manifest hash chain is broken.")
        action = event.get("action")
        next_state = TRANSITIONS.get((expected_state, action))
        if next_state is None:
            raise ModelCandidateRegistryError("Candidate event transition is invalid.")
        if event.get("from_state") != expected_state or event.get("to_state") != next_state:
            raise ModelCandidateRegistryError("Candidate event states are inconsistent.")
        if manifest.get("candidate_state") != next_state:
            raise ModelCandidateRegistryError(
                "Manifest state does not match its review event."
            )
        if manifest.get("latest_event_hash") != event.get("event_hash"):
            raise ModelCandidateRegistryError(
                "Manifest does not reference its corresponding review event."
            )
        previous_manifest_hash = manifest["manifest_hash"]
        previous_event_hash = event["event_hash"]
        expected_state = next_state
    return deepcopy(manifest_list[-1])


def write_candidate_revision(
    candidate_directory: Path,
    manifest: dict[str, Any],
    event: dict[str, Any],
) -> tuple[Path, Path]:
    """Write one immutable event/manifest pair without replacing prior revisions."""
    candidate_directory = Path(candidate_directory)
    candidate_directory.mkdir(parents=True, exist_ok=True)
    revision = int(manifest["candidate_revision"])
    if revision != int(event["event_sequence"]):
        raise ModelCandidateRegistryError(
            "Manifest revision and event sequence must match."
        )
    if candidate_directory.name != manifest["candidate_id"]:
        raise ModelCandidateRegistryError(
            "Candidate directory name must equal candidate_id."
        )
    event_path = candidate_directory / f"event_v{revision:03d}.json"
    manifest_path = candidate_directory / f"manifest_v{revision:03d}.json"
    temporary_event = event_path.with_suffix(".tmp")
    temporary_manifest = manifest_path.with_suffix(".tmp")
    for path in (event_path, manifest_path, temporary_event, temporary_manifest):
        if path.exists():
            raise FileExistsError(f"Refusing to overwrite {path}.")
    try:
        temporary_event.write_text(
            json.dumps(_canonical(event), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        temporary_manifest.write_text(
            json.dumps(_canonical(manifest), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        temporary_event.replace(event_path)
        temporary_manifest.replace(manifest_path)
    finally:
        temporary_event.unlink(missing_ok=True)
        temporary_manifest.unlink(missing_ok=True)
    return manifest_path, event_path


def load_candidate_history(
    candidate_directory: Path,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    candidate_directory = Path(candidate_directory)
    manifests = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in sorted(candidate_directory.glob("manifest_v*.json"))
    ]
    events = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in sorted(candidate_directory.glob("event_v*.json"))
    ]
    latest = verify_candidate_history(manifests, events)
    if candidate_directory.name != latest["candidate_id"]:
        raise ModelCandidateRegistryError(
            "Candidate directory name does not match the verified history."
        )
    return manifests, events, latest

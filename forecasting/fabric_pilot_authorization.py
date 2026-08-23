from __future__ import annotations

import hashlib
import json
import re
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from forecasting.evidence_bundle import (
    verify_evidence_bundle,
    verify_recovered_bundle,
)
from forecasting.fabric_pilot import (
    FabricPilotError,
    load_fabric_pilot_plan,
    verify_fabric_pilot_plan,
)
from forecasting.model_registry import load_candidate_history


PREFLIGHT_CONTRACT_VERSION = "controlled-fabric-pilot-preflight-v1"
AUTHORIZATION_CONTRACT_VERSION = "controlled-fabric-pilot-authorization-v1"
PREFLIGHT_ID_PATTERN = re.compile(r"^fpf-[0-9a-f]{24}$")
AUTHORIZATION_ID_PATTERN = re.compile(r"^fpa-[0-9a-f]{24}$")
SHA40_PATTERN = re.compile(r"^[0-9a-f]{40}$")
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
SECRET_REFERENCE_PATTERN = re.compile(r"^[A-Z][A-Z0-9_]{2,127}$")
AUTOMATION_IDENTITY_PATTERN = re.compile(
    r"(^|[-_.])(bot|automation|github-actions|ci|runner|service-account)([-_.]|$)",
    re.IGNORECASE,
)
ELIGIBLE_STATUS = "eligible_for_human_authorization"
BLOCKED_STATUS = "blocked"

ENVIRONMENT_REQUIRED = {
    "snapshot_id",
    "captured_at_utc",
    "environment",
    "workspace_name",
    "lakehouse_name",
    "capacity_name",
    "workspace_exists",
    "lakehouse_exists",
    "capacity_available",
    "available_capacity_units",
    "current_capacity_utilization_pct",
    "active_job_count",
    "pilot_schedule_count",
    "current_active_model",
    "credential_values_included",
}
PROVIDER_HEALTH_REQUIRED = {
    "monitor_run_id",
    "monitor_timestamp_utc",
    "monitor_as_of_utc",
    "monitor_status",
    "automatic_remediation_allowed",
    "failed_error_check_count",
}


class FabricPilotAuthorizationError(ValueError):
    """Raised when pilot preflight or human authorization evidence is unsafe."""


def _required_text(value: Any, name: str) -> str:
    if value is None:
        raise FabricPilotAuthorizationError(f"{name} must be non-empty.")
    text = str(value).strip()
    if not text:
        raise FabricPilotAuthorizationError(f"{name} must be non-empty.")
    return text


def _utc_timestamp(value: Any, name: str) -> pd.Timestamp:
    try:
        timestamp = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise FabricPilotAuthorizationError(
            f"{name} must be a valid timezone-aware timestamp."
        ) from exc
    if pd.isna(timestamp) or timestamp.tzinfo is None:
        raise FabricPilotAuthorizationError(f"{name} must be timezone-aware.")
    return timestamp.tz_convert("UTC")


def _utc_iso(value: Any, name: str) -> str:
    return _utc_timestamp(value, name).isoformat()


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


def _digest(payload: dict[str, Any], *, exclude: Iterable[str] = ()) -> str:
    excluded = set(exclude)
    material = {key: value for key, value in payload.items() if key not in excluded}
    return hashlib.sha256(
        json.dumps(
            _canonical(material),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
    ).hexdigest()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _boolean(value: Any, name: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str) and value.strip().lower() in {"true", "false"}:
        return value.strip().lower() == "true"
    raise FabricPilotAuthorizationError(f"{name} must be boolean.")


def _non_negative_int(value: Any, name: str) -> int:
    if isinstance(value, bool):
        raise FabricPilotAuthorizationError(f"{name} must be a non-negative integer.")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise FabricPilotAuthorizationError(
            f"{name} must be a non-negative integer."
        ) from exc
    if parsed < 0:
        raise FabricPilotAuthorizationError(f"{name} must be a non-negative integer.")
    return parsed


def _finite_number(value: Any, name: str, *, minimum: float | None = None) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise FabricPilotAuthorizationError(f"{name} must be numeric.") from exc
    if not pd.notna(parsed) or parsed in {float("inf"), float("-inf")}:
        raise FabricPilotAuthorizationError(f"{name} must be finite.")
    if minimum is not None and parsed < minimum:
        raise FabricPilotAuthorizationError(f"{name} must be at least {minimum}.")
    return parsed


def _single_row(frame: pd.DataFrame, required: set[str], label: str) -> dict[str, Any]:
    missing = sorted(required - set(frame.columns))
    if missing:
        raise FabricPilotAuthorizationError(
            f"{label} is missing required columns: {', '.join(missing)}."
        )
    if len(frame) != 1:
        raise FabricPilotAuthorizationError(
            f"{label} must contain exactly one row; found {len(frame)}."
        )
    return frame.iloc[0].to_dict()


def _secret_references(values: Iterable[Any], name: str) -> list[str]:
    references = [_required_text(value, name) for value in values]
    if len(set(references)) != len(references):
        raise FabricPilotAuthorizationError(f"{name} must not contain duplicates.")
    for reference in references:
        if not SECRET_REFERENCE_PATTERN.fullmatch(reference):
            raise FabricPilotAuthorizationError(
                f"{name} must contain uppercase reference names only; values and assignments are forbidden."
            )
    return references


def _human_identity(value: Any, name: str) -> str:
    identity = _required_text(value, name)
    if AUTOMATION_IDENTITY_PATTERN.search(identity):
        raise FabricPilotAuthorizationError(
            f"{name} must identify a human operator, not automation."
        )
    return identity


def _safe_repository_relative(value: Any, name: str) -> Path:
    text = _required_text(value, name).replace("\\", "/")
    relative = Path(text)
    if relative.is_absolute() or ".." in relative.parts or "." in relative.parts:
        raise FabricPilotAuthorizationError(
            f"{name} must be a safe repository-relative path."
        )
    return relative


def _repository_root(path: Path) -> Path:
    root = Path(path)
    if root.is_symlink() or not root.is_dir():
        raise FabricPilotAuthorizationError(
            "repository_root must be a regular non-symlink directory."
        )
    return root.resolve()


def _repository_file(root: Path, relative: Path) -> Path:
    current = root
    for part in relative.parts:
        current = current / part
        if current.is_symlink():
            raise FabricPilotAuthorizationError(
                f"Repository path contains a symbolic link: {relative.as_posix()}."
            )
    try:
        resolved = current.resolve(strict=True)
    except FileNotFoundError as exc:
        raise FabricPilotAuthorizationError(
            f"Repository file is missing: {relative.as_posix()}."
        ) from exc
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise FabricPilotAuthorizationError(
            f"Repository file escapes repository_root: {relative.as_posix()}."
        ) from exc
    if not resolved.is_file():
        raise FabricPilotAuthorizationError(
            f"Repository path is not a regular file: {relative.as_posix()}."
        )
    return resolved


def _check(
    sequence: int,
    scope: str,
    name: str,
    passed: bool,
    observed: Any,
    expected: Any,
    details: str,
) -> dict[str, Any]:
    return {
        "check_sequence": sequence,
        "check_scope": scope,
        "check_name": name,
        "passed": bool(passed),
        "observed_value": str(observed),
        "expected_value": str(expected),
        "details": _required_text(details, "check details"),
    }


def _append_check(
    checks: list[dict[str, Any]],
    scope: str,
    name: str,
    passed: bool,
    observed: Any,
    expected: Any,
    details: str,
) -> None:
    checks.append(
        _check(
            len(checks) + 1,
            scope,
            name,
            passed,
            observed,
            expected,
            details,
        )
    )


def _environment_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(snapshot, dict):
        raise FabricPilotAuthorizationError(
            "environment_snapshot must be a JSON object."
        )
    missing = sorted(ENVIRONMENT_REQUIRED - set(snapshot))
    if missing:
        raise FabricPilotAuthorizationError(
            "Environment snapshot is missing required fields: "
            + ", ".join(missing)
            + "."
        )
    result = {
        "snapshot_id": _required_text(snapshot["snapshot_id"], "snapshot_id"),
        "captured_at_utc": _utc_iso(snapshot["captured_at_utc"], "captured_at_utc"),
        "environment": _required_text(snapshot["environment"], "environment").lower(),
        "workspace_name": _required_text(snapshot["workspace_name"], "workspace_name"),
        "lakehouse_name": _required_text(snapshot["lakehouse_name"], "lakehouse_name"),
        "capacity_name": _required_text(snapshot["capacity_name"], "capacity_name"),
        "workspace_exists": _boolean(snapshot["workspace_exists"], "workspace_exists"),
        "lakehouse_exists": _boolean(snapshot["lakehouse_exists"], "lakehouse_exists"),
        "capacity_available": _boolean(snapshot["capacity_available"], "capacity_available"),
        "available_capacity_units": _finite_number(
            snapshot["available_capacity_units"],
            "available_capacity_units",
            minimum=0,
        ),
        "current_capacity_utilization_pct": _finite_number(
            snapshot["current_capacity_utilization_pct"],
            "current_capacity_utilization_pct",
            minimum=0,
        ),
        "active_job_count": _non_negative_int(snapshot["active_job_count"], "active_job_count"),
        "pilot_schedule_count": _non_negative_int(
            snapshot["pilot_schedule_count"], "pilot_schedule_count"
        ),
        "current_active_model": _required_text(
            snapshot["current_active_model"], "current_active_model"
        ),
        "credential_values_included": _boolean(
            snapshot["credential_values_included"], "credential_values_included"
        ),
    }
    if result["current_capacity_utilization_pct"] > 100:
        raise FabricPilotAuthorizationError(
            "current_capacity_utilization_pct must not exceed 100."
        )
    return result


def _provider_health(frame: pd.DataFrame) -> dict[str, Any]:
    row = _single_row(frame, PROVIDER_HEALTH_REQUIRED, "Provider-health summary")
    return {
        "monitor_run_id": _required_text(row["monitor_run_id"], "monitor_run_id"),
        "monitor_timestamp_utc": _utc_iso(
            row["monitor_timestamp_utc"], "monitor_timestamp_utc"
        ),
        "monitor_as_of_utc": _utc_iso(row["monitor_as_of_utc"], "monitor_as_of_utc"),
        "monitor_status": _required_text(row["monitor_status"], "monitor_status"),
        "automatic_remediation_allowed": _boolean(
            row["automatic_remediation_allowed"],
            "automatic_remediation_allowed",
        ),
        "failed_error_check_count": _non_negative_int(
            row["failed_error_check_count"], "failed_error_check_count"
        ),
    }


def assess_fabric_pilot_preflight(
    plan: dict[str, Any],
    candidate_directory: Path,
    evidence_bundle_path: Path,
    recovered_bundle_directory: Path,
    repository_root: Path,
    provider_health_summary: pd.DataFrame,
    environment_snapshot: dict[str, Any],
    *,
    current_code_commit_sha: str,
    current_code_tree_sha: str,
    available_credential_references: Iterable[Any],
    as_of_utc: Any | None = None,
    max_plan_age_minutes: int = 10_080,
    max_provider_health_age_minutes: int = 1_440,
    max_environment_snapshot_age_minutes: int = 60,
    max_capacity_utilization_pct: float = 70.0,
    max_active_job_count: int = 0,
) -> dict[str, Any]:
    """Create a non-executing repository and environment preflight document."""
    try:
        verify_fabric_pilot_plan(plan)
    except FabricPilotError as exc:
        raise FabricPilotAuthorizationError(f"Pilot plan failed verification: {exc}") from exc

    as_of = _utc_timestamp(as_of_utc or datetime.now(timezone.utc), "as_of_utc")
    commit_sha = _required_text(current_code_commit_sha, "current_code_commit_sha").lower()
    tree_sha = _required_text(current_code_tree_sha, "current_code_tree_sha").lower()
    if not SHA40_PATTERN.fullmatch(commit_sha) or not SHA40_PATTERN.fullmatch(tree_sha):
        raise FabricPilotAuthorizationError(
            "Current code commit and tree SHAs must be lowercase 40-character hex."
        )
    max_plan_age = _non_negative_int(max_plan_age_minutes, "max_plan_age_minutes")
    max_health_age = _non_negative_int(
        max_provider_health_age_minutes, "max_provider_health_age_minutes"
    )
    max_snapshot_age = _non_negative_int(
        max_environment_snapshot_age_minutes,
        "max_environment_snapshot_age_minutes",
    )
    max_utilization = _finite_number(
        max_capacity_utilization_pct,
        "max_capacity_utilization_pct",
        minimum=0,
    )
    if max_utilization > 100:
        raise FabricPilotAuthorizationError(
            "max_capacity_utilization_pct must not exceed 100."
        )
    max_jobs = _non_negative_int(max_active_job_count, "max_active_job_count")
    available_references = _secret_references(
        available_credential_references, "available_credential_reference"
    )
    snapshot = _environment_snapshot(environment_snapshot)
    health = _provider_health(provider_health_summary)
    root = _repository_root(repository_root)

    checks: list[dict[str, Any]] = []
    repository_files: list[dict[str, Any]] = []

    try:
        _, _, candidate = load_candidate_history(Path(candidate_directory))
        candidate_ok = (
            candidate.get("candidate_state") == "approved"
            and candidate.get("manifest_hash") == plan.get("candidate_manifest_hash")
            and candidate.get("candidate_id") == plan.get("candidate_id")
            and candidate.get("deployment_authorized") is False
            and candidate.get("active_model_unchanged") is True
        )
        candidate_observed = candidate.get("candidate_state")
        candidate_details = "Approved candidate history was re-verified and bound to the plan."
    except Exception as exc:
        candidate_ok = False
        candidate_observed = f"verification error: {exc}"
        candidate_details = "Candidate history could not be safely re-verified."
    _append_check(
        checks,
        "governance",
        "approved_candidate_history_reverified",
        candidate_ok,
        candidate_observed,
        "approved candidate matching plan manifest hash",
        candidate_details,
    )

    try:
        bundle_manifest, bundle_verification = verify_evidence_bundle(
            Path(evidence_bundle_path)
        )
        bundle_ok = (
            bundle_manifest.get("bundle_id") == plan.get("bundle_id")
            and bundle_manifest.get("manifest_hash") == plan.get("bundle_manifest_hash")
            and bundle_verification.get("bundle_sha256") == plan.get("bundle_sha256")
            and bundle_verification.get("verification_status") == "verified"
            and bundle_manifest.get("candidate_id") == plan.get("candidate_id")
        )
        bundle_observed = bundle_manifest.get("bundle_id")
        bundle_details = "Approved-candidate evidence bundle was re-verified."
    except Exception as exc:
        bundle_ok = False
        bundle_observed = f"verification error: {exc}"
        bundle_details = "Evidence bundle could not be safely re-verified."
    _append_check(
        checks,
        "governance",
        "evidence_bundle_reverified",
        bundle_ok,
        bundle_observed,
        plan.get("bundle_id"),
        bundle_details,
    )

    try:
        recovery_verification = verify_recovered_bundle(
            Path(recovered_bundle_directory)
        )
        recovery_ok = (
            recovery_verification.get("recovery_id") == plan.get("recovery_id")
            and recovery_verification.get("bundle_id") == plan.get("bundle_id")
            and recovery_verification.get("candidate_id") == plan.get("candidate_id")
            and recovery_verification.get("verification_status") == "verified"
        )
        recovery_observed = recovery_verification.get("recovery_id")
        recovery_details = "Clean recovered evidence was re-verified."
    except Exception as exc:
        recovery_ok = False
        recovery_observed = f"verification error: {exc}"
        recovery_details = "Recovered evidence could not be safely re-verified."
    _append_check(
        checks,
        "governance",
        "recovered_evidence_reverified",
        recovery_ok,
        recovery_observed,
        plan.get("recovery_id"),
        recovery_details,
    )

    _append_check(
        checks,
        "repository",
        "current_commit_matches_plan",
        commit_sha == plan.get("code_commit_sha"),
        commit_sha,
        plan.get("code_commit_sha"),
        "Current checkout commit must match the candidate-bound plan.",
    )
    _append_check(
        checks,
        "repository",
        "current_tree_matches_plan",
        tree_sha == plan.get("code_tree_sha"),
        tree_sha,
        plan.get("code_tree_sha"),
        "Current checkout tree must match the candidate-bound plan.",
    )

    for category, values in (
        ("notebook", plan.get("notebook_paths", [])),
        ("contract", plan.get("contract_paths", [])),
    ):
        for raw in values:
            relative = _safe_repository_relative(raw, f"{category}_path")
            try:
                source = _repository_file(root, relative)
                item = {
                    "file_sequence": len(repository_files) + 1,
                    "file_role": category,
                    "repository_path": relative.as_posix(),
                    "size_bytes": source.stat().st_size,
                    "sha256": _file_sha256(source),
                }
                repository_files.append(item)
                passed = True
                observed = item["sha256"]
                details = "Repository file exists as a regular non-symlink file."
            except Exception as exc:
                passed = False
                observed = f"verification error: {exc}"
                details = "Repository file could not be safely verified."
            _append_check(
                checks,
                "repository",
                f"planned_{category}_file_verified",
                passed,
                observed,
                relative.as_posix(),
                details,
            )

    planned_at = _utc_timestamp(plan["planned_at_utc"], "planned_at_utc")
    plan_age = (as_of - planned_at).total_seconds() / 60.0
    if plan_age < 0:
        raise FabricPilotAuthorizationError(
            "as_of_utc cannot precede the pilot planning timestamp."
        )
    _append_check(
        checks,
        "governance",
        "pilot_plan_age_minutes",
        plan_age <= max_plan_age,
        plan_age,
        f"<= {max_plan_age}",
        "Pilot plans must be refreshed after their allowed age boundary.",
    )

    captured_at = _utc_timestamp(snapshot["captured_at_utc"], "captured_at_utc")
    snapshot_age = (as_of - captured_at).total_seconds() / 60.0
    if snapshot_age < 0:
        raise FabricPilotAuthorizationError(
            "as_of_utc cannot precede the environment snapshot."
        )
    environment_checks = [
        ("environment_matches_plan", snapshot["environment"] == plan["environment"], snapshot["environment"], plan["environment"]),
        ("workspace_matches_plan", snapshot["workspace_name"] == plan["workspace_name"], snapshot["workspace_name"], plan["workspace_name"]),
        ("lakehouse_matches_plan", snapshot["lakehouse_name"] == plan["lakehouse_name"], snapshot["lakehouse_name"], plan["lakehouse_name"]),
        ("capacity_matches_plan", snapshot["capacity_name"] == plan["capacity_name"], snapshot["capacity_name"], plan["capacity_name"]),
        ("environment_snapshot_age_minutes", snapshot_age <= max_snapshot_age, snapshot_age, f"<= {max_snapshot_age}"),
        ("workspace_exists", snapshot["workspace_exists"], snapshot["workspace_exists"], True),
        ("lakehouse_exists", snapshot["lakehouse_exists"], snapshot["lakehouse_exists"], True),
        ("capacity_is_available", snapshot["capacity_available"], snapshot["capacity_available"], True),
        ("available_capacity_units", snapshot["available_capacity_units"] >= float(plan["max_capacity_units"]), snapshot["available_capacity_units"], f">= {plan['max_capacity_units']}"),
        ("capacity_utilization_pct", snapshot["current_capacity_utilization_pct"] <= max_utilization, snapshot["current_capacity_utilization_pct"], f"<= {max_utilization}"),
        ("active_job_count", snapshot["active_job_count"] <= max_jobs, snapshot["active_job_count"], f"<= {max_jobs}"),
        ("pilot_schedule_count", snapshot["pilot_schedule_count"] == 0, snapshot["pilot_schedule_count"], 0),
        ("active_model_is_control", snapshot["current_active_model"] == "ridge_weather_lag", snapshot["current_active_model"], "ridge_weather_lag"),
        ("environment_snapshot_contains_no_credential_values", snapshot["credential_values_included"] is False, snapshot["credential_values_included"], False),
    ]
    for name, passed, observed, expected in environment_checks:
        _append_check(
            checks,
            "environment",
            name,
            passed,
            observed,
            expected,
            "Operator-supplied non-production environment snapshot preflight.",
        )

    planned_references = set(plan.get("credential_references", []))
    available_reference_set = set(available_references)
    missing_references = sorted(planned_references - available_reference_set)
    _append_check(
        checks,
        "credentials",
        "planned_credential_references_available",
        not missing_references,
        ",".join(sorted(available_reference_set)),
        ",".join(sorted(planned_references)),
        (
            "All planned credential reference names are available without exposing values."
            if not missing_references
            else "Missing credential references: " + ", ".join(missing_references)
        ),
    )

    health_as_of = _utc_timestamp(health["monitor_as_of_utc"], "monitor_as_of_utc")
    health_age = (as_of - health_as_of).total_seconds() / 60.0
    if health_age < 0:
        raise FabricPilotAuthorizationError(
            "as_of_utc cannot precede provider-health evidence."
        )
    health_checks = [
        ("provider_health_age_minutes", health_age <= max_health_age, health_age, f"<= {max_health_age}"),
        ("provider_health_is_healthy", health["monitor_status"] == "healthy", health["monitor_status"], "healthy"),
        ("provider_health_failed_error_checks", health["failed_error_check_count"] == 0, health["failed_error_check_count"], 0),
        ("provider_health_prohibits_automatic_remediation", health["automatic_remediation_allowed"] is False, health["automatic_remediation_allowed"], False),
    ]
    for name, passed, observed, expected in health_checks:
        _append_check(
            checks,
            "provider_health",
            name,
            passed,
            observed,
            expected,
            "Fresh provider-health evidence is required before authorization.",
        )

    failed_count = sum(not check["passed"] for check in checks)
    core = {
        "pilot_id": plan["pilot_id"],
        "plan_hash": plan["plan_hash"],
        "candidate_id": plan["candidate_id"],
        "bundle_id": plan["bundle_id"],
        "recovery_id": plan["recovery_id"],
        "preflight_timestamp_utc": as_of.isoformat(),
        "repository_commit_sha": commit_sha,
        "repository_tree_sha": tree_sha,
        "provider_health_monitor_run_id": health["monitor_run_id"],
        "environment_snapshot_id": snapshot["snapshot_id"],
        "verified_credential_references": sorted(available_reference_set),
        "repository_files": repository_files,
        "checks": checks,
    }
    preflight_id = "fpf-" + _digest(core)[:24]
    document = {
        "preflight_id": preflight_id,
        "preflight_revision": 1,
        **core,
        "preflight_status": ELIGIBLE_STATUS if failed_count == 0 else BLOCKED_STATUS,
        "check_count": len(checks),
        "passed_check_count": len(checks) - failed_count,
        "failed_check_count": failed_count,
        "manual_authorization_required": True,
        "automatic_authorization_allowed": False,
        "execution_authorized": False,
        "execution_performed": False,
        "schedule_activation_allowed": False,
        "deployment_authorized": False,
        "active_model_unchanged": True,
        "credential_values_recorded": False,
        "source_evidence_mutated": False,
        "preflight_contract_version": PREFLIGHT_CONTRACT_VERSION,
    }
    document["preflight_hash"] = _digest(document, exclude=("preflight_hash",))
    verify_fabric_pilot_preflight(document)
    return document


def verify_fabric_pilot_preflight(document: dict[str, Any]) -> None:
    if not isinstance(document, dict):
        raise FabricPilotAuthorizationError("Preflight document must be a JSON object.")
    if document.get("preflight_contract_version") != PREFLIGHT_CONTRACT_VERSION:
        raise FabricPilotAuthorizationError("Unsupported Fabric pilot preflight contract.")
    if document.get("preflight_hash") != _digest(
        document, exclude=("preflight_hash",)
    ):
        raise FabricPilotAuthorizationError("Fabric pilot preflight hash is invalid.")
    if not PREFLIGHT_ID_PATTERN.fullmatch(
        _required_text(document.get("preflight_id"), "preflight_id")
    ):
        raise FabricPilotAuthorizationError("Preflight ID is malformed.")
    if document.get("preflight_revision") != 1:
        raise FabricPilotAuthorizationError("Preflight revision must be 1.")
    checks = document.get("checks")
    if not isinstance(checks, list) or not checks:
        raise FabricPilotAuthorizationError("Preflight must contain checks.")
    for sequence, check in enumerate(checks, start=1):
        if not isinstance(check, dict) or check.get("check_sequence") != sequence:
            raise FabricPilotAuthorizationError(
                "Preflight checks must be contiguous objects."
            )
        if not isinstance(check.get("passed"), bool):
            raise FabricPilotAuthorizationError("Preflight check passed must be boolean.")
    failed = sum(not check["passed"] for check in checks)
    expected_status = ELIGIBLE_STATUS if failed == 0 else BLOCKED_STATUS
    if document.get("preflight_status") != expected_status:
        raise FabricPilotAuthorizationError("Preflight status is inconsistent.")
    if document.get("check_count") != len(checks):
        raise FabricPilotAuthorizationError("Preflight check count is inconsistent.")
    if document.get("failed_check_count") != failed:
        raise FabricPilotAuthorizationError("Preflight failed-check count is inconsistent.")
    if document.get("passed_check_count") != len(checks) - failed:
        raise FabricPilotAuthorizationError("Preflight passed-check count is inconsistent.")
    safety = {
        "manual_authorization_required": True,
        "automatic_authorization_allowed": False,
        "execution_authorized": False,
        "execution_performed": False,
        "schedule_activation_allowed": False,
        "deployment_authorized": False,
        "active_model_unchanged": True,
        "credential_values_recorded": False,
        "source_evidence_mutated": False,
    }
    for field, expected in safety.items():
        if document.get(field) is not expected:
            raise FabricPilotAuthorizationError(
                f"Preflight safety flag {field} is invalid."
            )
    files = document.get("repository_files")
    if not isinstance(files, list):
        raise FabricPilotAuthorizationError("repository_files must be a list.")
    for sequence, item in enumerate(files, start=1):
        if item.get("file_sequence") != sequence:
            raise FabricPilotAuthorizationError(
                "Repository file manifest sequence is invalid."
            )
        if not SHA256_PATTERN.fullmatch(
            _required_text(item.get("sha256"), "repository file sha256")
        ):
            raise FabricPilotAuthorizationError("Repository file SHA-256 is malformed.")


def create_fabric_pilot_authorization(
    plan: dict[str, Any],
    preflight: dict[str, Any],
    *,
    confirm_pilot_id: str,
    confirm_preflight_id: str,
    authorizer: str,
    operator: str,
    review_ticket: str,
    reason: str,
    authorized_at_utc: Any | None = None,
    valid_from_utc: Any | None = None,
    valid_until_utc: Any,
    max_authorization_window_minutes: int = 480,
    max_preflight_age_minutes: int = 60,
) -> dict[str, Any]:
    """Create a single-use, time-bounded human authorization record."""
    try:
        verify_fabric_pilot_plan(plan)
    except FabricPilotError as exc:
        raise FabricPilotAuthorizationError(f"Pilot plan failed verification: {exc}") from exc
    verify_fabric_pilot_preflight(preflight)
    if preflight["preflight_status"] != ELIGIBLE_STATUS:
        raise FabricPilotAuthorizationError(
            "A blocked preflight cannot be authorized."
        )
    if preflight["failed_check_count"] != 0:
        raise FabricPilotAuthorizationError(
            "Preflight contains failed checks."
        )
    if preflight["pilot_id"] != plan["pilot_id"] or preflight["plan_hash"] != plan["plan_hash"]:
        raise FabricPilotAuthorizationError(
            "Preflight does not match the pilot plan."
        )
    if _required_text(confirm_pilot_id, "confirm_pilot_id") != plan["pilot_id"]:
        raise FabricPilotAuthorizationError(
            "confirm_pilot_id must exactly match the pilot plan."
        )
    if _required_text(confirm_preflight_id, "confirm_preflight_id") != preflight["preflight_id"]:
        raise FabricPilotAuthorizationError(
            "confirm_preflight_id must exactly match the preflight."
        )
    authorizer = _human_identity(authorizer, "authorizer")
    operator = _human_identity(operator, "operator")
    review_ticket = _required_text(review_ticket, "review_ticket")
    reason = _required_text(reason, "reason")
    if len(reason) < 20:
        raise FabricPilotAuthorizationError("reason must contain at least 20 characters.")

    authorized_at = _utc_timestamp(
        authorized_at_utc or datetime.now(timezone.utc), "authorized_at_utc"
    )
    valid_from = _utc_timestamp(valid_from_utc or authorized_at, "valid_from_utc")
    valid_until = _utc_timestamp(valid_until_utc, "valid_until_utc")
    if valid_from < authorized_at:
        raise FabricPilotAuthorizationError(
            "valid_from_utc cannot precede authorization creation."
        )
    if valid_until <= valid_from:
        raise FabricPilotAuthorizationError(
            "valid_until_utc must occur after valid_from_utc."
        )
    window_minutes = (valid_until - valid_from).total_seconds() / 60.0
    max_window = _non_negative_int(
        max_authorization_window_minutes, "max_authorization_window_minutes"
    )
    if max_window < 1 or window_minutes > max_window:
        raise FabricPilotAuthorizationError(
            "Authorization window exceeds the configured maximum."
        )
    required_window = int(plan["max_duration_minutes"]) + 15
    if window_minutes < required_window:
        raise FabricPilotAuthorizationError(
            "Authorization window must cover pilot duration plus a 15-minute safety margin."
        )
    preflight_as_of = _utc_timestamp(
        preflight["preflight_timestamp_utc"], "preflight_timestamp_utc"
    )
    preflight_age = (authorized_at - preflight_as_of).total_seconds() / 60.0
    if preflight_age < 0:
        raise FabricPilotAuthorizationError(
            "Authorization cannot precede the preflight timestamp."
        )
    max_preflight_age = _non_negative_int(
        max_preflight_age_minutes, "max_preflight_age_minutes"
    )
    if preflight_age > max_preflight_age:
        raise FabricPilotAuthorizationError(
            "Preflight is too old for authorization; rerun preflight."
        )

    core = {
        "pilot_id": plan["pilot_id"],
        "plan_hash": plan["plan_hash"],
        "preflight_id": preflight["preflight_id"],
        "preflight_hash": preflight["preflight_hash"],
        "candidate_id": plan["candidate_id"],
        "bundle_id": plan["bundle_id"],
        "recovery_id": plan["recovery_id"],
        "repository": plan["repository"],
        "code_commit_sha": plan["code_commit_sha"],
        "code_tree_sha": plan["code_tree_sha"],
        "environment": plan["environment"],
        "workspace_name": plan["workspace_name"],
        "lakehouse_name": plan["lakehouse_name"],
        "capacity_name": plan["capacity_name"],
        "authorizer": authorizer,
        "operator": operator,
        "review_ticket": review_ticket,
        "reason": reason,
        "authorized_at_utc": authorized_at.isoformat(),
        "valid_from_utc": valid_from.isoformat(),
        "valid_until_utc": valid_until.isoformat(),
        "authorization_window_minutes": window_minutes,
    }
    authorization_id = "fpa-" + _digest(core)[:24]
    authorization = {
        "authorization_id": authorization_id,
        "authorization_revision": 1,
        "authorization_state": "authorized",
        **core,
        "single_use": True,
        "authorization_consumed": False,
        "authorization_revocable": True,
        "execution_authorized": True,
        "execution_performed": False,
        "manual_execution_required": True,
        "automatic_execution_allowed": False,
        "schedule_activation_allowed": False,
        "deployment_authorized": False,
        "model_activation_authorized": False,
        "active_model_before": "ridge_weather_lag",
        "active_model_expected_after": "ridge_weather_lag",
        "active_model_unchanged": True,
        "credential_values_recorded": False,
        "source_evidence_mutated": False,
        "authorization_contract_version": AUTHORIZATION_CONTRACT_VERSION,
    }
    authorization["authorization_hash"] = _digest(
        authorization, exclude=("authorization_hash",)
    )
    verify_fabric_pilot_authorization(authorization)
    return authorization


def verify_fabric_pilot_authorization(
    authorization: dict[str, Any],
    *,
    as_of_utc: Any | None = None,
    require_current: bool = False,
) -> str:
    if not isinstance(authorization, dict):
        raise FabricPilotAuthorizationError(
            "Authorization document must be a JSON object."
        )
    if authorization.get("authorization_contract_version") != AUTHORIZATION_CONTRACT_VERSION:
        raise FabricPilotAuthorizationError(
            "Unsupported Fabric pilot authorization contract."
        )
    if authorization.get("authorization_hash") != _digest(
        authorization, exclude=("authorization_hash",)
    ):
        raise FabricPilotAuthorizationError("Fabric pilot authorization hash is invalid.")
    if not AUTHORIZATION_ID_PATTERN.fullmatch(
        _required_text(authorization.get("authorization_id"), "authorization_id")
    ):
        raise FabricPilotAuthorizationError("Authorization ID is malformed.")
    if authorization.get("authorization_revision") != 1:
        raise FabricPilotAuthorizationError("Authorization revision must be 1.")
    if authorization.get("authorization_state") != "authorized":
        raise FabricPilotAuthorizationError("Authorization state must be authorized.")
    _human_identity(authorization.get("authorizer"), "authorizer")
    _human_identity(authorization.get("operator"), "operator")
    authorized_at = _utc_timestamp(
        authorization.get("authorized_at_utc"), "authorized_at_utc"
    )
    valid_from = _utc_timestamp(
        authorization.get("valid_from_utc"), "valid_from_utc"
    )
    valid_until = _utc_timestamp(
        authorization.get("valid_until_utc"), "valid_until_utc"
    )
    if valid_from < authorized_at or valid_until <= valid_from:
        raise FabricPilotAuthorizationError(
            "Authorization timestamp ordering is invalid."
        )
    if authorization.get("single_use") is not True:
        raise FabricPilotAuthorizationError("Pilot authorization must be single-use.")
    safety = {
        "authorization_consumed": False,
        "authorization_revocable": True,
        "execution_authorized": True,
        "execution_performed": False,
        "manual_execution_required": True,
        "automatic_execution_allowed": False,
        "schedule_activation_allowed": False,
        "deployment_authorized": False,
        "model_activation_authorized": False,
        "active_model_unchanged": True,
        "credential_values_recorded": False,
        "source_evidence_mutated": False,
    }
    for field, expected in safety.items():
        if authorization.get(field) is not expected:
            raise FabricPilotAuthorizationError(
                f"Authorization safety flag {field} is invalid."
            )
    if authorization.get("active_model_before") != "ridge_weather_lag" or authorization.get(
        "active_model_expected_after"
    ) != "ridge_weather_lag":
        raise FabricPilotAuthorizationError(
            "Authorization cannot permit an active-model change."
        )
    status = "not_yet_valid"
    if as_of_utc is not None:
        as_of = _utc_timestamp(as_of_utc, "as_of_utc")
        if valid_from <= as_of < valid_until:
            status = "current"
        elif as_of >= valid_until:
            status = "expired"
        if require_current and status != "current":
            raise FabricPilotAuthorizationError(
                f"Authorization is not current; status={status}."
            )
    return status


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


def write_fabric_pilot_preflight(output_root: Path, document: dict[str, Any]) -> Path:
    verify_fabric_pilot_preflight(document)
    path = (
        Path(output_root)
        / document["pilot_id"]
        / f"pilot_preflight_{document['preflight_id']}.json"
    )
    return _write_json_exclusive(path, document)


def write_fabric_pilot_authorization(
    output_root: Path, authorization: dict[str, Any]
) -> Path:
    verify_fabric_pilot_authorization(authorization)
    path = (
        Path(output_root)
        / authorization["pilot_id"]
        / f"pilot_authorization_{authorization['authorization_id']}.json"
    )
    return _write_json_exclusive(path, authorization)


def load_fabric_pilot_preflight(path: Path) -> dict[str, Any]:
    source = Path(path)
    if source.is_symlink() or not source.is_file():
        raise FabricPilotAuthorizationError(
            "Preflight path must be a regular non-symlink file."
        )
    document = json.loads(source.read_text(encoding="utf-8"))
    verify_fabric_pilot_preflight(document)
    return deepcopy(document)


def load_fabric_pilot_authorization(path: Path) -> dict[str, Any]:
    source = Path(path)
    if source.is_symlink() or not source.is_file():
        raise FabricPilotAuthorizationError(
            "Authorization path must be a regular non-symlink file."
        )
    document = json.loads(source.read_text(encoding="utf-8"))
    verify_fabric_pilot_authorization(document)
    return deepcopy(document)

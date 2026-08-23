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
    verify_fabric_pilot_preflight,
)


RECEIPT_CONTRACT_VERSION = "controlled-fabric-pilot-run-receipt-v1"
ASSESSMENT_CONTRACT_VERSION = "controlled-fabric-pilot-run-assessment-v1"
RECEIPT_ID_PATTERN = re.compile(r"^fpr-[0-9a-f]{24}$")
ASSESSMENT_ID_PATTERN = re.compile(r"^fra-[0-9a-f]{24}$")
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
SECRET_REFERENCE_PATTERN = re.compile(r"^[A-Z][A-Z0-9_]{2,127}$")
ALLOWED_RUN_STATUSES = {"completed", "failed", "aborted", "rolled_back"}
ELIGIBLE_OUTCOME = "eligible_for_post_pilot_review"
FAILED_OUTCOME = "pilot_failed"
DEFAULT_REQUIRED_EVIDENCE_ROLES = (
    "run_log",
    "forecast_weather_output",
    "comparison_predictions",
    "comparison_metrics",
    "quality_results",
)

RUN_REPORT_REQUIRED = {
    "external_run_id",
    "operator",
    "run_status",
    "started_at_utc",
    "ended_at_utc",
    "environment",
    "workspace_name",
    "lakehouse_name",
    "capacity_name",
    "repository",
    "code_commit_sha",
    "code_tree_sha",
    "executed_notebook_paths",
    "written_tables",
    "credential_references_used",
    "forecast_record_count",
    "prediction_row_count",
    "metric_row_count",
    "failed_quality_check_count",
    "notebook_retry_count",
    "peak_capacity_units",
    "schedule_created",
    "deployment_performed",
    "model_activation_performed",
    "active_model_before",
    "active_model_after",
    "rollback_performed",
    "rollback_reason",
    "credential_values_included",
}


class FabricPilotReceiptError(ValueError):
    """Raised when a pilot receipt or post-run assessment is unsafe."""


def _required_text(value: Any, name: str) -> str:
    if value is None:
        raise FabricPilotReceiptError(f"{name} must be non-empty.")
    text = str(value).strip()
    if not text:
        raise FabricPilotReceiptError(f"{name} must be non-empty.")
    return text


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _utc_timestamp(value: Any, name: str) -> pd.Timestamp:
    try:
        timestamp = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise FabricPilotReceiptError(
            f"{name} must be a valid timezone-aware timestamp."
        ) from exc
    if pd.isna(timestamp) or timestamp.tzinfo is None:
        raise FabricPilotReceiptError(f"{name} must be timezone-aware.")
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
    raise FabricPilotReceiptError(f"{name} must be boolean.")


def _non_negative_int(value: Any, name: str) -> int:
    if isinstance(value, bool):
        raise FabricPilotReceiptError(f"{name} must be a non-negative integer.")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise FabricPilotReceiptError(
            f"{name} must be a non-negative integer."
        ) from exc
    if parsed < 0:
        raise FabricPilotReceiptError(f"{name} must be a non-negative integer.")
    return parsed


def _finite_number(value: Any, name: str, *, minimum: float | None = None) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise FabricPilotReceiptError(f"{name} must be numeric.") from exc
    if not pd.notna(parsed) or parsed in {float("inf"), float("-inf")}:
        raise FabricPilotReceiptError(f"{name} must be finite.")
    if minimum is not None and parsed < minimum:
        raise FabricPilotReceiptError(f"{name} must be at least {minimum}.")
    return parsed


def _unique_texts(values: Any, name: str) -> list[str]:
    if not isinstance(values, (list, tuple)):
        raise FabricPilotReceiptError(f"{name} must be a list.")
    result = [_required_text(value, name) for value in values]
    if len(set(result)) != len(result):
        raise FabricPilotReceiptError(f"{name} must not contain duplicates.")
    return result


def _secret_references(values: Any, name: str) -> list[str]:
    references = _unique_texts(values, name)
    for reference in references:
        if not SECRET_REFERENCE_PATTERN.fullmatch(reference):
            raise FabricPilotReceiptError(
                f"{name} must contain uppercase reference names only; "
                "values and assignments are forbidden."
            )
    return references


def _safe_relative(value: Any, name: str) -> Path:
    text = _required_text(value, name).replace("\\", "/")
    relative = Path(text)
    if relative.is_absolute() or ".." in relative.parts or "." in relative.parts:
        raise FabricPilotReceiptError(f"{name} must be a safe relative path.")
    return relative


def _evidence_root(path: Path) -> Path:
    root = Path(path)
    if root.is_symlink() or not root.is_dir():
        raise FabricPilotReceiptError(
            "evidence_root must be a regular non-symlink directory."
        )
    return root.resolve()


def _evidence_file(root: Path, relative: Path) -> Path:
    current = root
    for part in relative.parts:
        current = current / part
        if current.is_symlink():
            raise FabricPilotReceiptError(
                f"Evidence path contains a symbolic link: {relative.as_posix()}."
            )
    try:
        resolved = current.resolve(strict=True)
    except FileNotFoundError as exc:
        raise FabricPilotReceiptError(
            f"Evidence file is missing: {relative.as_posix()}."
        ) from exc
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise FabricPilotReceiptError(
            f"Evidence file escapes evidence_root: {relative.as_posix()}."
        ) from exc
    if not resolved.is_file():
        raise FabricPilotReceiptError(
            f"Evidence path is not a regular file: {relative.as_posix()}."
        )
    return resolved


def _verify_chain(
    plan: dict[str, Any],
    preflight: dict[str, Any],
    authorization: dict[str, Any],
) -> None:
    try:
        verify_fabric_pilot_plan(plan)
    except FabricPilotError as exc:
        raise FabricPilotReceiptError(f"Pilot plan failed verification: {exc}") from exc
    try:
        verify_fabric_pilot_preflight(preflight)
        verify_fabric_pilot_authorization(authorization)
    except FabricPilotAuthorizationError as exc:
        raise FabricPilotReceiptError(
            f"Pilot preflight or authorization failed verification: {exc}"
        ) from exc
    if preflight.get("pilot_id") != plan.get("pilot_id") or preflight.get(
        "plan_hash"
    ) != plan.get("plan_hash"):
        raise FabricPilotReceiptError("Preflight does not match the pilot plan.")
    if authorization.get("pilot_id") != plan.get("pilot_id") or authorization.get(
        "plan_hash"
    ) != plan.get("plan_hash"):
        raise FabricPilotReceiptError("Authorization does not match the pilot plan.")
    if authorization.get("preflight_id") != preflight.get(
        "preflight_id"
    ) or authorization.get("preflight_hash") != preflight.get("preflight_hash"):
        raise FabricPilotReceiptError("Authorization does not match the preflight.")


def _normalize_run_report(report: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(report, dict):
        raise FabricPilotReceiptError("run_report must be a JSON object.")
    missing = sorted(RUN_REPORT_REQUIRED - set(report))
    if missing:
        raise FabricPilotReceiptError(
            "Run report is missing required fields: " + ", ".join(missing) + "."
        )
    status = _required_text(report["run_status"], "run_status").lower()
    if status not in ALLOWED_RUN_STATUSES:
        raise FabricPilotReceiptError(
            "run_status must be one of: " + ", ".join(sorted(ALLOWED_RUN_STATUSES)) + "."
        )
    started = _utc_timestamp(report["started_at_utc"], "started_at_utc")
    ended = _utc_timestamp(report["ended_at_utc"], "ended_at_utc")
    if ended <= started:
        raise FabricPilotReceiptError("ended_at_utc must occur after started_at_utc.")
    rollback_performed = _boolean(report["rollback_performed"], "rollback_performed")
    rollback_reason = _optional_text(report["rollback_reason"])
    if rollback_performed and not rollback_reason:
        raise FabricPilotReceiptError(
            "rollback_reason is required when rollback_performed is true."
        )
    return {
        "external_run_id": _required_text(report["external_run_id"], "external_run_id"),
        "operator": _required_text(report["operator"], "operator"),
        "run_status": status,
        "started_at_utc": started.isoformat(),
        "ended_at_utc": ended.isoformat(),
        "duration_minutes": (ended - started).total_seconds() / 60.0,
        "environment": _required_text(report["environment"], "environment").lower(),
        "workspace_name": _required_text(report["workspace_name"], "workspace_name"),
        "lakehouse_name": _required_text(report["lakehouse_name"], "lakehouse_name"),
        "capacity_name": _required_text(report["capacity_name"], "capacity_name"),
        "repository": _required_text(report["repository"], "repository"),
        "code_commit_sha": _required_text(report["code_commit_sha"], "code_commit_sha").lower(),
        "code_tree_sha": _required_text(report["code_tree_sha"], "code_tree_sha").lower(),
        "executed_notebook_paths": _unique_texts(
            report["executed_notebook_paths"], "executed_notebook_paths"
        ),
        "written_tables": _unique_texts(report["written_tables"], "written_tables"),
        "credential_references_used": _secret_references(
            report["credential_references_used"], "credential_references_used"
        ),
        "forecast_record_count": _non_negative_int(
            report["forecast_record_count"], "forecast_record_count"
        ),
        "prediction_row_count": _non_negative_int(
            report["prediction_row_count"], "prediction_row_count"
        ),
        "metric_row_count": _non_negative_int(
            report["metric_row_count"], "metric_row_count"
        ),
        "failed_quality_check_count": _non_negative_int(
            report["failed_quality_check_count"], "failed_quality_check_count"
        ),
        "notebook_retry_count": _non_negative_int(
            report["notebook_retry_count"], "notebook_retry_count"
        ),
        "peak_capacity_units": _finite_number(
            report["peak_capacity_units"], "peak_capacity_units", minimum=0
        ),
        "schedule_created": _boolean(report["schedule_created"], "schedule_created"),
        "deployment_performed": _boolean(
            report["deployment_performed"], "deployment_performed"
        ),
        "model_activation_performed": _boolean(
            report["model_activation_performed"], "model_activation_performed"
        ),
        "active_model_before": _required_text(
            report["active_model_before"], "active_model_before"
        ),
        "active_model_after": _required_text(
            report["active_model_after"], "active_model_after"
        ),
        "rollback_performed": rollback_performed,
        "rollback_reason": rollback_reason,
        "credential_values_included": _boolean(
            report["credential_values_included"], "credential_values_included"
        ),
    }


def create_fabric_pilot_run_receipt(
    plan: dict[str, Any],
    preflight: dict[str, Any],
    authorization: dict[str, Any],
    run_report: dict[str, Any],
    evidence_root: Path,
    evidence_paths: dict[str, Any],
    *,
    confirm_authorization_id: str,
    recorded_at_utc: Any | None = None,
) -> dict[str, Any]:
    """Record one externally performed pilot without executing anything."""
    _verify_chain(plan, preflight, authorization)
    if _required_text(
        confirm_authorization_id, "confirm_authorization_id"
    ) != authorization["authorization_id"]:
        raise FabricPilotReceiptError(
            "confirm_authorization_id must exactly match the authorization."
        )
    report = _normalize_run_report(run_report)
    started = _utc_timestamp(report["started_at_utc"], "started_at_utc")
    try:
        verify_fabric_pilot_authorization(
            authorization, as_of_utc=started, require_current=True
        )
    except FabricPilotAuthorizationError as exc:
        raise FabricPilotReceiptError(
            f"Authorization was not current when the run started: {exc}"
        ) from exc
    if report["operator"] != authorization["operator"]:
        raise FabricPilotReceiptError(
            "Run operator does not match the authorized human operator."
        )
    for field in (
        "environment",
        "workspace_name",
        "lakehouse_name",
        "capacity_name",
        "repository",
        "code_commit_sha",
        "code_tree_sha",
    ):
        if report[field] != plan[field]:
            raise FabricPilotReceiptError(
                f"Run report does not match planned field {field}."
            )
    recorded_at = _utc_timestamp(
        recorded_at_utc or datetime.now(timezone.utc), "recorded_at_utc"
    )
    ended = _utc_timestamp(report["ended_at_utc"], "ended_at_utc")
    if recorded_at < ended:
        raise FabricPilotReceiptError(
            "recorded_at_utc cannot precede the run end timestamp."
        )
    if not isinstance(evidence_paths, dict) or not evidence_paths:
        raise FabricPilotReceiptError(
            "evidence_paths must map at least one unique evidence role to a file."
        )
    root = _evidence_root(evidence_root)
    manifest: list[dict[str, Any]] = []
    seen_paths: set[Path] = set()
    for role in sorted(evidence_paths):
        role_name = _required_text(role, "evidence_role")
        relative = _safe_relative(evidence_paths[role], "evidence_path")
        if relative in seen_paths:
            raise FabricPilotReceiptError(
                f"Evidence path is supplied more than once: {relative.as_posix()}."
            )
        seen_paths.add(relative)
        source = _evidence_file(root, relative)
        manifest.append(
            {
                "evidence_sequence": len(manifest) + 1,
                "evidence_role": role_name,
                "relative_path": relative.as_posix(),
                "size_bytes": source.stat().st_size,
                "sha256": _file_sha256(source),
            }
        )
    core = {
        "pilot_id": plan["pilot_id"],
        "plan_hash": plan["plan_hash"],
        "preflight_id": preflight["preflight_id"],
        "preflight_hash": preflight["preflight_hash"],
        "authorization_id": authorization["authorization_id"],
        "authorization_hash": authorization["authorization_hash"],
        "candidate_id": plan["candidate_id"],
        "bundle_id": plan["bundle_id"],
        "recovery_id": plan["recovery_id"],
        **report,
        "evidence_files": manifest,
        "recorded_at_utc": recorded_at.isoformat(),
    }
    receipt = {
        "receipt_id": "fpr-" + _digest(core)[:24],
        "receipt_revision": 1,
        "receipt_state": "recorded",
        **core,
        "single_use_authorization": True,
        "authorization_consumed": True,
        "execution_authorized": True,
        "execution_performed": True,
        "manual_execution_confirmed": True,
        "automatic_execution_used": False,
        "schedule_activation_allowed": False,
        "deployment_authorized": False,
        "model_activation_authorized": False,
        "credential_values_recorded": False,
        "source_evidence_mutated": False,
        "receipt_contract_version": RECEIPT_CONTRACT_VERSION,
    }
    receipt["receipt_hash"] = _digest(receipt, exclude=("receipt_hash",))
    verify_fabric_pilot_run_receipt(receipt)
    return receipt


def verify_fabric_pilot_run_receipt(receipt: dict[str, Any]) -> None:
    if not isinstance(receipt, dict):
        raise FabricPilotReceiptError("Run receipt must be a JSON object.")
    if receipt.get("receipt_contract_version") != RECEIPT_CONTRACT_VERSION:
        raise FabricPilotReceiptError("Unsupported Fabric pilot receipt contract.")
    if receipt.get("receipt_hash") != _digest(receipt, exclude=("receipt_hash",)):
        raise FabricPilotReceiptError("Fabric pilot receipt hash is invalid.")
    if not RECEIPT_ID_PATTERN.fullmatch(
        _required_text(receipt.get("receipt_id"), "receipt_id")
    ):
        raise FabricPilotReceiptError("Receipt ID is malformed.")
    if receipt.get("receipt_revision") != 1 or receipt.get("receipt_state") != "recorded":
        raise FabricPilotReceiptError("Receipt state or revision is invalid.")
    if receipt.get("run_status") not in ALLOWED_RUN_STATUSES:
        raise FabricPilotReceiptError("Receipt run status is invalid.")
    started = _utc_timestamp(receipt.get("started_at_utc"), "started_at_utc")
    ended = _utc_timestamp(receipt.get("ended_at_utc"), "ended_at_utc")
    recorded = _utc_timestamp(receipt.get("recorded_at_utc"), "recorded_at_utc")
    if not started < ended <= recorded:
        raise FabricPilotReceiptError("Receipt timestamp ordering is invalid.")
    manifest = receipt.get("evidence_files")
    if not isinstance(manifest, list) or not manifest:
        raise FabricPilotReceiptError("Receipt must contain evidence files.")
    roles: set[str] = set()
    paths: set[str] = set()
    for sequence, item in enumerate(manifest, start=1):
        if not isinstance(item, dict) or item.get("evidence_sequence") != sequence:
            raise FabricPilotReceiptError("Receipt evidence sequence is invalid.")
        role = _required_text(item.get("evidence_role"), "evidence_role")
        path = _required_text(item.get("relative_path"), "relative_path")
        if role in roles or path in paths:
            raise FabricPilotReceiptError("Receipt evidence roles and paths must be unique.")
        roles.add(role)
        paths.add(path)
        if not SHA256_PATTERN.fullmatch(
            _required_text(item.get("sha256"), "evidence sha256")
        ):
            raise FabricPilotReceiptError("Evidence SHA-256 is malformed.")
        _non_negative_int(item.get("size_bytes"), "size_bytes")
    safety = {
        "single_use_authorization": True,
        "authorization_consumed": True,
        "execution_authorized": True,
        "execution_performed": True,
        "manual_execution_confirmed": True,
        "automatic_execution_used": False,
        "schedule_activation_allowed": False,
        "deployment_authorized": False,
        "model_activation_authorized": False,
        "credential_values_recorded": False,
        "source_evidence_mutated": False,
    }
    for field, expected in safety.items():
        if receipt.get(field) is not expected:
            raise FabricPilotReceiptError(f"Receipt safety flag {field} is invalid.")
    _secret_references(
        receipt.get("credential_references_used"), "credential_references_used"
    )


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
        {
            "check_sequence": len(checks) + 1,
            "check_scope": scope,
            "check_name": name,
            "passed": bool(passed),
            "observed_value": str(observed),
            "expected_value": str(expected),
            "details": _required_text(details, "check details"),
        }
    )


def assess_fabric_pilot_run(
    plan: dict[str, Any],
    authorization: dict[str, Any],
    receipt: dict[str, Any],
    *,
    required_evidence_roles: Iterable[str] = DEFAULT_REQUIRED_EVIDENCE_ROLES,
    assessed_at_utc: Any | None = None,
) -> dict[str, Any]:
    """Assess one immutable receipt against the exact plan and safety limits."""
    try:
        verify_fabric_pilot_plan(plan)
        verify_fabric_pilot_authorization(authorization)
    except (FabricPilotError, FabricPilotAuthorizationError) as exc:
        raise FabricPilotReceiptError(f"Plan or authorization failed verification: {exc}") from exc
    verify_fabric_pilot_run_receipt(receipt)
    if receipt["pilot_id"] != plan["pilot_id"] or receipt["plan_hash"] != plan["plan_hash"]:
        raise FabricPilotReceiptError("Receipt does not match the pilot plan.")
    if receipt["authorization_id"] != authorization["authorization_id"] or receipt[
        "authorization_hash"
    ] != authorization["authorization_hash"]:
        raise FabricPilotReceiptError("Receipt does not match the authorization.")
    required_roles = _unique_texts(list(required_evidence_roles), "required_evidence_roles")
    assessed_at = _utc_timestamp(
        assessed_at_utc or datetime.now(timezone.utc), "assessed_at_utc"
    )
    if assessed_at < _utc_timestamp(receipt["recorded_at_utc"], "recorded_at_utc"):
        raise FabricPilotReceiptError("assessed_at_utc cannot precede receipt creation.")

    checks: list[dict[str, Any]] = []
    run_start = _utc_timestamp(receipt["started_at_utc"], "started_at_utc")
    run_end = _utc_timestamp(receipt["ended_at_utc"], "ended_at_utc")
    auth_start = _utc_timestamp(authorization["valid_from_utc"], "valid_from_utc")
    auth_end = _utc_timestamp(authorization["valid_until_utc"], "valid_until_utc")
    _append_check(
        checks,
        "authorization",
        "run_started_within_authorization_window",
        auth_start <= run_start < auth_end,
        run_start.isoformat(),
        f"[{auth_start.isoformat()}, {auth_end.isoformat()})",
        "The external run must start while the single-use authorization is current.",
    )
    _append_check(
        checks,
        "authorization",
        "run_ended_within_authorization_window",
        run_end <= auth_end,
        run_end.isoformat(),
        f"<= {auth_end.isoformat()}",
        "The external run must finish before the authorization expires.",
    )
    _append_check(
        checks,
        "authorization",
        "operator_matches_authorization",
        receipt["operator"] == authorization["operator"],
        receipt["operator"],
        authorization["operator"],
        "The named run operator must match the human authorization.",
    )
    for field in ("repository", "code_commit_sha", "code_tree_sha"):
        _append_check(
            checks,
            "authorization",
            f"{field}_matches_plan",
            receipt[field] == plan[field],
            receipt[field],
            plan[field],
            "The recorded code identity must match the candidate-bound pilot plan.",
        )

    limits = (
        ("duration_minutes", "max_duration_minutes"),
        ("peak_capacity_units", "max_capacity_units"),
        ("forecast_record_count", "max_forecast_records"),
        ("prediction_row_count", "max_prediction_rows"),
        ("metric_row_count", "max_metric_rows"),
        ("failed_quality_check_count", "max_failed_quality_checks"),
        ("notebook_retry_count", "max_notebook_retries"),
    )
    for observed_field, limit_field in limits:
        observed = receipt[observed_field]
        limit = plan[limit_field]
        _append_check(
            checks,
            "limits",
            f"{observed_field}_within_plan",
            observed <= limit,
            observed,
            f"<= {limit}",
            "The external pilot must remain within its immutable plan limit.",
        )

    executed = receipt["executed_notebook_paths"]
    planned = plan["notebook_paths"]
    notebooks_ok = executed == planned if receipt["run_status"] == "completed" else executed == planned[: len(executed)]
    _append_check(
        checks,
        "execution",
        "executed_notebooks_follow_plan",
        notebooks_ok,
        ",".join(executed),
        ",".join(planned),
        "Completed runs require the exact sequence; interrupted runs may record a valid prefix.",
    )
    unexpected_tables = sorted(set(receipt["written_tables"]) - set(plan["allowed_tables"]))
    _append_check(
        checks,
        "execution",
        "written_tables_are_allowlisted",
        not unexpected_tables,
        ",".join(receipt["written_tables"]),
        ",".join(plan["allowed_tables"]),
        "Unexpected tables: " + ", ".join(unexpected_tables) if unexpected_tables else "All written tables are allowlisted.",
    )
    unexpected_refs = sorted(
        set(receipt["credential_references_used"]) - set(plan["credential_references"])
    )
    _append_check(
        checks,
        "execution",
        "credential_references_follow_plan",
        not unexpected_refs,
        ",".join(receipt["credential_references_used"]),
        ",".join(plan["credential_references"]),
        "Unexpected credential references: " + ", ".join(unexpected_refs) if unexpected_refs else "Only planned credential references were recorded.",
    )

    for name, passed, observed, expected in (
        ("no_schedule_created", receipt["schedule_created"] is False, receipt["schedule_created"], False),
        ("no_deployment_performed", receipt["deployment_performed"] is False, receipt["deployment_performed"], False),
        ("no_model_activation_performed", receipt["model_activation_performed"] is False, receipt["model_activation_performed"], False),
        ("active_model_before_is_control", receipt["active_model_before"] == "ridge_weather_lag", receipt["active_model_before"], "ridge_weather_lag"),
        ("active_model_after_is_control", receipt["active_model_after"] == "ridge_weather_lag", receipt["active_model_after"], "ridge_weather_lag"),
        ("no_credential_values_included", receipt["credential_values_included"] is False, receipt["credential_values_included"], False),
    ):
        _append_check(
            checks,
            "safety",
            name,
            passed,
            observed,
            expected,
            "The pilot cannot schedule, deploy, activate a model, or record credential values.",
        )

    evidence_roles = {item["evidence_role"] for item in receipt["evidence_files"]}
    expected_roles = set(required_roles) if receipt["run_status"] == "completed" else {"run_log"}
    missing_roles = sorted(expected_roles - evidence_roles)
    _append_check(
        checks,
        "evidence",
        "required_evidence_roles_present",
        not missing_roles,
        ",".join(sorted(evidence_roles)),
        ",".join(sorted(expected_roles)),
        "Missing evidence roles: " + ", ".join(missing_roles) if missing_roles else "Required evidence roles are present.",
    )

    operational_failure = any(
        not check["passed"]
        for check in checks
        if check["check_scope"] in {"authorization", "limits", "execution", "safety", "evidence"}
    )
    rollback_required = receipt["run_status"] != "completed" or operational_failure
    _append_check(
        checks,
        "rollback",
        "rollback_performed_when_required",
        (not rollback_required) or receipt["rollback_performed"],
        receipt["rollback_performed"],
        rollback_required,
        "Failed, interrupted, or out-of-bounds runs require explicit rollback evidence.",
    )
    if receipt["rollback_performed"]:
        _append_check(
            checks,
            "rollback",
            "rollback_reason_recorded",
            bool(receipt["rollback_reason"]),
            receipt["rollback_reason"],
            "non-empty rollback reason",
            "Rollback evidence must explain why it was performed.",
        )

    failed = sum(not check["passed"] for check in checks)
    successful = receipt["run_status"] == "completed" and failed == 0
    core = {
        "pilot_id": plan["pilot_id"],
        "receipt_id": receipt["receipt_id"],
        "receipt_hash": receipt["receipt_hash"],
        "authorization_id": authorization["authorization_id"],
        "external_run_id": receipt["external_run_id"],
        "assessed_at_utc": assessed_at.isoformat(),
        "checks": checks,
    }
    assessment = {
        "assessment_id": "fra-" + _digest(core)[:24],
        "assessment_revision": 1,
        **core,
        "assessment_outcome": ELIGIBLE_OUTCOME if successful else FAILED_OUTCOME,
        "run_status": receipt["run_status"],
        "rollback_required": rollback_required,
        "rollback_performed": receipt["rollback_performed"],
        "check_count": len(checks),
        "passed_check_count": len(checks) - failed,
        "failed_check_count": failed,
        "post_pilot_human_decision_required": True,
        "automatic_model_activation_allowed": False,
        "deployment_authorized": False,
        "active_model_unchanged": receipt["active_model_after"] == "ridge_weather_lag",
        "source_evidence_mutated": False,
        "assessment_contract_version": ASSESSMENT_CONTRACT_VERSION,
    }
    assessment["assessment_hash"] = _digest(assessment, exclude=("assessment_hash",))
    verify_fabric_pilot_run_assessment(assessment)
    return assessment


def verify_fabric_pilot_run_assessment(assessment: dict[str, Any]) -> None:
    if not isinstance(assessment, dict):
        raise FabricPilotReceiptError("Run assessment must be a JSON object.")
    if assessment.get("assessment_contract_version") != ASSESSMENT_CONTRACT_VERSION:
        raise FabricPilotReceiptError("Unsupported Fabric pilot assessment contract.")
    if assessment.get("assessment_hash") != _digest(
        assessment, exclude=("assessment_hash",)
    ):
        raise FabricPilotReceiptError("Fabric pilot assessment hash is invalid.")
    if not ASSESSMENT_ID_PATTERN.fullmatch(
        _required_text(assessment.get("assessment_id"), "assessment_id")
    ):
        raise FabricPilotReceiptError("Assessment ID is malformed.")
    if assessment.get("assessment_revision") != 1:
        raise FabricPilotReceiptError("Assessment revision must be 1.")
    checks = assessment.get("checks")
    if not isinstance(checks, list) or not checks:
        raise FabricPilotReceiptError("Assessment must contain checks.")
    for sequence, check in enumerate(checks, start=1):
        if not isinstance(check, dict) or check.get("check_sequence") != sequence:
            raise FabricPilotReceiptError("Assessment checks must be contiguous.")
        if not isinstance(check.get("passed"), bool):
            raise FabricPilotReceiptError("Assessment check passed must be boolean.")
    failed = sum(not check["passed"] for check in checks)
    if assessment.get("failed_check_count") != failed:
        raise FabricPilotReceiptError("Assessment failed-check count is inconsistent.")
    if assessment.get("passed_check_count") != len(checks) - failed:
        raise FabricPilotReceiptError("Assessment passed-check count is inconsistent.")
    if assessment.get("check_count") != len(checks):
        raise FabricPilotReceiptError("Assessment check count is inconsistent.")
    expected_outcome = (
        ELIGIBLE_OUTCOME
        if assessment.get("run_status") == "completed" and failed == 0
        else FAILED_OUTCOME
    )
    if assessment.get("assessment_outcome") != expected_outcome:
        raise FabricPilotReceiptError("Assessment outcome is inconsistent.")
    for field, expected in {
        "post_pilot_human_decision_required": True,
        "automatic_model_activation_allowed": False,
        "deployment_authorized": False,
        "source_evidence_mutated": False,
    }.items():
        if assessment.get(field) is not expected:
            raise FabricPilotReceiptError(
                f"Assessment safety flag {field} is invalid."
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


def write_fabric_pilot_run_receipt(output_root: Path, receipt: dict[str, Any]) -> Path:
    verify_fabric_pilot_run_receipt(receipt)
    return _write_json_exclusive(
        Path(output_root)
        / receipt["pilot_id"]
        / f"pilot_run_receipt_{receipt['authorization_id']}.json",
        receipt,
    )


def write_fabric_pilot_run_assessment(
    output_root: Path, assessment: dict[str, Any]
) -> Path:
    verify_fabric_pilot_run_assessment(assessment)
    return _write_json_exclusive(
        Path(output_root)
        / assessment["pilot_id"]
        / f"pilot_run_assessment_{assessment['receipt_id']}.json",
        assessment,
    )


def load_fabric_pilot_run_receipt(path: Path) -> dict[str, Any]:
    source = Path(path)
    if source.is_symlink() or not source.is_file():
        raise FabricPilotReceiptError("Receipt path must be a regular non-symlink file.")
    receipt = json.loads(source.read_text(encoding="utf-8"))
    verify_fabric_pilot_run_receipt(receipt)
    return deepcopy(receipt)


def load_fabric_pilot_run_assessment(path: Path) -> dict[str, Any]:
    source = Path(path)
    if source.is_symlink() or not source.is_file():
        raise FabricPilotReceiptError(
            "Assessment path must be a regular non-symlink file."
        )
    assessment = json.loads(source.read_text(encoding="utf-8"))
    verify_fabric_pilot_run_assessment(assessment)
    return deepcopy(assessment)

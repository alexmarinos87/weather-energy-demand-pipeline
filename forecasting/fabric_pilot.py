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
from forecasting.model_registry import load_candidate_history


PILOT_PLAN_CONTRACT_VERSION = "controlled-fabric-pilot-plan-v1"
PILOT_ID_PATTERN = re.compile(r"^fpl-[0-9a-f]{24}$")
SHA_PATTERN = re.compile(r"^[0-9a-f]{40}$")
SECRET_REFERENCE_PATTERN = re.compile(r"^[A-Z][A-Z0-9_]{2,127}$")
ALLOWED_ENVIRONMENTS = {"sandbox", "development", "test", "non-production"}
DEFAULT_NOTEBOOK_PATHS = (
    "fabric/notebooks/01b_ingest_forecast_weather_to_bronze.py",
    "fabric/notebooks/02b_forecast_weather_to_silver.py",
    "fabric/notebooks/05c_target_weather_model_comparison.py",
    "fabric/notebooks/06c_target_weather_comparison_quality_checks.py",
)
DEFAULT_CONTRACT_PATHS = (
    "data-contracts/source_areas.json",
    "data-contracts/openweather_forecast_raw_schema.json",
    "data-contracts/forecast_weather_schema.json",
)
DEFAULT_ALLOWED_TABLES = (
    "silver_forecast_weather",
    "forecast_weather_comparison_predictions",
    "forecast_weather_comparison_metrics",
    "dq_run_results",
)
DEFAULT_ROLLBACK_STEPS = (
    "Stop the manual pilot sequence and do not retry automatically.",
    "Preserve pilot logs, outputs, and quality evidence without overwriting prior evidence.",
    "Confirm ridge_weather_lag remains the active model and no schedule was created.",
)


class FabricPilotError(ValueError):
    """Raised when a controlled Fabric pilot plan is unsafe or inconsistent."""


def _required_text(value: Any, name: str) -> str:
    if value is None:
        raise FabricPilotError(f"{name} must be non-empty.")
    text = str(value).strip()
    if not text:
        raise FabricPilotError(f"{name} must be non-empty.")
    return text


def _utc_iso(value: Any, name: str) -> str:
    try:
        timestamp = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise FabricPilotError(
            f"{name} must be a valid timezone-aware timestamp."
        ) from exc
    if pd.isna(timestamp) or timestamp.tzinfo is None:
        raise FabricPilotError(f"{name} must be timezone-aware.")
    return timestamp.tz_convert("UTC").isoformat()


def _positive_int(value: Any, name: str, *, maximum: int | None = None) -> int:
    if isinstance(value, bool):
        raise FabricPilotError(f"{name} must be a positive integer.")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise FabricPilotError(f"{name} must be a positive integer.") from exc
    if parsed < 1 or (maximum is not None and parsed > maximum):
        suffix = f" and no greater than {maximum}" if maximum is not None else ""
        raise FabricPilotError(
            f"{name} must be a positive integer{suffix}."
        )
    return parsed


def _positive_float(value: Any, name: str, *, maximum: float | None = None) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise FabricPilotError(f"{name} must be positive.") from exc
    if not pd.notna(parsed) or parsed <= 0 or (
        maximum is not None and parsed > maximum
    ):
        suffix = f" and no greater than {maximum}" if maximum is not None else ""
        raise FabricPilotError(f"{name} must be positive{suffix}.")
    return parsed


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
    encoded = json.dumps(
        _canonical(material),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _unique_texts(values: Iterable[Any], name: str) -> list[str]:
    result = [_required_text(value, name) for value in values]
    if not result:
        raise FabricPilotError(f"{name} must contain at least one value.")
    if len(set(result)) != len(result):
        raise FabricPilotError(f"{name} must not contain duplicates.")
    return result


def _safe_repository_path(value: Any, name: str) -> str:
    text = _required_text(value, name).replace("\\", "/")
    path = Path(text)
    if path.is_absolute() or ".." in path.parts or "." in path.parts:
        raise FabricPilotError(f"{name} must be a safe repository-relative path.")
    return path.as_posix()


def _secret_references(values: Iterable[Any]) -> list[str]:
    references = _unique_texts(values, "credential_reference")
    for reference in references:
        if not SECRET_REFERENCE_PATTERN.fullmatch(reference):
            raise FabricPilotError(
                "credential references must be uppercase names only; "
                "credential values and assignments are forbidden."
            )
    return references


def _environment(value: Any) -> str:
    environment = _required_text(value, "environment").lower()
    if environment not in ALLOWED_ENVIRONMENTS:
        raise FabricPilotError(
            "environment must be one of: "
            + ", ".join(sorted(ALLOWED_ENVIRONMENTS))
            + "."
        )
    return environment


def _verified_inputs(
    candidate_directory: Path,
    evidence_bundle_path: Path,
    recovered_bundle_directory: Path,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    try:
        _, _, candidate = load_candidate_history(Path(candidate_directory))
    except Exception as exc:
        raise FabricPilotError(f"Candidate history failed verification: {exc}") from exc
    if candidate.get("candidate_state") != "approved":
        raise FabricPilotError("A controlled pilot requires an approved candidate.")
    if candidate.get("deployment_authorized") is not False:
        raise FabricPilotError("Candidate history cannot authorize deployment.")
    if candidate.get("active_model_unchanged") is not True:
        raise FabricPilotError("Candidate history must retain the active model.")

    try:
        bundle_manifest, bundle_verification = verify_evidence_bundle(
            Path(evidence_bundle_path)
        )
    except Exception as exc:
        raise FabricPilotError(f"Evidence bundle failed verification: {exc}") from exc
    try:
        recovery_verification = verify_recovered_bundle(
            Path(recovered_bundle_directory)
        )
    except Exception as exc:
        raise FabricPilotError(f"Recovered evidence failed verification: {exc}") from exc

    candidate_id = _required_text(candidate.get("candidate_id"), "candidate_id")
    if bundle_manifest.get("candidate_id") != candidate_id:
        raise FabricPilotError("Evidence bundle does not match the approved candidate.")
    if recovery_verification.get("candidate_id") != candidate_id:
        raise FabricPilotError("Recovered evidence does not match the approved candidate.")
    if recovery_verification.get("bundle_id") != bundle_manifest.get("bundle_id"):
        raise FabricPilotError("Recovered evidence does not match the verified bundle.")
    for field in ("code_commit_sha", "code_tree_sha"):
        if bundle_manifest.get(field) != candidate.get(field):
            raise FabricPilotError(
                f"Evidence bundle does not match candidate field {field}."
            )
    if bundle_verification.get("verification_status") != "verified":
        raise FabricPilotError("Evidence bundle verification is not successful.")
    if recovery_verification.get("verification_status") != "verified":
        raise FabricPilotError("Recovered evidence verification is not successful.")
    return candidate, bundle_manifest, bundle_verification, recovery_verification


def create_fabric_pilot_plan(
    candidate_directory: Path,
    evidence_bundle_path: Path,
    recovered_bundle_directory: Path,
    *,
    environment: str,
    workspace_name: str,
    lakehouse_name: str,
    capacity_name: str,
    credential_references: Iterable[Any] = ("OPENWEATHER_API_KEY",),
    notebook_paths: Iterable[Any] = DEFAULT_NOTEBOOK_PATHS,
    contract_paths: Iterable[Any] = DEFAULT_CONTRACT_PATHS,
    allowed_tables: Iterable[Any] = DEFAULT_ALLOWED_TABLES,
    rollback_steps: Iterable[Any] = DEFAULT_ROLLBACK_STEPS,
    max_duration_minutes: int = 60,
    max_capacity_units: float = 16.0,
    max_forecast_records: int = 40,
    max_prediction_rows: int = 100_000,
    max_metric_rows: int = 10_000,
    max_failed_quality_checks: int = 0,
    max_notebook_retries: int = 0,
    actor: str,
    review_ticket: str,
    reason: str,
    planned_at_utc: Any | None = None,
) -> dict[str, Any]:
    """Create a deterministic, non-executing controlled Fabric pilot plan."""
    candidate, bundle, bundle_verification, recovery = _verified_inputs(
        candidate_directory, evidence_bundle_path, recovered_bundle_directory
    )
    environment = _environment(environment)
    workspace_name = _required_text(workspace_name, "workspace_name")
    lakehouse_name = _required_text(lakehouse_name, "lakehouse_name")
    capacity_name = _required_text(capacity_name, "capacity_name")
    actor = _required_text(actor, "actor")
    review_ticket = _required_text(review_ticket, "review_ticket")
    reason = _required_text(reason, "reason")
    if len(reason) < 20:
        raise FabricPilotError("reason must contain at least 20 characters.")
    planned_at = _utc_iso(
        planned_at_utc or datetime.now(timezone.utc), "planned_at_utc"
    )

    notebook_list = [
        _safe_repository_path(value, "notebook_path")
        for value in _unique_texts(notebook_paths, "notebook_path")
    ]
    contract_list = [
        _safe_repository_path(value, "contract_path")
        for value in _unique_texts(contract_paths, "contract_path")
    ]
    table_list = _unique_texts(allowed_tables, "allowed_table")
    rollback_list = _unique_texts(rollback_steps, "rollback_step")
    if len(rollback_list) < 3:
        raise FabricPilotError("rollback_steps must contain at least three steps.")
    secret_list = _secret_references(credential_references)

    commit_sha = _required_text(candidate.get("code_commit_sha"), "code_commit_sha")
    tree_sha = _required_text(candidate.get("code_tree_sha"), "code_tree_sha")
    if not SHA_PATTERN.fullmatch(commit_sha) or not SHA_PATTERN.fullmatch(tree_sha):
        raise FabricPilotError("Candidate code SHAs must be lowercase 40-character hex.")

    core = {
        "candidate_id": candidate["candidate_id"],
        "candidate_version": candidate["candidate_version"],
        "bundle_id": bundle["bundle_id"],
        "bundle_sha256": bundle_verification["bundle_sha256"],
        "recovery_id": recovery["recovery_id"],
        "repository": candidate["repository"],
        "code_commit_sha": commit_sha,
        "code_tree_sha": tree_sha,
        "environment": environment,
        "workspace_name": workspace_name,
        "lakehouse_name": lakehouse_name,
        "capacity_name": capacity_name,
        "credential_references": secret_list,
        "notebook_paths": notebook_list,
        "contract_paths": contract_list,
        "allowed_tables": table_list,
        "rollback_steps": rollback_list,
        "max_duration_minutes": _positive_int(
            max_duration_minutes, "max_duration_minutes", maximum=240
        ),
        "max_capacity_units": _positive_float(
            max_capacity_units, "max_capacity_units", maximum=128.0
        ),
        "max_forecast_records": _positive_int(
            max_forecast_records, "max_forecast_records", maximum=40
        ),
        "max_prediction_rows": _positive_int(
            max_prediction_rows, "max_prediction_rows", maximum=10_000_000
        ),
        "max_metric_rows": _positive_int(
            max_metric_rows, "max_metric_rows", maximum=1_000_000
        ),
        "max_failed_quality_checks": int(max_failed_quality_checks),
        "max_notebook_retries": int(max_notebook_retries),
        "actor": actor,
        "review_ticket": review_ticket,
        "reason": reason,
        "planned_at_utc": planned_at,
    }
    for name in ("max_failed_quality_checks", "max_notebook_retries"):
        value = core[name]
        if isinstance(value, bool) or value < 0:
            raise FabricPilotError(f"{name} must be a non-negative integer.")

    pilot_id = "fpl-" + _digest(core)[:24]
    plan = {
        "pilot_id": pilot_id,
        "pilot_revision": 1,
        "pilot_state": "draft",
        **core,
        "promotion_assessment_id": candidate["promotion_assessment_id"],
        "comparison_run_id": candidate["comparison_run_id"],
        "reconciliation_run_id": candidate["reconciliation_run_id"],
        "provider_health_monitor_run_id": candidate[
            "provider_health_monitor_run_id"
        ],
        "candidate_manifest_hash": candidate["manifest_hash"],
        "bundle_manifest_hash": bundle["manifest_hash"],
        "execution_mode": "manual",
        "manual_execution_required": True,
        "automatic_execution_allowed": False,
        "schedule_activation_allowed": False,
        "execution_authorized": False,
        "execution_performed": False,
        "deployment_authorized": False,
        "active_model_before": "ridge_weather_lag",
        "active_model_expected_after": "ridge_weather_lag",
        "active_model_unchanged": True,
        "credential_values_recorded": False,
        "source_evidence_mutated": False,
        "pilot_plan_contract_version": PILOT_PLAN_CONTRACT_VERSION,
    }
    plan["plan_hash"] = _digest(plan, exclude=("plan_hash",))
    verify_fabric_pilot_plan(plan)
    return plan


def verify_fabric_pilot_plan(plan: dict[str, Any]) -> None:
    """Verify the plan hash and non-production, non-executing safety boundary."""
    if not isinstance(plan, dict):
        raise FabricPilotError("Pilot plan must be a JSON object.")
    if plan.get("pilot_plan_contract_version") != PILOT_PLAN_CONTRACT_VERSION:
        raise FabricPilotError("Unsupported Fabric pilot plan contract.")
    if plan.get("plan_hash") != _digest(plan, exclude=("plan_hash",)):
        raise FabricPilotError("Fabric pilot plan hash is invalid.")
    pilot_id = _required_text(plan.get("pilot_id"), "pilot_id")
    if not PILOT_ID_PATTERN.fullmatch(pilot_id):
        raise FabricPilotError("Pilot ID is malformed.")
    if plan.get("pilot_revision") != 1 or plan.get("pilot_state") != "draft":
        raise FabricPilotError("A new pilot plan must be draft revision 1.")
    _environment(plan.get("environment"))
    for field in ("code_commit_sha", "code_tree_sha"):
        if not SHA_PATTERN.fullmatch(_required_text(plan.get(field), field)):
            raise FabricPilotError(f"{field} is malformed.")
    safety_expectations = {
        "manual_execution_required": True,
        "automatic_execution_allowed": False,
        "schedule_activation_allowed": False,
        "execution_authorized": False,
        "execution_performed": False,
        "deployment_authorized": False,
        "active_model_unchanged": True,
        "credential_values_recorded": False,
        "source_evidence_mutated": False,
    }
    for field, expected in safety_expectations.items():
        if plan.get(field) is not expected:
            raise FabricPilotError(f"Pilot safety flag {field} is invalid.")
    if plan.get("execution_mode") != "manual":
        raise FabricPilotError("Pilot execution mode must remain manual.")
    if plan.get("active_model_before") != "ridge_weather_lag":
        raise FabricPilotError("Pilot baseline model is invalid.")
    if plan.get("active_model_expected_after") != "ridge_weather_lag":
        raise FabricPilotError("Pilot cannot plan to change the active model.")
    _secret_references(plan.get("credential_references", ()))
    for field in ("notebook_paths", "contract_paths"):
        values = plan.get(field)
        if not isinstance(values, list):
            raise FabricPilotError(f"{field} must be a list.")
        safe = [_safe_repository_path(value, field[:-1]) for value in values]
        if len(set(safe)) != len(safe) or not safe:
            raise FabricPilotError(f"{field} must contain unique values.")
    for field in ("allowed_tables", "rollback_steps"):
        values = plan.get(field)
        if not isinstance(values, list) or not values:
            raise FabricPilotError(f"{field} must be a non-empty list.")
        if len(set(values)) != len(values):
            raise FabricPilotError(f"{field} must not contain duplicates.")
    if len(plan["rollback_steps"]) < 3:
        raise FabricPilotError("Pilot plan must contain at least three rollback steps.")
    _positive_int(plan.get("max_duration_minutes"), "max_duration_minutes", maximum=240)
    _positive_float(plan.get("max_capacity_units"), "max_capacity_units", maximum=128.0)
    _positive_int(plan.get("max_forecast_records"), "max_forecast_records", maximum=40)
    _positive_int(plan.get("max_prediction_rows"), "max_prediction_rows", maximum=10_000_000)
    _positive_int(plan.get("max_metric_rows"), "max_metric_rows", maximum=1_000_000)
    for field in ("max_failed_quality_checks", "max_notebook_retries"):
        value = plan.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise FabricPilotError(f"{field} must be a non-negative integer.")


def write_fabric_pilot_plan(
    output_root: Path, plan: dict[str, Any]
) -> Path:
    """Write one immutable plan file."""
    verify_fabric_pilot_plan(plan)
    root = Path(output_root)
    pilot_directory = root / plan["pilot_id"]
    path = pilot_directory / "pilot_plan_v001.json"
    temporary = path.with_suffix(".tmp")
    for candidate in (path, temporary):
        if candidate.exists() or candidate.is_symlink():
            raise FileExistsError(f"Refusing to overwrite {candidate}.")
    pilot_directory.mkdir(parents=True, exist_ok=True)
    try:
        with temporary.open("x", encoding="utf-8") as handle:
            json.dump(_canonical(plan), handle, indent=2, sort_keys=True)
            handle.write("\n")
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)
    return path


def load_fabric_pilot_plan(path: Path) -> dict[str, Any]:
    source = Path(path)
    if source.is_symlink() or not source.is_file():
        raise FabricPilotError("Pilot plan path must be a regular file.")
    plan = json.loads(source.read_text(encoding="utf-8"))
    verify_fabric_pilot_plan(plan)
    return deepcopy(plan)

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path

import pandas as pd
import pytest
from jsonschema import Draft202012Validator, FormatChecker

import forecasting.fabric_pilot as pilot_plan
import forecasting.fabric_pilot_authorization as pilot_auth
from forecasting.fabric_pilot_authorization import (
    BLOCKED_STATUS,
    ELIGIBLE_STATUS,
    FabricPilotAuthorizationError,
    assess_fabric_pilot_preflight,
    create_fabric_pilot_authorization,
    load_fabric_pilot_authorization,
    load_fabric_pilot_preflight,
    verify_fabric_pilot_authorization,
    verify_fabric_pilot_preflight,
    write_fabric_pilot_authorization,
    write_fabric_pilot_preflight,
)


COMMIT = "a" * 40
TREE = "b" * 40
BUNDLE_SHA = "c" * 64
MANIFEST_HASH = "d" * 64
BUNDLE_MANIFEST_HASH = "e" * 64
AS_OF = pd.Timestamp("2026-08-23T12:00:00Z")


def build_plan() -> dict:
    core = {
        "candidate_id": "twc-candidate",
        "candidate_version": "0.1.0",
        "bundle_id": "ceb-bundle",
        "bundle_sha256": BUNDLE_SHA,
        "recovery_id": "rcv-recovery",
        "repository": "alexmarinos87/weather-energy-demand-pipeline",
        "code_commit_sha": COMMIT,
        "code_tree_sha": TREE,
        "environment": "non-production",
        "workspace_name": "weather-pilot",
        "lakehouse_name": "weather_energy_lakehouse",
        "capacity_name": "sandbox-capacity",
        "credential_references": ["OPENWEATHER_API_KEY"],
        "notebook_paths": [
            "fabric/notebooks/01b_ingest_forecast_weather_to_bronze.py",
            "fabric/notebooks/02b_forecast_weather_to_silver.py",
        ],
        "contract_paths": [
            "data-contracts/source_areas.json",
            "data-contracts/forecast_weather_schema.json",
        ],
        "allowed_tables": [
            "silver_forecast_weather",
            "forecast_weather_comparison_predictions",
        ],
        "rollback_steps": [
            "Stop the pilot.",
            "Preserve evidence.",
            "Confirm the control model remains active.",
        ],
        "max_duration_minutes": 60,
        "max_capacity_units": 16.0,
        "max_forecast_records": 40,
        "max_prediction_rows": 100000,
        "max_metric_rows": 10000,
        "max_failed_quality_checks": 0,
        "max_notebook_retries": 0,
        "actor": "alexmarinos87",
        "review_ticket": "PILOT-001",
        "reason": "Prepare a bounded non-production Fabric pilot.",
        "planned_at_utc": "2026-08-23T11:00:00+00:00",
    }
    pilot_id = "fpl-" + pilot_plan._digest(core)[:24]
    plan = {
        "pilot_id": pilot_id,
        "pilot_revision": 1,
        "pilot_state": "draft",
        **core,
        "promotion_assessment_id": "assessment-1",
        "comparison_run_id": "comparison-1",
        "reconciliation_run_id": "reconciliation-1",
        "provider_health_monitor_run_id": "historical-monitor",
        "candidate_manifest_hash": MANIFEST_HASH,
        "bundle_manifest_hash": BUNDLE_MANIFEST_HASH,
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
        "pilot_plan_contract_version": "controlled-fabric-pilot-plan-v1",
    }
    plan["plan_hash"] = pilot_plan._digest(plan, exclude=("plan_hash",))
    pilot_plan.verify_fabric_pilot_plan(plan)
    return plan


def candidate(state: str = "approved") -> dict:
    return {
        "candidate_id": "twc-candidate",
        "candidate_state": state,
        "manifest_hash": MANIFEST_HASH,
        "deployment_authorized": False,
        "active_model_unchanged": True,
    }


def bundle(candidate_id: str = "twc-candidate"):
    return (
        {
            "bundle_id": "ceb-bundle",
            "candidate_id": candidate_id,
            "manifest_hash": BUNDLE_MANIFEST_HASH,
        },
        {
            "bundle_id": "ceb-bundle",
            "bundle_sha256": BUNDLE_SHA,
            "candidate_id": candidate_id,
            "verification_status": "verified",
        },
    )


def recovery(candidate_id: str = "twc-candidate") -> dict:
    return {
        "recovery_id": "rcv-recovery",
        "bundle_id": "ceb-bundle",
        "candidate_id": candidate_id,
        "verification_status": "verified",
    }


def health(*, status: str = "healthy", age_minutes: int = 10) -> pd.DataFrame:
    monitored = AS_OF - pd.Timedelta(minutes=age_minutes)
    return pd.DataFrame(
        [
            {
                "monitor_run_id": "fresh-monitor",
                "monitor_timestamp_utc": monitored,
                "monitor_as_of_utc": monitored,
                "monitor_status": status,
                "automatic_remediation_allowed": False,
                "failed_error_check_count": 0 if status == "healthy" else 1,
            }
        ]
    )


def environment(**overrides) -> dict:
    snapshot = {
        "snapshot_id": "env-snapshot-1",
        "captured_at_utc": (AS_OF - pd.Timedelta(minutes=5)).isoformat(),
        "environment": "non-production",
        "workspace_name": "weather-pilot",
        "lakehouse_name": "weather_energy_lakehouse",
        "capacity_name": "sandbox-capacity",
        "workspace_exists": True,
        "lakehouse_exists": True,
        "capacity_available": True,
        "available_capacity_units": 32.0,
        "current_capacity_utilization_pct": 20.0,
        "active_job_count": 0,
        "pilot_schedule_count": 0,
        "current_active_model": "ridge_weather_lag",
        "credential_values_included": False,
    }
    snapshot.update(overrides)
    return snapshot


@pytest.fixture
def repository_root(tmp_path: Path) -> Path:
    files = {
        "fabric/notebooks/01b_ingest_forecast_weather_to_bronze.py": "print('one')\n",
        "fabric/notebooks/02b_forecast_weather_to_silver.py": "print('two')\n",
        "data-contracts/source_areas.json": "{}\n",
        "data-contracts/forecast_weather_schema.json": "{}\n",
    }
    for relative, content in files.items():
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
    return tmp_path


@pytest.fixture
def verified_sources(monkeypatch):
    monkeypatch.setattr(
        pilot_auth, "load_candidate_history", lambda path: ([], [], candidate())
    )
    monkeypatch.setattr(pilot_auth, "verify_evidence_bundle", lambda path: bundle())
    monkeypatch.setattr(
        pilot_auth, "verify_recovered_bundle", lambda path: recovery()
    )


def preflight(repository_root: Path, **overrides) -> dict:
    kwargs = dict(
        plan=build_plan(),
        candidate_directory=Path("candidate"),
        evidence_bundle_path=Path("bundle.tar"),
        recovered_bundle_directory=Path("recovered"),
        repository_root=repository_root,
        provider_health_summary=health(),
        environment_snapshot=environment(),
        current_code_commit_sha=COMMIT,
        current_code_tree_sha=TREE,
        available_credential_references=("OPENWEATHER_API_KEY",),
        as_of_utc=AS_OF,
    )
    kwargs.update(overrides)
    return assess_fabric_pilot_preflight(**kwargs)


def authorization(plan: dict, preflight_document: dict, **overrides) -> dict:
    kwargs = dict(
        plan=plan,
        preflight=preflight_document,
        confirm_pilot_id=plan["pilot_id"],
        confirm_preflight_id=preflight_document["preflight_id"],
        authorizer="Alex Reviewer",
        operator="Alex Operator",
        review_ticket="PILOT-AUTH-001",
        reason="Authorize one bounded manual non-production pilot run.",
        authorized_at_utc=AS_OF + pd.Timedelta(minutes=5),
        valid_from_utc=AS_OF + pd.Timedelta(minutes=10),
        valid_until_utc=AS_OF + pd.Timedelta(minutes=100),
    )
    kwargs.update(overrides)
    return create_fabric_pilot_authorization(**kwargs)


def test_eligible_preflight_reverifies_all_dependencies_and_files(
    repository_root, verified_sources
):
    document = preflight(repository_root)
    assert document["preflight_status"] == ELIGIBLE_STATUS
    assert document["failed_check_count"] == 0
    assert document["check_count"] == len(document["checks"])
    assert len(document["repository_files"]) == 4
    assert all(item["sha256"] for item in document["repository_files"])
    assert not document["execution_authorized"]
    assert not document["execution_performed"]


def test_environment_capacity_or_schedule_failure_blocks_preflight(
    repository_root, verified_sources
):
    document = preflight(
        repository_root,
        environment_snapshot=environment(
            available_capacity_units=8.0,
            pilot_schedule_count=1,
            current_active_model="ridge_target_weather",
        ),
    )
    assert document["preflight_status"] == BLOCKED_STATUS
    failed = {check["check_name"] for check in document["checks"] if not check["passed"]}
    assert failed.issuperset(
        {"available_capacity_units", "pilot_schedule_count", "active_model_is_control"}
    )


def test_stale_or_unhealthy_provider_evidence_blocks_preflight(
    repository_root, verified_sources
):
    document = preflight(
        repository_root,
        provider_health_summary=health(status="failed", age_minutes=2000),
    )
    failed = {check["check_name"] for check in document["checks"] if not check["passed"]}
    assert document["preflight_status"] == BLOCKED_STATUS
    assert failed.issuperset(
        {
            "provider_health_age_minutes",
            "provider_health_is_healthy",
            "provider_health_failed_error_checks",
        }
    )


def test_missing_repository_file_or_credential_reference_blocks(
    repository_root, verified_sources
):
    (repository_root / "data-contracts/source_areas.json").unlink()
    document = preflight(
        repository_root,
        available_credential_references=("OTHER_REFERENCE",),
    )
    failed = {check["check_name"] for check in document["checks"] if not check["passed"]}
    assert document["preflight_status"] == BLOCKED_STATUS
    assert "planned_contract_file_verified" in failed
    assert "planned_credential_references_available" in failed


def test_candidate_or_bundle_mismatch_is_recorded_as_blocked(
    repository_root, monkeypatch
):
    monkeypatch.setattr(
        pilot_auth,
        "load_candidate_history",
        lambda path: ([], [], candidate("review_requested")),
    )
    monkeypatch.setattr(
        pilot_auth, "verify_evidence_bundle", lambda path: bundle("other")
    )
    monkeypatch.setattr(
        pilot_auth, "verify_recovered_bundle", lambda path: recovery("other")
    )
    document = preflight(repository_root)
    failed = {check["check_name"] for check in document["checks"] if not check["passed"]}
    assert document["preflight_status"] == BLOCKED_STATUS
    assert failed.issuperset(
        {
            "approved_candidate_history_reverified",
            "evidence_bundle_reverified",
            "recovered_evidence_reverified",
        }
    )


def test_credential_values_are_rejected_before_preflight(
    repository_root, verified_sources
):
    with pytest.raises(FabricPilotAuthorizationError, match="values and assignments"):
        preflight(
            repository_root,
            available_credential_references=("OPENWEATHER_API_KEY=secret",),
        )


def test_authorization_requires_eligible_preflight(repository_root, verified_sources):
    plan = build_plan()
    blocked = preflight(
        repository_root,
        environment_snapshot=environment(workspace_exists=False),
    )
    with pytest.raises(FabricPilotAuthorizationError, match="blocked preflight"):
        authorization(plan, blocked)


def test_authorization_is_single_use_time_bounded_and_non_activating(
    repository_root, verified_sources
):
    plan = build_plan()
    document = preflight(repository_root)
    record = authorization(plan, document)
    assert record["execution_authorized"]
    assert not record["execution_performed"]
    assert record["single_use"]
    assert not record["authorization_consumed"]
    assert not record["automatic_execution_allowed"]
    assert not record["schedule_activation_allowed"]
    assert not record["deployment_authorized"]
    assert not record["model_activation_authorized"]
    assert record["active_model_expected_after"] == "ridge_weather_lag"


def test_automation_identity_and_confirmation_mismatch_are_rejected(
    repository_root, verified_sources
):
    plan = build_plan()
    document = preflight(repository_root)
    with pytest.raises(FabricPilotAuthorizationError, match="human operator"):
        authorization(plan, document, authorizer="github-actions-bot")
    with pytest.raises(FabricPilotAuthorizationError, match="confirm_pilot_id"):
        authorization(plan, document, confirm_pilot_id="wrong")


def test_authorization_window_must_cover_pilot_and_remain_bounded(
    repository_root, verified_sources
):
    plan = build_plan()
    document = preflight(repository_root)
    with pytest.raises(FabricPilotAuthorizationError, match="15-minute safety margin"):
        authorization(
            plan,
            document,
            valid_until_utc=AS_OF + pd.Timedelta(minutes=50),
        )
    with pytest.raises(FabricPilotAuthorizationError, match="configured maximum"):
        authorization(
            plan,
            document,
            valid_until_utc=AS_OF + pd.Timedelta(hours=10),
        )


def test_authorization_verification_reports_current_and_expired(
    repository_root, verified_sources
):
    plan = build_plan()
    document = preflight(repository_root)
    record = authorization(plan, document)
    assert (
        verify_fabric_pilot_authorization(
            record, as_of_utc=AS_OF + pd.Timedelta(minutes=20), require_current=True
        )
        == "current"
    )
    assert (
        verify_fabric_pilot_authorization(
            record, as_of_utc=AS_OF + pd.Timedelta(minutes=120)
        )
        == "expired"
    )
    with pytest.raises(FabricPilotAuthorizationError, match="not current"):
        verify_fabric_pilot_authorization(
            record,
            as_of_utc=AS_OF + pd.Timedelta(minutes=120),
            require_current=True,
        )


def test_tampered_preflight_or_authorization_hash_is_rejected(
    repository_root, verified_sources
):
    plan = build_plan()
    document = preflight(repository_root)
    tampered = dict(document)
    tampered["environment_snapshot_id"] = "changed"
    with pytest.raises(FabricPilotAuthorizationError, match="hash is invalid"):
        verify_fabric_pilot_preflight(tampered)
    record = authorization(plan, document)
    unsafe = dict(record)
    unsafe["automatic_execution_allowed"] = True
    unsafe["authorization_hash"] = pilot_auth._digest(
        unsafe, exclude=("authorization_hash",)
    )
    with pytest.raises(FabricPilotAuthorizationError, match="automatic_execution_allowed"):
        verify_fabric_pilot_authorization(unsafe)


def test_preflight_and_authorization_writes_are_immutable(
    tmp_path, repository_root, verified_sources
):
    plan = build_plan()
    document = preflight(repository_root)
    preflight_path = write_fabric_pilot_preflight(tmp_path, document)
    assert load_fabric_pilot_preflight(preflight_path) == document
    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        write_fabric_pilot_preflight(tmp_path, document)
    record = authorization(plan, document)
    authorization_path = write_fabric_pilot_authorization(tmp_path, record)
    assert load_fabric_pilot_authorization(authorization_path) == record
    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        write_fabric_pilot_authorization(tmp_path, record)


def test_documents_satisfy_versioned_schemas(repository_root, verified_sources):
    plan = build_plan()
    document = preflight(repository_root)
    record = authorization(plan, document)
    root = Path(__file__).resolve().parents[1] / "data-contracts"
    for filename, payload in (
        ("fabric_pilot_preflight_schema.json", document),
        ("fabric_pilot_authorization_schema.json", record),
        ("fabric_pilot_environment_snapshot_schema.json", environment()),
    ):
        schema = json.loads((root / filename).read_text(encoding="utf-8"))
        errors = list(
            Draft202012Validator(
                schema, format_checker=FormatChecker()
            ).iter_errors(payload)
        )
        assert errors == []

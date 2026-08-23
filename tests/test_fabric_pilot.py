from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker

import forecasting.fabric_pilot as pilot
from forecasting.fabric_pilot import (
    FabricPilotError,
    create_fabric_pilot_plan,
    load_fabric_pilot_plan,
    verify_fabric_pilot_plan,
    write_fabric_pilot_plan,
)


COMMIT = "a" * 40
TREE = "b" * 40
BUNDLE_SHA = "c" * 64


def candidate(state: str = "approved") -> dict:
    return {
        "candidate_id": "twc-candidate",
        "candidate_version": "0.1.0",
        "candidate_state": state,
        "repository": "alexmarinos87/weather-energy-demand-pipeline",
        "code_commit_sha": COMMIT,
        "code_tree_sha": TREE,
        "promotion_assessment_id": "assessment-1",
        "comparison_run_id": "comparison-1",
        "reconciliation_run_id": "reconciliation-1",
        "provider_health_monitor_run_id": "monitor-1",
        "manifest_hash": "d" * 64,
        "deployment_authorized": False,
        "active_model_unchanged": True,
    }


def bundle(candidate_id: str = "twc-candidate") -> tuple[dict, dict]:
    return (
        {
            "bundle_id": "ceb-bundle",
            "candidate_id": candidate_id,
            "code_commit_sha": COMMIT,
            "code_tree_sha": TREE,
            "manifest_hash": "e" * 64,
        },
        {
            "bundle_id": "ceb-bundle",
            "bundle_sha256": BUNDLE_SHA,
            "verification_status": "verified",
            "candidate_id": candidate_id,
        },
    )


def recovery(candidate_id: str = "twc-candidate", bundle_id: str = "ceb-bundle") -> dict:
    return {
        "recovery_id": "rcv-recovery",
        "bundle_id": bundle_id,
        "candidate_id": candidate_id,
        "verification_status": "verified",
    }


@pytest.fixture
def verified_inputs(monkeypatch):
    monkeypatch.setattr(
        pilot, "load_candidate_history", lambda path: ([], [], candidate())
    )
    monkeypatch.setattr(pilot, "verify_evidence_bundle", lambda path: bundle())
    monkeypatch.setattr(
        pilot, "verify_recovered_bundle", lambda path: recovery()
    )


def make_plan(**overrides):
    kwargs = dict(
        candidate_directory=Path("candidate"),
        evidence_bundle_path=Path("bundle.tar"),
        recovered_bundle_directory=Path("recovered"),
        environment="non-production",
        workspace_name="weather-pilot",
        lakehouse_name="weather_energy_lakehouse",
        capacity_name="sandbox-capacity",
        actor="alexmarinos87",
        review_ticket="PILOT-001",
        reason="Prepare a bounded non-production Fabric pilot.",
        planned_at_utc=datetime(2026, 8, 23, 12, 0, tzinfo=timezone.utc),
    )
    kwargs.update(overrides)
    return create_fabric_pilot_plan(**kwargs)


def test_plan_binds_candidate_bundle_recovery_and_safety_flags(verified_inputs):
    plan = make_plan()
    assert plan["candidate_id"] == "twc-candidate"
    assert plan["bundle_id"] == "ceb-bundle"
    assert plan["recovery_id"] == "rcv-recovery"
    assert plan["execution_mode"] == "manual"
    assert plan["manual_execution_required"]
    assert not plan["automatic_execution_allowed"]
    assert not plan["schedule_activation_allowed"]
    assert not plan["execution_authorized"]
    assert not plan["execution_performed"]
    assert not plan["deployment_authorized"]
    assert plan["active_model_before"] == "ridge_weather_lag"
    assert plan["active_model_expected_after"] == "ridge_weather_lag"
    assert plan["active_model_unchanged"]


def test_plan_identity_is_deterministic_for_same_evidence_and_timestamp(verified_inputs):
    first = make_plan()
    second = make_plan()
    assert first["pilot_id"] == second["pilot_id"]
    assert first["plan_hash"] == second["plan_hash"]


def test_unapproved_candidate_is_rejected(monkeypatch):
    monkeypatch.setattr(
        pilot, "load_candidate_history", lambda path: ([], [], candidate("review_requested"))
    )
    monkeypatch.setattr(pilot, "verify_evidence_bundle", lambda path: bundle())
    monkeypatch.setattr(pilot, "verify_recovered_bundle", lambda path: recovery())
    with pytest.raises(FabricPilotError, match="approved candidate"):
        make_plan()


def test_bundle_or_recovery_mismatch_is_rejected(monkeypatch):
    monkeypatch.setattr(
        pilot, "load_candidate_history", lambda path: ([], [], candidate())
    )
    monkeypatch.setattr(
        pilot, "verify_evidence_bundle", lambda path: bundle("other")
    )
    monkeypatch.setattr(pilot, "verify_recovered_bundle", lambda path: recovery())
    with pytest.raises(FabricPilotError, match="bundle does not match"):
        make_plan()

    monkeypatch.setattr(pilot, "verify_evidence_bundle", lambda path: bundle())
    monkeypatch.setattr(
        pilot,
        "verify_recovered_bundle",
        lambda path: recovery(bundle_id="other-bundle"),
    )
    with pytest.raises(FabricPilotError, match="Recovered evidence does not match"):
        make_plan()


def test_production_environment_is_rejected(verified_inputs):
    with pytest.raises(FabricPilotError, match="environment must be one of"):
        make_plan(environment="production")


def test_credential_values_or_assignments_are_rejected(verified_inputs):
    with pytest.raises(FabricPilotError, match="values and assignments"):
        make_plan(credential_references=("OPENWEATHER_API_KEY=secret",))
    with pytest.raises(FabricPilotError, match="values and assignments"):
        make_plan(credential_references=("plain-secret",))


def test_bounds_and_rollback_contract_are_enforced(verified_inputs):
    with pytest.raises(FabricPilotError, match="no greater than 40"):
        make_plan(max_forecast_records=41)
    with pytest.raises(FabricPilotError, match="at least three"):
        make_plan(rollback_steps=("Stop.", "Preserve."))


def test_tampered_plan_hash_or_safety_flag_is_rejected(verified_inputs):
    plan = make_plan()
    tampered = dict(plan)
    tampered["workspace_name"] = "different"
    with pytest.raises(FabricPilotError, match="hash is invalid"):
        verify_fabric_pilot_plan(tampered)

    unsafe = dict(plan)
    unsafe["execution_authorized"] = True
    unsafe["plan_hash"] = pilot._digest(unsafe, exclude=("plan_hash",))
    with pytest.raises(FabricPilotError, match="execution_authorized"):
        verify_fabric_pilot_plan(unsafe)


def test_plan_write_is_immutable_and_reload_verifies(tmp_path, verified_inputs):
    plan = make_plan()
    path = write_fabric_pilot_plan(tmp_path, plan)
    assert load_fabric_pilot_plan(path) == plan
    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        write_fabric_pilot_plan(tmp_path, plan)


def test_plan_satisfies_versioned_json_schema(verified_inputs):
    plan = make_plan()
    schema_path = (
        Path(__file__).resolve().parents[1]
        / "data-contracts"
        / "fabric_pilot_plan_schema.json"
    )
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    errors = list(
        Draft202012Validator(
            schema, format_checker=FormatChecker()
        ).iter_errors(plan)
    )
    assert errors == []

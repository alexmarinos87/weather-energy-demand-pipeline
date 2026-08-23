from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path

import pandas as pd
import pytest
from jsonschema import Draft202012Validator, FormatChecker

import forecasting.fabric_pilot as pilot_plan
import forecasting.fabric_pilot_authorization as pilot_auth
import forecasting.fabric_pilot_receipt as pilot_receipt
from forecasting.fabric_pilot_receipt import (
    ELIGIBLE_OUTCOME,
    FAILED_OUTCOME,
    FabricPilotReceiptError,
    assess_fabric_pilot_run,
    create_fabric_pilot_run_receipt,
    load_fabric_pilot_run_assessment,
    load_fabric_pilot_run_receipt,
    verify_fabric_pilot_run_assessment,
    verify_fabric_pilot_run_receipt,
    write_fabric_pilot_run_assessment,
    write_fabric_pilot_run_receipt,
)


COMMIT = "a" * 40
TREE = "b" * 40
AS_OF = pd.Timestamp("2026-08-23T12:00:00Z")


def build_plan() -> dict:
    core = {
        "candidate_id": "twc-candidate",
        "candidate_version": "0.1.0",
        "bundle_id": "ceb-bundle",
        "bundle_sha256": "c" * 64,
        "recovery_id": "rcv-recovery",
        "repository": "alexmarinos87/weather-energy-demand-pipeline",
        "code_commit_sha": COMMIT,
        "code_tree_sha": TREE,
        "environment": "non-production",
        "workspace_name": "weather-pilot",
        "lakehouse_name": "weather_energy_lakehouse",
        "capacity_name": "sandbox-capacity",
        "credential_references": ["OPENWEATHER_API_KEY"],
        "notebook_paths": ["one.py", "two.py"],
        "contract_paths": ["contract.json"],
        "allowed_tables": ["silver_forecast_weather", "comparison_predictions"],
        "rollback_steps": ["Stop.", "Preserve.", "Confirm control model."],
        "max_duration_minutes": 60,
        "max_capacity_units": 16.0,
        "max_forecast_records": 40,
        "max_prediction_rows": 1000,
        "max_metric_rows": 100,
        "max_failed_quality_checks": 0,
        "max_notebook_retries": 0,
        "actor": "Alex",
        "review_ticket": "PILOT-001",
        "reason": "Prepare a bounded non-production Fabric pilot.",
        "planned_at_utc": "2026-08-23T11:00:00+00:00",
    }
    plan = {
        "pilot_id": "fpl-" + pilot_plan._digest(core)[:24],
        "pilot_revision": 1,
        "pilot_state": "draft",
        **core,
        "promotion_assessment_id": "assessment-1",
        "comparison_run_id": "comparison-1",
        "reconciliation_run_id": "reconciliation-1",
        "provider_health_monitor_run_id": "monitor-1",
        "candidate_manifest_hash": "d" * 64,
        "bundle_manifest_hash": "e" * 64,
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


def build_preflight(plan: dict) -> dict:
    core = {
        "pilot_id": plan["pilot_id"],
        "plan_hash": plan["plan_hash"],
        "candidate_id": plan["candidate_id"],
        "bundle_id": plan["bundle_id"],
        "recovery_id": plan["recovery_id"],
        "preflight_timestamp_utc": AS_OF.isoformat(),
        "repository_commit_sha": COMMIT,
        "repository_tree_sha": TREE,
        "provider_health_monitor_run_id": "fresh-monitor",
        "environment_snapshot_id": "env-1",
        "verified_credential_references": ["OPENWEATHER_API_KEY"],
        "repository_files": [],
        "checks": [
            {
                "check_sequence": 1,
                "check_scope": "governance",
                "check_name": "all_required_preflight_evidence",
                "passed": True,
                "observed_value": "verified",
                "expected_value": "verified",
                "details": "Synthetic eligible preflight for receipt tests.",
            }
        ],
    }
    document = {
        "preflight_id": "fpf-" + pilot_auth._digest(core)[:24],
        "preflight_revision": 1,
        **core,
        "preflight_status": "eligible_for_human_authorization",
        "check_count": 1,
        "passed_check_count": 1,
        "failed_check_count": 0,
        "manual_authorization_required": True,
        "automatic_authorization_allowed": False,
        "execution_authorized": False,
        "execution_performed": False,
        "schedule_activation_allowed": False,
        "deployment_authorized": False,
        "active_model_unchanged": True,
        "credential_values_recorded": False,
        "source_evidence_mutated": False,
        "preflight_contract_version": "controlled-fabric-pilot-preflight-v1",
    }
    document["preflight_hash"] = pilot_auth._digest(
        document, exclude=("preflight_hash",)
    )
    pilot_auth.verify_fabric_pilot_preflight(document)
    return document


def build_authorization(plan: dict, preflight: dict) -> dict:
    core = {
        "pilot_id": plan["pilot_id"],
        "plan_hash": plan["plan_hash"],
        "preflight_id": preflight["preflight_id"],
        "preflight_hash": preflight["preflight_hash"],
        "candidate_id": plan["candidate_id"],
        "bundle_id": plan["bundle_id"],
        "recovery_id": plan["recovery_id"],
        "repository": plan["repository"],
        "code_commit_sha": COMMIT,
        "code_tree_sha": TREE,
        "environment": plan["environment"],
        "workspace_name": plan["workspace_name"],
        "lakehouse_name": plan["lakehouse_name"],
        "capacity_name": plan["capacity_name"],
        "authorizer": "Alex Reviewer",
        "operator": "Alex Operator",
        "review_ticket": "PILOT-AUTH-001",
        "reason": "Authorize one bounded manual non-production pilot run.",
        "authorized_at_utc": (AS_OF + pd.Timedelta(minutes=5)).isoformat(),
        "valid_from_utc": (AS_OF + pd.Timedelta(minutes=10)).isoformat(),
        "valid_until_utc": (AS_OF + pd.Timedelta(minutes=100)).isoformat(),
        "authorization_window_minutes": 90.0,
    }
    authorization = {
        "authorization_id": "fpa-" + pilot_auth._digest(core)[:24],
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
        "authorization_contract_version": "controlled-fabric-pilot-authorization-v1",
    }
    authorization["authorization_hash"] = pilot_auth._digest(
        authorization, exclude=("authorization_hash",)
    )
    pilot_auth.verify_fabric_pilot_authorization(authorization)
    return authorization


def run_report(**overrides) -> dict:
    report = {
        "external_run_id": "fabric-run-1",
        "operator": "Alex Operator",
        "run_status": "completed",
        "started_at_utc": (AS_OF + pd.Timedelta(minutes=20)).isoformat(),
        "ended_at_utc": (AS_OF + pd.Timedelta(minutes=60)).isoformat(),
        "environment": "non-production",
        "workspace_name": "weather-pilot",
        "lakehouse_name": "weather_energy_lakehouse",
        "capacity_name": "sandbox-capacity",
        "repository": "alexmarinos87/weather-energy-demand-pipeline",
        "code_commit_sha": COMMIT,
        "code_tree_sha": TREE,
        "executed_notebook_paths": ["one.py", "two.py"],
        "written_tables": ["silver_forecast_weather", "comparison_predictions"],
        "credential_references_used": ["OPENWEATHER_API_KEY"],
        "forecast_record_count": 40,
        "prediction_row_count": 100,
        "metric_row_count": 10,
        "failed_quality_check_count": 0,
        "notebook_retry_count": 0,
        "peak_capacity_units": 8.0,
        "schedule_created": False,
        "deployment_performed": False,
        "model_activation_performed": False,
        "active_model_before": "ridge_weather_lag",
        "active_model_after": "ridge_weather_lag",
        "rollback_performed": False,
        "rollback_reason": None,
        "credential_values_included": False,
    }
    report.update(overrides)
    return report


@pytest.fixture
def chain():
    plan = build_plan()
    preflight = build_preflight(plan)
    authorization = build_authorization(plan, preflight)
    return plan, preflight, authorization


@pytest.fixture
def evidence_root(tmp_path: Path):
    paths = {}
    for role in pilot_receipt.DEFAULT_REQUIRED_EVIDENCE_ROLES:
        relative = Path("exports") / f"{role}.txt"
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"evidence for {role}\n", encoding="utf-8")
        paths[role] = relative.as_posix()
    return tmp_path, paths


def receipt(chain, evidence_root, report=None):
    plan, preflight, authorization = chain
    root, paths = evidence_root
    return create_fabric_pilot_run_receipt(
        plan,
        preflight,
        authorization,
        report or run_report(),
        root,
        paths,
        confirm_authorization_id=authorization["authorization_id"],
        recorded_at_utc=AS_OF + pd.Timedelta(minutes=65),
    )


def assess(chain, recorded):
    plan, _, authorization = chain
    return assess_fabric_pilot_run(
        plan,
        authorization,
        recorded,
        assessed_at_utc=AS_OF + pd.Timedelta(minutes=70),
    )


def test_successful_receipt_and_assessment_remain_human_review_only(chain, evidence_root):
    recorded = receipt(chain, evidence_root)
    assessment = assess(chain, recorded)
    assert recorded["authorization_consumed"]
    assert recorded["execution_performed"]
    assert not recorded["model_activation_authorized"]
    assert assessment["assessment_outcome"] == ELIGIBLE_OUTCOME
    assert assessment["failed_check_count"] == 0
    assert assessment["post_pilot_human_decision_required"]
    assert not assessment["automatic_model_activation_allowed"]


def test_run_must_start_while_authorization_is_current(chain, evidence_root):
    report = run_report(
        started_at_utc=(AS_OF + pd.Timedelta(minutes=105)).isoformat(),
        ended_at_utc=(AS_OF + pd.Timedelta(minutes=115)).isoformat(),
    )
    with pytest.raises(FabricPilotReceiptError, match="not current"):
        receipt(chain, evidence_root, report)


def test_operator_identity_and_credential_references_are_strict(chain, evidence_root):
    with pytest.raises(FabricPilotReceiptError, match="operator"):
        receipt(chain, evidence_root, run_report(operator="Other Human"))
    with pytest.raises(FabricPilotReceiptError, match="workspace_name"):
        receipt(chain, evidence_root, run_report(workspace_name="other"))
    with pytest.raises(FabricPilotReceiptError, match="values and assignments"):
        receipt(
            chain,
            evidence_root,
            run_report(credential_references_used=["OPENWEATHER_API_KEY=secret"]),
        )


def test_limit_violation_fails_and_requires_rollback(chain, evidence_root):
    recorded = receipt(chain, evidence_root, run_report(prediction_row_count=2000))
    assessment = assess(chain, recorded)
    failed = {check["check_name"] for check in assessment["checks"] if not check["passed"]}
    assert assessment["assessment_outcome"] == FAILED_OUTCOME
    assert assessment["rollback_required"]
    assert "prediction_row_count_within_plan" in failed
    assert "rollback_performed_when_required" in failed


def test_limit_violation_with_rollback_still_fails_post_pilot_review(chain, evidence_root):
    recorded = receipt(
        chain,
        evidence_root,
        run_report(
            peak_capacity_units=20.0,
            rollback_performed=True,
            rollback_reason="Capacity limit exceeded; manual rollback completed.",
        ),
    )
    assessment = assess(chain, recorded)
    rollback = {
        check["check_name"]: check["passed"]
        for check in assessment["checks"]
        if check["check_scope"] == "rollback"
    }
    assert assessment["assessment_outcome"] == FAILED_OUTCOME
    assert rollback["rollback_performed_when_required"]
    assert rollback["rollback_reason_recorded"]


def test_interrupted_run_may_use_notebook_prefix_but_requires_rollback(chain, evidence_root):
    plan, preflight, authorization = chain
    root, paths = evidence_root
    recorded = create_fabric_pilot_run_receipt(
        plan,
        preflight,
        authorization,
        run_report(
            run_status="aborted",
            executed_notebook_paths=["one.py"],
            written_tables=[],
            rollback_performed=True,
            rollback_reason="Operator aborted and preserved the control model.",
        ),
        root,
        {"run_log": paths["run_log"]},
        confirm_authorization_id=authorization["authorization_id"],
        recorded_at_utc=AS_OF + pd.Timedelta(minutes=65),
    )
    assessment = assess(chain, recorded)
    notebook_check = next(
        check
        for check in assessment["checks"]
        if check["check_name"] == "executed_notebooks_follow_plan"
    )
    assert notebook_check["passed"]
    assert assessment["assessment_outcome"] == FAILED_OUTCOME


def test_completed_run_requires_evidence_and_allowlisted_execution(chain, evidence_root):
    plan, preflight, authorization = chain
    root, paths = evidence_root
    paths.pop("quality_results")
    recorded = create_fabric_pilot_run_receipt(
        plan,
        preflight,
        authorization,
        run_report(
            executed_notebook_paths=["one.py", "unexpected.py"],
            written_tables=["unexpected_table"],
            model_activation_performed=True,
            active_model_after="ridge_target_weather",
        ),
        root,
        paths,
        confirm_authorization_id=authorization["authorization_id"],
        recorded_at_utc=AS_OF + pd.Timedelta(minutes=65),
    )
    assessment = assess(chain, recorded)
    failed = {check["check_name"] for check in assessment["checks"] if not check["passed"]}
    assert assessment["assessment_outcome"] == FAILED_OUTCOME
    assert failed.issuperset(
        {
            "executed_notebooks_follow_plan",
            "written_tables_are_allowlisted",
            "no_model_activation_performed",
            "active_model_after_is_control",
            "required_evidence_roles_present",
        }
    )


def test_evidence_symlink_or_duplicate_path_is_rejected(chain, tmp_path):
    target = tmp_path / "real.txt"
    target.write_text("evidence", encoding="utf-8")
    link = tmp_path / "link.txt"
    link.symlink_to(target)
    plan, preflight, authorization = chain
    with pytest.raises(FabricPilotReceiptError, match="symbolic link"):
        create_fabric_pilot_run_receipt(
            plan,
            preflight,
            authorization,
            run_report(),
            tmp_path,
            {"run_log": "link.txt"},
            confirm_authorization_id=authorization["authorization_id"],
            recorded_at_utc=AS_OF + pd.Timedelta(minutes=65),
        )
    with pytest.raises(FabricPilotReceiptError, match="supplied more than once"):
        create_fabric_pilot_run_receipt(
            plan,
            preflight,
            authorization,
            run_report(),
            tmp_path,
            {"run_log": "real.txt", "quality_results": "real.txt"},
            confirm_authorization_id=authorization["authorization_id"],
            recorded_at_utc=AS_OF + pd.Timedelta(minutes=65),
        )


def test_receipt_and_assessment_writes_are_immutable(chain, evidence_root, tmp_path):
    recorded = receipt(chain, evidence_root)
    receipt_path = write_fabric_pilot_run_receipt(tmp_path, recorded)
    assert load_fabric_pilot_run_receipt(receipt_path) == recorded
    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        write_fabric_pilot_run_receipt(tmp_path, recorded)
    assessment = assess(chain, recorded)
    assessment_path = write_fabric_pilot_run_assessment(tmp_path, assessment)
    assert load_fabric_pilot_run_assessment(assessment_path) == assessment
    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        write_fabric_pilot_run_assessment(tmp_path, assessment)


def test_hash_tampering_is_rejected(chain, evidence_root):
    recorded = receipt(chain, evidence_root)
    tampered = dict(recorded)
    tampered["prediction_row_count"] = 999
    with pytest.raises(FabricPilotReceiptError, match="hash is invalid"):
        verify_fabric_pilot_run_receipt(tampered)
    assessment = assess(chain, recorded)
    altered = dict(assessment)
    altered["automatic_model_activation_allowed"] = True
    altered["assessment_hash"] = pilot_receipt._digest(
        altered, exclude=("assessment_hash",)
    )
    with pytest.raises(FabricPilotReceiptError, match="automatic_model_activation_allowed"):
        verify_fabric_pilot_run_assessment(altered)


def test_report_receipt_and_assessment_satisfy_schemas(chain, evidence_root):
    report = run_report()
    recorded = receipt(chain, evidence_root, report)
    assessment = assess(chain, recorded)
    root = Path(__file__).resolve().parents[1] / "data-contracts"
    for filename, payload in (
        ("fabric_pilot_run_report_schema.json", report),
        ("fabric_pilot_run_receipt_schema.json", recorded),
        ("fabric_pilot_run_assessment_schema.json", assessment),
    ):
        schema = json.loads((root / filename).read_text(encoding="utf-8"))
        errors = list(
            Draft202012Validator(
                schema, format_checker=FormatChecker()
            ).iter_errors(payload)
        )
        assert errors == []

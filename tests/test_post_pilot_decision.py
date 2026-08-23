from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker

import forecasting.post_pilot_decision as decision_module
from forecasting.post_pilot_decision import (
    ELIGIBLE_ASSESSMENT,
    FAILED_ASSESSMENT,
    PostPilotDecisionError,
    create_post_pilot_decision,
    load_post_pilot_decision,
    verify_post_pilot_decision,
    write_post_pilot_decision,
)


PILOT_ID = "fpl-" + "1" * 24
AUTHORIZATION_ID = "fpa-" + "2" * 24
RECEIPT_ID = "fpr-" + "3" * 24
ASSESSMENT_ID = "fra-" + "4" * 24
PLAN_HASH = "a" * 64
AUTHORIZATION_HASH = "b" * 64
RECEIPT_HASH = "c" * 64
ASSESSMENT_HASH = "d" * 64
DECIDED_AT = datetime(2026, 8, 23, 16, 0, tzinfo=timezone.utc)


def plan() -> dict:
    return {
        "pilot_id": PILOT_ID,
        "plan_hash": PLAN_HASH,
        "candidate_id": "target-weather-candidate",
        "candidate_version": "0.1.0",
    }


def authorization() -> dict:
    return {
        "pilot_id": PILOT_ID,
        "plan_hash": PLAN_HASH,
        "authorization_id": AUTHORIZATION_ID,
        "authorization_hash": AUTHORIZATION_HASH,
    }


def receipt(*, run_status: str = "completed") -> dict:
    return {
        "pilot_id": PILOT_ID,
        "plan_hash": PLAN_HASH,
        "authorization_id": AUTHORIZATION_ID,
        "authorization_hash": AUTHORIZATION_HASH,
        "receipt_id": RECEIPT_ID,
        "receipt_hash": RECEIPT_HASH,
        "external_run_id": "fabric-run-1",
        "run_status": run_status,
    }


def assessment(
    *,
    outcome: str = ELIGIBLE_ASSESSMENT,
    run_status: str = "completed",
) -> dict:
    return {
        "pilot_id": PILOT_ID,
        "authorization_id": AUTHORIZATION_ID,
        "receipt_id": RECEIPT_ID,
        "receipt_hash": RECEIPT_HASH,
        "assessment_id": ASSESSMENT_ID,
        "assessment_hash": ASSESSMENT_HASH,
        "external_run_id": "fabric-run-1",
        "assessment_outcome": outcome,
        "run_status": run_status,
        "assessed_at_utc": "2026-08-23T15:00:00+00:00",
    }


@pytest.fixture(autouse=True)
def verified_chain(monkeypatch):
    monkeypatch.setattr(decision_module, "verify_fabric_pilot_plan", lambda value: None)
    monkeypatch.setattr(
        decision_module, "verify_fabric_pilot_authorization", lambda value: None
    )
    monkeypatch.setattr(
        decision_module, "verify_fabric_pilot_run_receipt", lambda value: None
    )
    monkeypatch.setattr(
        decision_module, "verify_fabric_pilot_run_assessment", lambda value: None
    )


def create(
    *,
    selected: str = "continue_evidence_collection",
    outcome: str = ELIGIBLE_ASSESSMENT,
    run_status: str = "completed",
    **overrides,
) -> dict:
    kwargs = {
        "plan": plan(),
        "authorization": authorization(),
        "receipt": receipt(run_status=run_status),
        "assessment": assessment(outcome=outcome, run_status=run_status),
        "confirm_pilot_id": PILOT_ID,
        "confirm_receipt_id": RECEIPT_ID,
        "confirm_assessment_id": ASSESSMENT_ID,
        "decision": selected,
        "decision_maker": "Alex Reviewer",
        "review_ticket": "PILOT-DECISION-001",
        "reason": "Record a reviewed post-pilot evidence disposition.",
        "action_items": ("Prepare the next reviewed evidence step.",),
        "decided_at_utc": DECIDED_AT,
    }
    kwargs.update(overrides)
    return create_post_pilot_decision(**kwargs)


def test_eligible_pilot_can_request_new_evidence_cycle_without_execution_authority():
    record = create()
    assert record["decision"] == "continue_evidence_collection"
    assert record["decision_effect"] == "request_new_pilot_cycle"
    assert record["new_pilot_cycle_requested"]
    assert not record["candidate_revision_requested"]
    assert not record["candidate_retirement_requested"]
    assert not record["authorization_reuse_allowed"]
    assert not record["pilot_reexecution_authorized"]
    assert not record["model_registry_mutation_allowed"]
    assert not record["model_activation_authorized"]
    assert record["active_model_unchanged"]


def test_failed_pilot_cannot_continue_unchanged():
    with pytest.raises(PostPilotDecisionError, match="revise or retire"):
        create(
            selected="continue_evidence_collection",
            outcome=FAILED_ASSESSMENT,
            run_status="failed",
        )


def test_failed_pilot_can_request_candidate_revision():
    record = create(
        selected="revise_candidate",
        outcome=FAILED_ASSESSMENT,
        run_status="failed",
    )
    assert record["decision_effect"] == "request_candidate_revision"
    assert record["candidate_revision_requested"]
    assert not record["new_pilot_cycle_requested"]
    assert not record["candidate_retirement_requested"]
    assert not record["model_registry_mutation_allowed"]


def test_retirement_is_a_request_not_a_registry_mutation():
    record = create(selected="retire_candidate")
    assert record["decision_effect"] == "request_candidate_retirement"
    assert record["candidate_retirement_requested"]
    assert not record["model_registry_mutation_allowed"]
    assert not record["deployment_authorized"]


def test_same_inputs_and_timestamp_produce_deterministic_identity():
    first = create()
    second = create()
    assert first["decision_id"] == second["decision_id"]
    assert first["decision_hash"] == second["decision_hash"]


def test_exact_confirmation_ids_are_required():
    with pytest.raises(PostPilotDecisionError, match="confirm_pilot_id"):
        create(confirm_pilot_id="wrong")
    with pytest.raises(PostPilotDecisionError, match="confirm_receipt_id"):
        create(confirm_receipt_id="wrong")
    with pytest.raises(PostPilotDecisionError, match="confirm_assessment_id"):
        create(confirm_assessment_id="wrong")


def test_automation_identity_is_rejected():
    with pytest.raises(PostPilotDecisionError, match="human reviewer"):
        create(decision_maker="github-actions-bot")


def test_reason_and_action_items_are_bounded_and_unique():
    with pytest.raises(PostPilotDecisionError, match="at least 20"):
        create(reason="too short")
    with pytest.raises(PostPilotDecisionError, match="at least one"):
        create(action_items=())
    with pytest.raises(PostPilotDecisionError, match="duplicates"):
        create(action_items=("Same action", "Same action"))


def test_decision_cannot_predate_the_assessment():
    with pytest.raises(PostPilotDecisionError, match="cannot precede"):
        create(decided_at_utc="2026-08-23T14:00:00Z")


def test_chain_mismatch_is_rejected_before_decision():
    mismatched_receipt = receipt()
    mismatched_receipt["authorization_id"] = "fpa-" + "9" * 24
    with pytest.raises(PostPilotDecisionError, match="authorization"):
        create(receipt=mismatched_receipt)

    mismatched_assessment = assessment()
    mismatched_assessment["receipt_hash"] = "e" * 64
    with pytest.raises(PostPilotDecisionError, match="run receipt"):
        create(assessment=mismatched_assessment)


def test_tampered_hash_and_safety_flag_are_rejected():
    record = create()
    tampered = dict(record)
    tampered["review_ticket"] = "changed"
    with pytest.raises(PostPilotDecisionError, match="hash is invalid"):
        verify_post_pilot_decision(tampered)

    unsafe = dict(record)
    unsafe["model_registry_mutation_allowed"] = True
    unsafe["decision_hash"] = decision_module._digest(
        unsafe, exclude=("decision_hash",)
    )
    with pytest.raises(
        PostPilotDecisionError, match="model_registry_mutation_allowed"
    ):
        verify_post_pilot_decision(unsafe)


def test_inconsistent_decision_mapping_is_rejected():
    record = create(selected="revise_candidate")
    inconsistent = dict(record)
    inconsistent["candidate_revision_requested"] = False
    inconsistent["decision_hash"] = decision_module._digest(
        inconsistent, exclude=("decision_hash",)
    )
    with pytest.raises(PostPilotDecisionError, match="mapping field"):
        verify_post_pilot_decision(inconsistent)


def test_decision_write_is_immutable_and_reload_verifies(tmp_path):
    record = create()
    path = write_post_pilot_decision(tmp_path, record)
    assert load_post_pilot_decision(path) == record
    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        write_post_pilot_decision(tmp_path, record)


def test_decision_satisfies_versioned_json_schema():
    record = create()
    schema_path = (
        Path(__file__).resolve().parents[1]
        / "data-contracts"
        / "post_pilot_decision_schema.json"
    )
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    errors = list(
        Draft202012Validator(
            schema, format_checker=FormatChecker()
        ).iter_errors(record)
    )
    assert errors == []

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker

import forecasting.post_pilot_followthrough as followthrough
from forecasting.post_pilot_followthrough import (
    PostPilotFollowthroughError,
    create_post_pilot_followthrough_request,
    load_post_pilot_followthrough_request,
    verify_post_pilot_followthrough_request,
    write_post_pilot_followthrough_request,
)


PILOT_ID = "fpl-" + "1" * 24
CLOSURE_ID = "pcl-" + "2" * 24
DECISION_ID = "fpd-" + "3" * 24
DECISION_HASH = "a" * 64
MANIFEST_HASH = "b" * 64
ARCHIVE_HASH = "c" * 64
REQUESTED_AT = datetime(2026, 8, 23, 18, 0, tzinfo=timezone.utc)


def decision(selected: str = "continue_evidence_collection") -> dict:
    return {
        "pilot_id": PILOT_ID,
        "decision_id": DECISION_ID,
        "decision_hash": DECISION_HASH,
        "decision": selected,
        "candidate_id": "target-weather-candidate",
        "candidate_version": "0.1.0",
        "external_run_id": "fabric-run-1",
        "decided_at_utc": "2026-08-23T16:00:00+00:00",
    }


def manifest(selected: str = "continue_evidence_collection") -> dict:
    source = decision(selected)
    return {
        "pilot_id": PILOT_ID,
        "closure_id": CLOSURE_ID,
        "manifest_hash": MANIFEST_HASH,
        "decision_id": source["decision_id"],
        "decision_hash": source["decision_hash"],
        "decision": source["decision"],
        "candidate_id": source["candidate_id"],
        "candidate_version": source["candidate_version"],
        "external_run_id": source["external_run_id"],
        "created_at_utc": "2026-08-23T17:00:00+00:00",
    }


@pytest.fixture(autouse=True)
def verified_sources(monkeypatch):
    monkeypatch.setattr(
        followthrough, "verify_post_pilot_decision", lambda value: None
    )

    def verify_bundle(path, verified_at_utc=None):
        selected = getattr(path, "decision", "continue_evidence_collection")
        return manifest(selected), {
            "manifest_hash": MANIFEST_HASH,
            "archive_sha256": ARCHIVE_HASH,
        }

    def verify_recovery(path, verified_at_utc=None):
        selected = getattr(path, "decision", "continue_evidence_collection")
        return manifest(selected), {"manifest_hash": MANIFEST_HASH}

    monkeypatch.setattr(
        followthrough, "verify_post_pilot_closure_bundle", verify_bundle
    )
    monkeypatch.setattr(
        followthrough, "verify_recovered_post_pilot_closure", verify_recovery
    )


class SourcePath(Path):
    _flavour = type(Path())._flavour

    def __new__(cls, value: str, decision_value: str):
        instance = super().__new__(cls, value)
        instance.decision = decision_value
        return instance


def create(selected="continue_evidence_collection", **overrides):
    kwargs = {
        "decision_record": decision(selected),
        "closure_bundle": SourcePath("closure.tar", selected),
        "recovered_directory": SourcePath("recovered", selected),
        "confirm_decision_id": DECISION_ID,
        "confirm_closure_id": CLOSURE_ID,
        "requested_by": "Alex Requester",
        "owner": "Data Engineering",
        "review_ticket": "PILOT-FOLLOWTHROUGH-001",
        "reason": "Prepare the separately reviewed next post-pilot workflow artifact.",
        "action_items": ("Create the required next workflow artifact.",),
        "requested_at_utc": REQUESTED_AT,
    }
    kwargs.update(overrides)
    return create_post_pilot_followthrough_request(**kwargs)


def test_continue_decision_requires_entirely_new_pilot_cycle():
    request = create()
    assert request["followthrough_type"] == "prepare_new_pilot_cycle"
    assert request["new_pilot_cycle_requested"]
    assert request["new_pilot_plan_required"]
    assert request["new_preflight_required"]
    assert request["new_authorization_required"]
    assert request["new_evidence_cycle_required"]
    assert not request["authorization_reuse_allowed"]
    assert not request["pilot_execution_authorized"]


def test_revision_decision_requests_new_candidate_version_without_registration():
    request = create("revise_candidate")
    assert request["followthrough_type"] == "prepare_candidate_revision"
    assert request["candidate_revision_requested"]
    assert request["new_candidate_version_required"]
    assert request["new_evidence_cycle_required"]
    assert not request["model_registry_mutation_allowed"]
    assert not request["deployment_authorized"]


def test_retirement_decision_requests_separate_registry_review_only():
    request = create("retire_candidate")
    assert request["followthrough_type"] == "prepare_registry_retirement_review"
    assert request["candidate_retirement_requested"]
    assert request["registry_retirement_review_required"]
    assert not request["model_registry_mutation_allowed"]
    assert not request["model_activation_authorized"]
    assert request["active_model_unchanged"]


def test_same_evidence_and_time_produce_deterministic_request():
    first = create()
    second = create()
    assert first["request_id"] == second["request_id"]
    assert first["request_hash"] == second["request_hash"]


def test_exact_decision_and_closure_confirmations_are_required():
    with pytest.raises(PostPilotFollowthroughError, match="confirm_decision_id"):
        create(confirm_decision_id="wrong")
    with pytest.raises(PostPilotFollowthroughError, match="confirm_closure_id"):
        create(confirm_closure_id="wrong")


def test_closure_identity_mismatch_is_rejected(monkeypatch):
    wrong = manifest()
    wrong["decision_hash"] = "d" * 64
    monkeypatch.setattr(
        followthrough,
        "verify_post_pilot_closure_bundle",
        lambda path, verified_at_utc=None: (
            wrong,
            {"manifest_hash": MANIFEST_HASH, "archive_sha256": ARCHIVE_HASH},
        ),
    )
    monkeypatch.setattr(
        followthrough,
        "verify_recovered_post_pilot_closure",
        lambda path, verified_at_utc=None: (
            wrong,
            {"manifest_hash": MANIFEST_HASH},
        ),
    )
    with pytest.raises(PostPilotFollowthroughError, match="decision_hash"):
        create()


def test_archive_and_recovery_must_match(monkeypatch):
    recovered = manifest()
    recovered["closure_id"] = "pcl-" + "9" * 24
    monkeypatch.setattr(
        followthrough,
        "verify_recovered_post_pilot_closure",
        lambda path, verified_at_utc=None: (
            recovered,
            {"manifest_hash": MANIFEST_HASH},
        ),
    )
    with pytest.raises(PostPilotFollowthroughError, match="Recovered closure"):
        create()


def test_human_requester_reason_owner_and_actions_are_required():
    with pytest.raises(PostPilotFollowthroughError, match="human requester"):
        create(requested_by="github-actions-bot")
    with pytest.raises(PostPilotFollowthroughError, match="owner"):
        create(owner="")
    with pytest.raises(PostPilotFollowthroughError, match="at least 20"):
        create(reason="too short")
    with pytest.raises(PostPilotFollowthroughError, match="at least one"):
        create(action_items=())
    with pytest.raises(PostPilotFollowthroughError, match="duplicates"):
        create(action_items=("Same action", "Same action"))


def test_request_cannot_predate_decision_or_closure():
    with pytest.raises(PostPilotFollowthroughError, match="cannot precede"):
        create(requested_at_utc="2026-08-23T15:00:00Z")


def test_tampered_hash_mapping_and_safety_flags_are_rejected():
    request = create()
    tampered = dict(request)
    tampered["owner"] = "Changed"
    with pytest.raises(PostPilotFollowthroughError, match="hash is invalid"):
        verify_post_pilot_followthrough_request(tampered)

    mapping = dict(request)
    mapping["new_authorization_required"] = False
    mapping["request_hash"] = followthrough._digest(
        mapping, exclude=("request_hash",)
    )
    with pytest.raises(PostPilotFollowthroughError, match="mapping field"):
        verify_post_pilot_followthrough_request(mapping)

    unsafe = dict(request)
    unsafe["automatic_execution_allowed"] = True
    unsafe["request_hash"] = followthrough._digest(
        unsafe, exclude=("request_hash",)
    )
    with pytest.raises(
        PostPilotFollowthroughError, match="automatic_execution_allowed"
    ):
        verify_post_pilot_followthrough_request(unsafe)


def test_request_write_is_immutable_and_reload_verifies(tmp_path):
    request = create()
    path = write_post_pilot_followthrough_request(tmp_path, request)
    assert load_post_pilot_followthrough_request(path) == request
    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        write_post_pilot_followthrough_request(tmp_path, request)


def test_request_satisfies_versioned_json_schema():
    request = create()
    schema_path = (
        Path(__file__).resolve().parents[1]
        / "data-contracts"
        / "post_pilot_followthrough_request_schema.json"
    )
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    errors = list(
        Draft202012Validator(
            schema, format_checker=FormatChecker()
        ).iter_errors(request)
    )
    assert errors == []

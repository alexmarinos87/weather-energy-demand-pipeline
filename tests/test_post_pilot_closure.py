from __future__ import annotations

import io
import json
import tarfile
from datetime import datetime, timezone
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker

import forecasting.post_pilot_closure as closure
from forecasting.post_pilot_closure import (
    PostPilotClosureError,
    create_post_pilot_closure_bundle,
    recover_post_pilot_closure_bundle,
    verify_post_pilot_closure_bundle,
    verify_post_pilot_closure_manifest,
    verify_post_pilot_closure_verification,
    verify_recovered_post_pilot_closure,
    write_post_pilot_closure_verification,
)


PILOT_ID = "fpl-" + "1" * 24
AUTHORIZATION_ID = "fpa-" + "2" * 24
RECEIPT_ID = "fpr-" + "3" * 24
ASSESSMENT_ID = "fra-" + "4" * 24
DECISION_ID = "fpd-" + "5" * 24
PLAN_HASH = "a" * 64
AUTHORIZATION_HASH = "b" * 64
RECEIPT_HASH = "c" * 64
ASSESSMENT_HASH = "d" * 64
DECISION_HASH = "e" * 64
CREATED_AT = datetime(2026, 8, 23, 17, 0, tzinfo=timezone.utc)


def plan() -> dict:
    return {
        "pilot_id": PILOT_ID,
        "plan_hash": PLAN_HASH,
        "candidate_id": "target-weather-candidate",
        "candidate_version": "0.1.0",
    }


def preflight() -> dict:
    return {
        "pilot_id": PILOT_ID,
        "plan_hash": PLAN_HASH,
        "preflight_id": "fpf-" + "6" * 24,
        "preflight_hash": "f" * 64,
    }


def authorization() -> dict:
    return {
        "pilot_id": PILOT_ID,
        "plan_hash": PLAN_HASH,
        "preflight_id": preflight()["preflight_id"],
        "preflight_hash": preflight()["preflight_hash"],
        "authorization_id": AUTHORIZATION_ID,
        "authorization_hash": AUTHORIZATION_HASH,
    }


def receipt(evidence_files: list[dict]) -> dict:
    return {
        "pilot_id": PILOT_ID,
        "plan_hash": PLAN_HASH,
        "authorization_id": AUTHORIZATION_ID,
        "authorization_hash": AUTHORIZATION_HASH,
        "receipt_id": RECEIPT_ID,
        "receipt_hash": RECEIPT_HASH,
        "external_run_id": "fabric-run-1",
        "evidence_files": evidence_files,
    }


def assessment() -> dict:
    return {
        "pilot_id": PILOT_ID,
        "authorization_id": AUTHORIZATION_ID,
        "receipt_id": RECEIPT_ID,
        "receipt_hash": RECEIPT_HASH,
        "assessment_id": ASSESSMENT_ID,
        "assessment_hash": ASSESSMENT_HASH,
        "external_run_id": "fabric-run-1",
    }


def decision() -> dict:
    return {
        "pilot_id": PILOT_ID,
        "plan_hash": PLAN_HASH,
        "authorization_id": AUTHORIZATION_ID,
        "authorization_hash": AUTHORIZATION_HASH,
        "receipt_id": RECEIPT_ID,
        "receipt_hash": RECEIPT_HASH,
        "assessment_id": ASSESSMENT_ID,
        "assessment_hash": ASSESSMENT_HASH,
        "decision_id": DECISION_ID,
        "decision_hash": DECISION_HASH,
        "decision": "continue_evidence_collection",
        "candidate_id": "target-weather-candidate",
        "candidate_version": "0.1.0",
        "external_run_id": "fabric-run-1",
    }


@pytest.fixture(autouse=True)
def verified_chain(monkeypatch):
    monkeypatch.setattr(closure, "verify_fabric_pilot_plan", lambda value: None)
    monkeypatch.setattr(
        closure, "verify_fabric_pilot_preflight", lambda value: None
    )
    monkeypatch.setattr(
        closure, "verify_fabric_pilot_authorization", lambda value: None
    )
    monkeypatch.setattr(
        closure, "verify_fabric_pilot_run_receipt", lambda value: None
    )
    monkeypatch.setattr(
        closure, "verify_fabric_pilot_run_assessment", lambda value: None
    )
    monkeypatch.setattr(closure, "verify_post_pilot_decision", lambda value: None)


@pytest.fixture
def evidence(tmp_path: Path) -> tuple[Path, list[dict]]:
    root = tmp_path / "evidence"
    files = {
        "run_log": "logs/pilot.log",
        "comparison_metrics": "outputs/metrics.parquet",
    }
    evidence_files = []
    for sequence, (role, relative) in enumerate(files.items(), start=1):
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = f"evidence for {role}\n".encode("utf-8")
        path.write_bytes(payload)
        evidence_files.append(
            {
                "evidence_sequence": sequence,
                "evidence_role": role,
                "relative_path": relative,
                "size_bytes": len(payload),
                "sha256": closure._bytes_sha256(payload),
            }
        )
    return root, evidence_files


def create_bundle(
    tmp_path: Path,
    evidence: tuple[Path, list[dict]],
    *,
    output_name: str = "closures",
    **overrides,
):
    root, evidence_files = evidence
    kwargs = {
        "plan": plan(),
        "preflight": preflight(),
        "authorization": authorization(),
        "receipt": receipt(evidence_files),
        "assessment": assessment(),
        "decision": decision(),
        "evidence_root": root,
        "output_root": tmp_path / output_name,
        "created_by": "Alex Reviewer",
        "review_ticket": "PILOT-CLOSURE-001",
        "reason": "Close and preserve the complete reviewed pilot evidence cycle.",
        "created_at_utc": CREATED_AT,
    }
    kwargs.update(overrides)
    return create_post_pilot_closure_bundle(**kwargs)


def test_create_verify_and_recover_complete_closure(tmp_path, evidence):
    bundle_path, manifest, archive_verification = create_bundle(tmp_path, evidence)
    verified_manifest, second_verification = verify_post_pilot_closure_bundle(
        bundle_path, verified_at_utc=CREATED_AT
    )
    recovered, recovery_verification = recover_post_pilot_closure_bundle(
        bundle_path,
        tmp_path / "recovered",
        verified_at_utc=CREATED_AT,
    )
    recovered_manifest, direct_recovery_verification = (
        verify_recovered_post_pilot_closure(
            recovered, verified_at_utc=CREATED_AT
        )
    )

    assert verified_manifest == manifest == recovered_manifest
    assert archive_verification["verification_kind"] == "archive"
    assert second_verification["archive_sha256"] == archive_verification[
        "archive_sha256"
    ]
    assert recovery_verification["verification_kind"] == "recovery"
    assert direct_recovery_verification["manifest_hash"] == manifest[
        "manifest_hash"
    ]
    assert not manifest["model_registry_mutation_allowed"]
    assert not manifest["authorization_reuse_allowed"]
    assert manifest["active_model_unchanged"]
    assert not manifest["permanent_deletion_allowed"]


def test_same_inputs_and_time_produce_same_identity_and_archive_bytes(
    tmp_path, evidence
):
    first_path, first_manifest, _ = create_bundle(
        tmp_path, evidence, output_name="first"
    )
    second_path, second_manifest, _ = create_bundle(
        tmp_path, evidence, output_name="second"
    )
    assert first_manifest["closure_id"] == second_manifest["closure_id"]
    assert first_manifest["manifest_hash"] == second_manifest["manifest_hash"]
    assert first_path.read_bytes() == second_path.read_bytes()


def test_changed_evidence_is_rejected_before_archive_creation(tmp_path, evidence):
    root, evidence_files = evidence
    (root / evidence_files[0]["relative_path"]).write_text(
        "tampered\n", encoding="utf-8"
    )
    with pytest.raises(PostPilotClosureError, match="no longer matches"):
        create_bundle(tmp_path, evidence)


def test_chain_mismatch_is_rejected(tmp_path, evidence):
    mismatched = decision()
    mismatched["assessment_hash"] = "9" * 64
    with pytest.raises(PostPilotClosureError, match="assessment"):
        create_bundle(tmp_path, evidence, decision=mismatched)


def test_symbolic_link_evidence_is_rejected(tmp_path, evidence):
    root, evidence_files = evidence
    original = root / evidence_files[0]["relative_path"]
    link = root / "logs" / "linked.log"
    link.symlink_to(original)
    linked_items = [dict(item) for item in evidence_files]
    linked_items[0] = {
        **linked_items[0],
        "relative_path": "logs/linked.log",
    }
    with pytest.raises(PostPilotClosureError, match="symbolic link"):
        create_bundle(
            tmp_path,
            (root, linked_items),
            receipt=receipt(linked_items),
        )


def test_automation_creator_reason_and_bounds_are_rejected(tmp_path, evidence):
    with pytest.raises(PostPilotClosureError, match="human reviewer"):
        create_bundle(tmp_path, evidence, created_by="github-actions-bot")
    with pytest.raises(PostPilotClosureError, match="at least 20"):
        create_bundle(tmp_path, evidence, reason="too short")
    with pytest.raises(PostPilotClosureError, match="max_members"):
        create_bundle(tmp_path, evidence, max_members=2)
    with pytest.raises(PostPilotClosureError, match="max_total_bytes"):
        create_bundle(tmp_path, evidence, max_total_bytes=1)


def _rewrite_tar(
    source: Path,
    destination: Path,
    *,
    mutate=None,
    extra: tuple[tarfile.TarInfo, bytes] | None = None,
) -> None:
    with tarfile.open(source, "r:*") as incoming, tarfile.open(
        destination, "w", format=tarfile.PAX_FORMAT
    ) as outgoing:
        for member in incoming.getmembers():
            handle = incoming.extractfile(member)
            payload = handle.read() if handle is not None else b""
            info = tarfile.TarInfo(member.name)
            info.size = len(payload)
            info.mode = member.mode
            info.uid = member.uid
            info.gid = member.gid
            info.mtime = member.mtime
            if mutate is not None:
                info, payload = mutate(info, payload)
            outgoing.addfile(info, io.BytesIO(payload))
        if extra is not None:
            info, payload = extra
            outgoing.addfile(info, io.BytesIO(payload))


def test_archive_rejects_path_traversal_and_non_regular_members(tmp_path, evidence):
    bundle_path, _, _ = create_bundle(tmp_path, evidence)

    traversal = tmp_path / "traversal.tar"
    info = tarfile.TarInfo("../escape.txt")
    payload = b"escape"
    info.size = len(payload)
    _rewrite_tar(bundle_path, traversal, extra=(info, payload))
    with pytest.raises(PostPilotClosureError, match="safe relative path"):
        verify_post_pilot_closure_bundle(traversal)

    linked = tmp_path / "linked.tar"
    with tarfile.open(bundle_path, "r:*") as incoming, tarfile.open(
        linked, "w"
    ) as outgoing:
        for member in incoming.getmembers():
            handle = incoming.extractfile(member)
            payload = handle.read() if handle is not None else b""
            clone = tarfile.TarInfo(member.name)
            clone.size = len(payload)
            outgoing.addfile(clone, io.BytesIO(payload))
        link_info = tarfile.TarInfo("unsafe-link")
        link_info.type = tarfile.SYMTYPE
        link_info.linkname = "closure_manifest.json"
        outgoing.addfile(link_info)
    with pytest.raises(PostPilotClosureError, match="not a regular file"):
        verify_post_pilot_closure_bundle(linked)


def test_archive_rejects_extra_member_and_hash_corruption(tmp_path, evidence):
    bundle_path, manifest, _ = create_bundle(tmp_path, evidence)

    extra_path = tmp_path / "extra.tar"
    info = tarfile.TarInfo("unexpected.txt")
    payload = b"unexpected"
    info.size = len(payload)
    _rewrite_tar(bundle_path, extra_path, extra=(info, payload))
    with pytest.raises(PostPilotClosureError, match="unexpected"):
        verify_post_pilot_closure_bundle(extra_path)

    target = manifest["items"][-1]["archive_path"]

    def corrupt(info, payload):
        if info.name == target:
            payload = payload + b"corrupt"
            info.size = len(payload)
        return info, payload

    corrupt_path = tmp_path / "corrupt.tar"
    _rewrite_tar(bundle_path, corrupt_path, mutate=corrupt)
    with pytest.raises(PostPilotClosureError, match="size mismatch"):
        verify_post_pilot_closure_bundle(corrupt_path)


def test_recovery_rejects_existing_target_and_extra_files(tmp_path, evidence):
    bundle_path, manifest, _ = create_bundle(tmp_path, evidence)
    recovery_root = tmp_path / "recovery"
    existing = recovery_root / manifest["closure_id"]
    existing.mkdir(parents=True)
    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        recover_post_pilot_closure_bundle(bundle_path, recovery_root)

    recovered, _ = recover_post_pilot_closure_bundle(
        bundle_path, tmp_path / "fresh-recovery"
    )
    (recovered / "extra.txt").write_text("unexpected", encoding="utf-8")
    with pytest.raises(PostPilotClosureError, match="unexpected"):
        verify_recovered_post_pilot_closure(recovered)


def test_recovered_symbolic_link_is_rejected(tmp_path, evidence):
    bundle_path, _, _ = create_bundle(tmp_path, evidence)
    recovered, _ = recover_post_pilot_closure_bundle(
        bundle_path, tmp_path / "recovery"
    )
    target = recovered / "documents" / "pilot_plan.json"
    target.unlink()
    target.symlink_to(recovered / "closure_manifest.json")
    with pytest.raises(PostPilotClosureError, match="symbolic link"):
        verify_recovered_post_pilot_closure(recovered)


def test_manifest_and_verification_tampering_are_rejected(tmp_path, evidence):
    _, manifest, verification = create_bundle(tmp_path, evidence)
    tampered = dict(manifest)
    tampered["review_ticket"] = "changed"
    with pytest.raises(PostPilotClosureError, match="hash is invalid"):
        verify_post_pilot_closure_manifest(tampered)

    unsafe = dict(verification)
    unsafe["authorization_reuse_allowed"] = True
    unsafe["verification_hash"] = closure._digest(
        unsafe, exclude=("verification_hash",)
    )
    with pytest.raises(
        PostPilotClosureError, match="authorization_reuse_allowed"
    ):
        verify_post_pilot_closure_verification(unsafe)


def test_verification_write_is_immutable(tmp_path, evidence):
    _, _, verification = create_bundle(tmp_path, evidence)
    path = write_post_pilot_closure_verification(
        tmp_path / "verification", verification
    )
    assert path.is_file()
    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        write_post_pilot_closure_verification(
            tmp_path / "verification", verification
        )


def _schema_row(value: dict) -> dict:
    return json.loads(json.dumps(value, default=str))


def test_manifest_and_both_verifications_satisfy_versioned_schemas(
    tmp_path, evidence
):
    bundle_path, manifest, archive_verification = create_bundle(tmp_path, evidence)
    recovered, recovery_verification = recover_post_pilot_closure_bundle(
        bundle_path, tmp_path / "recovered", verified_at_utc=CREATED_AT
    )
    assert recovered.is_dir()
    root = Path(__file__).resolve().parents[1] / "data-contracts"
    manifest_schema = json.loads(
        (root / "post_pilot_closure_manifest_schema.json").read_text(
            encoding="utf-8"
        )
    )
    verification_schema = json.loads(
        (root / "post_pilot_closure_verification_schema.json").read_text(
            encoding="utf-8"
        )
    )
    assert list(
        Draft202012Validator(
            manifest_schema, format_checker=FormatChecker()
        ).iter_errors(_schema_row(manifest))
    ) == []
    for verification in (archive_verification, recovery_verification):
        assert list(
            Draft202012Validator(
                verification_schema, format_checker=FormatChecker()
            ).iter_errors(_schema_row(verification))
        ) == []

from __future__ import annotations

import io
import json
import tarfile
from pathlib import Path

import pandas as pd
import pytest
from jsonschema import Draft202012Validator, FormatChecker

from forecasting.evidence_bundle import (
    EvidenceBundleError,
    create_evidence_bundle,
    recover_evidence_bundle,
    verify_evidence_bundle,
    verify_recovered_bundle,
)
from forecasting.model_registry import (
    register_candidate,
    transition_candidate,
    write_candidate_revision,
)
from forecasting.run_evidence_bundle import main


CREATED = "2026-02-01T12:00:00Z"


def promotion() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "assessment_id": "assessment-bundle",
                "assessment_timestamp_utc": "2026-02-01T09:00:00Z",
                "comparison_run_id": "comparison-bundle",
                "reconciliation_run_id": "reconciliation-bundle",
                "baseline_model": "ridge_weather_lag",
                "candidate_model": "ridge_target_weather",
                "assessment_status": "eligible_for_human_review",
                "automatic_promotion_allowed": False,
                "failed_check_count": 0,
            }
        ]
    )


def health() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "monitor_run_id": "monitor-bundle",
                "monitor_timestamp_utc": "2026-02-01T10:00:00Z",
                "monitor_status": "healthy",
                "automatic_remediation_allowed": False,
                "failed_error_check_count": 0,
                "failed_warning_check_count": 0,
            }
        ]
    )


def candidate(root: Path, *, state: str = "approved") -> Path:
    draft, registered = register_candidate(
        promotion(),
        health(),
        repository="alexmarinos87/weather-energy-demand-pipeline",
        code_commit_sha="a" * 40,
        code_tree_sha="b" * 40,
        candidate_version="0.1.0",
        training_data_boundary_utc="2026-02-01T08:00:00Z",
        feature_contract_versions=(
            "time-horizon-v1",
            "weather-model-comparison-v1",
        ),
        forecast_weather_contract_version="target-weather-v1",
        actor="owner",
        reason="Register candidate",
        created_at_utc="2026-02-01T10:30:00Z",
    )
    directory = root / "model-registry" / draft["candidate_id"]
    write_candidate_revision(directory, draft, registered)
    if state == "draft":
        return directory
    requested, request_event = transition_candidate(
        draft,
        action="review_requested",
        actor="requester",
        reason="Ready for review",
        event_timestamp_utc="2026-02-01T10:40:00Z",
    )
    write_candidate_revision(directory, requested, request_event)
    if state == "review_requested":
        return directory
    approved, approval_event = transition_candidate(
        requested,
        action="approved",
        actor="reviewer",
        reason="Approved for controlled evidence trial",
        review_ticket="REVIEW-BUNDLE-1",
        event_timestamp_utc="2026-02-01T10:50:00Z",
    )
    write_candidate_revision(directory, approved, approval_event)
    return directory


def evidence(root: Path) -> dict[str, Path]:
    paths = {
        "promotion_summary": root
        / "promotion"
        / "target_weather"
        / "target_weather_promotion_summary_assessment-bundle.csv",
        "comparison_predictions": root
        / "forecasting"
        / "weather_comparison_predictions.csv",
        "reconciliation_metrics": root
        / "reconciliation"
        / "forecast_weather"
        / "forecast_weather_quality_metrics_reconciliation-bundle.csv",
        "provider_health_summary": root
        / "monitoring"
        / "forecast_provider"
        / "forecast_provider_health_summary_monitor-bundle.csv",
    }
    for path in paths.values():
        path.parent.mkdir(parents=True, exist_ok=True)
    promotion().to_csv(paths["promotion_summary"], index=False)
    pd.DataFrame(
        [
            {
                "run_id": "comparison-bundle",
                "model_name": "ridge_weather_lag",
                "actual_demand_mw": 100.0,
                "predicted_demand_mw": 101.0,
            },
            {
                "run_id": "comparison-bundle",
                "model_name": "ridge_target_weather",
                "actual_demand_mw": 100.0,
                "predicted_demand_mw": 100.5,
            },
        ]
    ).to_csv(paths["comparison_predictions"], index=False)
    pd.DataFrame(
        [
            {
                "reconciliation_run_id": "reconciliation-bundle",
                "forecast_provider": "openweather",
                "temperature_mae_c": 1.0,
            }
        ]
    ).to_csv(paths["reconciliation_metrics"], index=False)
    health().to_csv(paths["provider_health_summary"], index=False)
    return paths


def create(root: Path, tmp_path: Path, *, candidate_state="approved", output_name="out"):
    directory = candidate(root, state=candidate_state)
    roles = evidence(root)
    return create_evidence_bundle(
        root,
        directory,
        roles,
        actor="operator",
        reason="Create recovery package",
        created_at_utc=CREATED,
        output_root=tmp_path / output_name,
    )


def test_create_and_verify_bundle_without_source_mutation(tmp_path):
    root = tmp_path / "data"
    directory = candidate(root)
    roles = evidence(root)
    before = {path: path.read_bytes() for path in roles.values()}

    manifest, bundle = create_evidence_bundle(
        root,
        directory,
        roles,
        actor="operator",
        reason="Create recovery package",
        created_at_utc=CREATED,
        output_root=tmp_path / "bundles",
    )
    verified_manifest, verification = verify_evidence_bundle(bundle)

    assert verified_manifest["bundle_id"] == manifest["bundle_id"]
    assert verification["verification_status"] == "verified"
    assert verification["candidate_state"] == "approved"
    assert not manifest["deployment_authorized"]
    assert manifest["active_model_unchanged"]
    assert not manifest["source_files_mutated"]
    for path, content in before.items():
        assert path.read_bytes() == content


def test_bundle_is_byte_reproducible_for_same_inputs_and_timestamp(tmp_path):
    root = tmp_path / "data"
    directory = candidate(root)
    roles = evidence(root)
    first_manifest, first = create_evidence_bundle(
        root,
        directory,
        roles,
        actor="operator",
        reason="Create recovery package",
        created_at_utc=CREATED,
        output_root=tmp_path / "one",
    )
    second_manifest, second = create_evidence_bundle(
        root,
        directory,
        roles,
        actor="operator",
        reason="Create recovery package",
        created_at_utc=CREATED,
        output_root=tmp_path / "two",
    )

    assert first_manifest["bundle_id"] == second_manifest["bundle_id"]
    assert first.read_bytes() == second.read_bytes()


def test_unapproved_candidate_is_rejected(tmp_path):
    root = tmp_path / "data"
    directory = candidate(root, state="review_requested")
    with pytest.raises(EvidenceBundleError, match="Only an approved"):
        create_evidence_bundle(
            root,
            directory,
            evidence(root),
            actor="operator",
            reason="Create",
        )


def test_required_roles_must_be_complete_and_exact(tmp_path):
    root = tmp_path / "data"
    directory = candidate(root)
    roles = evidence(root)
    roles.pop("provider_health_summary")
    with pytest.raises(EvidenceBundleError, match="roles are incomplete"):
        create_evidence_bundle(
            root,
            directory,
            roles,
            actor="operator",
            reason="Create",
        )


def test_role_run_id_mismatch_is_rejected(tmp_path):
    root = tmp_path / "data"
    directory = candidate(root)
    roles = evidence(root)
    frame = pd.read_csv(roles["comparison_predictions"])
    frame["run_id"] = "wrong-run"
    frame.to_csv(roles["comparison_predictions"], index=False)
    with pytest.raises(EvidenceBundleError, match="does not match"):
        create_evidence_bundle(
            root,
            directory,
            roles,
            actor="operator",
            reason="Create",
        )


def test_symlink_evidence_is_rejected(tmp_path):
    root = tmp_path / "data"
    directory = candidate(root)
    roles = evidence(root)
    target = roles["comparison_predictions"]
    link = target.with_name("comparison-link.csv")
    link.symlink_to(target)
    roles["comparison_predictions"] = link
    with pytest.raises(EvidenceBundleError, match="symbolic-link"):
        create_evidence_bundle(
            root,
            directory,
            roles,
            actor="operator",
            reason="Create",
        )


def rewrite_tar(source: Path, destination: Path, mutate):
    members = []
    with tarfile.open(source, "r:") as archive:
        for member in archive.getmembers():
            extracted = archive.extractfile(member)
            members.append((member.name, extracted.read() if extracted else b""))
    members = mutate(members)
    with tarfile.open(destination, "w", format=tarfile.PAX_FORMAT) as archive:
        for name, data in members:
            info = tarfile.TarInfo(name)
            info.size = len(data)
            info.mtime = 0
            info.mode = 0o644
            info.uid = info.gid = 0
            archive.addfile(info, io.BytesIO(data))


def test_tampered_bundle_entry_is_rejected(tmp_path):
    root = tmp_path / "data"
    _, bundle = create(root, tmp_path)
    tampered = tmp_path / "tampered.tar"

    def mutate(members):
        name, _ = next(item for item in members if "comparison_predictions" in item[0])
        return [(member, b"tampered" if member == name else data) for member, data in members]

    rewrite_tar(bundle, tampered, mutate)
    with pytest.raises(EvidenceBundleError, match="identity is invalid"):
        verify_evidence_bundle(tampered)


def test_unexpected_and_traversal_tar_members_are_rejected(tmp_path):
    root = tmp_path / "data"
    _, bundle = create(root, tmp_path)
    unexpected = tmp_path / "unexpected.tar"
    rewrite_tar(bundle, unexpected, lambda members: [*members, ("unexpected.txt", b"x")])
    with pytest.raises(EvidenceBundleError, match="unexpected members"):
        verify_evidence_bundle(unexpected)

    traversal = tmp_path / "traversal.tar"
    with tarfile.open(traversal, "w") as archive:
        info = tarfile.TarInfo("../escape.txt")
        info.size = 1
        archive.addfile(info, io.BytesIO(b"x"))
    with pytest.raises(EvidenceBundleError, match="safe relative archive"):
        verify_evidence_bundle(traversal)


def test_tampered_candidate_history_in_bundle_is_rejected(tmp_path):
    root = tmp_path / "data"
    _, bundle = create(root, tmp_path)
    tampered = tmp_path / "candidate-tampered.tar"

    def mutate(members):
        result = []
        changed = False
        for name, data in members:
            if not changed and "/manifest_v" in name:
                payload = json.loads(data.decode("utf-8"))
                payload["candidate_version"] = "9.9.9"
                data = json.dumps(payload).encode("utf-8")
                changed = True
            result.append((name, data))
        return result

    rewrite_tar(bundle, tampered, mutate)
    with pytest.raises(EvidenceBundleError, match="identity is invalid|history failed"):
        verify_evidence_bundle(tampered)


def test_recover_and_reverify_clean_destination(tmp_path):
    root = tmp_path / "data"
    manifest, bundle = create(root, tmp_path)
    destination = tmp_path / "recovered"

    event, event_path = recover_evidence_bundle(
        bundle,
        destination,
        confirm_bundle_id=manifest["bundle_id"],
        actor="recovery-operator",
        reason="Recovery exercise",
        recovered_at_utc="2026-02-01T13:00:00Z",
    )
    verification = verify_recovered_bundle(destination)

    assert event_path.exists()
    assert event["recovery_status"] == "verified"
    assert verification["verification_status"] == "verified"
    assert verification["candidate_state"] == "approved"
    assert not verification["deployment_authorized"]
    assert verification["active_model_unchanged"]
    assert not verification["source_files_mutated"]


def test_recovery_refuses_existing_destination_and_wrong_confirmation(tmp_path):
    root = tmp_path / "data"
    manifest, bundle = create(root, tmp_path)
    destination = tmp_path / "recovered"
    destination.mkdir()
    with pytest.raises(EvidenceBundleError, match="must not already exist"):
        recover_evidence_bundle(
            bundle,
            destination,
            confirm_bundle_id=manifest["bundle_id"],
            actor="operator",
            reason="Recover",
        )
    destination.rmdir()
    with pytest.raises(EvidenceBundleError, match="confirm_bundle_id"):
        recover_evidence_bundle(
            bundle,
            destination,
            confirm_bundle_id="ceb-" + "0" * 24,
            actor="operator",
            reason="Recover",
        )


def test_changed_recovered_entry_is_detected(tmp_path):
    root = tmp_path / "data"
    manifest, bundle = create(root, tmp_path)
    destination = tmp_path / "recovered"
    recover_evidence_bundle(
        bundle,
        destination,
        confirm_bundle_id=manifest["bundle_id"],
        actor="operator",
        reason="Recover",
    )
    evidence_file = next(
        path
        for path in destination.rglob("*")
        if path.is_file() and "comparison_predictions" in path.as_posix()
    )
    evidence_file.write_bytes(b"changed")
    with pytest.raises(EvidenceBundleError, match="Recovered entry changed"):
        verify_recovered_bundle(destination)


def test_max_bundle_bound_is_enforced(tmp_path):
    root = tmp_path / "data"
    directory = candidate(root)
    roles = evidence(root)
    with pytest.raises(EvidenceBundleError, match="above max_bundle_bytes"):
        create_evidence_bundle(
            root,
            directory,
            roles,
            actor="operator",
            reason="Create",
            max_bundle_bytes=10,
        )


def test_cli_create_verify_recover_and_verify_recovery(tmp_path):
    root = tmp_path / "data"
    directory = candidate(root)
    roles = evidence(root)
    output = tmp_path / "bundles"
    create_args = [
        "create",
        "--data-root",
        str(root),
        "--candidate-dir",
        str(directory),
        "--promotion-summary",
        str(roles["promotion_summary"]),
        "--comparison-predictions",
        str(roles["comparison_predictions"]),
        "--reconciliation-metrics",
        str(roles["reconciliation_metrics"]),
        "--provider-health-summary",
        str(roles["provider_health_summary"]),
        "--actor",
        "operator",
        "--reason",
        "Create",
        "--created-at-utc",
        CREATED,
        "--output-root",
        str(output),
    ]
    assert main(create_args) == 0
    bundles = list(output.rglob("*.tar"))
    assert len(bundles) == 1
    manifest, _ = verify_evidence_bundle(bundles[0])
    assert main(["verify", "--bundle", str(bundles[0])]) == 0
    destination = tmp_path / "recovered"
    assert main(
        [
            "recover",
            "--bundle",
            str(bundles[0]),
            "--destination",
            str(destination),
            "--confirm-bundle-id",
            manifest["bundle_id"],
            "--actor",
            "operator",
            "--reason",
            "Recover",
            "--recovered-at-utc",
            "2026-02-01T13:00:00Z",
        ]
    ) == 0
    assert main(["verify-recovery", "--destination", str(destination)]) == 0


def test_bundle_manifest_and_recovery_event_satisfy_schemas(tmp_path):
    root = tmp_path / "data"
    manifest, bundle = create(root, tmp_path)
    destination = tmp_path / "recovered"
    event, _ = recover_evidence_bundle(
        bundle,
        destination,
        confirm_bundle_id=manifest["bundle_id"],
        actor="operator",
        reason="Recover",
        recovered_at_utc="2026-02-01T13:00:00Z",
    )
    contracts = Path(__file__).resolve().parents[1] / "data-contracts"
    bundle_schema = json.loads(
        (contracts / "candidate_evidence_bundle_schema.json").read_text(
            encoding="utf-8"
        )
    )
    recovery_schema = json.loads(
        (contracts / "candidate_evidence_recovery_schema.json").read_text(
            encoding="utf-8"
        )
    )
    checker = FormatChecker()
    assert list(
        Draft202012Validator(bundle_schema, format_checker=checker).iter_errors(
            manifest
        )
    ) == []
    assert list(
        Draft202012Validator(
            recovery_schema, format_checker=checker
        ).iter_errors(event)
    ) == []

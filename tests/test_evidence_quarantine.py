from datetime import datetime, timezone
import json
import os
from pathlib import Path

import pandas as pd
import pytest
from jsonschema import Draft202012Validator, FormatChecker

from forecasting.evidence_lifecycle import (
    inventory_evidence,
    load_retention_policy,
    plan_evidence_lifecycle,
)
from forecasting.evidence_quarantine import (
    EvidenceQuarantineError,
    load_quarantine_manifest,
    prepare_quarantine_candidates,
    quarantine_evidence,
    restore_evidence,
    verify_quarantine_state,
)
from forecasting.run_evidence_quarantine import main


AS_OF = pd.Timestamp("2026-01-31T00:00:00Z")


def policy_path(tmp_path: Path) -> Path:
    path = tmp_path / "policy.json"
    path.write_text(
        json.dumps(
            {
                "contract_version": "evidence-retention-policy-v1",
                "excluded_prefixes": ["lifecycle/", "quarantine/"],
                "protected_candidate_states": ["review_requested", "approved"],
                "unclassified_action": "retain",
                "categories": [
                    {
                        "category": "test_evidence",
                        "patterns": ["evidence/**"],
                        "retention_days": 30,
                        "min_keep_latest": 1,
                        "compact_after_days": None,
                        "compaction_formats": [],
                        "min_compaction_files": 0,
                        "max_compaction_source_bytes": 0,
                        "always_protect": False,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    return path


def evidence_file(root: Path, name: str, age_days: int, content: bytes) -> Path:
    path = root / "evidence" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    timestamp = (AS_OF - pd.Timedelta(days=age_days)).timestamp()
    os.utime(path, (timestamp, timestamp))
    return path


def lifecycle(root: Path, tmp_path: Path):
    policy = load_retention_policy(policy_path(tmp_path))
    inventory = inventory_evidence(root, policy, as_of_utc=AS_OF)
    plan, summary = plan_evidence_lifecycle(
        inventory,
        policy,
        plan_created_at_utc=AS_OF,
    )
    return plan, summary


def prepared(root: Path, tmp_path: Path):
    plan, summary = lifecycle(root, tmp_path)
    candidates, plan_id = prepare_quarantine_candidates(
        plan, summary, confirm_plan_id=summary["plan_id"]
    )
    return plan, summary, candidates, plan_id


def test_apply_moves_only_verified_quarantine_candidates(tmp_path):
    root = tmp_path / "data"
    old = evidence_file(root, "old.csv", 100, b"old")
    latest = evidence_file(root, "latest.csv", 90, b"latest")
    _, _, candidates, plan_id = prepared(root, tmp_path)

    manifest, manifest_path = quarantine_evidence(
        root,
        candidates,
        plan_id=plan_id,
        actor="operator",
        reason="Reviewed retention application",
        applied_at_utc="2026-01-31T01:00:00Z",
    )

    assert not old.exists()
    assert latest.read_bytes() == b"latest"
    quarantined = root / manifest["entries"][0]["quarantine_relative_path"]
    assert quarantined.read_bytes() == b"old"
    assert manifest_path.exists()
    assert not manifest["permanent_deletion_performed"]
    assert manifest["restore_available"]
    assert verify_quarantine_state(root, manifest)["state"] == "quarantined"


def test_confirm_plan_id_must_match_exactly(tmp_path):
    root = tmp_path / "data"
    evidence_file(root, "old.csv", 100, b"old")
    evidence_file(root, "latest.csv", 1, b"latest")
    plan, summary = lifecycle(root, tmp_path)

    with pytest.raises(EvidenceQuarantineError, match="confirm_plan_id"):
        prepare_quarantine_candidates(
            plan, summary, confirm_plan_id="elp-" + "0" * 24
        )


def test_tampered_plan_identity_is_rejected(tmp_path):
    root = tmp_path / "data"
    evidence_file(root, "old.csv", 100, b"old")
    evidence_file(root, "latest.csv", 1, b"latest")
    plan, summary = lifecycle(root, tmp_path)
    plan.loc[plan["file_name"] == "old.csv", "planned_action"] = "retain"

    with pytest.raises(EvidenceQuarantineError, match="identity"):
        prepare_quarantine_candidates(
            plan, summary, confirm_plan_id=summary["plan_id"]
        )


def test_source_hash_change_blocks_all_moves(tmp_path):
    root = tmp_path / "data"
    first = evidence_file(root, "first.csv", 100, b"first")
    second = evidence_file(root, "second.csv", 99, b"second")
    latest = evidence_file(root, "latest.csv", 1, b"latest")
    _, _, candidates, plan_id = prepared(root, tmp_path)
    second.write_bytes(b"changed")

    with pytest.raises(
        EvidenceQuarantineError, match="changed after lifecycle planning"
    ):
        quarantine_evidence(
            root,
            candidates,
            plan_id=plan_id,
            actor="operator",
            reason="Apply",
        )

    assert first.read_bytes() == b"first"
    assert second.read_bytes() == b"changed"
    assert latest.read_bytes() == b"latest"
    assert not (root / "quarantine").exists()


def test_protected_or_always_protected_plan_rows_are_rejected(tmp_path):
    root = tmp_path / "data"
    evidence_file(root, "old.csv", 100, b"old")
    evidence_file(root, "latest.csv", 1, b"latest")
    plan, summary = lifecycle(root, tmp_path)
    old_index = plan.index[plan["file_name"] == "old.csv"][0]
    plan.loc[old_index, "protected_by_candidate"] = True
    with pytest.raises(EvidenceQuarantineError, match="Candidate-protected"):
        prepare_quarantine_candidates(
            plan, summary, confirm_plan_id=summary["plan_id"]
        )

    plan.loc[old_index, "protected_by_candidate"] = False
    candidates, _ = prepare_quarantine_candidates(
        plan, summary, confirm_plan_id=summary["plan_id"]
    )
    candidates.loc[0, "always_protect"] = True
    with pytest.raises(EvidenceQuarantineError, match="Always-protected"):
        prepare_quarantine_candidates(
            pd.concat(
                [
                    plan.loc[plan["planned_action"] != "quarantine_candidate"],
                    candidates,
                ],
                ignore_index=True,
            ),
            summary,
            confirm_plan_id=summary["plan_id"],
        )


def test_path_traversal_is_rejected(tmp_path):
    root = tmp_path / "data"
    evidence_file(root, "old.csv", 100, b"old")
    evidence_file(root, "latest.csv", 1, b"latest")
    plan, summary = lifecycle(root, tmp_path)
    plan.loc[plan["file_name"] == "old.csv", "relative_path"] = "../escape.csv"
    with pytest.raises(EvidenceQuarantineError, match="identity|safe relative"):
        prepare_quarantine_candidates(
            plan, summary, confirm_plan_id=summary["plan_id"]
        )


def test_symlink_source_is_rejected(tmp_path):
    root = tmp_path / "data"
    target = evidence_file(root, "target.csv", 100, b"target")
    latest = evidence_file(root, "latest.csv", 1, b"latest")
    link = root / "evidence" / "link.csv"
    link.symlink_to(target)
    candidates = pd.DataFrame(
        [
            {
                "relative_path": "evidence/link.csv",
                "category": "test_evidence",
                "size_bytes": target.stat().st_size,
                "sha256": "0" * 64,
                "modified_at_utc": AS_OF - pd.Timedelta(days=100),
            }
        ]
    )
    with pytest.raises(EvidenceQuarantineError, match="symbolic-link"):
        quarantine_evidence(
            root,
            candidates,
            plan_id="elp-" + "a" * 24,
            actor="operator",
            reason="Apply",
        )
    assert latest.exists()


def test_existing_destination_blocks_preflight(tmp_path):
    root = tmp_path / "data"
    old = evidence_file(root, "old.csv", 100, b"old")
    evidence_file(root, "latest.csv", 1, b"latest")
    _, _, candidates, plan_id = prepared(root, tmp_path)
    destination = root / "quarantine" / plan_id / "files" / "evidence" / "old.csv"
    destination.parent.mkdir(parents=True)
    destination.write_bytes(b"collision")

    with pytest.raises(EvidenceQuarantineError, match="already exists"):
        quarantine_evidence(
            root,
            candidates,
            plan_id=plan_id,
            actor="operator",
            reason="Apply",
        )
    assert old.read_bytes() == b"old"


def test_late_move_failure_rolls_back_completed_moves(tmp_path, monkeypatch):
    root = tmp_path / "data"
    first = evidence_file(root, "first.csv", 100, b"first")
    second = evidence_file(root, "second.csv", 99, b"second")
    evidence_file(root, "latest.csv", 1, b"latest")
    _, _, candidates, plan_id = prepared(root, tmp_path)
    original_replace = Path.replace

    def failing_replace(self, target):
        if self.name == "second.csv" and "quarantine" not in self.as_posix():
            raise OSError("simulated move failure")
        return original_replace(self, target)

    monkeypatch.setattr(Path, "replace", failing_replace)
    with pytest.raises(OSError, match="simulated"):
        quarantine_evidence(
            root,
            candidates,
            plan_id=plan_id,
            actor="operator",
            reason="Apply",
        )
    assert first.read_bytes() == b"first"
    assert second.read_bytes() == b"second"


def test_restore_returns_exact_content_and_writes_event(tmp_path):
    root = tmp_path / "data"
    source = evidence_file(root, "old.csv", 100, b"old")
    evidence_file(root, "latest.csv", 1, b"latest")
    _, _, candidates, plan_id = prepared(root, tmp_path)
    manifest, _ = quarantine_evidence(
        root,
        candidates,
        plan_id=plan_id,
        actor="operator",
        reason="Apply",
        applied_at_utc="2026-01-31T01:00:00Z",
    )

    event, event_path = restore_evidence(
        root,
        manifest,
        confirm_operation_id=manifest["operation_id"],
        actor="reviewer",
        reason="Restore for evidence review",
        restored_at_utc="2026-01-31T02:00:00Z",
    )

    assert source.read_bytes() == b"old"
    assert verify_quarantine_state(root, manifest)["state"] == "restored"
    assert event_path.exists()
    assert not event["permanent_deletion_performed"]


def test_restore_refuses_to_overwrite_recreated_source(tmp_path):
    root = tmp_path / "data"
    source = evidence_file(root, "old.csv", 100, b"old")
    evidence_file(root, "latest.csv", 1, b"latest")
    _, _, candidates, plan_id = prepared(root, tmp_path)
    manifest, _ = quarantine_evidence(
        root,
        candidates,
        plan_id=plan_id,
        actor="operator",
        reason="Apply",
    )
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_bytes(b"replacement")

    with pytest.raises(EvidenceQuarantineError, match="ambiguous|already exists"):
        restore_evidence(
            root,
            manifest,
            confirm_operation_id=manifest["operation_id"],
            actor="reviewer",
            reason="Restore",
        )
    assert source.read_bytes() == b"replacement"


def test_tampered_manifest_is_rejected(tmp_path):
    root = tmp_path / "data"
    evidence_file(root, "old.csv", 100, b"old")
    evidence_file(root, "latest.csv", 1, b"latest")
    _, _, candidates, plan_id = prepared(root, tmp_path)
    manifest, manifest_path = quarantine_evidence(
        root,
        candidates,
        plan_id=plan_id,
        actor="operator",
        reason="Apply",
    )
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["reason"] = "tampered"
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(EvidenceQuarantineError, match="manifest hash"):
        load_quarantine_manifest(manifest_path)
    assert manifest["operation_id"]


def test_mixed_quarantine_state_is_rejected(tmp_path):
    root = tmp_path / "data"
    evidence_file(root, "first.csv", 100, b"first")
    evidence_file(root, "second.csv", 99, b"second")
    evidence_file(root, "latest.csv", 1, b"latest")
    _, _, candidates, plan_id = prepared(root, tmp_path)
    manifest, _ = quarantine_evidence(
        root,
        candidates,
        plan_id=plan_id,
        actor="operator",
        reason="Apply",
    )
    first = manifest["entries"][0]
    quarantine = root / first["quarantine_relative_path"]
    source = root / first["source_relative_path"]
    source.parent.mkdir(parents=True, exist_ok=True)
    quarantine.replace(source)
    with pytest.raises(EvidenceQuarantineError, match="mixed state"):
        verify_quarantine_state(root, manifest)


def test_cli_apply_verify_and_restore(tmp_path):
    root = tmp_path / "data"
    source = evidence_file(root, "old.csv", 100, b"old")
    evidence_file(root, "latest.csv", 1, b"latest")
    plan, summary = lifecycle(root, tmp_path)
    plan_path = tmp_path / "plan.csv"
    summary_path = tmp_path / "summary.json"
    plan.to_csv(plan_path, index=False)
    summary_path.write_text(json.dumps(summary), encoding="utf-8")

    assert main(
        [
            "apply",
            "--data-root",
            str(root),
            "--plan",
            str(plan_path),
            "--summary",
            str(summary_path),
            "--confirm-plan-id",
            summary["plan_id"],
            "--actor",
            "operator",
            "--reason",
            "Apply",
            "--applied-at-utc",
            "2026-01-31T01:00:00Z",
        ]
    ) == 0
    manifests = list(
        (root / "quarantine" / summary["plan_id"]).glob(
            "quarantine_manifest_*.json"
        )
    )
    assert len(manifests) == 1
    manifest = load_quarantine_manifest(manifests[0])
    assert main(
        [
            "verify",
            "--data-root",
            str(root),
            "--manifest",
            str(manifests[0]),
        ]
    ) == 0
    assert main(
        [
            "restore",
            "--data-root",
            str(root),
            "--manifest",
            str(manifests[0]),
            "--confirm-operation-id",
            manifest["operation_id"],
            "--actor",
            "reviewer",
            "--reason",
            "Restore",
            "--restored-at-utc",
            "2026-01-31T02:00:00Z",
        ]
    ) == 0
    assert source.read_bytes() == b"old"


def test_manifest_and_restore_event_satisfy_schemas(tmp_path):
    root = tmp_path / "data"
    evidence_file(root, "old.csv", 100, b"old")
    evidence_file(root, "latest.csv", 1, b"latest")
    _, _, candidates, plan_id = prepared(root, tmp_path)
    manifest, _ = quarantine_evidence(
        root,
        candidates,
        plan_id=plan_id,
        actor="operator",
        reason="Apply",
        applied_at_utc="2026-01-31T01:00:00Z",
    )
    event, _ = restore_evidence(
        root,
        manifest,
        confirm_operation_id=manifest["operation_id"],
        actor="reviewer",
        reason="Restore",
        restored_at_utc="2026-01-31T02:00:00Z",
    )
    contracts = Path(__file__).resolve().parents[1] / "data-contracts"
    manifest_schema = json.loads(
        (contracts / "evidence_quarantine_manifest_schema.json").read_text(
            encoding="utf-8"
        )
    )
    event_schema = json.loads(
        (contracts / "evidence_restore_event_schema.json").read_text(
            encoding="utf-8"
        )
    )
    checker = FormatChecker()
    assert list(
        Draft202012Validator(
            manifest_schema, format_checker=checker
        ).iter_errors(manifest)
    ) == []
    assert list(
        Draft202012Validator(event_schema, format_checker=checker).iter_errors(
            event
        )
    ) == []

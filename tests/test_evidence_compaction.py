from __future__ import annotations

import json
import os
from pathlib import Path

import pandas as pd
import pytest
from jsonschema import Draft202012Validator, FormatChecker

from forecasting.evidence_compaction import (
    EvidenceCompactionError,
    load_compaction_manifest,
    prepare_compaction_groups,
    stage_compactions,
    verify_staged_compaction,
)
from forecasting.evidence_lifecycle import (
    inventory_evidence,
    load_retention_policy,
    plan_evidence_lifecycle,
)
from forecasting.run_evidence_compaction import main


AS_OF = pd.Timestamp("2026-02-01T00:00:00Z")


def policy_path(tmp_path: Path, *, minimum: int = 3) -> Path:
    path = tmp_path / "policy.json"
    path.write_text(
        json.dumps(
            {
                "contract_version": "evidence-retention-policy-v1",
                "excluded_prefixes": ["lifecycle/", "quarantine/", "compacted/"],
                "protected_candidate_states": ["review_requested", "approved"],
                "unclassified_action": "retain",
                "categories": [
                    {
                        "category": "test_evidence",
                        "patterns": ["evidence/**"],
                        "retention_days": 100,
                        "min_keep_latest": 0,
                        "compact_after_days": 10,
                        "compaction_formats": [".csv", ".parquet", ".pq"],
                        "min_compaction_files": minimum,
                        "max_compaction_source_bytes": 1048576,
                        "always_protect": False,
                    },
                    {
                        "category": "compacted_evidence",
                        "patterns": ["compacted/**"],
                        "retention_days": None,
                        "min_keep_latest": 0,
                        "compact_after_days": None,
                        "compaction_formats": [],
                        "min_compaction_files": 0,
                        "max_compaction_source_bytes": 0,
                        "always_protect": True,
                    },
                ],
            }
        ),
        encoding="utf-8",
    )
    return path


def write_frame(
    root: Path,
    relative: str,
    frame: pd.DataFrame,
    *,
    age_days: int = 20,
) -> Path:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.suffix == ".csv":
        frame.to_csv(path, index=False)
    else:
        frame.to_parquet(path, index=False)
    timestamp = (AS_OF - pd.Timedelta(days=age_days)).timestamp()
    os.utime(path, (timestamp, timestamp))
    return path


def sample(start: int, rows: int = 2) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "event_id": list(range(start, start + rows)),
            "value": [float(index) / 10 for index in range(start, start + rows)],
            "label": [f"row-{index}" for index in range(start, start + rows)],
        }
    )


def lifecycle(root: Path, tmp_path: Path, *, minimum: int = 3):
    policy = load_retention_policy(policy_path(tmp_path, minimum=minimum))
    inventory = inventory_evidence(root, policy, as_of_utc=AS_OF)
    plan, summary = plan_evidence_lifecycle(
        inventory,
        policy,
        plan_created_at_utc=AS_OF,
    )
    return plan, summary


def prepared(root: Path, tmp_path: Path, *, minimum: int = 3):
    plan, summary = lifecycle(root, tmp_path, minimum=minimum)
    groups, plan_id = prepare_compaction_groups(
        plan, summary, confirm_plan_id=summary["plan_id"]
    )
    return plan, summary, groups, plan_id


def create_csv_group(root: Path, *, count: int = 3, parent: str = "partition=a"):
    paths = []
    for index in range(count):
        paths.append(
            write_frame(
                root,
                f"evidence/{parent}/part-{index}.csv",
                sample(index * 2),
            )
        )
    return paths


def test_stage_csv_group_preserves_sources_and_verifies_output(tmp_path):
    root = tmp_path / "data"
    sources = create_csv_group(root)
    before = {path: path.read_bytes() for path in sources}
    _, _, groups, plan_id = prepared(root, tmp_path)

    results = stage_compactions(
        root,
        groups,
        plan_id=plan_id,
        actor="operator",
        reason="Stage verified compaction",
        staged_at_utc="2026-02-01T01:00:00Z",
    )

    assert len(results) == 1
    manifest, manifest_path = results[0]
    assert manifest_path.exists()
    assert not manifest["source_files_mutated"]
    assert not manifest["source_files_permanently_deleted"]
    assert not manifest["replacement_authorized"]
    for path in sources:
        assert path.read_bytes() == before[path]
    output = root / manifest["compacted_output_relative_path"]
    assert len(pd.read_parquet(output)) == 6
    verification = verify_staged_compaction(root, manifest)
    assert verification["verification_status"] == "verified"
    assert verification["verified_source_file_count"] == 3


def test_stage_parquet_group_preserves_schema_and_row_order(tmp_path):
    root = tmp_path / "data"
    for index in range(3):
        write_frame(
            root,
            f"evidence/partition=a/part-{index}.parquet",
            sample(index * 2),
        )
    _, _, groups, plan_id = prepared(root, tmp_path)
    manifest, _ = stage_compactions(
        root,
        groups,
        plan_id=plan_id,
        actor="operator",
        reason="Stage",
        staged_at_utc="2026-02-01T01:00:00Z",
    )[0]

    output = pd.read_parquet(root / manifest["compacted_output_relative_path"])
    assert output["event_id"].tolist() == [0, 1, 2, 3, 4, 5]
    assert manifest["source_schema"]["columns"] == ["event_id", "value", "label"]
    assert manifest["source_total_rows"] == 6


def test_schema_mismatch_blocks_output_before_publication(tmp_path):
    root = tmp_path / "data"
    create_csv_group(root, count=2)
    write_frame(
        root,
        "evidence/partition=a/part-2.csv",
        pd.DataFrame({"different": [1], "value": [1.0]}),
    )
    _, _, groups, plan_id = prepared(root, tmp_path)

    with pytest.raises(EvidenceCompactionError, match="schema differs"):
        stage_compactions(
            root,
            groups,
            plan_id=plan_id,
            actor="operator",
            reason="Stage",
        )
    assert not (root / "compacted").exists()


def test_source_change_blocks_all_groups_before_output(tmp_path):
    root = tmp_path / "data"
    first_group = create_csv_group(root, parent="partition=a")
    create_csv_group(root, parent="partition=b")
    _, _, groups, plan_id = prepared(root, tmp_path)
    first_group[1].write_text("changed\n", encoding="utf-8")

    with pytest.raises(EvidenceCompactionError, match="changed after planning"):
        stage_compactions(
            root,
            groups,
            plan_id=plan_id,
            actor="operator",
            reason="Stage",
        )
    assert not (root / "compacted").exists()


def test_candidate_protected_compaction_row_is_rejected(tmp_path):
    root = tmp_path / "data"
    create_csv_group(root)
    plan, summary = lifecycle(root, tmp_path)
    plan.loc[plan["planned_action"] == "compact_candidate", "protected_by_candidate"] = True

    with pytest.raises(EvidenceCompactionError, match="Candidate-protected"):
        prepare_compaction_groups(
            plan, summary, confirm_plan_id=summary["plan_id"]
        )


def test_plan_confirmation_and_identity_are_required(tmp_path):
    root = tmp_path / "data"
    create_csv_group(root)
    plan, summary = lifecycle(root, tmp_path)
    with pytest.raises(EvidenceCompactionError, match="confirm_plan_id"):
        prepare_compaction_groups(
            plan, summary, confirm_plan_id="elp-" + "0" * 24
        )
    plan.loc[0, "planned_action"] = "retain"
    with pytest.raises(EvidenceCompactionError, match="identity"):
        prepare_compaction_groups(
            plan, summary, confirm_plan_id=summary["plan_id"]
        )


def test_group_must_meet_planned_minimum_file_count(tmp_path):
    root = tmp_path / "data"
    create_csv_group(root, count=2)
    plan, summary = lifecycle(root, tmp_path, minimum=2)
    selected = plan["planned_action"] == "compact_candidate"
    plan.loc[selected, "min_compaction_files"] = 3
    # The protection/threshold field is a direct safety gate even though the
    # plan identity deliberately binds only paths, hashes, and actions.
    with pytest.raises(EvidenceCompactionError, match="minimum is 3"):
        prepare_compaction_groups(
            plan, summary, confirm_plan_id=summary["plan_id"]
        )


def test_existing_output_collision_is_rejected_without_source_change(tmp_path):
    root = tmp_path / "data"
    sources = create_csv_group(root)
    _, _, groups, plan_id = prepared(root, tmp_path)
    kwargs = dict(
        plan_id=plan_id,
        actor="operator",
        reason="Stage",
        staged_at_utc="2026-02-01T01:00:00Z",
    )
    stage_compactions(root, groups, **kwargs)
    before = {path: path.read_bytes() for path in sources}
    with pytest.raises(FileExistsError, match="already exists"):
        stage_compactions(root, groups, **kwargs)
    for path in sources:
        assert path.read_bytes() == before[path]


def test_tampered_output_and_manifest_are_rejected(tmp_path):
    root = tmp_path / "data"
    create_csv_group(root)
    _, _, groups, plan_id = prepared(root, tmp_path)
    manifest, manifest_path = stage_compactions(
        root,
        groups,
        plan_id=plan_id,
        actor="operator",
        reason="Stage",
    )[0]
    output = root / manifest["compacted_output_relative_path"]
    output.write_bytes(b"tampered")
    with pytest.raises(EvidenceCompactionError, match="output file identity"):
        verify_staged_compaction(root, manifest)

    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["reason"] = "tampered"
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(EvidenceCompactionError, match="manifest hash"):
        load_compaction_manifest(manifest_path)


def test_failure_in_later_group_removes_earlier_generated_artifacts(tmp_path, monkeypatch):
    root = tmp_path / "data"
    create_csv_group(root, parent="partition=a")
    create_csv_group(root, parent="partition=b")
    _, _, groups, plan_id = prepared(root, tmp_path)
    original = pd.DataFrame.to_parquet
    calls = {"count": 0}

    def fail_second(self, path, *args, **kwargs):
        calls["count"] += 1
        if calls["count"] == 2:
            raise OSError("simulated parquet failure")
        return original(self, path, *args, **kwargs)

    monkeypatch.setattr(pd.DataFrame, "to_parquet", fail_second)
    with pytest.raises(OSError, match="simulated"):
        stage_compactions(
            root,
            groups,
            plan_id=plan_id,
            actor="operator",
            reason="Stage",
        )
    assert list((root / "compacted").rglob("*.parquet")) == []
    assert list((root / "compacted").rglob("*.json")) == []


def test_cli_stage_and_verify(tmp_path):
    root = tmp_path / "data"
    create_csv_group(root)
    plan, summary = lifecycle(root, tmp_path)
    plan_path = tmp_path / "plan.csv"
    summary_path = tmp_path / "summary.json"
    plan.to_csv(plan_path, index=False)
    summary_path.write_text(json.dumps(summary), encoding="utf-8")

    assert main(
        [
            "stage",
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
            "Stage",
            "--staged-at-utc",
            "2026-02-01T01:00:00Z",
        ]
    ) == 0
    manifests = list((root / "compacted").rglob("compaction_manifest_*.json"))
    assert len(manifests) == 1
    assert main(
        [
            "verify",
            "--data-root",
            str(root),
            "--manifest",
            str(manifests[0]),
        ]
    ) == 0


def test_compaction_manifest_satisfies_schema(tmp_path):
    root = tmp_path / "data"
    create_csv_group(root)
    _, _, groups, plan_id = prepared(root, tmp_path)
    manifest, _ = stage_compactions(
        root,
        groups,
        plan_id=plan_id,
        actor="operator",
        reason="Stage",
        staged_at_utc="2026-02-01T01:00:00Z",
    )[0]
    schema = json.loads(
        (
            Path(__file__).resolve().parents[1]
            / "data-contracts"
            / "evidence_compaction_manifest_schema.json"
        ).read_text(encoding="utf-8")
    )
    assert list(
        Draft202012Validator(
            schema, format_checker=FormatChecker()
        ).iter_errors(manifest)
    ) == []

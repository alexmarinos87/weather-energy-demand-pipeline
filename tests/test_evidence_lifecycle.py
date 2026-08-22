from datetime import datetime, timezone
import json
import os
from pathlib import Path

import pandas as pd
import pytest
from jsonschema import Draft202012Validator, FormatChecker

from forecasting.evidence_lifecycle import (
    EvidenceLifecycleError,
    inventory_evidence,
    load_protected_candidate_references,
    load_retention_policy,
    plan_evidence_lifecycle,
)
from forecasting.model_registry import (
    register_candidate,
    transition_candidate,
    write_candidate_revision,
)
from forecasting.run_evidence_lifecycle import main


AS_OF = pd.Timestamp("2026-01-31T00:00:00Z")


def category(
    *,
    name="test_evidence",
    pattern="evidence/**",
    retention_days=30,
    min_keep_latest=0,
    compact_after_days=None,
    compaction_formats=None,
    min_compaction_files=0,
    max_compaction_source_bytes=0,
    always_protect=False,
):
    return {
        "category": name,
        "patterns": [pattern],
        "retention_days": retention_days,
        "min_keep_latest": min_keep_latest,
        "compact_after_days": compact_after_days,
        "compaction_formats": compaction_formats or [],
        "min_compaction_files": min_compaction_files,
        "max_compaction_source_bytes": max_compaction_source_bytes,
        "always_protect": always_protect,
    }


def policy_file(tmp_path: Path, categories, *, states=None) -> Path:
    path = tmp_path / "policy.json"
    path.write_text(
        json.dumps(
            {
                "contract_version": "evidence-retention-policy-v1",
                "excluded_prefixes": ["lifecycle/", "quarantine/"],
                "protected_candidate_states": states
                or ["review_requested", "approved"],
                "unclassified_action": "retain",
                "categories": categories,
            }
        ),
        encoding="utf-8",
    )
    return path


def write_file(root: Path, relative: str, *, age_days: float, content=b"data") -> Path:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    timestamp = (AS_OF - pd.Timedelta(days=age_days)).timestamp()
    os.utime(path, (timestamp, timestamp))
    return path


def plan(root: Path, policy_path: Path, *, references=None, cost=0.0):
    loaded = load_retention_policy(policy_path)
    inventory = inventory_evidence(root, loaded, as_of_utc=AS_OF)
    return inventory, *plan_evidence_lifecycle(
        inventory,
        loaded,
        protected_references=references,
        monthly_storage_cost_per_gib=cost,
        plan_created_at_utc=AS_OF,
    )


def test_inventory_classifies_hashes_and_retains_unclassified_by_default(tmp_path):
    root = tmp_path / "data"
    known = write_file(root, "evidence/known.csv", age_days=5, content=b"known")
    unknown = write_file(root, "misc/unknown.bin", age_days=200, content=b"unknown")
    policy_path = policy_file(tmp_path, [category()])

    inventory, lifecycle, summary = plan(root, policy_path)

    known_row = inventory.loc[inventory["relative_path"] == "evidence/known.csv"].iloc[0]
    unknown_row = lifecycle.loc[
        lifecycle["relative_path"] == "misc/unknown.bin"
    ].iloc[0]
    assert known_row["category"] == "test_evidence"
    assert known_row["classified"]
    assert known_row["sha256"] != ""
    assert known.read_bytes() == b"known"
    assert not unknown_row["classified"]
    assert unknown_row["planned_action"] == "retain"
    assert unknown.read_bytes() == b"unknown"
    assert not summary["mutation_performed"]


def test_old_unprotected_file_becomes_quarantine_candidate(tmp_path):
    root = tmp_path / "data"
    write_file(root, "evidence/old.parquet", age_days=31)
    policy_path = policy_file(tmp_path, [category()])

    _, lifecycle, summary = plan(root, policy_path)

    row = lifecycle.iloc[0]
    assert row["planned_action"] == "quarantine_candidate"
    assert row["requires_explicit_apply"]
    assert row["estimated_reclaimable_bytes"] == row["size_bytes"]
    assert summary["quarantine_candidate_file_count"] == 1


def test_minimum_latest_floor_precedes_retention_age(tmp_path):
    root = tmp_path / "data"
    write_file(root, "evidence/older.csv", age_days=100, content=b"old")
    write_file(root, "evidence/latest.csv", age_days=90, content=b"new")
    policy_path = policy_file(
        tmp_path, [category(retention_days=30, min_keep_latest=1)]
    )

    _, lifecycle, _ = plan(root, policy_path)
    actions = dict(zip(lifecycle["file_name"], lifecycle["planned_action"]))
    assert actions["latest.csv"] == "retain"
    assert actions["older.csv"] == "quarantine_candidate"


def test_compaction_requires_threshold_number_in_same_parent(tmp_path):
    root = tmp_path / "data"
    for index in range(5):
        write_file(
            root,
            f"evidence/partition=a/part-{index}.parquet",
            age_days=20,
            content=f"{index}".encode(),
        )
    write_file(
        root,
        "evidence/partition=b/only.parquet",
        age_days=20,
        content=b"single",
    )
    policy_path = policy_file(
        tmp_path,
        [
            category(
                retention_days=100,
                compact_after_days=10,
                compaction_formats=[".parquet"],
                min_compaction_files=5,
                max_compaction_source_bytes=100,
            )
        ],
    )

    _, lifecycle, summary = plan(root, policy_path)

    partition_a = lifecycle["relative_path"].str.contains("partition=a")
    partition_b = lifecycle["relative_path"].str.contains("partition=b")
    assert set(lifecycle.loc[partition_a, "planned_action"]) == {
        "compact_candidate"
    }
    assert set(lifecycle.loc[partition_b, "planned_action"]) == {"retain"}
    assert summary["compact_candidate_file_count"] == 5


def test_always_protected_category_cannot_be_quarantined(tmp_path):
    root = tmp_path / "data"
    write_file(root, "registry/manifest.json", age_days=1000)
    policy_path = policy_file(
        tmp_path,
        [
            category(
                name="registry",
                pattern="registry/**",
                retention_days=1,
                always_protect=True,
            )
        ],
    )

    _, lifecycle, _ = plan(root, policy_path)
    assert lifecycle.loc[0, "planned_action"] == "retain"
    assert "always protected" in lifecycle.loc[0, "action_reason"]


def _promotion_summary(comparison_run_id="comparison-keep"):
    return pd.DataFrame(
        [
            {
                "assessment_id": "assessment-keep",
                "assessment_timestamp_utc": "2026-01-30T00:00:00Z",
                "comparison_run_id": comparison_run_id,
                "reconciliation_run_id": "reconciliation-keep",
                "baseline_model": "ridge_weather_lag",
                "candidate_model": "ridge_target_weather",
                "assessment_status": "eligible_for_human_review",
                "automatic_promotion_allowed": False,
                "failed_check_count": 0,
            }
        ]
    )


def _health_summary():
    return pd.DataFrame(
        [
            {
                "monitor_run_id": "monitor-keep",
                "monitor_timestamp_utc": "2026-01-30T01:00:00Z",
                "monitor_status": "healthy",
                "automatic_remediation_allowed": False,
                "failed_error_check_count": 0,
                "failed_warning_check_count": 0,
            }
        ]
    )


def approved_candidate(root: Path):
    draft, registered = register_candidate(
        _promotion_summary(),
        _health_summary(),
        repository="alexmarinos87/weather-energy-demand-pipeline",
        code_commit_sha="a" * 40,
        code_tree_sha="b" * 40,
        candidate_version="0.1.0",
        training_data_boundary_utc="2026-01-29T00:00:00Z",
        feature_contract_versions=("time-horizon-v1",),
        forecast_weather_contract_version="target-weather-v1",
        actor="owner",
        reason="Register",
        created_at_utc="2026-01-30T02:00:00Z",
    )
    directory = root / "model-registry" / draft["candidate_id"]
    write_candidate_revision(directory, draft, registered)
    requested, request_event = transition_candidate(
        draft,
        action="review_requested",
        actor="requester",
        reason="Ready",
        event_timestamp_utc="2026-01-30T03:00:00Z",
    )
    write_candidate_revision(directory, requested, request_event)
    approved, approval_event = transition_candidate(
        requested,
        action="approved",
        actor="reviewer",
        reason="Approve",
        review_ticket="REVIEW-1",
        event_timestamp_utc="2026-01-30T04:00:00Z",
    )
    write_candidate_revision(directory, approved, approval_event)
    timestamp = pd.Timestamp("2026-01-30T05:00:00Z").timestamp()
    for path in directory.glob("*.json"):
        os.utime(path, (timestamp, timestamp))
    return directory


def test_approved_candidate_reference_protects_matching_evidence_path(tmp_path):
    root = tmp_path / "data"
    approved_candidate(root)
    write_file(
        root,
        "evidence/weather_comparison_predictions_comparison-keep.parquet",
        age_days=500,
    )
    policy_path = policy_file(
        tmp_path,
        [
            category(
                pattern="evidence/**", retention_days=30, min_keep_latest=0
            ),
            category(
                name="model_registry",
                pattern="model-registry/**",
                retention_days=None,
                always_protect=True,
            ),
        ],
    )
    loaded = load_retention_policy(policy_path)
    references = load_protected_candidate_references(root, loaded)
    inventory = inventory_evidence(root, loaded, as_of_utc=AS_OF)
    lifecycle, _ = plan_evidence_lifecycle(
        inventory,
        loaded,
        protected_references=references,
        plan_created_at_utc=AS_OF,
    )

    row = lifecycle.loc[
        lifecycle["relative_path"].str.contains("comparison-keep")
    ].iloc[0]
    assert row["protected_by_candidate"]
    assert row["planned_action"] == "retain"
    assert "state=approved" in row["candidate_protection_reason"]


def test_broken_candidate_history_blocks_lifecycle_planning(tmp_path):
    root = tmp_path / "data"
    directory = approved_candidate(root)
    manifest = sorted(directory.glob("manifest_v*.json"))[-1]
    payload = json.loads(manifest.read_text(encoding="utf-8"))
    payload["candidate_version"] = "9.9.9"
    manifest.write_text(json.dumps(payload), encoding="utf-8")
    policy_path = policy_file(
        tmp_path,
        [
            category(
                name="model_registry",
                pattern="model-registry/**",
                retention_days=None,
                always_protect=True,
            )
        ],
    )
    loaded = load_retention_policy(policy_path)
    with pytest.raises(Exception, match="manifest hash"):
        load_protected_candidate_references(root, loaded)


def test_symlinks_are_rejected(tmp_path):
    root = tmp_path / "data"
    target = write_file(root, "outside.txt", age_days=1)
    link = root / "evidence" / "link.txt"
    link.parent.mkdir(parents=True)
    link.symlink_to(target)
    policy_path = policy_file(tmp_path, [category()])
    loaded = load_retention_policy(policy_path)
    with pytest.raises(EvidenceLifecycleError, match="symbolic links"):
        inventory_evidence(root, loaded, as_of_utc=AS_OF)


def test_future_modified_time_is_rejected(tmp_path):
    root = tmp_path / "data"
    path = write_file(root, "evidence/future.csv", age_days=0)
    future = (AS_OF + pd.Timedelta(minutes=1)).timestamp()
    os.utime(path, (future, future))
    loaded = load_retention_policy(policy_file(tmp_path, [category()]))
    with pytest.raises(EvidenceLifecycleError, match="after as_of_utc"):
        inventory_evidence(root, loaded, as_of_utc=AS_OF)


def test_plan_id_and_cost_estimate_are_deterministic(tmp_path):
    root = tmp_path / "data"
    write_file(root, "evidence/old.csv", age_days=100, content=b"x" * 1024)
    policy_path = policy_file(tmp_path, [category(retention_days=30)])
    first = plan(root, policy_path, cost=2.0)
    second = plan(root, policy_path, cost=2.0)
    assert first[2]["plan_id"] == second[2]["plan_id"]
    assert first[2]["estimated_current_monthly_storage_cost"] > 0
    assert first[2]["estimated_reclaimable_monthly_storage_cost"] > 0


def test_invalid_policy_cannot_enable_unclassified_deletion(tmp_path):
    path = policy_file(tmp_path, [category()])
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["unclassified_action"] = "quarantine_candidate"
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(EvidenceLifecycleError, match="retained by default"):
        load_retention_policy(path)


def test_cli_writes_plan_without_mutating_source_files(tmp_path):
    root = tmp_path / "data"
    source = write_file(root, "evidence/old.csv", age_days=100, content=b"source")
    policy_path = policy_file(tmp_path, [category(retention_days=30)])
    output = root / "lifecycle"
    before = source.read_bytes()

    assert main(
        [
            "--data-root",
            str(root),
            "--policy",
            str(policy_path),
            "--as-of-utc",
            AS_OF.isoformat(),
            "--output-dir",
            str(output),
            "--output-format",
            "csv",
        ]
    ) == 0

    assert source.exists()
    assert source.read_bytes() == before
    assert len(list(output.glob("evidence_inventory_*.csv"))) == 1
    assert len(list(output.glob("evidence_lifecycle_plan_*.csv"))) == 1
    summaries = list(output.glob("evidence_lifecycle_summary_*.json"))
    assert len(summaries) == 1
    assert not json.loads(summaries[0].read_text())["mutation_performed"]


def json_row(row: pd.Series) -> dict:
    result = row.to_dict()
    for key, value in list(result.items()):
        if isinstance(value, pd.Timestamp):
            result[key] = value.isoformat()
        elif key == "compaction_formats":
            result[key] = ",".join(value)
        elif pd.isna(value):
            result[key] = None
        elif hasattr(value, "item"):
            result[key] = value.item()
    return result


def test_inventory_plan_and_summary_satisfy_versioned_schemas(tmp_path):
    root = tmp_path / "data"
    write_file(root, "evidence/old.csv", age_days=100)
    policy_path = policy_file(tmp_path, [category(retention_days=30)])
    inventory, lifecycle, summary = plan(root, policy_path)
    contracts = Path(__file__).resolve().parents[1] / "data-contracts"
    inventory_schema = json.loads(
        (contracts / "evidence_inventory_schema.json").read_text(encoding="utf-8")
    )
    plan_schema = json.loads(
        (contracts / "evidence_lifecycle_plan_schema.json").read_text(
            encoding="utf-8"
        )
    )
    summary_schema = json.loads(
        (contracts / "evidence_lifecycle_summary_schema.json").read_text(
            encoding="utf-8"
        )
    )
    checker = FormatChecker()
    assert list(
        Draft202012Validator(
            inventory_schema, format_checker=checker
        ).iter_errors(json_row(inventory.iloc[0]))
    ) == []
    assert list(
        Draft202012Validator(plan_schema, format_checker=checker).iter_errors(
            json_row(lifecycle.iloc[0])
        )
    ) == []
    assert list(
        Draft202012Validator(summary_schema, format_checker=checker).iter_errors(
            summary
        )
    ) == []

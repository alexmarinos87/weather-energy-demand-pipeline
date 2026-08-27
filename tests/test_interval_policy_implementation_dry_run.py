from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
from jsonschema import Draft202012Validator, FormatChecker

import forecasting.interval_policy_implementation_dry_run as module
from forecasting._interval_policy_candidate_revision_common import (
    active_policy_snapshot,
    canonical,
    digest,
)
from forecasting.interval_policy_implementation_dry_run import (
    IMPLEMENTATION_DRY_RUN_SAFETY_FIELDS,
    IntervalPolicyImplementationDryRunError,
    create_interval_policy_implementation_dry_run,
    verify_interval_policy_implementation_dry_run,
    write_interval_policy_implementation_dry_run,
)
from forecasting.run_interval_policy_implementation_dry_run import main


ROOT = Path(__file__).resolve().parents[1]
BASE_COMMIT = "1" * 40
BASE_TREE = "2" * 40


def source_text() -> str:
    return (ROOT / "forecasting" / "interval_monitoring.py").read_text(
        encoding="utf-8"
    )


def revised_candidate() -> dict:
    values = active_policy_snapshot()
    values["max_recent_coverage_shortfall_pct_points"] = 3.0
    return {
        "candidate_id": "revised-candidate-r3",
        "candidate_role": "review_candidate",
        "candidate_version": "interval-monitoring-review-candidate-v4",
        "rationale": "Bounded revised monitoring thresholds for reviewed evidence.",
        **values,
    }


def package() -> dict:
    revised = revised_candidate()
    active = active_policy_snapshot()
    return {
        "revision_package_id": "ipr-" + "3" * 24,
        "revision_package_sha256": "3" * 64,
        "revised_candidate": revised,
        "revised_candidate_sha256": digest(canonical(revised)),
        "active_policy_snapshot": active,
        "active_policy_sha256": digest(active),
    }


def disposition() -> dict:
    return {
        "disposition_id": "iprd-" + "4" * 24,
        "disposition_sha256": "4" * 64,
        "disposition": "suitable_for_separate_implementation_proposal",
        "disposition_effect": "separate_implementation_proposal_review_required",
        "follow_up_human_action_required": True,
        "target_candidate_id": revised_candidate()["candidate_id"],
        "revised_candidate_sha256": package()["revised_candidate_sha256"],
        "sensitivity_run_id": "ips-" + "5" * 24,
        "revision_sensitivity_summary_sha256": "5" * 64,
        "revision_sensitivity_manifest_sha256": "6" * 64,
        "disposed_at_utc": "2026-01-20T05:00:00Z",
    }


def decision() -> dict:
    return {
        "decision_id": "ipd-" + "7" * 24,
        "decision_sha256": "7" * 64,
    }


@pytest.fixture(autouse=True)
def upstream_verification(monkeypatch):
    monkeypatch.setattr(
        module,
        "verify_revision_sensitivity_disposition",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        module,
        "verify_candidate_revision_package",
        lambda *args, **kwargs: None,
    )


def create(**overrides):
    values = {
        "repository_base_commit": BASE_COMMIT,
        "repository_base_tree": BASE_TREE,
        "policy_source_text": source_text(),
        "prepared_by": "Alex Engineer",
        "preparer_role": "Data Platform Owner",
        "implementation_ticket": "GOV-133",
        "rationale": (
            "Prepare an exact repository-base-bound source diff for separate "
            "human review without applying it."
        ),
        "intended_paths": [
            "forecasting/interval_monitoring.py",
            "tests/test_interval_monitoring.py",
        ],
        "validation_commands": [
            "python -m compileall -q forecasting tests",
            "python -m pytest -q",
        ],
        "prepared_at_utc": "2026-01-20T06:00:00Z",
    }
    values.update(overrides)
    return create_interval_policy_implementation_dry_run(
        disposition(),
        pd.DataFrame(),
        {},
        package(),
        decision(),
        pd.DataFrame(),
        **values,
    )


def test_dry_run_is_base_bound_exact_and_non_applying():
    proposal = create()
    verify_interval_policy_implementation_dry_run(
        proposal,
        disposition(),
        pd.DataFrame(),
        {},
        package(),
        decision(),
        pd.DataFrame(),
        repository_base_commit=BASE_COMMIT,
        repository_base_tree=BASE_TREE,
        policy_source_text=source_text(),
    )
    assert proposal["repository_base_commit"] == BASE_COMMIT
    assert proposal["repository_base_tree"] == BASE_TREE
    assert proposal["active_to_proposed_changes"] == [
        {
            "field": "max_recent_coverage_shortfall_pct_points",
            "current_value": 5.0,
            "proposed_value": 3.0,
        }
    ]
    assert (
        "-    max_recent_coverage_shortfall_pct_points: float = 5.0"
        in proposal["dry_run_patch"]
    )
    assert (
        "+    max_recent_coverage_shortfall_pct_points: float = 3.0"
        in proposal["dry_run_patch"]
    )
    assert all(
        proposal[field] is False
        for field in IMPLEMENTATION_DRY_RUN_SAFETY_FIELDS
    )


def test_wrong_disposition_or_stale_base_is_rejected():
    wrong = disposition()
    wrong["disposition"] = "retain_active_policy"
    with pytest.raises(IntervalPolicyImplementationDryRunError, match="requires"):
        create_interval_policy_implementation_dry_run(
            wrong,
            pd.DataFrame(),
            {},
            package(),
            decision(),
            pd.DataFrame(),
            repository_base_commit=BASE_COMMIT,
            repository_base_tree=BASE_TREE,
            policy_source_text=source_text(),
            prepared_by="Alex",
            preparer_role="Owner",
            implementation_ticket="GOV-133",
            rationale="A sufficiently long rationale for a rejected proposal.",
            intended_paths=["forecasting/interval_monitoring.py"],
            validation_commands=[
                "python -m compileall -q forecasting",
                "python -m pytest -q",
            ],
        )
    proposal = create()
    with pytest.raises(IntervalPolicyImplementationDryRunError, match="base commit"):
        verify_interval_policy_implementation_dry_run(
            proposal,
            disposition(),
            pd.DataFrame(),
            {},
            package(),
            decision(),
            pd.DataFrame(),
            repository_base_commit="9" * 40,
            repository_base_tree=BASE_TREE,
            policy_source_text=source_text(),
        )


def test_source_or_proposal_tampering_is_rejected():
    with pytest.raises(IntervalPolicyImplementationDryRunError, match="defaults"):
        create(
            policy_source_text=source_text().replace(
                "max_recent_coverage_shortfall_pct_points: float = 5.0",
                "max_recent_coverage_shortfall_pct_points: float = 4.0",
            )
        )
    proposal = create()
    proposal["dry_run_patch"] += "\n# changed"
    with pytest.raises(IntervalPolicyImplementationDryRunError, match="hash"):
        verify_interval_policy_implementation_dry_run(
            proposal,
            disposition(),
            pd.DataFrame(),
            {},
            package(),
            decision(),
            pd.DataFrame(),
            repository_base_commit=BASE_COMMIT,
            repository_base_tree=BASE_TREE,
            policy_source_text=source_text(),
        )


def test_proposal_satisfies_versioned_schema():
    proposal = create()
    schema = json.loads(
        (
            ROOT
            / "data-contracts"
            / "interval_policy_implementation_dry_run_schema.json"
        ).read_text(encoding="utf-8")
    )
    errors = list(
        Draft202012Validator(
            schema, format_checker=FormatChecker()
        ).iter_errors(proposal)
    )
    assert errors == []


def test_immutable_writer_and_cli(tmp_path):
    proposal = create()
    direct = tmp_path / "direct"
    write_interval_policy_implementation_dry_run(
        direct,
        proposal,
        disposition(),
        pd.DataFrame(),
        {},
        package(),
        decision(),
        pd.DataFrame(),
        repository_base_commit=BASE_COMMIT,
        repository_base_tree=BASE_TREE,
        policy_source_text=source_text(),
    )
    assert len(list(direct.iterdir())) == 2
    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        write_interval_policy_implementation_dry_run(
            direct,
            proposal,
            disposition(),
            pd.DataFrame(),
            {},
            package(),
            decision(),
            pd.DataFrame(),
            repository_base_commit=BASE_COMMIT,
            repository_base_tree=BASE_TREE,
            policy_source_text=source_text(),
        )

    inputs = tmp_path / "inputs"
    inputs.mkdir()
    (inputs / "disposition.json").write_text(
        json.dumps(disposition()), encoding="utf-8"
    )
    (inputs / "manifest.json").write_text("{}", encoding="utf-8")
    (inputs / "package.json").write_text(json.dumps(package()), encoding="utf-8")
    (inputs / "decision.json").write_text(json.dumps(decision()), encoding="utf-8")
    pd.DataFrame({"placeholder": [1]}).to_csv(
        inputs / "revision.csv", index=False
    )
    pd.DataFrame({"placeholder": [1]}).to_csv(
        inputs / "source.csv", index=False
    )
    output = tmp_path / "cli"
    args = [
        "--disposition",
        str(inputs / "disposition.json"),
        "--revision-sensitivity-summary",
        str(inputs / "revision.csv"),
        "--revision-sensitivity-manifest",
        str(inputs / "manifest.json"),
        "--revision-package",
        str(inputs / "package.json"),
        "--source-decision",
        str(inputs / "decision.json"),
        "--source-sensitivity-summary",
        str(inputs / "source.csv"),
        "--policy-source",
        str(ROOT / "forecasting" / "interval_monitoring.py"),
        "--repository-base-commit",
        BASE_COMMIT,
        "--repository-base-tree",
        BASE_TREE,
        "--prepared-by",
        "Alex Engineer",
        "--preparer-role",
        "Data Platform Owner",
        "--implementation-ticket",
        "GOV-133",
        "--rationale",
        "Prepare an exact non-applying source diff for separate review.",
        "--intended-path",
        "forecasting/interval_monitoring.py",
        "--validation-command",
        "python -m compileall -q forecasting tests",
        "--validation-command",
        "python -m pytest -q",
        "--prepared-at-utc",
        "2026-01-20T06:00:00Z",
        "--output-dir",
        str(output),
    ]
    assert main(args) == 0
    assert len(list(output.iterdir())) == 2

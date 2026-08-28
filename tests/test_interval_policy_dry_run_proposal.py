from __future__ import annotations

import json
import subprocess
from dataclasses import asdict
from pathlib import Path

import pandas as pd
import pytest
from jsonschema import Draft202012Validator, FormatChecker

import forecasting.interval_policy_dry_run_proposal as subject
from forecasting.interval_monitoring import PredictionIntervalMonitoringConfig


ROOT = Path(__file__).resolve().parents[1]


def git_repo(tmp_path: Path) -> Path:
    root = tmp_path / "repo"
    (root / "forecasting").mkdir(parents=True)
    (root / "forecasting" / "interval_monitoring.py").write_text(
        "ACTIVE_POLICY = True\n", encoding="utf-8"
    )
    subprocess.run(["git", "-C", str(root), "init", "-q"], check=True)
    subprocess.run(
        ["git", "-C", str(root), "config", "user.name", "Test"], check=True
    )
    subprocess.run(
        ["git", "-C", str(root), "config", "user.email", "test@example.com"],
        check=True,
    )
    subprocess.run(["git", "-C", str(root), "add", "."], check=True)
    subprocess.run(
        ["git", "-C", str(root), "commit", "-qm", "base"], check=True
    )
    return root


def inputs():
    proposed = asdict(PredictionIntervalMonitoringConfig())
    proposed["max_recent_coverage_shortfall_pct_points"] = 3.0
    proposed.update(
        {
            "candidate_id": "stricter-review-r1",
            "candidate_role": "review_candidate",
            "candidate_version": "candidate-v2",
            "rationale": "Reviewed revised monitoring thresholds.",
        }
    )
    disposition = {
        "disposition": "suitable_for_separate_implementation_proposal",
        "revision_disposition_id": "ird-" + "1" * 24,
        "revision_disposition_sha256": "a" * 64,
        "disposition_timestamp_utc": "2026-01-20T05:00:00Z",
        "revised_candidate_id": "stricter-review-r1",
        "source_revision_sensitivity_run_id": "ips-" + "2" * 24,
        "source_revision_sensitivity_manifest_sha256": "b" * 64,
    }
    package = {
        "revision_package_id": "ipr-" + "3" * 24,
        "revision_package_sha256": "c" * 64,
        "revised_candidate": proposed,
    }
    summary = pd.DataFrame(
        [{"candidate_id": "active-reference"}, {"candidate_id": "stricter-review-r1"}]
    )
    manifest = {"manifest_sha256": "d" * 64}
    decision = {"decision_id": "ipd-" + "4" * 24}
    source_summary = pd.DataFrame([{"candidate_id": "stricter-review"}])
    return disposition, summary, manifest, package, decision, source_summary


@pytest.fixture(autouse=True)
def bypass_upstream_verifiers(monkeypatch):
    monkeypatch.setattr(subject, "verify_revision_disposition", lambda *args: None)
    monkeypatch.setattr(
        subject, "verify_candidate_revision_package", lambda *args: None
    )


def create(tmp_path: Path):
    return subject.create_dry_run_implementation_proposal(
        *inputs(),
        repository_root=git_repo(tmp_path),
        repository_full_name="alexmarinos87/weather-energy-demand-pipeline",
        proposed_by="Alex Engineer",
        proposer_role="Data Platform Owner",
        proposal_ticket="GOV-133",
        rationale="Prepare an exact repository-bound dry-run diff.",
        proposed_at_utc="2026-01-20T06:00:00Z",
    )


def test_proposal_binds_clean_repository_and_exact_policy_diff(tmp_path):
    proposal = create(tmp_path)
    assert proposal["repository_base_commit_sha"]
    assert proposal["repository_base_tree_sha"]
    assert proposal["changed_threshold_count"] == 1
    assert proposal["changed_thresholds"][0]["field"] == (
        "max_recent_coverage_shortfall_pct_points"
    )
    assert proposal["changed_thresholds"][0]["application_state"] == "not_applied"
    assert all(proposal[field] is False for field in subject.SAFETY_FIELDS)


def test_non_suitable_disposition_is_rejected(tmp_path):
    disposition, *rest = inputs()
    disposition["disposition"] = "retain_active_policy"
    with pytest.raises(subject.IntervalPolicyDryRunProposalError, match="requires"):
        subject.create_dry_run_implementation_proposal(
            disposition,
            *rest,
            repository_root=git_repo(tmp_path),
            repository_full_name="owner/repo",
            proposed_by="Engineer",
            proposer_role="Owner",
            proposal_ticket="GOV-1",
            rationale="Dry run.",
            proposed_at_utc="2026-01-20T06:00:00Z",
        )


def test_dirty_repository_is_rejected(tmp_path):
    root = git_repo(tmp_path)
    (root / "untracked.txt").write_text("dirty", encoding="utf-8")
    with pytest.raises(subject.IntervalPolicyDryRunProposalError, match="clean"):
        subject.create_dry_run_implementation_proposal(
            *inputs(),
            repository_root=root,
            repository_full_name="owner/repo",
            proposed_by="Engineer",
            proposer_role="Owner",
            proposal_ticket="GOV-1",
            rationale="Dry run.",
            proposed_at_utc="2026-01-20T06:00:00Z",
        )


def test_candidate_identity_mismatch_is_rejected(tmp_path):
    values = list(inputs())
    values[3]["revised_candidate"]["candidate_id"] = "different"
    with pytest.raises(subject.IntervalPolicyDryRunProposalError, match="different"):
        subject.create_dry_run_implementation_proposal(
            *values,
            repository_root=git_repo(tmp_path),
            repository_full_name="owner/repo",
            proposed_by="Engineer",
            proposer_role="Owner",
            proposal_ticket="GOV-1",
            rationale="Dry run.",
            proposed_at_utc="2026-01-20T06:00:00Z",
        )


def test_repository_or_document_tampering_is_rejected(tmp_path):
    root = git_repo(tmp_path)
    proposal = subject.create_dry_run_implementation_proposal(
        *inputs(),
        repository_root=root,
        repository_full_name="owner/repo",
        proposed_by="Engineer",
        proposer_role="Owner",
        proposal_ticket="GOV-1",
        rationale="Dry run.",
        proposed_at_utc="2026-01-20T06:00:00Z",
    )
    proposal["proposal_status"] = "changed"
    with pytest.raises(subject.IntervalPolicyDryRunProposalError, match="hash"):
        subject.verify_dry_run_implementation_proposal(
            proposal, *inputs(), repository_root=root
        )


def test_schema_and_immutable_writer(tmp_path):
    proposal = create(tmp_path / "source")
    schema = json.loads(
        (
            ROOT
            / "data-contracts"
            / "interval_policy_dry_run_proposal_schema.json"
        ).read_text(encoding="utf-8")
    )
    assert list(
        Draft202012Validator(
            schema, format_checker=FormatChecker()
        ).iter_errors(proposal)
    ) == []
    output = tmp_path / "out"
    paths = subject.write_dry_run_implementation_proposal(output, proposal)
    assert all(path.is_file() for path in paths)
    with pytest.raises(FileExistsError, match="Refusing"):
        subject.write_dry_run_implementation_proposal(output, proposal)

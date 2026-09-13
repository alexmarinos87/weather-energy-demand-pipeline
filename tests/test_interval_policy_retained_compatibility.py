from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
from jsonschema import Draft202012Validator, FormatChecker

from interval_policy_evidence_fixtures import three_scenario_evidence
from forecasting._interval_policy_retained_compatibility_common import (
    COMPATIBILITY_SAFETY_FIELDS, CURRENT_POLICY_ID, PREVIOUS_POLICY_ID,
    IntervalPolicyRetainedCompatibilityError, prepare_compatibility_summary,
)
from forecasting.interval_policy_compatibility import assess_retained_policy_compatibility
from forecasting.interval_policy_retained_compatibility import evaluate_retained_policy_compatibility
from forecasting.interval_policy_retained_compatibility_manifest import (
    build_compatibility_manifest, verify_compatibility_manifest,
)
from forecasting.run_interval_policy_retained_compatibility import main

ROOT = Path(__file__).resolve().parents[1]
RUN_ID = "ipca-" + "1" * 24
RUN_TIMESTAMP = "2026-01-20T01:00:00Z"


def slice_trends() -> pd.DataFrame:
    return three_scenario_evidence()[2]


def run():
    _, checks, trends = three_scenario_evidence()
    return evaluate_retained_policy_compatibility(
        trends, retained_health_checks=checks,
        compatibility_run_id=RUN_ID, compatibility_run_timestamp=RUN_TIMESTAMP,
    )


def test_assessment_preserves_retained_status_and_exposes_tightening():
    slices, summary, report = run()
    keyed = summary.set_index("scenario")
    assert keyed.loc["stable", "previous_policy_status"] == "healthy"
    assert keyed.loc["stable", "current_policy_status"] == "healthy"
    assert keyed.loc["tightened", "previous_policy_status"] == "healthy"
    assert keyed.loc["tightened", "current_policy_status"] == "failed"
    assert keyed.loc["tightened", "retained_status_compatibility"] == "matches_previous_policy_only"
    assert keyed.loc["tightened", "newly_failed_slice_count"] == 1
    assert bool(keyed.loc["tightened", "human_review_required"])
    assert keyed.loc["failed", "previous_policy_status"] == "failed"
    assert keyed.loc["failed", "current_policy_status"] == "failed"
    assert all(not bool(summary[field].any()) for field in COMPATIBILITY_SAFETY_FIELDS)
    assert all(not bool(slices[field].any()) for field in COMPATIBILITY_SAFETY_FIELDS)
    assert "Historical monitor statuses are preserved" in report


def test_assessment_matches_canonical_retained_check_engine():
    slices, summary, _ = run()
    _, checks, _ = three_scenario_evidence()
    _, direct = assess_retained_policy_compatibility(
        checks, assessment_run_id="ipc-" + "1" * 24,
        assessment_timestamp_utc=RUN_TIMESTAMP,
    )
    expected = direct.set_index("source_monitor_run_id")
    for row in summary.itertuples(index=False):
        original = expected.loc[row.source_monitor_run_id]
        assert row.previous_policy_status == original["previous_overall_status"]
        assert row.current_policy_status == original["current_overall_status"]
        assert row.newly_failed_slice_count == original["newly_failed_slice_count"]
        assert row.source_health_checks_sha256 == original["source_health_checks_sha256"]
    assert set(slices["policy_id"]) == {PREVIOUS_POLICY_ID, CURRENT_POLICY_ID}


def test_manifest_binds_exact_artifacts_and_schema(tmp_path):
    slices, summary, report = run()
    paths = {"slices": tmp_path / "slices.csv", "summary": tmp_path / "summary.csv",
             "report": tmp_path / "report.md"}
    slices.to_csv(paths["slices"], index=False)
    summary.to_csv(paths["summary"], index=False)
    paths["report"].write_text(report, encoding="utf-8")
    manifest = build_compatibility_manifest(summary, artifacts=paths)
    verify_compatibility_manifest(manifest, pd.read_csv(paths["summary"]), artifact_directory=tmp_path)
    schema = json.loads((ROOT / "data-contracts" /
                         "interval_policy_retained_compatibility_manifest_schema.json").read_text(encoding="utf-8"))
    assert list(Draft202012Validator(schema, format_checker=FormatChecker()).iter_errors(manifest)) == []


def test_manifest_rejects_artifact_or_summary_tampering(tmp_path):
    slices, summary, report = run()
    paths = {"slices": tmp_path / "slices.csv", "summary": tmp_path / "summary.csv",
             "report": tmp_path / "report.md"}
    slices.to_csv(paths["slices"], index=False)
    summary.to_csv(paths["summary"], index=False)
    paths["report"].write_text(report, encoding="utf-8")
    manifest = build_compatibility_manifest(summary, artifacts=paths)
    paths["report"].write_text(report + "\nchanged\n", encoding="utf-8")
    with pytest.raises(IntervalPolicyRetainedCompatibilityError, match="artifact hash"):
        verify_compatibility_manifest(manifest, summary, artifact_directory=tmp_path)
    paths["report"].write_text(report, encoding="utf-8")
    changed = summary.copy()
    changed.loc[0, "changed_slice_count"] = 99
    with pytest.raises(IntervalPolicyRetainedCompatibilityError, match="summary digest"):
        verify_compatibility_manifest(manifest, changed, artifact_directory=tmp_path)


@pytest.mark.parametrize("output_format", ["csv", "parquet"])
def test_cli_writes_immutable_assessment_outputs(tmp_path, output_format):
    _, checks, trends = three_scenario_evidence()
    input_path, checks_path = tmp_path / "slice-trends.csv", tmp_path / "checks.csv"
    output_dir = tmp_path / "output"
    trends.to_csv(input_path, index=False)
    checks.to_csv(checks_path, index=False)
    args = ["--slice-trends", str(input_path), "--health-checks", str(checks_path),
            "--output-dir", str(output_dir), "--compatibility-run-id", RUN_ID,
            "--compatibility-run-timestamp", RUN_TIMESTAMP, "--output-format", output_format]
    assert main(args) == 0
    assert len(list(output_dir.glob(f"*.{output_format}"))) == 2
    assert len(list(output_dir.glob("*.md"))) == 1
    assert len(list(output_dir.glob("*.json"))) == 1
    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        main(args)


def test_cli_requires_checks_before_reading_or_writing(tmp_path):
    with pytest.raises(SystemExit) as exc:
        main(["--slice-trends", str(tmp_path / "missing.csv"), "--output-dir", str(tmp_path / "output")])
    assert exc.value.code == 2
    assert not (tmp_path / "output").exists()


def test_v1_summary_cannot_be_relabelled_or_silently_accepted():
    _, summary, _ = run()
    legacy = summary.copy()
    legacy["compatibility_contract_version"] = "interval-policy-retained-compatibility-v1"
    with pytest.raises(IntervalPolicyRetainedCompatibilityError, match="legacy v1"):
        prepare_compatibility_summary(legacy)
    with pytest.raises(IntervalPolicyRetainedCompatibilityError, match="source_health_checks_sha256"):
        prepare_compatibility_summary(summary.drop(columns="source_health_checks_sha256"))

from __future__ import annotations

import json
from pathlib import Path
import shutil

import pandas as pd
import pytest
from jsonschema import Draft202012Validator, FormatChecker

from forecasting.portfolio_interval_health_demo import (
    EXPECTED_SOURCE_AREAS,
    HEALTH_ARTIFACT_ROLES,
    HISTORY_RUNS_PER_SCENARIO,
    MANIFEST_CONTRACT_VERSION,
    SCENARIOS,
    PortfolioIntervalHealthError,
    run_portfolio_interval_health_demo,
    verify_portfolio_interval_health_manifest,
)
from forecasting.run_portfolio_interval_health_demo import build_parser
from forecasting.seasonal_baselines import ALL_MODELS


ROOT = Path(__file__).resolve().parents[1]
RUN_ID = "pih-" + "2" * 24
RUN_TIMESTAMP = "2026-01-20T00:00:00Z"
COVERAGE_LEVELS = {0.80, 0.90, 0.95}


@pytest.fixture(scope="module")
def health_demo(tmp_path_factory):
    output_root = tmp_path_factory.mktemp("portfolio-interval-health")
    manifest, manifest_path = run_portfolio_interval_health_demo(
        output_root,
        output_format="csv",
        run_id=RUN_ID,
        run_timestamp=RUN_TIMESTAMP,
    )
    return output_root, manifest, manifest_path


def _by_role(manifest: dict, manifest_path: Path) -> dict[str, Path]:
    return {
        item["artifact_role"]: manifest_path.parent / item["relative_path"]
        for item in manifest["artifacts"]
    }


def test_manifest_records_repeated_four_area_health_contract(health_demo):
    _, manifest, manifest_path = health_demo
    assert manifest_path.is_file()
    assert manifest["health_demo_run_id"] == RUN_ID
    assert manifest["manifest_contract_version"] == MANIFEST_CONTRACT_VERSION
    assert manifest["manifest_contract_version"] == (
        "portfolio-interval-health-manifest-v1"
    )
    assert manifest["source_groups"] == 4
    assert {
        binding["source_area"] for binding in manifest["source_bindings"]
    } == EXPECTED_SOURCE_AREAS
    assert manifest["interval_health_scenarios"] == list(SCENARIOS)
    assert manifest["interval_health_expected_status_by_scenario"] == {
        "healthy": "healthy",
        "warning": "warning",
        "failed": "failed",
    }
    assert manifest["interval_history_runs_per_scenario"] == 9
    assert set(manifest["interval_health_models"]) == set(ALL_MODELS)
    assert manifest["interval_health_horizons_minutes"] == [30, 60]
    assert set(manifest["interval_health_coverage_levels"]) == COVERAGE_LEVELS
    assert manifest["minimum_interval_calibration_rows"] == 24
    assert {
        item["artifact_role"] for item in manifest["artifacts"]
    } == HEALTH_ARTIFACT_ROLES
    assert len(manifest["artifacts"]) == 4


def test_repeated_metric_history_retains_every_slice_and_nine_runs(health_demo):
    _, manifest, manifest_path = health_demo
    history = pd.read_csv(
        _by_role(manifest, manifest_path)["repeated_interval_metric_history"]
    )
    assert set(history["scenario"]) == set(SCENARIOS)
    assert set(history["source_area"]) == EXPECTED_SOURCE_AREAS
    assert set(history["requested_horizon_minutes"].astype(int)) == {30, 60}
    assert set(history["model_name"]) == set(ALL_MODELS)
    assert set(history["target_coverage_level"].astype(float)) == COVERAGE_LEVELS
    expected_slices = 4 * 2 * len(ALL_MODELS) * len(COVERAGE_LEVELS)
    for scenario, group in history.groupby("scenario"):
        run_sizes = group.groupby("interval_run_id").size()
        assert len(run_sizes) == HISTORY_RUNS_PER_SCENARIO
        assert set(run_sizes) == {expected_slices}, scenario
        assert set(group["history_sequence"].astype(int)) == set(range(1, 10))


def test_monitor_evidence_proves_healthy_warning_and_failed_paths(health_demo):
    _, manifest, manifest_path = health_demo
    by_role = _by_role(manifest, manifest_path)
    checks = pd.read_csv(by_role["prediction_interval_health_checks"])
    summaries = pd.read_csv(by_role["prediction_interval_health_summary"])
    statuses = dict(zip(summaries["scenario"], summaries["monitor_status"]))
    assert statuses == {
        "healthy": "healthy",
        "warning": "warning",
        "failed": "failed",
    }
    assert set(checks["source_area"]) == EXPECTED_SOURCE_AREAS
    healthy = checks.loc[checks["scenario"] == "healthy"]
    warning = checks.loc[checks["scenario"] == "warning"]
    failed = checks.loc[checks["scenario"] == "failed"]
    assert healthy["passed"].astype(bool).all()
    assert not (
        (warning["severity"] == "error")
        & (~warning["passed"].astype(bool))
    ).any()
    assert (
        (warning["severity"] == "warning")
        & (~warning["passed"].astype(bool))
    ).any()
    assert (
        (failed["severity"] == "error")
        & (~failed["passed"].astype(bool))
    ).any()


def test_monitor_summary_retains_no_automatic_action_authority(health_demo):
    _, manifest, manifest_path = health_demo
    summaries = pd.read_csv(
        _by_role(manifest, manifest_path)["prediction_interval_health_summary"]
    )
    for column in (
        "automatic_remediation_allowed",
        "automatic_recalibration_allowed",
        "automatic_model_change_allowed",
        "automatic_schedule_change_allowed",
        "automatic_promotion_allowed",
    ):
        assert not summaries[column].astype(bool).any()
    for field in (
        "live_source_calls_performed",
        "fabric_operations_performed",
        "schedule_activation_performed",
        "automatic_remediation_performed",
        "automatic_recalibration_performed",
        "automatic_model_change_performed",
        "automatic_promotion_performed",
        "alert_delivery_performed",
        "external_publication_performed",
    ):
        assert manifest[field] is False


def test_operator_report_is_area_specific_and_advisory(health_demo):
    _, manifest, manifest_path = health_demo
    report = _by_role(manifest, manifest_path)[
        "interval_health_operator_report"
    ].read_text(encoding="utf-8")
    for token in (*SCENARIOS, *sorted(EXPECTED_SOURCE_AREAS)):
        assert token in report
    assert "Automatic recalibration" in report
    assert "model changes" in report
    assert "schedule changes" in report
    assert "promotion changes" in report
    assert "not an unconditional future guarantee" in report


def test_manifest_and_reopened_artifacts_verify_against_schema(health_demo):
    _, manifest, manifest_path = health_demo
    verify_portfolio_interval_health_manifest(manifest, manifest_path.parent)
    schema = json.loads(
        (
            ROOT
            / "data-contracts"
            / "portfolio_interval_health_manifest_schema.json"
        ).read_text(encoding="utf-8")
    )
    errors = list(
        Draft202012Validator(
            schema, format_checker=FormatChecker()
        ).iter_errors(manifest)
    )
    assert errors == []
    for artifact in manifest["artifacts"]:
        path = manifest_path.parent / artifact["relative_path"]
        assert path.stat().st_size == artifact["size_bytes"]
        if artifact["row_count"] is not None:
            assert len(pd.read_csv(path)) == artifact["row_count"]


def test_verifier_detects_artifact_and_status_tampering(health_demo, tmp_path):
    _, manifest, manifest_path = health_demo
    copied = tmp_path / "tampered"
    shutil.copytree(manifest_path.parent, copied)
    target = copied / _by_role(manifest, manifest_path)[
        "prediction_interval_health_summary"
    ].name
    summary = pd.read_csv(target)
    summary.loc[summary["scenario"] == "warning", "monitor_status"] = "healthy"
    summary.to_csv(target, index=False)
    with pytest.raises(
        PortfolioIntervalHealthError, match="size changed|hash changed"
    ):
        verify_portfolio_interval_health_manifest(manifest, copied)


def test_run_directory_is_immutable(health_demo):
    output_root, _, _ = health_demo
    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        run_portfolio_interval_health_demo(
            output_root,
            output_format="csv",
            run_id=RUN_ID,
            run_timestamp=RUN_TIMESTAMP,
        )


def test_cli_contract_exposes_credential_free_output_controls():
    args = build_parser().parse_args(
        [
            "--output-root",
            "example-output",
            "--output-format",
            "parquet",
            "--run-id",
            RUN_ID,
            "--run-timestamp",
            RUN_TIMESTAMP,
        ]
    )
    assert args.output_root == Path("example-output")
    assert args.output_format == "parquet"
    assert args.run_id == RUN_ID
    assert args.run_timestamp == RUN_TIMESTAMP

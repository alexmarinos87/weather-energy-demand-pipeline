from __future__ import annotations

import json
from pathlib import Path
import shutil

import pandas as pd
import pytest
from jsonschema import Draft202012Validator, FormatChecker

from forecasting.portfolio_demo import (
    EXPECTED_ARTIFACT_ROLES,
    PortfolioDemoError,
    run_portfolio_demo,
    verify_portfolio_demo_manifest,
)
from forecasting.run_portfolio_demo import main


ROOT = Path(__file__).resolve().parents[1]
RUN_ID = "pdm-" + "1" * 24
RUN_TIMESTAMP = "2026-01-03T00:00:00Z"


@pytest.fixture(scope="module")
def demo_run(tmp_path_factory):
    output_root = tmp_path_factory.mktemp("portfolio-demo")
    manifest, manifest_path = run_portfolio_demo(
        output_root,
        output_format="csv",
        run_id=RUN_ID,
        run_timestamp=RUN_TIMESTAMP,
    )
    return output_root, manifest, manifest_path


def test_demo_manifest_records_complete_product_journey(demo_run):
    _, manifest, manifest_path = demo_run
    assert manifest_path.is_file()
    assert manifest["demo_run_id"] == RUN_ID
    assert manifest["source_mode"] == "deterministic_credential_free_demo"
    assert manifest["demand_horizons_minutes"] == [30, 60]
    assert set(manifest["baseline_models"]) == {
        "persistence_current_value",
        "ridge_weather_lag",
    }
    assert set(manifest["comparison_models"]) == {
        "ridge_weather_lag",
        "ridge_target_weather",
    }
    assert {item["artifact_role"] for item in manifest["artifacts"]} == (
        EXPECTED_ARTIFACT_ROLES
    )
    assert len(manifest["artifacts"]) == 11


def test_demo_safety_boundary_is_explicit_and_false_for_side_effects(demo_run):
    _, manifest, _ = demo_run
    assert manifest["credential_free"] is True
    assert manifest["live_source_calls_performed"] is False
    assert manifest["fabric_operations_performed"] is False
    assert manifest["schedule_activation_performed"] is False
    assert manifest["model_promotion_performed"] is False
    assert manifest["external_publication_performed"] is False


def test_every_demo_artifact_hash_size_and_row_count_verifies(demo_run):
    _, manifest, manifest_path = demo_run
    verify_portfolio_demo_manifest(manifest, manifest_path.parent)
    for artifact in manifest["artifacts"]:
        path = manifest_path.parent / artifact["relative_path"]
        assert path.stat().st_size == artifact["size_bytes"]
        if artifact["row_count"] is not None:
            frame = pd.read_csv(path)
            assert len(frame) == artifact["row_count"]
            assert len(frame) > 0


def test_demo_outputs_contain_baseline_comparison_and_analytics_evidence(demo_run):
    _, manifest, manifest_path = demo_run
    by_role = {
        artifact["artifact_role"]: manifest_path.parent
        / artifact["relative_path"]
        for artifact in manifest["artifacts"]
    }
    baseline = pd.read_csv(by_role["baseline_predictions"])
    comparison = pd.read_csv(by_role["weather_comparison_predictions"])
    overview = pd.read_csv(by_role["demand_weather_overview"])
    report = by_role["demand_weather_markdown"].read_text(encoding="utf-8")
    assert set(baseline["requested_horizon_minutes"]) == {30, 60}
    assert set(baseline["model_name"]) == {
        "persistence_current_value",
        "ridge_weather_lag",
    }
    assert set(comparison["model_name"]) == {
        "ridge_weather_lag",
        "ridge_target_weather",
    }
    assert set(comparison["weather_feature_mode"]) == {
        "observed_at_feature",
        "target_forecast",
    }
    assert overview.loc[0, "observation_count"] > 100
    assert "This report is descriptive" in report
    assert "Correlation does not establish causation" in report


def test_demo_manifest_satisfies_versioned_schema(demo_run):
    _, manifest, _ = demo_run
    schema = json.loads(
        (
            ROOT / "data-contracts" / "portfolio_demo_manifest_schema.json"
        ).read_text(encoding="utf-8")
    )
    errors = list(
        Draft202012Validator(
            schema, format_checker=FormatChecker()
        ).iter_errors(manifest)
    )
    assert errors == []


def test_demo_verification_detects_artifact_tampering(demo_run, tmp_path):
    _, manifest, manifest_path = demo_run
    copied = tmp_path / "copied-run"
    shutil.copytree(manifest_path.parent, copied)
    target = copied / manifest["artifacts"][0]["relative_path"]
    target.write_bytes(target.read_bytes() + b"tampered")
    with pytest.raises(PortfolioDemoError, match="size changed|hash changed"):
        verify_portfolio_demo_manifest(manifest, copied)


def test_demo_run_directory_is_immutable(demo_run):
    output_root, _, _ = demo_run
    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        run_portfolio_demo(
            output_root,
            output_format="csv",
            run_id=RUN_ID,
            run_timestamp=RUN_TIMESTAMP,
        )


def test_invalid_demo_run_identity_and_format_are_rejected(tmp_path):
    with pytest.raises(PortfolioDemoError, match="run_id"):
        run_portfolio_demo(tmp_path, run_id="wrong")
    with pytest.raises(PortfolioDemoError, match="csv or parquet"):
        run_portfolio_demo(tmp_path, output_format="json")


def test_cli_is_credential_free_even_when_network_calls_are_blocked(
    tmp_path, monkeypatch
):
    import requests

    def blocked(*args, **kwargs):
        raise AssertionError("The credential-free portfolio demo made a network call")

    monkeypatch.setattr(requests, "get", blocked)
    output = tmp_path / "cli"
    assert (
        main(
            [
                "--output-root",
                str(output),
                "--output-format",
                "csv",
                "--run-id",
                "pdm-" + "2" * 24,
                "--run-timestamp",
                RUN_TIMESTAMP,
            ]
        )
        == 0
    )
    manifests = list(output.glob("pdm-*/portfolio_demo_manifest.json"))
    assert len(manifests) == 1
    payload = json.loads(manifests[0].read_text(encoding="utf-8"))
    verify_portfolio_demo_manifest(payload, manifests[0].parent)


def test_parquet_demo_round_trips_under_the_supported_environment(tmp_path):
    manifest, path = run_portfolio_demo(
        tmp_path,
        output_format="parquet",
        run_id="pdm-" + "3" * 24,
        run_timestamp=RUN_TIMESTAMP,
    )
    verify_portfolio_demo_manifest(manifest, path.parent)
    assert all(
        artifact["content_type"] in {"application/x-parquet", "text/markdown"}
        for artifact in manifest["artifacts"]
    )


def test_readme_leads_with_the_one_command_product_journey():
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    quickstart = readme.index("## One-command credential-free demo")
    architecture = readme.index("## Architecture")
    assert quickstart < architecture
    assert "constraints/ci-python311-linux.txt" in readme
    assert "python -m forecasting.run_portfolio_demo" in readme
    assert "portfolio_demo_manifest.json" in readme
    assert "## Capability index" in readme


def test_portfolio_demo_document_matches_cli_and_safety_contract():
    document = (ROOT / "PORTFOLIO_DEMO.md").read_text(encoding="utf-8")
    assert "python -m forecasting.run_portfolio_demo" in document
    assert "constraints/ci-python311-linux.txt" in document
    assert "credential_free=true" in document
    assert "live_source_calls_performed=false" in document
    assert "model_promotion_performed=false" in document
    assert "portfolio_demo_manifest.json" in document

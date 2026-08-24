from __future__ import annotations

import json
from pathlib import Path
import shutil

import pandas as pd
import pytest
from jsonschema import Draft202012Validator, FormatChecker

from forecasting.demo import (
    build_demo_feature_frame,
    build_multi_area_demo_feature_frame,
)
from forecasting.portfolio_demo import (
    EXPECTED_ARTIFACT_ROLES,
    EXPECTED_SOURCE_AREAS,
    MANIFEST_CONTRACT_VERSION,
    PortfolioDemoError,
    run_portfolio_demo,
    verify_portfolio_demo_manifest,
)
from forecasting.run_portfolio_demo import main


ROOT = Path(__file__).resolve().parents[1]
RUN_ID = "pdm-" + "1" * 24
RUN_TIMESTAMP = "2026-01-03T00:00:00Z"
SEASONAL_MODELS = {
    "persistence_current_value",
    "seasonal_previous_day",
    "seasonal_previous_week",
    "ridge_weather_lag",
}
MODEL_FAMILY_MODELS = {
    "persistence_current_value",
    "seasonal_previous_day",
    "seasonal_previous_week",
    "ridge_weather_lag_utc",
    "ridge_weather_lag_uk_local",
}
INTERVAL_COVERAGE_LEVELS = {0.80, 0.90, 0.95}


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


def _by_role(manifest: dict, manifest_path: Path) -> dict[str, Path]:
    return {
        artifact["artifact_role"]: manifest_path.parent
        / artifact["relative_path"]
        for artifact in manifest["artifacts"]
    }


def test_single_area_demo_api_remains_backwards_compatible():
    frame = build_demo_feature_frame(periods=96)
    assert set(frame["source_area"]) == {"east_midlands"}
    assert frame.groupby(["source_area", "resource_id", "city"]).ngroups == 1


def test_multi_area_demo_covers_every_source_contract_group_without_collisions():
    frame = build_multi_area_demo_feature_frame(periods=96)
    assert set(frame["source_area"]) == EXPECTED_SOURCE_AREAS
    assert frame.groupby(["source_area", "resource_id", "city"]).ngroups == 4
    assert not frame.duplicated(
        subset=["source_area", "resource_id", "city", "event_timestamp_utc"],
        keep=False,
    ).any()
    counts = frame.groupby(["source_area", "resource_id", "city"]).size()
    assert set(counts) == {95}
    assert frame.groupby("source_area")["demand_mw"].mean().nunique() == 4


def test_demo_manifest_records_complete_multi_area_product_journey(demo_run):
    _, manifest, manifest_path = demo_run
    assert manifest_path.is_file()
    assert manifest["demo_run_id"] == RUN_ID
    assert manifest["source_mode"] == "deterministic_credential_free_demo"
    assert manifest["manifest_contract_version"] == MANIFEST_CONTRACT_VERSION
    assert manifest["manifest_contract_version"] == "portfolio-demo-manifest-v4"
    assert manifest["source_groups"] == 4
    assert {
        binding["source_area"] for binding in manifest["source_bindings"]
    } == EXPECTED_SOURCE_AREAS
    assert len({binding["resource_id"] for binding in manifest["source_bindings"]}) == 4
    assert len({binding["city"] for binding in manifest["source_bindings"]}) == 4
    assert manifest["source_area_contract_version"] == "1.1.0"
    assert manifest["demand_horizons_minutes"] == [30, 60]
    assert set(manifest["baseline_models"]) == {
        "persistence_current_value",
        "ridge_weather_lag",
    }
    assert set(manifest["comparison_models"]) == {
        "ridge_weather_lag",
        "ridge_target_weather",
    }
    assert set(manifest["seasonal_models"]) == SEASONAL_MODELS
    assert set(manifest["model_family_models"]) == MODEL_FAMILY_MODELS
    assert manifest["calendar_feature_contract_versions"] == [
        "time-horizon-v1",
        "time-horizon-uk-calendar-v1",
    ]
    assert manifest["seasonal_reference_periods_minutes"] == [1440, 10080]
    assert manifest["seasonal_source_cadence_minutes"] == 30
    assert manifest["seasonal_demo_days"] == 12
    assert set(manifest["interval_models"]) == SEASONAL_MODELS
    assert set(manifest["interval_coverage_levels"]) == INTERVAL_COVERAGE_LEVELS
    assert manifest["minimum_interval_calibration_rows"] == 24
    assert (
        manifest["interval_contract_version"]
        == "split-conformal-absolute-residual-v1"
    )
    assert manifest["interval_source_feature_contract_version"] == (
        "time-horizon-v1"
    )
    assert {item["artifact_role"] for item in manifest["artifacts"]} == (
        EXPECTED_ARTIFACT_ROLES
    )
    assert len(manifest["artifacts"]) == 24


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


def test_demo_outputs_preserve_area_isolation_across_product_evidence(demo_run):
    _, manifest, manifest_path = demo_run
    by_role = _by_role(manifest, manifest_path)
    features = pd.read_csv(by_role["demo_features"])
    area_summary = pd.read_csv(by_role["demo_source_area_summary"])
    baseline = pd.read_csv(by_role["baseline_predictions"])
    comparison = pd.read_csv(by_role["weather_comparison_predictions"])
    overview = pd.read_csv(by_role["demand_weather_overview"])
    seasonal_utc = pd.read_csv(by_role["seasonal_utc_predictions"])
    seasonal_uk = pd.read_csv(by_role["seasonal_uk_local_predictions"])
    scorecard = pd.read_csv(by_role["model_family_scorecard"])
    pairwise = pd.read_csv(by_role["model_family_pairwise_metrics"])
    intervals = pd.read_csv(by_role["prediction_intervals"])
    interval_metrics = pd.read_csv(by_role["prediction_interval_metrics"])
    interval_summary = pd.read_csv(by_role["interval_coverage_summary"])
    report = by_role["demand_weather_markdown"].read_text(encoding="utf-8")
    scorecard_report = by_role["model_family_summary_markdown"].read_text(
        encoding="utf-8"
    )
    interval_report = by_role["prediction_interval_summary_markdown"].read_text(
        encoding="utf-8"
    )
    for frame in (
        features,
        area_summary,
        baseline,
        comparison,
        overview,
        seasonal_utc,
        seasonal_uk,
        scorecard,
        pairwise,
        intervals,
        interval_metrics,
        interval_summary,
    ):
        assert set(frame["source_area"]) == EXPECTED_SOURCE_AREAS
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
    assert set(seasonal_utc["model_name"]) == SEASONAL_MODELS
    assert set(seasonal_uk["model_name"]) == SEASONAL_MODELS
    assert set(scorecard["model_name"]) == MODEL_FAMILY_MODELS
    assert set(pairwise["reference_model_name"]) == {
        "persistence_current_value"
    }
    assert set(intervals["model_name"]) == SEASONAL_MODELS
    assert set(interval_metrics["model_name"]) == SEASONAL_MODELS
    assert set(intervals["target_coverage_level"].astype(float)) == (
        INTERVAL_COVERAGE_LEVELS
    )
    assert set(interval_summary["target_coverage_level"].astype(float)) == (
        INTERVAL_COVERAGE_LEVELS
    )
    assert len(area_summary) == 4
    assert len(overview) == 4
    assert (overview["observation_count"] > 100).all()
    assert "This report is descriptive" in report
    assert "Correlation does not establish causation" in report
    assert "comparison evidence only" in scorecard_report
    assert "unconditional future guarantee" in interval_report
    for area in EXPECTED_SOURCE_AREAS:
        assert area in report
        assert area in scorecard_report
        assert area in interval_report


def test_seasonal_artifacts_retain_exact_elapsed_time_and_calendar_contracts(demo_run):
    _, manifest, manifest_path = demo_run
    by_role = _by_role(manifest, manifest_path)
    features = pd.read_csv(
        by_role["seasonal_demo_features"], parse_dates=["event_timestamp_utc"]
    )
    for _, group in features.groupby(
        ["source_area", "resource_id", "city"], sort=True
    ):
        deltas = (
            group.sort_values("event_timestamp_utc")["event_timestamp_utc"]
            .diff()
            .dropna()
            .dt.total_seconds()
            / 60.0
        )
        assert set(deltas.astype(int)) == {30}
    utc = pd.read_csv(by_role["seasonal_utc_predictions"])
    uk = pd.read_csv(by_role["seasonal_uk_local_predictions"])
    assert set(utc["feature_contract_version"]) == {"time-horizon-v1"}
    assert set(uk["feature_contract_version"]) == {
        "time-horizon-uk-calendar-v1"
    }
    for frame in (utc, uk):
        seasonal = frame.loc[
            frame["model_name"].isin(
                ["seasonal_previous_day", "seasonal_previous_week"]
            )
        ]
        assert set(
            seasonal["seasonal_reference_absolute_offset_minutes"].astype(float)
        ) == {0.0}
        assert set(seasonal["seasonal_reference_period_minutes"].astype(int)) == {
            1440,
            10080,
        }


def test_model_family_scorecard_has_one_paired_cohort_per_area_horizon_slice(demo_run):
    _, manifest, manifest_path = demo_run
    scorecard = pd.read_csv(
        _by_role(manifest, manifest_path)["model_family_scorecard"]
    )
    paired = scorecard.groupby(
        [
            "source_area",
            "resource_id",
            "city",
            "requested_horizon_minutes",
            "split",
        ],
        dropna=False,
    ).agg(
        model_count=("model_name", "nunique"),
        digest_count=("paired_target_identity_sha256", "nunique"),
        observation_count=("paired_observation_count", "nunique"),
    )
    assert set(paired["model_count"]) == {5}
    assert set(paired["digest_count"]) == {1}
    assert set(paired["observation_count"]) == {1}
    assert set(scorecard["requested_horizon_minutes"]) == {30, 60}


def test_interval_artifacts_retain_causal_calibration_and_finite_sample_ranks(demo_run):
    _, manifest, manifest_path = demo_run
    by_role = _by_role(manifest, manifest_path)
    intervals = pd.read_csv(by_role["prediction_intervals"])
    calibration_through = pd.to_datetime(
        intervals["calibration_label_available_through_utc"], utc=True
    )
    feature_time = pd.to_datetime(intervals["feature_timestamp_utc"], utc=True)
    target_time = pd.to_datetime(intervals["event_timestamp_utc"], utc=True)
    trained_through = pd.to_datetime(
        intervals["point_model_trained_through_utc"], utc=True
    )
    assert (
        (calibration_through < feature_time)
        & (trained_through < feature_time)
        & (feature_time < target_time)
    ).all()
    counts = intervals["calibration_observation_count"].astype(int)
    coverage = intervals["target_coverage_level"].astype(float)
    expected = [
        min(count, max(1, __import__("math").ceil((count + 1) * level)))
        for count, level in zip(counts, coverage)
    ]
    assert intervals["calibration_quantile_rank"].astype(int).tolist() == expected
    assert counts.min() >= 24
    assert set(intervals["feature_contract_version"]) == {"time-horizon-v1"}
    assert set(intervals["requested_horizon_minutes"].astype(int)) == {30, 60}


def test_interval_summary_retains_every_area_horizon_level_and_four_models(demo_run):
    _, manifest, manifest_path = demo_run
    summary = pd.read_csv(
        _by_role(manifest, manifest_path)["interval_coverage_summary"]
    )
    assert len(summary) == 4 * 2 * 3
    assert set(summary["model_count"].astype(int)) == {4}
    assert set(summary["requested_horizon_minutes"].astype(int)) == {30, 60}
    assert set(summary["target_coverage_level"].astype(float)) == (
        INTERVAL_COVERAGE_LEVELS
    )
    for column in (
        "empirical_coverage_pct_mean",
        "empirical_coverage_pct_min",
        "empirical_coverage_pct_max",
    ):
        assert summary[column].astype(float).between(0, 100).all()


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


def test_demo_verification_detects_interval_contract_tampering(demo_run, tmp_path):
    _, manifest, manifest_path = demo_run
    copied = tmp_path / "interval-tamper"
    shutil.copytree(manifest_path.parent, copied)
    payload = json.loads(json.dumps(manifest))
    payload["minimum_interval_calibration_rows"] = 1
    from forecasting.portfolio_demo import _document_hash

    payload["manifest_hash"] = _document_hash(payload)
    with pytest.raises(PortfolioDemoError, match="calibration"):
        verify_portfolio_demo_manifest(payload, copied)


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
    assert payload["source_groups"] == 4
    assert len(payload["artifacts"]) == 24
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


def test_portfolio_demo_document_matches_interval_spatial_and_safety_contract():
    document = (ROOT / "PORTFOLIO_DEMO.md").read_text(encoding="utf-8")
    assert "python -m forecasting.run_portfolio_demo" in document
    assert "constraints/ci-python311-linux.txt" in document
    assert "four contracted NGED licence areas" in document
    assert "seasonal_previous_day" in document
    assert "ridge_weather_lag_uk_local" in document
    assert "prediction_intervals" in document
    assert "0.80" in document and "0.90" in document and "0.95" in document
    assert "minimum calibration" in document.lower()
    assert "credential_free=true" in document
    assert "live_source_calls_performed=false" in document
    assert "model_promotion_performed=false" in document
    assert "portfolio-demo-manifest-v4" in document

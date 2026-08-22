from datetime import datetime, timezone
from pathlib import Path
import json

import pandas as pd
import pytest

from forecasting.promotion_assessment import (
    BASELINE_MODEL,
    BLOCKED_STATUS,
    CANDIDATE_MODEL,
    ELIGIBLE_STATUS,
    TargetWeatherPromotionError,
    TargetWeatherPromotionPolicy,
    assess_target_weather_promotion,
)
from forecasting.run_promotion_assessment import main


def comparison(
    *,
    rows_per_slice: int = 24,
    baseline_error: float = 2.0,
    candidate_error: float = 1.0,
    coverage: float = 100.0,
    horizons=(30, 60),
    splits=("validation", "test"),
    run_id="comparison-1",
):
    rows = []
    start = pd.Timestamp("2026-01-01T00:00:00Z")
    for horizon in horizons:
        for split_index, split in enumerate(splits):
            boundary = start + pd.Timedelta(hours=12 * split_index) - pd.Timedelta(minutes=30)
            offset = pd.Timedelta(days=split_index)
            for index in range(rows_per_slice):
                feature = start + offset + pd.Timedelta(minutes=5 * index)
                target = feature + pd.Timedelta(minutes=horizon)
                actual = 100.0 + index
                for model, error in (
                    (BASELINE_MODEL, baseline_error),
                    (CANDIDATE_MODEL, candidate_error),
                ):
                    rows.append(
                        {
                            "run_id": run_id,
                            "source_area": "east_midlands",
                            "resource_id": "resource-1",
                            "city": "Nottingham",
                            "feature_timestamp_utc": feature,
                            "event_timestamp_utc": target,
                            "requested_horizon_minutes": horizon,
                            "split": split,
                            "model_name": model,
                            "actual_demand_mw": actual,
                            "predicted_demand_mw": actual + error,
                            "trained_through_utc": boundary,
                            "forecast_weather_coverage_pct": coverage,
                            "target_weather_provider": "openweather",
                            "target_weather_model": "5-day-3-hour",
                            "target_weather_forecast_ingested_at_utc": feature - pd.Timedelta(minutes=10),
                            "target_weather_forecast_valid_at_utc": target,
                            "origin_fold": None,
                        }
                    )
    return pd.DataFrame(rows)


def reconciliation(
    *,
    matched: int = 30,
    eligible: int = 30,
    temperature_mae: float = 1.0,
    humidity_mae: float = 5.0,
    buckets=("00-06h",),
    run_id="reconciliation-1",
):
    return pd.DataFrame(
        [
            {
                "reconciliation_run_id": run_id,
                "source_area": "east_midlands",
                "city": "Nottingham",
                "forecast_provider": "openweather",
                "forecast_model": "5-day-3-hour",
                "forecast_issue_basis": "retrieval_time_surrogate",
                "forecast_lead_time_bucket": bucket,
                "eligible_forecast_count": eligible,
                "matched_forecast_count": matched,
                "forecast_observation_coverage_pct": matched / eligible * 100.0,
                "temperature_mae_c": temperature_mae,
                "humidity_mae_pct": humidity_mae,
            }
            for bucket in buckets
        ]
    )


def assess(cmp=None, rec=None, policy=None):
    return assess_target_weather_promotion(
        cmp if cmp is not None else comparison(),
        rec if rec is not None else reconciliation(),
        policy=policy,
        assessment_id="assessment-1",
        assessment_timestamp=datetime(2026, 1, 3, tzinfo=timezone.utc),
    )


def test_eligible_assessment_still_forbids_automatic_promotion():
    checks, summary = assess()
    assert summary.loc[0, "assessment_status"] == ELIGIBLE_STATUS
    assert not summary.loc[0, "automatic_promotion_allowed"]
    assert summary.loc[0, "failed_check_count"] == 0
    assert checks["passed"].all()


def test_candidate_accuracy_regression_blocks_review_eligibility():
    checks, summary = assess(comparison(candidate_error=3.0))
    assert summary.loc[0, "assessment_status"] == BLOCKED_STATUS
    failed = checks.loc[~checks["passed"], "check_name"].tolist()
    assert "minimum_candidate_mae_improvement_pct" in failed
    assert "minimum_candidate_rmse_improvement_pct" in failed


def test_insufficient_model_rows_and_forecast_coverage_block():
    policy = TargetWeatherPromotionPolicy(min_model_observations=24)
    checks, summary = assess(comparison(rows_per_slice=10, coverage=80.0), policy=policy)
    assert summary.loc[0, "assessment_status"] == BLOCKED_STATUS
    failed = set(checks.loc[~checks["passed"], "check_name"])
    assert "minimum_paired_model_observations" in failed
    assert "minimum_model_forecast_weather_coverage_pct" in failed


def test_missing_horizon_and_test_split_block_completeness():
    checks, summary = assess(comparison(horizons=(30,), splits=("validation",)))
    failed = set(checks.loc[~checks["passed"], "check_name"])
    assert summary.loc[0, "assessment_status"] == BLOCKED_STATUS
    assert "required_horizon_present" in failed
    assert "required_test_split_present" in failed


def test_reconciliation_volume_coverage_and_error_thresholds_block():
    rec = reconciliation(matched=10, eligible=20, temperature_mae=4.0, humidity_mae=20.0)
    checks, summary = assess(rec=rec)
    failed = set(checks.loc[~checks["passed"], "check_name"])
    assert summary.loc[0, "assessment_status"] == BLOCKED_STATUS
    assert failed.issuperset(
        {
            "minimum_reconciled_observations",
            "minimum_reconciliation_coverage_pct",
            "maximum_temperature_mae_c",
            "maximum_humidity_mae_pct",
        }
    )


def test_explicit_required_lead_bucket_without_evidence_blocks():
    policy = TargetWeatherPromotionPolicy(required_lead_buckets=("00-06h", "06-12h"))
    checks, summary = assess(policy=policy)
    failed = checks.loc[
        (~checks["passed"])
        & (checks["forecast_lead_time_bucket"] == "06-12h"),
        "check_name",
    ]
    assert summary.loc[0, "assessment_status"] == BLOCKED_STATUS
    assert set(failed).issuperset(
        {
            "minimum_reconciled_observations",
            "minimum_reconciliation_coverage_pct",
        }
    )


def test_unpaired_model_cohort_is_rejected_not_scored():
    cmp = comparison()
    cmp = cmp.drop(index=cmp.loc[cmp["model_name"] == CANDIDATE_MODEL].index[0])
    with pytest.raises(TargetWeatherPromotionError, match="identical paired target rows"):
        assess(cmp=cmp)


def test_different_training_boundaries_are_rejected():
    cmp = comparison()
    candidate_index = cmp.loc[cmp["model_name"] == CANDIDATE_MODEL].index[0]
    cmp.loc[candidate_index, "trained_through_utc"] = pd.Timestamp("2025-12-30T00:00:00Z")
    with pytest.raises(TargetWeatherPromotionError, match="training boundary"):
        assess(cmp=cmp)


def test_multiple_run_ids_require_selection_before_assessment():
    cmp = pd.concat([comparison(run_id="one"), comparison(run_id="two")], ignore_index=True)
    with pytest.raises(TargetWeatherPromotionError, match="exactly one run_id"):
        assess(cmp=cmp)


def test_cli_writes_advisory_outputs_and_optional_blocking_exit(tmp_path):
    comparison_path = tmp_path / "comparison.csv"
    reconciliation_path = tmp_path / "reconciliation.csv"
    comparison(candidate_error=3.0).to_csv(comparison_path, index=False)
    reconciliation().to_csv(reconciliation_path, index=False)
    output = tmp_path / "output"
    exit_code = main(
        [
            "--comparison-predictions",
            str(comparison_path),
            "--reconciliation-metrics",
            str(reconciliation_path),
            "--output-dir",
            str(output),
            "--output-format",
            "csv",
            "--require-eligible",
        ]
    )
    assert exit_code == 2
    summary_files = list(output.glob("target_weather_promotion_summary_*.csv"))
    check_files = list(output.glob("target_weather_promotion_checks_*.csv"))
    assert len(summary_files) == len(check_files) == 1
    summary = pd.read_csv(summary_files[0])
    assert summary.loc[0, "assessment_status"] == BLOCKED_STATUS
    assert not bool(summary.loc[0, "automatic_promotion_allowed"])


def test_policy_rejects_unsupported_lead_bucket():
    with pytest.raises(TargetWeatherPromotionError, match="unsupported"):
        TargetWeatherPromotionPolicy(required_lead_buckets=("wrong",)).validate()


def _schema_row(frame: pd.DataFrame) -> dict:
    row = frame.iloc[0].to_dict()
    for key, value in list(row.items()):
        if isinstance(value, pd.Timestamp):
            row[key] = value.isoformat()
        elif pd.isna(value):
            row[key] = None
    return row


def test_promotion_check_and_summary_satisfy_versioned_schemas():
    from jsonschema import Draft202012Validator, FormatChecker

    checks, summary = assess()
    root = Path(__file__).resolve().parents[1] / "data-contracts"
    check_schema = json.loads(
        (root / "target_weather_promotion_check_schema.json").read_text(
            encoding="utf-8"
        )
    )
    summary_schema = json.loads(
        (root / "target_weather_promotion_summary_schema.json").read_text(
            encoding="utf-8"
        )
    )
    assert list(
        Draft202012Validator(
            check_schema, format_checker=FormatChecker()
        ).iter_errors(_schema_row(checks))
    ) == []
    assert list(
        Draft202012Validator(
            summary_schema, format_checker=FormatChecker()
        ).iter_errors(_schema_row(summary))
    ) == []

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def text(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def test_fabric_seasonal_comparison_reuses_one_retained_baseline_run():
    notebook = text("fabric/notebooks/05d_seasonal_baseline_comparison.py")
    assert 'POINT_PREDICTIONS_TABLE = "forecast_baseline_predictions"' in notebook
    assert 'POINT_PREDICTION_RUN_ID = ""' in notebook
    assert 'BASELINE_MODELS = ("persistence_current_value", "ridge_weather_lag")' in notebook
    assert "LinearRegression" not in notebook
    assert ".fit(" not in notebook
    assert "exact persistence/ridge pairs" in notebook


def test_fabric_seasonal_comparison_uses_elapsed_target_minus_period_matching():
    notebook = text("fabric/notebooks/05d_seasonal_baseline_comparison.py")
    assert '"seasonal_previous_day": ("previous_day", 1440)' in notebook
    assert '"seasonal_previous_week": ("previous_week", 10080)' in notebook
    assert 'F.col(TARGET_TIMESTAMP_COLUMN).cast("long")' in notebook
    assert 'F.lit(minutes * 60)' in notebook
    assert "shift(288" not in notebook
    assert "shift(2016" not in notebook


def test_fabric_seasonal_comparison_enforces_identity_availability_and_tolerance():
    notebook = text("fabric/notebooks/05d_seasonal_baseline_comparison.py")
    assert 'F.col(f"feature.{column}") == F.col(f"source.{column}")' in notebook
    assert '<= F.col(f"feature.{FEATURE_TIMESTAMP_COLUMN}")' in notebook
    assert 'config["reference_tolerance_minutes"] * 60' in notebook
    assert 'F.col(f"{name}_offset_minutes") > F.lit(0.0)' in notebook
    assert 'F.abs(F.col(f"{name}_offset_minutes"))' in notebook


def test_fabric_seasonal_comparison_retains_period_coverage_evidence():
    notebook = text("fabric/notebooks/05d_seasonal_baseline_comparison.py")
    for period in ("previous_day", "previous_week"):
        assert f'"{period}_eligible_count"' in notebook
        assert f'"{period}_matched_count"' in notebook
        assert f'"{period}_coverage_pct"' in notebook
    assert 'MIN_REFERENCE_COVERAGE = 0.90' in notebook
    assert "coverage is inadequate" in notebook


def test_fabric_seasonal_comparison_uses_one_common_four_model_cohort():
    notebook = text("fabric/notebooks/05d_seasonal_baseline_comparison.py")
    assert "ALL_MODELS = (*BASELINE_MODELS, *SEASONAL_MODELS)" in notebook
    assert '"previous_day_source_timestamp_utc"' in notebook
    assert '"previous_week_source_timestamp_utc"' in notebook
    assert 'F.countDistinct("actual_demand_mw")' in notebook
    assert 'F.countDistinct("trained_through_utc")' in notebook
    assert "exact paired rows" in notebook


def test_fabric_seasonal_outputs_are_separate_and_versioned():
    notebook = text("fabric/notebooks/05d_seasonal_baseline_comparison.py")
    ordinary = text("fabric/notebooks/05_baseline_forecasting.py")
    assert 'PREDICTIONS_TABLE = "forecast_seasonal_comparison_predictions"' in notebook
    assert 'METRICS_TABLE = "forecast_seasonal_comparison_metrics"' in notebook
    assert 'SEASONAL_CONTRACT_VERSION = "elapsed-seasonal-v1"' in notebook
    assert "forecast_seasonal_comparison_predictions" not in ordinary
    assert "forecast_seasonal_comparison_metrics" not in ordinary


def test_fabric_seasonal_quality_gate_checks_pairing_causality_coverage_and_sequence():
    checks = text("fabric/notebooks/06d_seasonal_baseline_quality_checks.py")
    for check_name in (
        "seasonal_comparison_exact_pairs",
        "seasonal_comparison_model_contract",
        "seasonal_comparison_reference_causality",
        "seasonal_comparison_coverage",
        "seasonal_comparison_time_boundaries",
        "seasonal_comparison_metrics_valid",
        "seasonal_comparison_rolling_sequence",
    ):
        assert f'"{check_name}"' in checks
    assert 'RESULTS_TABLE = "dq_run_results"' in checks


def test_fabric_seasonal_views_are_pass_through_only():
    views = text("fabric/sql/forecast_seasonal_comparison_views_tsql.sql")
    assert "FROM dbo.forecast_seasonal_comparison_predictions" in views
    assert "FROM dbo.forecast_seasonal_comparison_metrics" in views
    assert "ROW_NUMBER" not in views
    assert "JOIN" not in views


def test_fabric_seasonal_pipeline_is_manual_and_non_promoting():
    pipeline = text("fabric/pipelines/seasonal_baseline_comparison_pipeline.md")
    assert "manual and evidence-only" in pipeline
    assert "does not replace `05_baseline_forecasting`" in pipeline
    assert "no model fitting occurs" in pipeline
    assert "Do not enable a trigger" in pipeline


def test_roadmap_advances_to_fabric_interval_parity():
    roadmap = text("ROADMAP.md")
    assert "G19b | Fabric parity for elapsed-time seasonal baseline comparison" in roadmap
    assert "Implemented as an optional manual subflow" in roadmap
    assert "G20b | Fabric parity for calibration-only prediction intervals" in roadmap
    assert "| Next |" in roadmap

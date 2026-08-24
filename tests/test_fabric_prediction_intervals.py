from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def text(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def test_fabric_intervals_consume_one_retained_seasonal_run_without_refitting():
    notebook = text("fabric/notebooks/05e_prediction_intervals.py")
    assert (
        'POINT_PREDICTIONS_TABLE = "forecast_seasonal_comparison_predictions"'
        in notebook
    )
    assert 'POINT_PREDICTION_RUN_ID = ""' in notebook
    assert "LinearRegression" not in notebook
    assert ".fit(" not in notebook
    assert "exact four-model pairs" in notebook
    assert "No point model is fitted or refitted" in notebook


def test_fabric_intervals_use_only_causally_available_validation_labels():
    notebook = text("fabric/notebooks/05e_prediction_intervals.py")
    assert 'F.col("split") == "validation"' in notebook
    assert 'F.col("split") == "test"' in notebook
    assert (
        'F.col(TARGET_TIMESTAMP_COLUMN)\n            '
        '< F.col("evaluation_feature_start_utc")'
        in notebook
    )
    assert (
        "Calibration includes labels unavailable before test feature time."
        in notebook
    )
    assert "predicted_demand_mw" in notebook
    assert "absolute_calibration_error_mw" in notebook


def test_fabric_intervals_use_the_finite_sample_n_plus_one_rank():
    notebook = text("fabric/notebooks/05e_prediction_intervals.py")
    assert (
        '(F.col("calibration_observation_count") + F.lit(1))'
        in notebook
    )
    assert 'F.col("target_coverage_level")' in notebook
    assert "F.ceil(" in notebook
    assert '"calibration_radius_mw"' in notebook
    assert 'COVERAGE_LEVELS = "0.80,0.90,0.95"' in notebook
    assert "MIN_CALIBRATION_ROWS = 24" in notebook


def test_fabric_intervals_retain_exact_four_model_target_pairs():
    notebook = text("fabric/notebooks/05e_prediction_intervals.py")
    assert '"persistence_current_value"' in notebook
    assert '"ridge_weather_lag"' in notebook
    assert '"seasonal_previous_day"' in notebook
    assert '"seasonal_previous_week"' in notebook
    assert 'F.countDistinct("actual_demand_mw")' in notebook
    assert 'F.countDistinct("point_model_trained_through_utc")' in notebook
    assert "exact four-model target pairs" in notebook


def test_fabric_interval_outputs_are_separate_versioned_delta_evidence():
    notebook = text("fabric/notebooks/05e_prediction_intervals.py")
    ordinary = text("fabric/notebooks/05_baseline_forecasting.py")
    seasonal = text("fabric/notebooks/05d_seasonal_baseline_comparison.py")
    assert 'INTERVALS_TABLE = "forecast_prediction_intervals"' in notebook
    assert (
        'METRICS_TABLE = "forecast_prediction_interval_metrics"'
        in notebook
    )
    assert (
        'INTERVAL_CONTRACT_VERSION = '
        '"split-conformal-absolute-residual-v1"'
        in notebook
    )
    assert "forecast_prediction_intervals" not in ordinary
    assert "forecast_prediction_intervals" not in seasonal
    assert '.mode("append")' in notebook
    assert '"mergeSchema"' in notebook and '"true"' in notebook


def test_fabric_interval_quality_gate_covers_all_blocking_contracts():
    checks = text("fabric/notebooks/06e_prediction_interval_quality_checks.py")
    for check_name in (
        "prediction_interval_single_point_run_binding",
        "prediction_interval_exact_model_pairs",
        "prediction_interval_calibration_causality",
        "prediction_interval_finite_sample_rank",
        "prediction_interval_fixed_radius",
        "prediction_interval_bounds",
        "prediction_interval_contract",
        "prediction_interval_coverage_levels_complete",
        "prediction_interval_metrics_valid",
        "prediction_interval_metric_consistency",
    ):
        assert f'"{check_name}"' in checks
    assert 'RESULTS_TABLE = "dq_run_results"' in checks
    assert "test_labels_used_for_calibration" not in checks


def test_fabric_interval_views_are_pass_through_only():
    views = text("fabric/sql/forecast_prediction_interval_views_tsql.sql")
    assert "FROM dbo.forecast_prediction_intervals" in views
    assert "FROM dbo.forecast_prediction_interval_metrics" in views
    assert "JOIN" not in views
    assert "ROW_NUMBER" not in views
    assert "percentile_approx" not in views


def test_fabric_interval_pipeline_is_manual_non_promoting_and_non_refitting():
    pipeline = text("fabric/pipelines/prediction_interval_pipeline.md")
    assert "manual and evidence-only" in pipeline
    assert "does not replace `05_baseline_forecasting`" in pipeline
    assert "No model fitting or refitting occurs" in pipeline
    assert "test_labels_used_for_calibration=false" in pipeline
    assert "Do not enable a trigger" in pipeline


def test_roadmap_records_fabric_interval_monitoring_and_advances_to_portfolio_history():
    roadmap = text("ROADMAP.md")
    assert "G20b | Fabric parity for calibration-only prediction intervals" in roadmap
    assert "G21a | Paired area-and-horizon model-family scorecards" in roadmap
    assert "G21b | Multi-area portfolio-demo integration for independently validated seasonal evidence | Implemented" in roadmap
    assert "G21c | Multi-area portfolio-demo integration for independently validated interval evidence | Implemented" in roadmap
    assert "G22 | Interval coverage, width, calibration-history, and freshness monitoring without automatic recalibration | Implemented as advisory local evidence" in roadmap
    assert "G23 | Fabric parity for advisory interval monitoring over retained interval metrics without automatic recalibration | Implemented as an optional manual subflow" in roadmap
    assert "G24 | Multi-area portfolio-demo integration for repeated interval-health evidence and advisory operator reporting | Next" in roadmap

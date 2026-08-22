from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def text(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def test_fabric_target_weather_comparison_uses_separate_evidence_tables():
    notebook = text("fabric/notebooks/05c_target_weather_model_comparison.py")
    ordinary = text("fabric/notebooks/05_baseline_forecasting.py")
    assert 'PREDICTIONS_TABLE = "forecast_weather_comparison_predictions"' in notebook
    assert 'METRICS_TABLE = "forecast_weather_comparison_metrics"' in notebook
    assert "forecast_weather_comparison_predictions" not in ordinary
    assert "forecast_weather_comparison_metrics" not in ordinary


def test_fabric_comparison_substitutes_only_weather_features():
    notebook = text("fabric/notebooks/05c_target_weather_model_comparison.py")
    assert 'DEMAND_FEATURE_COLUMNS = [' in notebook
    assert '"demand_mw"' in notebook
    assert '"demand_lag_1"' in notebook
    assert '"demand_rolling_mean_12"' in notebook
    assert 'OBSERVED_FEATURE_COLUMNS = [' in notebook
    assert '"temperature"' in notebook
    assert '"humidity"' in notebook
    assert '"weather_age_minutes"' in notebook
    assert 'TARGET_WEATHER_FEATURE_COLUMNS = [' in notebook
    assert '"target_weather_temperature_c"' in notebook
    assert '"target_weather_humidity_pct"' in notebook
    assert '"target_weather_availability_age_minutes"' in notebook


def test_fabric_comparison_matches_only_causally_available_target_weather():
    notebook = text("fabric/notebooks/05c_target_weather_model_comparison.py")
    assert 'F.col("weather.forecast_issued_at_utc")' in notebook
    assert '<= F.col(f"feature.{FEATURE_TIMESTAMP_COLUMN}")' in notebook
    assert 'F.col("weather.forecast_ingested_at_utc")' in notebook
    assert 'F.col("weather.forecast_valid_at_utc")' in notebook
    assert '> F.col(f"feature.{FEATURE_TIMESTAMP_COLUMN}")' in notebook
    assert 'config["max_forecast_availability_age_minutes"] * 60' in notebook
    assert 'config["forecast_valid_tolerance_minutes"] * 60' in notebook
    assert 'F.lower(F.col("feature.city")) == F.lower(F.col("weather.city"))' in notebook


def test_fabric_comparison_builds_one_covered_cohort_and_one_label_purge():
    notebook = text("fabric/notebooks/05c_target_weather_model_comparison.py")
    assert 'MIN_FORECAST_WEATHER_COVERAGE = 0.90' in notebook
    assert '"forecast_weather_eligible_count"' in notebook
    assert '"forecast_weather_matched_count"' in notebook
    assert '"forecast_weather_coverage_pct"' in notebook
    pair_section = notebook.split("def _evaluate_pair(", 1)[1].split(
        "def _balanced_partition_sizes", 1
    )[0]
    assert pair_section.count("_purge_training(") == 1
    assert 'OBSERVED_FEATURE_COLUMNS' in pair_section
    assert 'TARGET_WEATHER_FEATURE_COLUMNS' in pair_section
    assert 'model_name=BASELINE_MODEL' in pair_section
    assert 'model_name=CANDIDATE_MODEL' in pair_section


def test_fabric_comparison_supports_holdout_and_rolling_origin_without_random_split():
    notebook = text("fabric/notebooks/05c_target_weather_model_comparison.py")
    assert 'SUPPORTED_EVALUATION_MODES = ("holdout", "rolling-origin")' in notebook
    assert 'HOLDOUT_EVALUATION_CONTRACT_VERSION = "fixed-holdout-v1"' in notebook
    assert 'ROLLING_ORIGIN_EVALUATION_CONTRACT_VERSION = "rolling-origin-v1"' in notebook
    assert 'def _rolling_folds(' in notebook
    assert 'def _evaluate_group(' in notebook
    assert 'randomSplit' not in notebook
    assert 'F.col(TARGET_TIMESTAMP_COLUMN) < F.lit(cutoff)' in notebook


def test_fabric_comparison_validates_exact_model_pairs_and_training_boundaries():
    notebook = text("fabric/notebooks/05c_target_weather_model_comparison.py")
    assert 'def _pair_failures(' in notebook
    assert 'F.countDistinct("model_name")' in notebook
    assert 'F.countDistinct("actual_demand_mw")' in notebook
    assert 'F.countDistinct("trained_through_utc")' in notebook
    assert 'Expected paired models' in notebook
    assert 'do not form exact paired rows' in notebook


def test_fabric_comparison_quality_gate_covers_pairing_causality_and_rolling_sequence():
    checks = text("fabric/notebooks/06c_target_weather_comparison_quality_checks.py")
    assert '"target_weather_comparison_exact_pairs"' in checks
    assert '"target_weather_comparison_causal_boundaries"' in checks
    assert '"target_weather_comparison_model_modes"' in checks
    assert '"target_weather_comparison_contract_versions"' in checks
    assert '"target_weather_comparison_coverage"' in checks
    assert '"target_weather_comparison_rolling_sequence"' in checks
    assert 'RESULTS_TABLE = "dq_run_results"' in checks


def test_fabric_comparison_views_are_pass_through_only():
    views = text("fabric/sql/forecast_weather_comparison_views_tsql.sql")
    assert "FROM dbo.forecast_weather_comparison_predictions" in views
    assert "FROM dbo.forecast_weather_comparison_metrics" in views
    assert "LinearRegression" not in views
    assert "ROW_NUMBER" not in views


def test_fabric_comparison_pipeline_remains_manual_and_non_promoting():
    pipeline = text("fabric/pipelines/target_weather_model_comparison_pipeline.md")
    assert "manual and evidence-only" in pipeline
    assert "does not replace `05_baseline_forecasting`" in pipeline
    assert "Do not enable a trigger" in pipeline
    assert "human-review-only promotion assessment" in pipeline


def test_roadmap_advances_to_provider_monitoring():
    roadmap = text("ROADMAP.md")
    assert "G4 | Fabric paired" in roadmap
    assert "Implemented as an optional manual subflow" in roadmap
    assert "G5 | Forecast-provider drift" in roadmap
    assert "| Next |" in roadmap

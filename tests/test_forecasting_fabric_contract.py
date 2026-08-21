from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _text(path: str) -> str:
    return (PROJECT_ROOT / path).read_text(encoding="utf-8")


def test_target_rolling_feature_excludes_current_demand_row():
    gold = _text("fabric/notebooks/03_build_gold_tables.py")

    demand_window = "ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING"
    assert demand_window in gold
    demand_section = gold.split("AVG(demand_mw) OVER", maxsplit=1)[1].split(
        ") AS demand_rolling_mean_12", maxsplit=1
    )[0]
    assert "CURRENT ROW" not in demand_section


def test_fabric_backtest_builds_bounded_time_targets():
    notebook = _text("fabric/notebooks/05_baseline_forecasting.py")

    assert 'SUPPORTED_HORIZON_MINUTES = (30, 60)' in notebook
    assert 'HORIZON_MINUTES = "30,60"' in notebook
    assert 'TARGET_TOLERANCE_MINUTES = 5' in notebook
    assert 'MIN_TARGET_COVERAGE = 0.90' in notebook
    assert 'REQUESTED_HORIZON_COLUMN = "requested_horizon_minutes"' in notebook
    assert 'TARGET_TIMESTAMP_COLUMN = "target_timestamp_utc"' in notebook
    assert 'F.col("feature._ideal_target_epoch")' in notebook
    assert 'F.col("feature._latest_target_epoch")' in notebook
    assert ".asc_nulls_last()" in notebook
    assert "F.lead(" not in notebook
    assert "randomSplit" not in notebook


def test_fabric_backtest_enforces_target_coverage():
    notebook = _text("fabric/notebooks/05_baseline_forecasting.py")

    assert 'F.count(F.lit(1)).alias("eligible_target_count")' in notebook
    assert "expected_combinations" in notebook
    assert '.fillna({"eligible_target_count": 0, "matched_target_count": 0})' in notebook
    assert 'alias("matched_target_count")' in notebook
    assert '"target_coverage_pct"' in notebook
    assert '"Forecast target coverage is below MIN_TARGET_COVERAGE: "' in notebook
    assert "allow_one=True" in notebook
    assert 'F.col("target_delay_minutes")' in notebook


def test_fabric_backtest_purges_labels_not_known_at_feature_time():
    notebook = _text("fabric/notebooks/05_baseline_forecasting.py")

    assert "def _purge_training(" in notebook
    assert "F.col(TARGET_TIMESTAMP_COLUMN) < F.lit(evaluation_start)" in notebook
    assert 'model_name="persistence_current_value"' in notebook
    assert 'labelCol=SUPERVISED_TARGET_COLUMN' in notebook
    assert 'F.col(TARGET_COLUMN).cast("double")' in notebook


def test_fabric_backtest_preserves_holdout_and_adds_explicit_rolling_mode():
    notebook = _text("fabric/notebooks/05_baseline_forecasting.py")

    assert 'SUPPORTED_EVALUATION_MODES = ("holdout", "rolling-origin")' in notebook
    assert 'EVALUATION_MODE = "holdout"' in notebook
    assert 'ROLLING_ORIGIN_FOLDS = 3' in notebook
    assert "def _evaluate_holdout_group(" in notebook
    assert "def _evaluate_rolling_group(" in notebook
    assert 'if config["evaluation_mode"] == "rolling-origin":' in notebook
    assert "return _evaluate_holdout_group(" in notebook


def test_fabric_rolling_origins_expand_history_and_reserve_final_test():
    notebook = _text("fabric/notebooks/05_baseline_forecasting.py")

    assert "def _balanced_partition_sizes(" in notebook
    assert "def _rolling_origin_folds(" in notebook
    assert 'validation_origin_count = origin_count - 1' in notebook
    assert 'F.col("_row_number") <= F.lit(cursor)' in notebook
    assert 'F.col("_row_number") <= F.lit(validation_end)' in notebook
    assert '"split": "validation"' in notebook
    assert '"split": "test"' in notebook
    assert "Rolling-origin cutoffs must be strictly increasing." in notebook


def test_fabric_backtest_persists_time_and_origin_evidence():
    notebook = _text("fabric/notebooks/05_baseline_forecasting.py")

    assert 'PREDICTIONS_TABLE = "forecast_baseline_predictions"' in notebook
    assert 'METRICS_TABLE = "forecast_baseline_metrics"' in notebook
    assert 'F.col(REQUESTED_HORIZON_COLUMN).cast("int")' in notebook
    assert 'F.col(TARGET_TOLERANCE_COLUMN).cast("int")' in notebook
    assert 'F.col("target_delay_minutes").cast("double")' in notebook
    assert 'HOLDOUT_EVALUATION_CONTRACT_VERSION = "fixed-holdout-v1"' in notebook
    assert (
        'ROLLING_ORIGIN_EVALUATION_CONTRACT_VERSION = "rolling-origin-v1"'
        in notebook
    )
    assert 'F.lit(origin_fold).cast("int").alias(ORIGIN_FOLD_COLUMN)' in notebook
    assert 'F.lit(origin_count).cast("int").alias(ORIGIN_COUNT_COLUMN)' in notebook
    assert 'alias(TRAINING_OBSERVATION_COUNT_COLUMN)' in notebook
    assert 'F.lit(evaluation_contract_version).alias(' in notebook
    assert 'EVALUATION_CONTRACT_VERSION_COLUMN' in notebook
    assert '.mode("append")' in notebook


def test_fabric_backtest_validates_complete_rolling_origin_sequences():
    notebook = _text("fabric/notebooks/05_baseline_forecasting.py")

    assert "def _rolling_origin_failures(" in notebook
    assert 'F.countDistinct(ORIGIN_FOLD_COLUMN).alias("_fold_count")' in notebook
    assert 'F.lag(ORIGIN_CUTOFF_COLUMN).over(order)' in notebook
    assert 'F.lag(TRAINING_OBSERVATION_COUNT_COLUMN).over(order)' in notebook
    assert 'F.countDistinct(ORIGIN_FOLD_COLUMN).alias("_origin_uses")' in notebook
    assert "Rolling-origin sequence evidence is invalid." in notebook


def test_final_quality_gate_covers_time_coverage_and_origin_boundaries():
    checks = _text("fabric/notebooks/06_forecast_quality_checks.py")

    assert '"check_name": "forecast_prediction_training_boundary"' in checks
    assert '"check_name": "forecast_prediction_time_horizon_valid"' in checks
    assert '"check_name": "forecast_target_coverage_valid"' in checks
    assert '"check_name": "forecast_evaluation_contract_valid"' in checks
    assert '"check_name": "forecast_rolling_origin_sequence_valid"' in checks
    assert '~F.col("requested_horizon_minutes").isin(' in checks
    assert 'F.col("target_delay_minutes")' in checks
    assert 'F.col("target_tolerance_minutes")' in checks
    assert 'F.col("target_coverage_pct")' in checks
    assert 'F.col("minimum_target_coverage_pct")' in checks
    assert "def _rolling_origin_sequence_failures(" in checks
    assert 'F.countDistinct("origin_fold").alias("_fold_count")' in checks


def test_fabric_pipeline_exposes_evaluation_mode_controls():
    pipeline = _text("fabric/pipelines/weather_energy_demand_pipeline.md")
    fabric_readme = _text("fabric/README.md")

    for document in (pipeline, fabric_readme):
        assert "EVALUATION_MODE" in document
        assert "ROLLING_ORIGIN_FOLDS" in document
        assert "fixed holdout" in document.lower()
        assert "rolling-origin" in document


def test_sql_quality_checks_cover_rolling_origin_contract():
    checks = _text("monitoring/forecast_quality_checks.sql")

    assert "forecast_evaluation_contract_valid" in checks
    assert "forecast_rolling_origin_sequence_valid" in checks
    assert "rolling_origin_order" in checks
    assert "COUNT(DISTINCT origin_fold)" in checks
    assert "training_observation_count" in checks
    assert "evaluation_contract_version" in checks


def test_sql_endpoint_forecast_views_are_pass_through_only():
    views = _text("fabric/sql/forecast_views_tsql.sql")

    assert "FROM dbo.forecast_baseline_predictions" in views
    assert "FROM dbo.forecast_baseline_metrics" in views
    assert "LinearRegression" not in views
    assert "ROW_NUMBER" not in views

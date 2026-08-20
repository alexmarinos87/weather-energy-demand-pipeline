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


def test_fabric_backtest_builds_explicit_future_targets():
    notebook = _text("fabric/notebooks/05_baseline_forecasting.py")

    assert 'HORIZON_STEPS = 1' in notebook
    assert 'FEATURE_TIMESTAMP_COLUMN = "feature_timestamp_utc"' in notebook
    assert 'TARGET_TIMESTAMP_COLUMN = "target_timestamp_utc"' in notebook
    assert "F.lead(F.col(SOURCE_TIMESTAMP_COLUMN), horizon_steps)" in notebook
    assert "F.lead(F.col(TARGET_COLUMN), horizon_steps)" in notebook
    assert 'labelCol=SUPERVISED_TARGET_COLUMN' in notebook
    assert '"demand_mw"' in notebook.split("FEATURE_COLUMNS = [", 1)[1].split(
        "]", 1
    )[0]
    assert "randomSplit" not in notebook


def test_fabric_backtest_purges_labels_not_known_at_feature_time():
    notebook = _text("fabric/notebooks/05_baseline_forecasting.py")

    assert "def _purge_training(" in notebook
    assert "F.col(TARGET_TIMESTAMP_COLUMN) < F.lit(evaluation_start)" in notebook
    assert "F.col(FEATURE_TIMESTAMP_COLUMN) <= F.col(\"trained_through_utc\")" in notebook
    assert 'model_name="persistence_lag_1"' in notebook
    assert 'F.col(TARGET_COLUMN).cast("double")' in notebook


def test_fabric_backtest_persists_horizon_evidence():
    notebook = _text("fabric/notebooks/05_baseline_forecasting.py")

    assert 'PREDICTIONS_TABLE = "forecast_baseline_predictions"' in notebook
    assert 'METRICS_TABLE = "forecast_baseline_metrics"' in notebook
    assert 'F.col(FEATURE_TIMESTAMP_COLUMN)' in notebook
    assert 'F.col(TARGET_TIMESTAMP_COLUMN).alias(SOURCE_TIMESTAMP_COLUMN)' in notebook
    assert 'F.lit(horizon_steps).cast("int").alias("horizon_steps")' in notebook
    assert 'F.col("horizon_minutes").cast("double")' in notebook
    assert 'F.lit(FEATURE_CONTRACT_VERSION)' in notebook
    assert '.mode("append")' in notebook


def test_final_quality_gate_covers_training_and_horizon_boundaries():
    checks = _text("fabric/notebooks/06_forecast_quality_checks.py")

    assert '"check_name": "forecast_predictions_not_empty"' in checks
    assert '"check_name": "forecast_metrics_not_empty"' in checks
    assert '"check_name": "forecast_prediction_training_boundary"' in checks
    assert '"check_name": "forecast_prediction_horizon_valid"' in checks
    assert 'F.col("feature_timestamp_utc") <= F.col("trained_through_utc")' in checks
    assert 'F.col("event_timestamp_utc") <= F.col("feature_timestamp_utc")' in checks


def test_sql_endpoint_forecast_views_are_pass_through_only():
    views = _text("fabric/sql/forecast_views_tsql.sql")

    assert "FROM dbo.forecast_baseline_predictions" in views
    assert "FROM dbo.forecast_baseline_metrics" in views
    assert "LinearRegression" not in views
    assert "ROW_NUMBER" not in views

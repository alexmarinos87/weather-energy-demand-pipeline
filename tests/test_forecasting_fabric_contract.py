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


def test_fabric_backtest_uses_chronological_splits_and_safe_features():
    notebook = _text("fabric/notebooks/05_baseline_forecasting.py")

    assert "Window.partitionBy(*GROUP_COLUMNS).orderBy(TIMESTAMP_COLUMN)" in notebook
    assert 'F.lit("train")' in notebook
    assert 'F.lit("validation")' in notebook
    assert 'F.lit("test")' in notebook
    assert "randomSplit" not in notebook
    assert '"demand_lag_1"' in notebook
    assert '"demand_rolling_mean_12"' in notebook
    assert "demand_mw" not in notebook.split("FEATURE_COLUMNS = [", 1)[1].split(
        "]", 1
    )[0]


def test_fabric_backtest_persists_predictions_metrics_and_training_evidence():
    notebook = _text("fabric/notebooks/05_baseline_forecasting.py")

    assert 'PREDICTIONS_TABLE = "forecast_baseline_predictions"' in notebook
    assert 'METRICS_TABLE = "forecast_baseline_metrics"' in notebook
    assert 'F.lit("persistence_lag_1")' not in notebook
    assert 'model_name="persistence_lag_1"' in notebook
    assert 'model_name="ridge_weather_lag"' in notebook
    assert 'F.col(TIMESTAMP_COLUMN) <= F.col("trained_through_utc")' in notebook
    assert '.mode("append")' in notebook


def test_final_quality_gate_covers_forecast_outputs():
    checks = _text("fabric/notebooks/06_forecast_quality_checks.py")

    assert '"check_name": "forecast_predictions_not_empty"' in checks
    assert '"check_name": "forecast_metrics_not_empty"' in checks
    assert '"check_name": "forecast_prediction_training_boundary"' in checks
    assert 'F.col("event_timestamp_utc") <= F.col("trained_through_utc")' in checks


def test_sql_endpoint_forecast_views_are_pass_through_only():
    views = _text("fabric/sql/forecast_views_tsql.sql")

    assert "FROM dbo.forecast_baseline_predictions" in views
    assert "FROM dbo.forecast_baseline_metrics" in views
    assert "LinearRegression" not in views
    assert "ROW_NUMBER" not in views

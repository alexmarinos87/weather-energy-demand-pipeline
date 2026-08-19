from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _text(path: str) -> str:
    return (PROJECT_ROOT / path).read_text(encoding="utf-8")


def test_spark_gold_join_is_same_area_and_past_only():
    sql = _text("fabric/notebooks/03_build_gold_tables.py")

    assert "e.source_area = w.source_area" in sql
    assert "w.event_timestamp_utc <= e.event_timestamp_utc" in sql
    assert "e.event_timestamp_utc - INTERVAL 6 HOURS" in sql
    assert "w.event_timestamp_utc <= e.event_timestamp_utc + INTERVAL 1 HOUR" not in sql
    assert "ORDER BY\n                    w.event_timestamp_utc DESC" in sql
    assert "ABS(unix_timestamp" not in sql


def test_gold_windows_are_partitioned_by_source_area():
    sql = _text("fabric/notebooks/03_build_gold_tables.py")

    assert "PARTITION BY source_area, resource_id, city" in sql
    assert "weather_age_minutes BETWEEN 0 AND 360" in sql


def test_sql_endpoint_views_do_not_reimplement_gold_logic():
    sql = _text("fabric/sql/gold_views_tsql.sql")

    assert "FROM dbo.gold_weather_demand_join" in sql
    assert "FROM dbo.gold_feature_engineering" in sql
    assert "FROM dbo.gold_demand_aggregation" in sql
    assert "ROW_NUMBER" not in sql
    assert "DATEDIFF" not in sql


def test_data_quality_blocks_cross_area_and_future_matches():
    notebook = _text("fabric/notebooks/04_data_quality_checks.py")

    assert '"check_name": "gold_weather_cross_area_match"' in notebook
    assert '"check_name": "gold_weather_future_match"' in notebook
    assert "weather_source_area <> source_area" in notebook
    assert "weather_event_timestamp_utc > event_timestamp_utc" in notebook

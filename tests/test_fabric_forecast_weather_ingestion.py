from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def text(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def test_fabric_forecast_bronze_uses_fixed_coordinate_endpoint():
    notebook = text("fabric/notebooks/01b_ingest_forecast_weather_to_bronze.py")
    assert 'OPENWEATHER_FORECAST_ENDPOINT = "https://api.openweathermap.org/data/2.5/forecast"' in notebook
    assert '"lat": binding["weather_proxy_latitude"]' in notebook
    assert '"lon": binding["weather_proxy_longitude"]' in notebook
    assert '"units": "metric"' in notebook
    assert '"cnt": count' in notebook
    assert "base_url" not in notebook


def test_fabric_forecast_preflights_before_secret_and_bounds_records():
    notebook = text("fabric/notebooks/01b_ingest_forecast_weather_to_bronze.py")
    fetch = notebook.split("def fetch_forecast_weather()", 1)[1]
    assert fetch.index("binding = _binding()") < fetch.index("api_key = _required_secret()")
    assert 'maximum=40' in fetch
    assert 'REQUEST_TIMEOUT_SECONDS' in fetch
    assert 'COORDINATE_TOLERANCE_DEGREES' in notebook


def test_fabric_forecast_raw_capture_is_validated_and_immutable():
    notebook = text("fabric/notebooks/01b_ingest_forecast_weather_to_bronze.py")
    assert 'RAW_CONTRACT_FILENAME = "openweather_forecast_raw_schema.json"' in notebook
    assert 'forecast_issue_basis": ISSUE_BASIS' in notebook
    assert 'raw_snapshot_id' in notebook
    assert 'hashlib.sha256' in notebook
    assert 'with path.open("x"' in notebook
    assert '"dataset": "forecast_weather"' in notebook
    assert '"provider": PROVIDER' in notebook


def test_fabric_forecast_silver_preserves_causal_availability_evidence():
    notebook = text("fabric/notebooks/02b_forecast_weather_to_silver.py")
    assert 'SILVER_FORECAST_WEATHER_TABLE = "silver_forecast_weather"' in notebook
    assert 'F.explode_outer("list")' in notebook
    assert '"forecast_issued_at_utc", retrieved_at' in notebook
    assert '"forecast_ingested_at_utc", retrieved_at' in notebook
    assert '"forecast_retrieved_at_utc", retrieved_at' in notebook
    assert 'F.col("forecast_valid_at_utc") <= F.col("forecast_ingested_at_utc")' in notebook
    assert 'F.regexp_replace(_metadata(raw, "weather_proxy_city")' in notebook


def test_fabric_forecast_silver_validates_and_deduplicates_before_delta_write():
    notebook = text("fabric/notebooks/02b_forecast_weather_to_silver.py")
    assert 'F.col("forecast_humidity_pct") > 100' in notebook
    assert 'F.col("forecast_provider") != F.lit(PROVIDER)' in notebook
    assert 'F.col("forecast_issue_basis") != F.lit(ISSUE_BASIS)' in notebook
    assert 'Window.partitionBy(' in notebook
    assert 'F.row_number().over(window)' in notebook
    assert '.partitionBy("forecast_valid_date_utc")' in notebook
    assert '.saveAsTable(SILVER_FORECAST_WEATHER_TABLE)' in notebook


def test_fabric_forecast_subflow_remains_optional_and_manual():
    guide = text("fabric/FORECAST_WEATHER.md")
    pipeline = text("fabric/pipelines/forecast_weather_evidence_pipeline.md")
    assert "optional/manual Fabric subflow" in guide
    assert "does not alter the existing six-stage" in guide
    assert "Run manually" in pipeline
    assert "does not run forecasting and cannot promote a model" in pipeline


def test_roadmap_advances_to_paired_fabric_model_execution():
    roadmap = text("ROADMAP.md")
    assert "G3 | Fabric bronze/silver ingestion parity" in roadmap
    assert "Implemented as an optional manual subflow" in roadmap
    assert "G4 | Fabric paired" in roadmap
    assert "| Next |" in roadmap

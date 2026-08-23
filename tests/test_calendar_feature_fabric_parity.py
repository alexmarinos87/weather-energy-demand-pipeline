import json
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker


ROOT = Path(__file__).resolve().parents[1]


def _text(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def test_fabric_gold_retains_utc_identity_and_derives_europe_london_time():
    notebook = _text("fabric/notebooks/03_build_gold_tables.py")
    assert 'spark.conf.set("spark.sql.session.timeZone", "UTC")' in notebook
    assert "from_utc_timestamp(" in notebook
    assert "'Europe/London'" in notebook
    assert "AS event_timestamp_local" in notebook
    assert "CAST(event_timestamp_local AS DATE) AS event_date_local" in notebook
    assert "'Europe/London' AS calendar_timezone" in notebook
    assert "'uk-local-calendar-v1' AS calendar_feature_contract_version" in notebook


def test_fabric_and_pandas_calendar_day_contract_is_iso_monday_one():
    notebook = _text("fabric/notebooks/03_build_gold_tables.py")
    assert "pmod(DAYOFWEEK(event_timestamp_utc) + 5, 7) + 1" in notebook
    assert "pmod(DAYOFWEEK(event_timestamp_local) + 5, 7) + 1" in notebook
    assert "IN (6, 7)" in notebook
    module = _text("forecasting/calendar_features.py")
    assert "utc.dt.dayofweek + 1" in module
    assert "local.dt.dayofweek + 1" in module


def test_fabric_gold_exposes_dst_offset_evidence_without_replacing_utc():
    notebook = _text("fabric/notebooks/03_build_gold_tables.py")
    required = (
        "event_timestamp_utc",
        "event_timestamp_local",
        "hour_of_day_utc",
        "day_of_week_utc",
        "is_weekend_utc",
        "hour_of_day_local",
        "day_of_week_local",
        "is_weekend_local",
        "local_utc_offset_minutes",
        "is_dst_local",
    )
    for field in required:
        assert field in notebook
    assert "unix_timestamp(event_timestamp_local)" in notebook
    assert "unix_timestamp(event_timestamp_utc)" in notebook
    assert "= 60 THEN 1" in notebook


def test_gold_feature_schema_requires_local_calendar_contract():
    schema = json.loads(
        _text("data-contracts/gold_features_schema.json")
    )
    assert schema["title"] == "weather_energy_features_v3"
    required = set(schema["required"])
    expected = {
        "event_timestamp_utc",
        "event_timestamp_local",
        "event_date_local",
        "hour_of_day_utc",
        "day_of_week_utc",
        "is_weekend_utc",
        "hour_of_day_local",
        "day_of_week_local",
        "is_weekend_local",
        "local_utc_offset_minutes",
        "is_dst_local",
        "calendar_timezone",
        "calendar_feature_contract_version",
    }
    assert expected.issubset(required)
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    row = {
        "event_timestamp_utc": "2026-07-01T12:00:00+00:00",
        "event_timestamp_local": "2026-07-01 13:00:00",
        "event_date_local": "2026-07-01",
        "source_area": "east_midlands",
        "resource_id": "resource-1",
        "city": "Nottingham",
        "temperature": 18.0,
        "humidity": 60.0,
        "demand_mw": 1500.0,
        "weather_age_minutes": 0.0,
        "hour_of_day_utc": 12,
        "day_of_week_utc": 3,
        "is_weekend_utc": 0,
        "hour_of_day_local": 13,
        "day_of_week_local": 3,
        "is_weekend_local": 0,
        "local_utc_offset_minutes": 60,
        "is_dst_local": 1,
        "calendar_timezone": "Europe/London",
        "calendar_feature_contract_version": "uk-local-calendar-v1",
    }
    assert list(validator.iter_errors(row)) == []


def test_calendar_document_preserves_utc_authority_and_local_opt_in():
    document = _text("CALENDAR_FEATURES.md")
    assert "event_timestamp_utc as canonical" in document
    assert "--calendar-mode uk-local" in document
    assert "time-horizon-uk-calendar-v1" in document
    assert "repeated local hour" in document
    assert "UTC continues to own ordering" in document

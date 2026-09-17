"""Recheck saved evidence through the real normalization boundary, offline."""
from copy import deepcopy
from datetime import datetime, timezone
import json

import pandas as pd
import pytest

from ingestion.forecast_weather import fetch_openweather_forecast as adapter
from test_openweather_forecast_ingestion import FakeResponse, _config, _payload


@pytest.fixture
def raw(monkeypatch):
    monkeypatch.setattr(adapter, "get_api_key", lambda config: "dummy-replay-test-key")
    evidence = adapter.fetch_openweather_forecast(
        _config(), request_get=lambda *a, **k: FakeResponse(_payload()),
        retrieved_at_utc=datetime(2026, 1, 1, 0, 5, tzinfo=timezone.utc),
    )
    def forbidden(*args, **kwargs):
        raise AssertionError("Replay must not request credentials, HTTP or current bindings")
    monkeypatch.setattr(adapter, "get_api_key", forbidden)
    monkeypatch.setattr(adapter.requests, "get", forbidden)
    monkeypatch.setattr(adapter, "validate_source_binding", forbidden)
    monkeypatch.setattr(adapter, "resolve_binding", forbidden)
    return evidence


@pytest.mark.parametrize("change,match", [
    ("count", "cnt="), ("duplicate", "duplicate"), ("reversed", "increasing"),
    ("country", "country"), ("latitude", "coordinates"), ("longitude", "coordinates"),
    ("requested-bound", "configured bound"), ("returned-count", "returned_record_count"),
])
def test_saved_provider_inconsistency_is_rejected(raw, change, match):
    if change == "count":
        raw["cnt"] = 2
    elif change == "duplicate":
        raw["list"][1]["dt"] = raw["list"][0]["dt"]
    elif change == "reversed":
        raw["list"].reverse()
    elif change == "country":
        raw["city"]["country"] = "US"
    elif change in {"latitude", "longitude"}:
        raw["city"]["coord"]["lat" if change == "latitude" else "lon"] += 2.0
    elif change == "requested-bound":
        raw["_pipeline_metadata"]["requested_record_count"] = 2
    else:
        raw["_pipeline_metadata"]["returned_record_count"] = 2
    before = deepcopy(raw)
    with pytest.raises(adapter.OpenWeatherForecastError, match=match):
        adapter.normalize_openweather_forecast(raw)
    assert raw == before


@pytest.mark.parametrize("invalid", [None, True, 2.5, 0, 41, "3"])
def test_explicit_malformed_requested_count_does_not_become_default(raw, invalid):
    raw["_pipeline_metadata"]["requested_record_count"] = invalid
    with pytest.raises(adapter.OpenWeatherForecastError, match="requested_record_count"):
        adapter.normalize_openweather_forecast(raw)


@pytest.mark.parametrize("invalid", [None, True, 2.5, 4, "3"])
def test_explicit_malformed_returned_count_is_rejected(raw, invalid):
    raw["_pipeline_metadata"]["returned_record_count"] = invalid
    with pytest.raises(adapter.OpenWeatherForecastError, match="returned_record_count"):
        adapter.normalize_openweather_forecast(raw)


@pytest.mark.parametrize("invalid", [None, True, float("nan"), float("inf"), 91.0])
def test_retained_proxy_coordinates_must_be_finite_and_in_range(raw, invalid):
    raw["_pipeline_metadata"]["weather_proxy_latitude"] = invalid
    with pytest.raises(adapter.OpenWeatherForecastError, match="weather_proxy_latitude"):
        adapter.normalize_openweather_forecast(raw)


@pytest.mark.parametrize("invalid", [None, "", "Nottingham", ",GB", []])
def test_retained_proxy_city_requires_a_name_and_country(raw, invalid):
    raw["_pipeline_metadata"]["weather_proxy_city"] = invalid
    with pytest.raises(adapter.OpenWeatherForecastError, match="weather_proxy_city"):
        adapter.normalize_openweather_forecast(raw)


def test_valid_saved_json_replays_identically_without_mutation(raw, tmp_path):
    expected = adapter.normalize_openweather_forecast(raw)
    path = tmp_path / "retained.json"
    path.write_text(json.dumps(raw, sort_keys=True), encoding="utf-8")
    before = path.read_bytes()
    reloaded = json.loads(before)
    assert adapter.normalize_openweather_forecast(reloaded) == expected
    assert reloaded == raw
    assert path.read_bytes() == before
    assert "dummy-replay-test-key" not in str(expected)


def test_legacy_missing_request_return_counts_uses_provider_cap(raw):
    expected = adapter.normalize_openweather_forecast(raw)
    raw["_pipeline_metadata"].pop("requested_record_count")
    raw["_pipeline_metadata"].pop("returned_record_count")
    assert adapter.normalize_openweather_forecast(raw) == expected


def test_replay_uses_retained_proxy_not_todays_catalogue(raw):
    raw["_pipeline_metadata"].update(
        weather_proxy_city="Historical Proxy,GB",
        weather_proxy_latitude=51.5, weather_proxy_longitude=0.12,
    )
    raw["city"]["coord"] = {"lat": 51.5, "lon": 0.12}
    result = adapter.normalize_openweather_forecast(raw)
    assert {record["city"] for record in result} == {"Historical Proxy"}
    assert {record["forecast_latitude"] for record in result} == {51.5}


def test_future_filter_preserves_retained_issue_time(raw):
    raw["_pipeline_metadata"]["retrieved_at_utc"] = "2026-01-01T03:00:00+00:00"
    result = adapter.normalize_openweather_forecast(raw)
    assert len(result) == 2
    assert all(pd.Timestamp(row["forecast_valid_at_utc"]) > pd.Timestamp(row["forecast_issued_at_utc"]) for row in result)
    assert {row["forecast_issued_at_utc"] for row in result} == {"2026-01-01T03:00:00+00:00"}


def test_expired_slots_still_participate_in_duplicate_validation(raw):
    raw["list"][1]["dt"] = raw["list"][0]["dt"]
    raw["_pipeline_metadata"]["retrieved_at_utc"] = "2026-01-01T04:00:00+00:00"
    with pytest.raises(adapter.OpenWeatherForecastError, match="duplicate"):
        adapter.normalize_openweather_forecast(raw)


def test_main_stops_before_both_savers_for_invalid_replay(raw, monkeypatch):
    raw["list"][1]["dt"] = raw["list"][0]["dt"]
    saved = []
    monkeypatch.setattr(adapter, "load_config", _config)
    monkeypatch.setattr(adapter, "fetch_openweather_forecast", lambda config: raw)
    monkeypatch.setattr(adapter, "save_raw_snapshot", lambda *a, **k: saved.append("raw"))
    monkeypatch.setattr(adapter, "save_normalized_forecast", lambda *a, **k: saved.append("normalized"))
    with pytest.raises(adapter.OpenWeatherForecastError, match="duplicate"):
        adapter.main()
    assert not saved

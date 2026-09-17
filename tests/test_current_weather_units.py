"""Verify current-weather request units and the resulting silver unit labels."""
from copy import deepcopy
import json

import pytest
import requests

from ingestion.common.contract_validator import ContractValidationError
from ingestion.weather import fetch_weather as adapter
from transformations.silver import clean_weather


class Response:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


def config():
    return {"source_area": "east_midlands", "api": {
        "base_url": "https://api.openweathermap.org/data/2.5",
        "city": "Nottingham,GB", "units": "metric",
        "api_key_env": "OPENWEATHER_API_KEY",
    }}


def payload():
    return {
        "id": 2641170, "dt": 1767225600, "name": "Nottingham",
        "coord": {"lat": 52.9548, "lon": -1.1581}, "sys": {"country": "GB"},
        "main": {"temp": 8.5, "feels_like": 6.0, "humidity": 80},
        "weather": [{"main": "Clouds", "description": "clouds"}],
        "wind": {"speed": 4.0}, "clouds": {"all": 70}, "cod": 200,
    }


@pytest.fixture
def source(monkeypatch):
    events = []
    observed = payload()
    captured = []

    def key(settings):
        events.append("credentials")
        return "unit-test-not-a-real-key"

    def get(url, *, params, timeout):
        events.append("http")
        captured.append((url, deepcopy(params), timeout))
        return Response(observed)

    monkeypatch.setattr(adapter, "get_api_key", key)
    monkeypatch.setattr(adapter.requests, "get", get)
    return events, captured, observed


@pytest.mark.parametrize("units", ["standard", "imperial", "", " ", None, True, 1, [], {}])
def test_nonmetric_units_fail_before_credentials_or_http(source, units):
    events, _, _ = source
    settings = config()
    settings["api"]["units"] = units
    before = deepcopy(settings)
    with pytest.raises(ValueError, match="api.units"):
        adapter.fetch_weather(settings)
    assert events == []
    assert settings == before


def test_omitted_units_fail_before_credentials_or_http(source):
    events, _, _ = source
    settings = config()
    del settings["api"]["units"]
    with pytest.raises(ValueError, match="api.units"):
        adapter.fetch_weather(settings)
    assert events == []


@pytest.mark.parametrize("units", ["metric", "METRIC", "  Metric  "])
def test_metric_request_preserves_celsius_and_mps_through_real_silver(source, units, tmp_path):
    events, captured, observed = source
    settings = config()
    settings["api"]["units"] = units
    settings_before, payload_before = deepcopy(settings), deepcopy(observed)
    raw = adapter.fetch_weather(settings)
    assert events == ["credentials", "http"]
    assert captured == [(
        "https://api.openweathermap.org/data/2.5/weather",
        {"q": "Nottingham,GB", "appid": "unit-test-not-a-real-key", "units": "metric"},
        30,
    )]
    path = tmp_path / "weather_20260101_010000.json"
    path.write_text(json.dumps(raw), encoding="utf-8")
    original_bytes = path.read_bytes()
    silver = clean_weather.transform_weather_files(tmp_path)
    assert len(silver) == 1
    assert silver.loc[0, "temperature_c"] == 8.5
    assert silver.loc[0, "feels_like_c"] == 6.0
    assert silver.loc[0, "wind_speed_mps"] == 4.0
    assert silver.loc[0, "source_area"] == "east_midlands"
    assert raw["_pipeline_metadata"]["weather_proxy_city"] == "Nottingham,GB"
    assert "unit-test-not-a-real-key" not in json.dumps(raw)
    assert settings == settings_before and observed == payload_before
    assert path.read_bytes() == original_bytes


def test_source_binding_still_precedes_credentials(source):
    events, _, _ = source
    settings = config()
    settings["api"]["city"] = "London,GB"
    with pytest.raises(ValueError):
        adapter.fetch_weather(settings)
    assert events == []


def test_metric_validation_does_not_bypass_provider_contract(source):
    events, _, observed = source
    del observed["main"]["temp"]
    with pytest.raises(ContractValidationError):
        adapter.fetch_weather(config())
    assert events == ["credentials", "http"]


def test_transport_failure_is_not_retried_or_replaced(source, monkeypatch):
    events, _, _ = source

    def fail(*args, **kwargs):
        events.append("http")
        raise requests.Timeout("injected timeout")

    monkeypatch.setattr(adapter.requests, "get", fail)
    with pytest.raises(requests.Timeout, match="injected timeout"):
        adapter.fetch_weather(config())
    assert events == ["credentials", "http"]

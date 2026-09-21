"""Reject non-finite provider measurements at fetch and offline replay boundaries."""
from copy import deepcopy
from datetime import datetime, timezone
import json
import math

import pytest

from ingestion.common.contract_validator import ContractValidationError
from ingestion.forecast_weather import fetch_openweather_forecast as adapter
from test_openweather_forecast_ingestion import FakeResponse, _config, _payload

RETRIEVED = datetime(2026, 1, 1, 0, 5, tzinfo=timezone.utc)


@pytest.fixture(params=["fetch", "replay"])
def boundary(request, monkeypatch):
    monkeypatch.setattr(adapter, "get_api_key", lambda config: "dummy-finite-test-key")
    def fetch(payload):
        return adapter.fetch_openweather_forecast(
            _config(), request_get=lambda *a, **k: FakeResponse(payload),
            retrieved_at_utc=RETRIEVED,
        )
    if request.param == "fetch":
        return _payload(), fetch
    raw = fetch(_payload())
    def forbidden(*args, **kwargs):
        raise AssertionError("Replay must stay offline")
    monkeypatch.setattr(adapter, "get_api_key", forbidden)
    monkeypatch.setattr(adapter, "resolve_binding", forbidden)
    monkeypatch.setattr(adapter.requests, "get", forbidden)
    return raw, adapter.normalize_openweather_forecast


def set_measurement(payload, field, value):
    if field in {"temp", "humidity"}:
        payload["list"][1]["main"][field] = value
    else:
        payload["city"]["coord"][field] = value


@pytest.mark.parametrize("field", ["temp", "humidity", "lat", "lon"])
@pytest.mark.parametrize("invalid", [float("nan"), float("inf"), float("-inf")], ids=["nan", "positive-infinity", "negative-infinity"])
def test_nonfinite_measurements_are_rejected_without_mutation(boundary, field, invalid):
    payload, invoke = boundary
    set_measurement(payload, field, invalid)
    before = json.dumps(payload, sort_keys=True)
    # Existing schema range checks may reject infinities before the finite guard.
    with pytest.raises((ContractValidationError, adapter.OpenWeatherForecastError)):
        invoke(payload)
    assert json.dumps(payload, sort_keys=True) == before


def test_json_decoded_overflow_is_not_a_valid_temperature(boundary):
    payload, invoke = boundary
    set_measurement(payload, "temp", json.loads("1e999"))
    with pytest.raises(adapter.OpenWeatherForecastError, match="finite.*temp|temp.*finite"):
        invoke(payload)


def test_unrepresentable_integer_temperature_is_rejected_contextually(boundary):
    payload, invoke = boundary
    set_measurement(payload, "temp", 10 ** 400)
    with pytest.raises(adapter.OpenWeatherForecastError, match="finite.*temp|temp.*finite"):
        invoke(payload)


@pytest.mark.parametrize("temperature", [-273.15, -0.0, 1e100])
def test_finite_values_are_not_clamped_or_replaced(boundary, temperature):
    payload, invoke = boundary
    set_measurement(payload, "temp", temperature)
    before = deepcopy(payload)
    result = invoke(payload)
    value = (result["list"][1]["main"]["temp"] if isinstance(result, dict)
             else result[1]["forecast_temperature_c"])
    assert value == temperature
    assert math.copysign(1.0, value) == math.copysign(1.0, temperature)
    assert payload == before


def test_expired_slot_cannot_conceal_nonfinite_temperature(monkeypatch):
    monkeypatch.setattr(adapter, "get_api_key", lambda config: "dummy-key")
    raw = adapter.fetch_openweather_forecast(
        _config(), request_get=lambda *a, **k: FakeResponse(_payload()),
        retrieved_at_utc=RETRIEVED,
    )
    raw["list"][0]["dt"] = 1767225600  # Before retained retrieval, still increasing.
    raw["list"][0]["main"]["temp"] = float("nan")
    with pytest.raises(adapter.OpenWeatherForecastError, match="finite.*temp|temp.*finite"):
        adapter.normalize_openweather_forecast(raw)


def test_main_stops_before_savers_for_nonfinite_replay(monkeypatch):
    monkeypatch.setattr(adapter, "get_api_key", lambda config: "dummy-key")
    raw = adapter.fetch_openweather_forecast(
        _config(), request_get=lambda *a, **k: FakeResponse(_payload()),
        retrieved_at_utc=RETRIEVED,
    )
    raw["list"][0]["main"]["temp"] = float("nan")
    writes = []
    monkeypatch.setattr(adapter, "load_config", _config)
    monkeypatch.setattr(adapter, "fetch_openweather_forecast", lambda config: raw)
    monkeypatch.setattr(adapter, "save_raw_snapshot", lambda *a, **k: writes.append("raw"))
    monkeypatch.setattr(adapter, "save_normalized_forecast", lambda *a, **k: writes.append("normalized"))
    with pytest.raises(adapter.OpenWeatherForecastError, match="finite.*temp|temp.*finite"):
        adapter.main()
    assert not writes

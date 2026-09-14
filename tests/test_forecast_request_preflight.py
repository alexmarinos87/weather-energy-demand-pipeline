"""Exercise forecast preflight without reading a key or contacting a provider."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone, tzinfo
import json

import pandas as pd
import pytest

from ingestion.forecast_weather import fetch_openweather_forecast as adapter
from test_openweather_forecast_ingestion import FakeResponse, _config, _payload


class UndefinedOffset(tzinfo):
    def utcoffset(self, dt):
        return None

    def dst(self, dt):
        return None


@pytest.fixture
def tracked_request(monkeypatch):
    events = []
    payload = _payload(count=1)

    def key(config):
        events.append("credentials")
        return "unit-test-not-a-real-key"

    def get(url, **kwargs):
        events.append("http")
        return FakeResponse(payload)

    monkeypatch.setattr(adapter, "get_api_key", key)
    return events, get


@pytest.mark.parametrize("field", ["timeout_seconds", "max_forecast_records"])
@pytest.mark.parametrize("value", [1.5, 2.0, float("inf"), float("nan"), True, None, "1.5", 0, -1])
def test_invalid_bounds_fail_before_credentials_or_http(tracked_request, field, value):
    events, get = tracked_request
    config = _config()
    config["api"][field] = value
    with pytest.raises(ValueError, match=f"api.{field}"):
        adapter.fetch_openweather_forecast(
            config, request_get=get,
            retrieved_at_utc=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )
    assert events == []


@pytest.mark.parametrize("field", ["timeout_seconds", "max_forecast_records"])
@pytest.mark.parametrize("value", [1, 3, "3", " +003 "])
def test_integer_settings_remain_supported(tracked_request, field, value):
    events, get = tracked_request
    config = _config()
    config["api"][field] = value
    result = adapter.fetch_openweather_forecast(
        config, request_get=get,
        retrieved_at_utc=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    assert events == ["credentials", "http"]
    assert result["cnt"] == 1


@pytest.mark.parametrize("value", [41, "41"])
def test_record_cap_is_enforced_before_credentials(tracked_request, value):
    events, get = tracked_request
    config = _config()
    config["api"]["max_forecast_records"] = value
    with pytest.raises(ValueError, match="no greater than 40"):
        adapter.fetch_openweather_forecast(config, request_get=get)
    assert events == []


@pytest.mark.parametrize("timestamp", [
    datetime(2026, 1, 1), "2026-01-01T00:00:00Z", "", 0, False, pd.NaT,
    datetime(2026, 1, 1, tzinfo=UndefinedOffset()),
], ids=["naive", "string", "empty", "zero", "false", "nat", "undefined-offset"])
def test_supplied_timestamp_is_validated_before_credentials_or_http(tracked_request, timestamp):
    events, get = tracked_request
    with pytest.raises(ValueError, match="retrieved_at_utc"):
        adapter.fetch_openweather_forecast(_config(), request_get=get, retrieved_at_utc=timestamp)
    assert events == []


def test_aware_offset_is_normalized_without_changing_input_or_request(monkeypatch):
    supplied = datetime(2026, 1, 1, 2, 5, tzinfo=timezone(timedelta(hours=2)))
    config, payload = _config(), _payload()
    original_config, original_payload = deepcopy(config), deepcopy(payload)
    captured = []
    monkeypatch.setattr(adapter, "get_api_key", lambda config: "unit-test-not-a-real-key")

    def get(url, **kwargs):
        captured.append((url, kwargs))
        return FakeResponse(payload)

    raw = adapter.fetch_openweather_forecast(config, request_get=get, retrieved_at_utc=supplied)
    normalized = adapter.normalize_openweather_forecast(raw)
    assert raw["_pipeline_metadata"]["retrieved_at_utc"] == "2026-01-01T00:05:00+00:00"
    assert normalized[0]["forecast_ingested_at_utc"] == "2026-01-01T00:05:00+00:00"
    assert len(normalized) == 3
    assert config == original_config and payload == original_payload
    assert supplied.utcoffset() == timedelta(hours=2)
    assert len(captured) == 1
    assert captured[0][1]["timeout"] == 20
    assert captured[0][1]["params"]["cnt"] == 3
    assert "unit-test-not-a-real-key" not in json.dumps(raw)


def test_default_retrieval_time_is_still_captured_after_response(monkeypatch):
    events = []

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            events.append("clock")
            return cls(2026, 1, 1, 0, 5, tzinfo=timezone.utc)

    def get(url, **kwargs):
        events.append("http")
        return FakeResponse(_payload())

    monkeypatch.setattr(adapter, "datetime", Clock)
    monkeypatch.setattr(adapter, "get_api_key", lambda config: "unit-test-not-a-real-key")
    raw = adapter.fetch_openweather_forecast(_config(), request_get=get)
    assert events == ["http", "clock"]
    assert raw["_pipeline_metadata"]["retrieved_at_utc"] == "2026-01-01T00:05:00+00:00"

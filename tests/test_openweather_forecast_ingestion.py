from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

from ingestion.common.contract_validator import ContractValidationError
from ingestion.common.source_area import (
    attach_pipeline_metadata,
    load_source_area_contract,
    resolve_source_area,
)
from ingestion.forecast_weather import fetch_openweather_forecast as adapter


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


def _payload(*, count: int = 3, latitude: float = 52.9548, longitude: float = -1.1581):
    start = 1_767_225_600  # 2026-01-01T00:00:00Z
    records = [
        {
            "dt": start + (index + 1) * 10_800,
            "main": {"temp": 7.0 + index, "humidity": 60 + index},
        }
        for index in range(count)
    ]
    return {
        "cod": "200",
        "message": 0,
        "cnt": len(records),
        "list": records,
        "city": {
            "id": 2641170,
            "name": "Nottingham",
            "country": "GB",
            "timezone": 0,
            "coord": {"lat": latitude, "lon": longitude},
        },
    }


def _config():
    return {
        "source_area": "east_midlands",
        "api": {
            "base_url": "https://api.openweathermap.org/data/2.5",
            "units": "metric",
            "api_key_env": "OPENWEATHER_API_KEY",
            "timeout_seconds": 20,
            "max_forecast_records": 3,
        },
    }


def test_source_area_contract_includes_unique_proxy_coordinates():
    contract = load_source_area_contract()
    coordinates = {
        (
            area["weather_proxy_latitude"],
            area["weather_proxy_longitude"],
        )
        for area in contract["areas"].values()
    }

    assert len(coordinates) == 4
    binding = resolve_source_area("east_midlands")
    assert binding["weather_proxy_latitude"] == pytest.approx(52.9548)
    assert binding["weather_proxy_longitude"] == pytest.approx(-1.1581)


def test_forecast_weather_metadata_is_supported_without_mutation():
    payload = _payload(count=1)
    binding = resolve_source_area("east_midlands")

    enriched = attach_pipeline_metadata(
        payload,
        dataset_name="forecast_weather",
        binding=binding,
    )

    assert "_pipeline_metadata" not in payload
    assert enriched["_pipeline_metadata"]["dataset"] == "forecast_weather"
    assert enriched["_pipeline_metadata"]["weather_proxy_latitude"] == pytest.approx(
        52.9548
    )


def test_endpoint_rejects_non_openweather_host_before_credentials(monkeypatch):
    config = _config()
    config["api"]["base_url"] = "https://example.com/data/2.5"
    monkeypatch.delenv("OPENWEATHER_API_KEY", raising=False)

    with pytest.raises(ValueError, match="exactly the HTTPS OpenWeather"):
        adapter.fetch_openweather_forecast(config, request_get=lambda *a, **k: None)


def test_fetch_uses_contract_coordinates_and_never_returns_api_key(monkeypatch):
    monkeypatch.setenv("OPENWEATHER_API_KEY", "secret-key")
    captured = {}

    def request_get(url, *, params, timeout):
        captured.update(url=url, params=params, timeout=timeout)
        return FakeResponse(_payload())

    retrieved_at = datetime(2026, 1, 1, 0, 5, tzinfo=timezone.utc)
    raw = adapter.fetch_openweather_forecast(
        _config(), request_get=request_get, retrieved_at_utc=retrieved_at
    )

    assert captured["url"] == "https://api.openweathermap.org/data/2.5/forecast"
    assert captured["params"]["lat"] == pytest.approx(52.9548)
    assert captured["params"]["lon"] == pytest.approx(-1.1581)
    assert captured["params"]["units"] == "metric"
    assert captured["params"]["cnt"] == 3
    assert captured["timeout"] == 20
    assert raw["_pipeline_metadata"]["forecast_issue_basis"] == (
        "retrieval_time_surrogate"
    )
    assert raw["_pipeline_metadata"]["retrieved_at_utc"] == (
        "2026-01-01T00:05:00+00:00"
    )
    assert "secret-key" not in json.dumps(raw)
    assert len(raw["_pipeline_metadata"]["raw_snapshot_id"]) == 64


def test_fetch_rejects_count_and_order_inconsistency(monkeypatch):
    monkeypatch.setenv("OPENWEATHER_API_KEY", "secret-key")
    mismatched = _payload()
    mismatched["cnt"] = 2

    with pytest.raises(adapter.OpenWeatherForecastError, match="cnt=2"):
        adapter.fetch_openweather_forecast(
            _config(),
            request_get=lambda *a, **k: FakeResponse(mismatched),
            retrieved_at_utc=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )

    unordered = _payload()
    unordered["list"][1]["dt"] = unordered["list"][0]["dt"]
    with pytest.raises(adapter.OpenWeatherForecastError, match="duplicate"):
        adapter.fetch_openweather_forecast(
            _config(),
            request_get=lambda *a, **k: FakeResponse(unordered),
            retrieved_at_utc=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )


def test_fetch_rejects_wrong_country_and_coordinates(monkeypatch):
    monkeypatch.setenv("OPENWEATHER_API_KEY", "secret-key")
    wrong_country = _payload()
    wrong_country["city"]["country"] = "US"
    with pytest.raises(adapter.OpenWeatherForecastError, match="country"):
        adapter.fetch_openweather_forecast(
            _config(),
            request_get=lambda *a, **k: FakeResponse(wrong_country),
            retrieved_at_utc=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )

    with pytest.raises(adapter.OpenWeatherForecastError, match="coordinates"):
        adapter.fetch_openweather_forecast(
            _config(),
            request_get=lambda *a, **k: FakeResponse(
                _payload(latitude=40.0, longitude=-1.1581)
            ),
            retrieved_at_utc=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )


def test_normalization_uses_retrieval_as_conservative_issue_and_availability(monkeypatch):
    monkeypatch.setenv("OPENWEATHER_API_KEY", "secret-key")
    retrieved_at = datetime(2026, 1, 1, 0, 5, tzinfo=timezone.utc)
    raw = adapter.fetch_openweather_forecast(
        _config(),
        request_get=lambda *a, **k: FakeResponse(_payload()),
        retrieved_at_utc=retrieved_at,
    )

    normalized = adapter.normalize_openweather_forecast(raw)

    assert len(normalized) == 3
    first = normalized[0]
    assert first["source_area"] == "east_midlands"
    assert first["city"] == "Nottingham"
    assert first["forecast_issued_at_utc"] == "2026-01-01T00:05:00+00:00"
    assert first["forecast_ingested_at_utc"] == "2026-01-01T00:05:00+00:00"
    assert first["forecast_issue_basis"] == "retrieval_time_surrogate"
    assert first["forecast_provider"] == "openweather"
    assert first["forecast_model"] == "5-day-3-hour"
    assert first["forecast_temperature_c"] == 7.0
    assert first["forecast_humidity_pct"] == 60.0
    assert pd.Timestamp(first["forecast_valid_at_utc"]) > pd.Timestamp(
        first["forecast_ingested_at_utc"]
    )


def test_normalization_discards_nonfuture_slots_and_requires_a_future_slot():
    binding = resolve_source_area("east_midlands")
    raw = attach_pipeline_metadata(
        _payload(count=1),
        dataset_name="forecast_weather",
        binding=binding,
    )
    raw["_pipeline_metadata"].update(
        {
            "provider": "openweather",
            "product": "5_day_3_hour",
            "forecast_issue_basis": "retrieval_time_surrogate",
            "retrieved_at_utc": "2026-01-01T04:00:00+00:00",
            "raw_snapshot_id": "a" * 64,
        }
    )

    with pytest.raises(adapter.OpenWeatherForecastError, match="no forecast slots"):
        adapter.normalize_openweather_forecast(raw)


def test_raw_snapshot_is_immutable_and_partitioned(monkeypatch, tmp_path):
    monkeypatch.setenv("OPENWEATHER_API_KEY", "secret-key")
    raw = adapter.fetch_openweather_forecast(
        _config(),
        request_get=lambda *a, **k: FakeResponse(_payload()),
        retrieved_at_utc=datetime(2026, 1, 1, 0, 5, tzinfo=timezone.utc),
    )

    path = adapter.save_raw_snapshot(raw, output_root=tmp_path / "raw")

    assert path.parent.name == "ingestion_date=2026-01-01"
    assert path.name.startswith("openweather_forecast_20260101_000500_")
    assert json.loads(path.read_text(encoding="utf-8"))["cnt"] == 3
    with pytest.raises(FileExistsError):
        adapter.save_raw_snapshot(raw, output_root=tmp_path / "raw")


def test_normalized_parquet_is_partitioned_and_round_trips(monkeypatch, tmp_path):
    monkeypatch.setenv("OPENWEATHER_API_KEY", "secret-key")
    raw = adapter.fetch_openweather_forecast(
        _config(),
        request_get=lambda *a, **k: FakeResponse(_payload()),
        retrieved_at_utc=datetime(2026, 1, 1, 0, 5, tzinfo=timezone.utc),
    )
    normalized = adapter.normalize_openweather_forecast(raw)

    path = adapter.save_normalized_forecast(
        normalized, output_root=tmp_path / "normalized"
    )
    saved = pd.read_parquet(path)

    assert path.parent.name == "ingestion_date=2026-01-01"
    assert len(saved) == 3
    assert set(saved["forecast_provider"]) == {"openweather"}
    assert str(saved["forecast_valid_at_utc"].dtype) == "datetime64[ns, UTC]"
    with pytest.raises(FileExistsError):
        adapter.save_normalized_forecast(
            normalized, output_root=tmp_path / "normalized"
        )


def test_main_does_not_write_when_normalization_fails(monkeypatch):
    writes = {"raw": False, "normalized": False}
    monkeypatch.setattr(adapter, "load_config", lambda: _config())
    monkeypatch.setattr(adapter, "fetch_openweather_forecast", lambda config: {})
    monkeypatch.setattr(
        adapter,
        "normalize_openweather_forecast",
        lambda payload: (_ for _ in ()).throw(ContractValidationError("invalid")),
    )
    monkeypatch.setattr(
        adapter,
        "save_raw_snapshot",
        lambda *a, **k: writes.update(raw=True),
    )
    monkeypatch.setattr(
        adapter,
        "save_normalized_forecast",
        lambda *a, **k: writes.update(normalized=True),
    )

    with pytest.raises(ContractValidationError):
        adapter.main()

    assert writes == {"raw": False, "normalized": False}

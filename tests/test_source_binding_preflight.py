import pytest

from ingestion.common.source_area import SourceAreaError
from ingestion.energy import fetch_energy
from ingestion.weather import fetch_weather


def test_weather_rejects_wrong_proxy_before_secret_or_network(monkeypatch):
    monkeypatch.delenv("OPENWEATHER_API_KEY", raising=False)
    called = {"network": False}
    monkeypatch.setattr(
        fetch_weather.requests,
        "get",
        lambda *args, **kwargs: called.update(network=True),
    )

    config = {
        "source_area": "east_midlands",
        "api": {
            "base_url": "https://example.test",
            "city": "London,GB",
            "units": "metric",
            "api_key_env": "OPENWEATHER_API_KEY",
        },
    }

    with pytest.raises(SourceAreaError, match="weather proxy"):
        fetch_weather.fetch_weather(config)
    assert called["network"] is False


def test_energy_rejects_wrong_resource_before_secret_or_network(monkeypatch):
    monkeypatch.delenv("NATIONAL_GRID_API_TOKEN", raising=False)
    called = {"network": False}
    monkeypatch.setattr(
        fetch_energy.requests,
        "get",
        lambda *args, **kwargs: called.update(network=True),
    )

    config = {
        "source_area": "east_midlands",
        "api": {
            "base_url": "https://example.test",
            "endpoint": "datastore_search",
            "api_key_env": "NATIONAL_GRID_API_TOKEN",
            "api_key_header": "Authorization",
            "params": {
                "resource_id": "38b81427-a2df-42f2-befa-4d6fe9b54c98"
            },
        },
    }

    with pytest.raises(SourceAreaError, match="requires NGED resource"):
        fetch_energy.fetch_energy(config)
    assert called["network"] is False

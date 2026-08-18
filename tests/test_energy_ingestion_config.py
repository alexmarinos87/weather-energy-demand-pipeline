import pytest

from ingestion.energy import fetch_energy


def test_load_config_raises_when_config_missing(tmp_path):
    missing_config_path = tmp_path / "config.yaml"

    with pytest.raises(FileNotFoundError, match="config.example.yaml"):
        fetch_energy.load_config(missing_config_path)


def test_build_headers_requires_both_api_header_fields():
    with pytest.raises(ValueError, match="set together"):
        fetch_energy.build_headers({"api_key_env": "NATIONAL_GRID_API_TOKEN"})


def test_build_headers_requires_api_key_value_when_configured(monkeypatch):
    monkeypatch.delenv("NATIONAL_GRID_API_TOKEN", raising=False)

    with pytest.raises(EnvironmentError, match="NATIONAL_GRID_API_TOKEN"):
        fetch_energy.build_headers(
            {
                "api_key_env": "NATIONAL_GRID_API_TOKEN",
                "api_key_header": "Authorization",
            }
        )


def test_build_headers_allows_no_auth_config():
    assert fetch_energy.build_headers({}) == {}


def test_fetch_energy_supports_legacy_limit_as_page_size(monkeypatch):
    captured = {}

    def fake_fetch_ckan_resource(**kwargs):
        captured.update(kwargs)
        return {"success": True}

    monkeypatch.setattr(fetch_energy, "fetch_ckan_resource", fake_fetch_ckan_resource)
    monkeypatch.setattr(fetch_energy.requests, "get", object())

    config = {
        "api": {
            "base_url": "https://example.test/api/3/action",
            "endpoint": "datastore_search",
            "params": {"resource_id": "resource-123", "limit": 250},
            "max_records": 5000,
        }
    }

    assert fetch_energy.fetch_energy(config) == {"success": True}
    assert captured["page_size"] == 250
    assert captured["max_records"] == 5000
    assert captured["params"] == {"resource_id": "resource-123"}

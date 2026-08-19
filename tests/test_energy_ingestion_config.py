from pathlib import Path

import pytest

from ingestion.energy import fetch_energy


def test_load_config_raises_when_config_missing(tmp_path):
    with pytest.raises(FileNotFoundError, match="config.example.yaml"):
        fetch_energy.load_config(tmp_path / "config.yaml")


def test_build_headers_requires_both_api_header_fields():
    with pytest.raises(ValueError, match="set together"):
        fetch_energy.build_headers({"api_key_env": "NATIONAL_GRID_API_TOKEN"})


def test_fetch_energy_supports_legacy_limit_as_page_size(monkeypatch):
    captured = {}

    def fake_fetch_ckan_resource(**kwargs):
        captured.update(kwargs)
        return {
            "help": "https://example.test",
            "success": True,
            "result": {
                "resource_id": "92d3431c-15d7-4aa6-ad34-2335596a026c",
                "records": [],
                "limit": 250,
                "total": 0,
            },
        }

    monkeypatch.setattr(fetch_energy, "fetch_ckan_resource", fake_fetch_ckan_resource)

    config = {
        "source_area": "east_midlands",
        "api": {
            "base_url": "https://example.test/api/3/action",
            "endpoint": "datastore_search",
            "params": {
                "resource_id": "92d3431c-15d7-4aa6-ad34-2335596a026c",
                "limit": 250,
            },
            "max_records": 5000,
        },
    }

    result = fetch_energy.fetch_energy(config)

    assert captured["page_size"] == 250
    assert captured["max_records"] == 5000
    assert result["_pipeline_metadata"]["source_area"] == "east_midlands"

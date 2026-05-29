import json
import runpy
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK_PATH = PROJECT_ROOT / "fabric" / "notebooks" / "01_ingest_api_to_bronze.py"


def _load_notebook_namespace() -> dict:
    return runpy.run_path(str(NOTEBOOK_PATH), run_name="fabric_ingest_notebook")


def _valid_weather_payload() -> dict:
    return {
        "dt": 1738800000,
        "name": "London",
        "cod": 200,
        "main": {
            "temp": 11.2,
            "feels_like": 9.8,
            "humidity": 82,
        },
        "weather": [{"main": "Clouds", "description": "broken clouds"}],
        "wind": {"speed": 4.1},
        "clouds": {"all": 70},
    }


def _valid_energy_payload() -> dict:
    return {
        "help": "https://connecteddata.nationalgrid.co.uk/",
        "success": True,
        "result": {
            "resource_id": "92d3431c-15d7-4aa6-ad34-2335596a026c",
            "records": [{"_id": 1, "SETTLEMENT_DATE": "2026-02-01"}],
            "limit": 1000,
            "total": 1,
        },
    }


def test_fabric_ingestion_validates_weather_with_json_contract():
    namespace = _load_notebook_namespace()
    payload = _valid_weather_payload()
    payload["cod"] = "200"

    with pytest.raises(ValueError, match="weather_schema.json"):
        namespace["_validate_payload"](payload, "weather")


def test_fabric_ingestion_validates_energy_with_json_contract():
    namespace = _load_notebook_namespace()
    payload = _valid_energy_payload()
    payload["result"].pop("limit")

    with pytest.raises(ValueError, match="energy_schema.json"):
        namespace["_validate_payload"](payload, "energy")


def test_fabric_ingestion_can_load_contracts_from_parameter(tmp_path):
    namespace = _load_notebook_namespace()
    contracts_root = tmp_path / "contracts"
    contracts_root.mkdir()
    contract_path = contracts_root / "weather_schema.json"
    contract_path.write_text(json.dumps({"type": "object"}))
    namespace["_resolve_contract_path"].__globals__["CONTRACTS_ROOT"] = str(contracts_root)

    assert namespace["_resolve_contract_path"]("weather") == contract_path

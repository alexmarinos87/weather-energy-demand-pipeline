from pathlib import Path

import pytest

from ingestion.common.contract_validator import ContractValidationError, validate_payload

PROJECT_ROOT = Path(__file__).resolve().parents[1]
WEATHER_CONTRACT = PROJECT_ROOT / "data-contracts" / "weather_schema.json"
ENERGY_CONTRACT = PROJECT_ROOT / "data-contracts" / "energy_schema.json"


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


def test_weather_contract_accepts_valid_payload():
    validate_payload(_valid_weather_payload(), WEATHER_CONTRACT, "weather")


def test_weather_contract_rejects_missing_timestamp():
    payload = _valid_weather_payload()
    payload.pop("dt")

    with pytest.raises(ContractValidationError, match="dt"):
        validate_payload(payload, WEATHER_CONTRACT, "weather")


def test_energy_contract_accepts_valid_payload():
    validate_payload(_valid_energy_payload(), ENERGY_CONTRACT, "energy")


def test_energy_contract_rejects_invalid_success_flag():
    payload = _valid_energy_payload()
    payload["success"] = False

    with pytest.raises(ContractValidationError, match="success"):
        validate_payload(payload, ENERGY_CONTRACT, "energy")

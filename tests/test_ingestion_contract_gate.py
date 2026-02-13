import pytest

from ingestion.common.contract_validator import ContractValidationError
from ingestion.energy import fetch_energy
from ingestion.weather import fetch_weather


def test_weather_ingestion_blocks_save_on_contract_failure(monkeypatch):
    saved = {"called": False}

    monkeypatch.setattr(fetch_weather, "load_config", lambda: {"api": {}})
    monkeypatch.setattr(fetch_weather, "fetch_weather", lambda config: {"invalid": True})
    monkeypatch.setattr(fetch_weather, "save_raw_data", lambda data: saved.update(called=True))

    with pytest.raises(ContractValidationError):
        fetch_weather.main()

    assert not saved["called"]


def test_energy_ingestion_blocks_save_on_contract_failure(monkeypatch):
    saved = {"called": False}

    monkeypatch.setattr(fetch_energy, "load_config", lambda: {"api": {}})
    monkeypatch.setattr(fetch_energy, "fetch_energy", lambda config: {"invalid": True})
    monkeypatch.setattr(fetch_energy, "save_raw_data", lambda data: saved.update(called=True))

    with pytest.raises(ContractValidationError):
        fetch_energy.main()

    assert not saved["called"]

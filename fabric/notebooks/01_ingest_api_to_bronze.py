# Fabric notebook source: 01_ingest_api_to_bronze
#
# Attach the Lakehouse `weather_energy_lakehouse` before running.
# Pipeline parameters may override the defaults below.

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests


DATASET = "all"  # all, weather, or energy
WEATHER_CITY = "London,GB"
OPENWEATHER_API_KEY = ""
NATIONAL_GRID_API_TOKEN = ""
NATIONAL_GRID_RESOURCE_ID = ""
ENERGY_LIMIT = 1000
LAKEHOUSE_FILES_ROOT = "/lakehouse/default/Files"

OPENWEATHER_BASE_URL = "https://api.openweathermap.org/data/2.5"
NATIONAL_GRID_BASE_URL = "https://connecteddata.nationalgrid.co.uk/api/3/action"


def _get_parameter(name: str, default: Any) -> Any:
    return globals().get(name, default)


def _required_secret(value: str, env_name: str) -> str:
    explicit_value = value or os.getenv(env_name, "")
    if explicit_value:
        return explicit_value
    raise ValueError(
        f"Missing {env_name}. Pass it as a secure pipeline parameter or configure "
        "a secure secret lookup before running this notebook."
    )


def _require_path(payload: dict[str, Any], path: list[str], dataset_name: str) -> None:
    current: Any = payload
    for key in path:
        if not isinstance(current, dict) or key not in current:
            dotted_path = ".".join(path)
            raise ValueError(f"{dataset_name} payload failed contract: missing {dotted_path}")
        current = current[key]


def _validate_weather(payload: dict[str, Any]) -> None:
    for path in [
        ["dt"],
        ["name"],
        ["main", "temp"],
        ["main", "feels_like"],
        ["main", "humidity"],
        ["wind", "speed"],
        ["clouds", "all"],
        ["cod"],
    ]:
        _require_path(payload, path, "weather")
    weather_items = payload.get("weather")
    if not isinstance(weather_items, list) or not weather_items:
        raise ValueError("weather payload failed contract: weather must be a non-empty array")


def _validate_energy(payload: dict[str, Any]) -> None:
    for path in [["help"], ["success"], ["result"], ["result", "resource_id"], ["result", "records"]]:
        _require_path(payload, path, "energy")
    if payload["success"] is not True:
        raise ValueError("energy payload failed contract: success must be true")
    if not isinstance(payload["result"]["records"], list):
        raise ValueError("energy payload failed contract: result.records must be an array")


def _write_raw_json(dataset_name: str, payload: dict[str, Any]) -> str:
    now_utc = datetime.now(timezone.utc)
    timestamp = now_utc.strftime("%Y%m%d_%H%M%S")
    ingestion_date = now_utc.strftime("%Y-%m-%d")
    output_dir = (
        Path(LAKEHOUSE_FILES_ROOT)
        / "raw"
        / dataset_name
        / f"ingestion_date={ingestion_date}"
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{dataset_name}_{timestamp}.json"
    with output_path.open("w") as f:
        json.dump(payload, f, indent=2)
    return str(output_path)


def fetch_weather() -> dict[str, Any]:
    api_key = _required_secret(
        _get_parameter("OPENWEATHER_API_KEY", OPENWEATHER_API_KEY),
        "OPENWEATHER_API_KEY",
    )
    city = _get_parameter("WEATHER_CITY", WEATHER_CITY)
    response = requests.get(
        f"{OPENWEATHER_BASE_URL}/weather",
        params={"q": city, "appid": api_key, "units": "metric"},
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    _validate_weather(payload)
    return payload


def fetch_energy() -> dict[str, Any]:
    api_token = _required_secret(
        _get_parameter("NATIONAL_GRID_API_TOKEN", NATIONAL_GRID_API_TOKEN),
        "NATIONAL_GRID_API_TOKEN",
    )
    resource_id = _get_parameter("NATIONAL_GRID_RESOURCE_ID", NATIONAL_GRID_RESOURCE_ID)
    if not resource_id:
        raise ValueError("Missing NATIONAL_GRID_RESOURCE_ID pipeline parameter.")

    response = requests.get(
        f"{NATIONAL_GRID_BASE_URL}/datastore_search",
        params={"resource_id": resource_id, "limit": int(_get_parameter("ENERGY_LIMIT", ENERGY_LIMIT))},
        headers={"Authorization": api_token},
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    _validate_energy(payload)
    return payload


dataset = str(_get_parameter("DATASET", DATASET)).lower()
if dataset not in {"all", "weather", "energy"}:
    raise ValueError("DATASET must be one of: all, weather, energy")

written_paths: list[str] = []
if dataset in {"all", "weather"}:
    written_paths.append(_write_raw_json("weather", fetch_weather()))
if dataset in {"all", "energy"}:
    written_paths.append(_write_raw_json("energy", fetch_energy()))

print(json.dumps({"written_paths": written_paths}, indent=2))

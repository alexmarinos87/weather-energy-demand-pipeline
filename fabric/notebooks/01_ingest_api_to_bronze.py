# Fabric notebook source: 01_ingest_api_to_bronze
#
# Attach the Lakehouse `weather_energy_lakehouse` before running.
# Pipeline parameters may override the defaults below.

import json
import os
from functools import lru_cache
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from jsonschema import Draft202012Validator


DATASET = "all"  # all, weather, or energy
WEATHER_CITY = "London,GB"
OPENWEATHER_API_KEY = ""
NATIONAL_GRID_API_TOKEN = ""
NATIONAL_GRID_RESOURCE_ID = ""
ENERGY_LIMIT = 1000
LAKEHOUSE_FILES_ROOT = "/lakehouse/default/Files"
CONTRACTS_ROOT = ""

OPENWEATHER_BASE_URL = "https://api.openweathermap.org/data/2.5"
NATIONAL_GRID_BASE_URL = "https://connecteddata.nationalgrid.co.uk/api/3/action"
CONTRACT_FILENAMES = {
    "weather": "weather_schema.json",
    "energy": "energy_schema.json",
}


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


def _unique_paths(paths: list[Path]) -> list[Path]:
    seen: set[str] = set()
    unique_paths = []
    for path in paths:
        normalized = str(path.expanduser())
        if normalized not in seen:
            unique_paths.append(path)
            seen.add(normalized)
    return unique_paths


def _candidate_contract_roots() -> list[Path]:
    configured_root = str(_get_parameter("CONTRACTS_ROOT", CONTRACTS_ROOT)).strip()
    roots = []
    if configured_root:
        roots.append(Path(configured_root))

    files_root = Path(str(_get_parameter("LAKEHOUSE_FILES_ROOT", LAKEHOUSE_FILES_ROOT)))
    roots.append(files_root / "data-contracts")

    if "__file__" in globals():
        roots.append(Path(__file__).resolve().parents[2] / "data-contracts")

    roots.extend(
        [
            Path.cwd() / "data-contracts",
            Path.cwd().parent / "data-contracts",
        ]
    )
    return _unique_paths(roots)


def _resolve_contract_path(dataset_name: str) -> Path:
    contract_filename = CONTRACT_FILENAMES[dataset_name]
    searched_paths = []
    for root in _candidate_contract_roots():
        contract_path = root / contract_filename
        searched_paths.append(contract_path)
        if contract_path.exists():
            return contract_path

    files_root = Path(str(_get_parameter("LAKEHOUSE_FILES_ROOT", LAKEHOUSE_FILES_ROOT)))
    default_lakehouse_path = files_root / "data-contracts"
    searched = ", ".join(str(path) for path in searched_paths)
    raise FileNotFoundError(
        f"Missing {contract_filename}. Upload data-contracts to "
        f"{default_lakehouse_path} or set CONTRACTS_ROOT. Searched: {searched}"
    )


@lru_cache(maxsize=8)
def _get_validator(contract_path: str) -> Draft202012Validator:
    with Path(contract_path).open("r") as f:
        schema = json.load(f)

    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema)


def _validate_payload(payload: dict[str, Any], dataset_name: str) -> None:
    contract_path = _resolve_contract_path(dataset_name)
    validator = _get_validator(str(contract_path.resolve()))
    errors = sorted(validator.iter_errors(payload), key=lambda err: list(err.absolute_path))

    if not errors:
        return

    lines = [
        (
            f"{dataset_name} payload failed contract "
            f"{contract_path.name} with {len(errors)} issue(s):"
        )
    ]

    for err in errors[:5]:
        path = ".".join(str(item) for item in err.absolute_path) or "<root>"
        lines.append(f"- {path}: {err.message}")

    if len(errors) > 5:
        lines.append(f"- ... {len(errors) - 5} additional issue(s)")

    raise ValueError("\n".join(lines))


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
    _validate_payload(payload, "weather")
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
    _validate_payload(payload, "energy")
    return payload


def main() -> list[str]:
    dataset = str(_get_parameter("DATASET", DATASET)).lower()
    if dataset not in {"all", "weather", "energy"}:
        raise ValueError("DATASET must be one of: all, weather, energy")

    written_paths: list[str] = []
    if dataset in {"all", "weather"}:
        written_paths.append(_write_raw_json("weather", fetch_weather()))
    if dataset in {"all", "energy"}:
        written_paths.append(_write_raw_json("energy", fetch_energy()))

    print(json.dumps({"written_paths": written_paths}, indent=2))
    return written_paths


if __name__ == "__main__":
    main()

# Fabric notebook source: 01b_ingest_forecast_weather_to_bronze
# Optional/manual forecast-weather evidence path. Attach weather_energy_lakehouse.

import hashlib
import json
import os
from copy import deepcopy
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any

import requests
from jsonschema import Draft202012Validator

SOURCE_AREA = "east_midlands"
OPENWEATHER_API_KEY = ""
FORECAST_MAX_RECORDS = 40
REQUEST_TIMEOUT_SECONDS = 30
COORDINATE_TOLERANCE_DEGREES = 0.25
LAKEHOUSE_FILES_ROOT = "/lakehouse/default/Files"
CONTRACTS_ROOT = ""

OPENWEATHER_FORECAST_ENDPOINT = "https://api.openweathermap.org/data/2.5/forecast"
RAW_CONTRACT_FILENAME = "openweather_forecast_raw_schema.json"
SOURCE_AREAS_FILENAME = "source_areas.json"
PROVIDER = "openweather"
PRODUCT = "5_day_3_hour"
MODEL = "5-day-3-hour"
ISSUE_BASIS = "retrieval_time_surrogate"


def _get_parameter(name: str, default: Any) -> Any:
    return globals().get(name, default)


def _positive_int(value: Any, name: str, *, maximum: int | None = None) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a positive integer.")
    parsed = int(value)
    if parsed < 1 or (maximum is not None and parsed > maximum):
        suffix = f" no greater than {maximum}" if maximum is not None else ""
        raise ValueError(f"{name} must be a positive integer{suffix}.")
    return parsed


def _candidate_contract_roots() -> list[Path]:
    roots: list[Path] = []
    configured = str(_get_parameter("CONTRACTS_ROOT", CONTRACTS_ROOT)).strip()
    if configured:
        roots.append(Path(configured))
    files_root = Path(str(_get_parameter("LAKEHOUSE_FILES_ROOT", LAKEHOUSE_FILES_ROOT)))
    roots.append(files_root / "data-contracts")
    if "__file__" in globals():
        roots.append(Path(__file__).resolve().parents[2] / "data-contracts")
    roots.extend([Path.cwd() / "data-contracts", Path.cwd().parent / "data-contracts"])
    unique: list[Path] = []
    for root in roots:
        if root not in unique:
            unique.append(root)
    return unique


def _contract_path(filename: str) -> Path:
    searched = []
    for root in _candidate_contract_roots():
        path = root / filename
        searched.append(path)
        if path.exists():
            return path
    raise FileNotFoundError(
        f"Missing {filename}. Upload contracts or set CONTRACTS_ROOT. Searched: "
        + ", ".join(str(path) for path in searched)
    )


@lru_cache(maxsize=4)
def _validator(path: str) -> Draft202012Validator:
    schema = json.loads(Path(path).read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema)


def _validate(payload: dict[str, Any]) -> None:
    path = _contract_path(RAW_CONTRACT_FILENAME)
    errors = sorted(
        _validator(str(path.resolve())).iter_errors(payload),
        key=lambda error: list(error.absolute_path),
    )
    if errors:
        detail = "; ".join(
            f"{'.'.join(str(part) for part in error.absolute_path) or '<root>'}: {error.message}"
            for error in errors[:5]
        )
        raise ValueError(f"OpenWeather forecast failed {path.name}: {detail}")


def _binding() -> dict[str, Any]:
    contract = json.loads(_contract_path(SOURCE_AREAS_FILENAME).read_text(encoding="utf-8"))
    area_key = str(_get_parameter("SOURCE_AREA", SOURCE_AREA)).strip().lower().replace("-", "_").replace(" ", "_")
    area = contract.get("areas", {}).get(area_key)
    if not isinstance(area, dict):
        raise ValueError(f"Unsupported SOURCE_AREA={area_key!r}.")
    required = {
        "display_name", "nged_resource_id", "weather_proxy_city",
        "weather_proxy_latitude", "weather_proxy_longitude",
    }
    missing = sorted(required - set(area))
    if missing:
        raise ValueError(f"Source-area binding is missing: {', '.join(missing)}.")
    return {
        "contract_version": str(contract["contract_version"]),
        "source_area": area_key,
        "source_area_name": str(area["display_name"]),
        "nged_resource_id": str(area["nged_resource_id"]),
        "weather_proxy_city": str(area["weather_proxy_city"]),
        "weather_proxy_latitude": float(area["weather_proxy_latitude"]),
        "weather_proxy_longitude": float(area["weather_proxy_longitude"]),
    }


def _required_secret() -> str:
    value = str(_get_parameter("OPENWEATHER_API_KEY", OPENWEATHER_API_KEY) or os.getenv("OPENWEATHER_API_KEY", "")).strip()
    if not value:
        raise ValueError("Missing OPENWEATHER_API_KEY secure pipeline parameter or environment secret.")
    return value


def _semantic_validate(payload: dict[str, Any], binding: dict[str, Any], maximum: int) -> None:
    records = payload.get("list")
    if not isinstance(records, list) or payload.get("cnt") != len(records):
        raise ValueError("OpenWeather cnt must equal the forecast list length.")
    if len(records) > maximum:
        raise ValueError("OpenWeather returned more forecast records than requested.")
    timestamps = [record.get("dt") for record in records]
    if any(isinstance(value, bool) or not isinstance(value, int) for value in timestamps):
        raise ValueError("Every OpenWeather forecast slot must have an integer dt.")
    if any(later <= earlier for earlier, later in zip(timestamps, timestamps[1:])):
        raise ValueError("OpenWeather forecast timestamps must be strictly increasing.")
    city = payload.get("city", {})
    if city.get("country") != "GB":
        raise ValueError("OpenWeather forecast returned an unexpected country.")
    coord = city.get("coord", {})
    tolerance = float(_get_parameter("COORDINATE_TOLERANCE_DEGREES", COORDINATE_TOLERANCE_DEGREES))
    if tolerance < 0:
        raise ValueError("COORDINATE_TOLERANCE_DEGREES must be non-negative.")
    if abs(float(coord.get("lat")) - binding["weather_proxy_latitude"]) > tolerance or abs(float(coord.get("lon")) - binding["weather_proxy_longitude"]) > tolerance:
        raise ValueError("OpenWeather forecast coordinates do not match the source-area proxy.")


def _snapshot_id(payload: dict[str, Any], binding: dict[str, Any], retrieved_at: datetime) -> str:
    evidence = {
        "provider_payload": payload,
        "source_area": binding["source_area"],
        "weather_proxy_city": binding["weather_proxy_city"],
        "weather_proxy_latitude": binding["weather_proxy_latitude"],
        "weather_proxy_longitude": binding["weather_proxy_longitude"],
        "retrieved_at_utc": retrieved_at.isoformat(),
    }
    return hashlib.sha256(
        json.dumps(evidence, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def fetch_forecast_weather() -> dict[str, Any]:
    # Source binding and bounded configuration are validated before credential access.
    binding = _binding()
    count = _positive_int(
        _get_parameter("FORECAST_MAX_RECORDS", FORECAST_MAX_RECORDS),
        "FORECAST_MAX_RECORDS",
        maximum=40,
    )
    timeout = _positive_int(
        _get_parameter("REQUEST_TIMEOUT_SECONDS", REQUEST_TIMEOUT_SECONDS),
        "REQUEST_TIMEOUT_SECONDS",
    )
    api_key = _required_secret()
    response = requests.get(
        OPENWEATHER_FORECAST_ENDPOINT,
        params={
            "lat": binding["weather_proxy_latitude"],
            "lon": binding["weather_proxy_longitude"],
            "appid": api_key,
            "units": "metric",
            "cnt": count,
        },
        timeout=timeout,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError("OpenWeather forecast response must be a JSON object.")
    _validate(payload)
    _semantic_validate(payload, binding, count)
    retrieved_at = datetime.now(timezone.utc)
    enriched = deepcopy(payload)
    enriched["_pipeline_metadata"] = {
        **binding,
        "dataset": "forecast_weather",
        "provider": PROVIDER,
        "product": PRODUCT,
        "forecast_model": MODEL,
        "forecast_issue_basis": ISSUE_BASIS,
        "retrieved_at_utc": retrieved_at.isoformat(),
        "requested_record_count": count,
        "returned_record_count": len(payload["list"]),
    }
    enriched["_pipeline_metadata"]["raw_snapshot_id"] = _snapshot_id(
        payload, binding, retrieved_at
    )
    _validate(enriched)
    return enriched


def write_raw_forecast(payload: dict[str, Any]) -> str:
    metadata = payload["_pipeline_metadata"]
    retrieved_at = datetime.fromisoformat(metadata["retrieved_at_utc"])
    root = Path(str(_get_parameter("LAKEHOUSE_FILES_ROOT", LAKEHOUSE_FILES_ROOT)))
    partition = root / "raw" / "forecast_weather" / f"ingestion_date={retrieved_at:%Y-%m-%d}"
    partition.mkdir(parents=True, exist_ok=True)
    path = partition / (
        f"openweather_forecast_{retrieved_at:%Y%m%d_%H%M%S}_"
        f"{metadata['raw_snapshot_id'][:12]}.json"
    )
    with path.open("x", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
    return str(path)


def main() -> str:
    path = write_raw_forecast(fetch_forecast_weather())
    print(json.dumps({"forecast_weather_raw_path": path}, indent=2))
    return path


if __name__ == "__main__":
    main()

from __future__ import annotations

import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlparse

import pandas as pd
import requests
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from ingestion.common.contract_validator import validate_payload
from ingestion.common.source_area import attach_pipeline_metadata, validate_source_binding

RAW_CONTRACT_PATH = (
    PROJECT_ROOT / "data-contracts" / "openweather_forecast_raw_schema.json"
)
NORMALIZED_CONTRACT_PATH = (
    PROJECT_ROOT / "data-contracts" / "forecast_weather_schema.json"
)
PROVIDER = "openweather"
PRODUCT = "5_day_3_hour"
MODEL = "5-day-3-hour"
ISSUE_BASIS = "retrieval_time_surrogate"
ALLOWED_HOST = "api.openweathermap.org"
ALLOWED_BASE_PATH = "/data/2.5"
MAX_PROVIDER_RECORDS = 40
COORDINATE_TOLERANCE_DEGREES = 0.25

RequestGet = Callable[..., Any]


class OpenWeatherForecastError(RuntimeError):
    """Raised when a bounded OpenWeather forecast snapshot is unsafe to publish."""


def load_config(config_path: Path | None = None) -> dict[str, Any]:
    if config_path is None:
        config_path = Path(__file__).parent / "config.yaml"
    if not config_path.exists():
        template_path = config_path.with_name("config.example.yaml")
        raise FileNotFoundError(
            f"Missing {config_path}. Create it from {template_path}."
        )
    with config_path.open("r", encoding="utf-8") as file_handle:
        loaded = yaml.safe_load(file_handle)
    if not isinstance(loaded, dict):
        raise ValueError("Forecast-weather config must be a YAML object.")
    return loaded


def _positive_int(value: Any, name: str, *, maximum: int | None = None) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a positive integer.")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a positive integer.") from exc
    if parsed < 1 or (maximum is not None and parsed > maximum):
        suffix = f" no greater than {maximum}" if maximum is not None else ""
        raise ValueError(f"{name} must be a positive integer{suffix}.")
    return parsed


def get_api_key(config: dict[str, Any]) -> str:
    api_config = config.get("api", {})
    api_key_env = api_config.get("api_key_env")
    if not api_key_env:
        raise ValueError("Missing api.api_key_env in forecast config.")
    api_key = os.getenv(str(api_key_env))
    if not api_key:
        raise EnvironmentError(f"Environment variable {api_key_env} is not set.")
    return api_key


def resolve_binding(config: dict[str, Any]) -> dict[str, Any]:
    return validate_source_binding(config.get("source_area"))


def resolve_endpoint(config: dict[str, Any]) -> str:
    api_config = config.get("api", {})
    base_url = str(api_config.get("base_url", "")).strip()
    if not base_url:
        raise ValueError("Missing api.base_url in forecast config.")
    parsed = urlparse(base_url)
    try:
        port = parsed.port
    except ValueError as exc:
        raise ValueError(
            "api.base_url must be exactly the HTTPS OpenWeather data/2.5 endpoint."
        ) from exc
    if (
        parsed.scheme != "https"
        or parsed.hostname != ALLOWED_HOST
        or parsed.username is not None
        or parsed.password is not None
        or port not in (None, 443)
        or parsed.query
        or parsed.fragment
        or parsed.path.rstrip("/") != ALLOWED_BASE_PATH
    ):
        raise ValueError(
            "api.base_url must be exactly the HTTPS OpenWeather data/2.5 endpoint."
        )
    return f"https://{ALLOWED_HOST}{ALLOWED_BASE_PATH}/forecast"


def _utc_timestamp(value: datetime | None = None) -> datetime:
    timestamp = value or datetime.now(timezone.utc)
    if timestamp.tzinfo is None:
        raise ValueError("retrieved_at_utc must be timezone-aware.")
    return timestamp.astimezone(timezone.utc)


def _snapshot_id(
    payload: dict[str, Any],
    *,
    binding: dict[str, Any],
    retrieved_at_utc: datetime,
) -> str:
    evidence = {
        "provider_payload": payload,
        "source_area": binding["source_area"],
        "weather_proxy_city": binding["weather_proxy_city"],
        "weather_proxy_latitude": binding["weather_proxy_latitude"],
        "weather_proxy_longitude": binding["weather_proxy_longitude"],
        "retrieved_at_utc": retrieved_at_utc.isoformat(),
    }
    encoded = json.dumps(
        evidence,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _validate_provider_snapshot(
    payload: dict[str, Any],
    *,
    binding: dict[str, Any],
    requested_count: int,
) -> None:
    validate_payload(payload, RAW_CONTRACT_PATH, "OpenWeather forecast")
    forecasts = payload["list"]
    returned_count = payload["cnt"]
    if returned_count != len(forecasts):
        raise OpenWeatherForecastError(
            f"OpenWeather cnt={returned_count} but returned {len(forecasts)} records."
        )
    if returned_count > requested_count:
        raise OpenWeatherForecastError(
            "OpenWeather returned more forecast records than the configured bound."
        )
    timestamps = [record["dt"] for record in forecasts]
    if len(timestamps) != len(set(timestamps)):
        raise OpenWeatherForecastError(
            "OpenWeather forecast contains duplicate valid timestamps."
        )
    if any(later <= earlier for earlier, later in zip(timestamps, timestamps[1:])):
        raise OpenWeatherForecastError(
            "OpenWeather forecast timestamps must be strictly increasing."
        )

    city = payload["city"]
    expected_country = binding["weather_proxy_city"].rsplit(",", 1)[-1].upper()
    if str(city["country"]).upper() != expected_country:
        raise OpenWeatherForecastError(
            f"OpenWeather returned country={city['country']!r}; "
            f"expected {expected_country!r}."
        )
    latitude_delta = abs(
        float(city["coord"]["lat"]) - float(binding["weather_proxy_latitude"])
    )
    longitude_delta = abs(
        float(city["coord"]["lon"]) - float(binding["weather_proxy_longitude"])
    )
    if (
        latitude_delta > COORDINATE_TOLERANCE_DEGREES
        or longitude_delta > COORDINATE_TOLERANCE_DEGREES
    ):
        raise OpenWeatherForecastError(
            "OpenWeather response coordinates do not match the configured "
            "source-area proxy."
        )


def fetch_openweather_forecast(
    config: dict[str, Any],
    *,
    request_get: RequestGet = requests.get,
    retrieved_at_utc: datetime | None = None,
) -> dict[str, Any]:
    """Fetch and validate one bounded OpenWeather 5-day/3-hour snapshot."""
    binding = resolve_binding(config)
    endpoint = resolve_endpoint(config)
    api_config = config.get("api", {})
    units = str(api_config.get("units", "metric")).strip().lower()
    if units != "metric":
        raise ValueError(
            "api.units must be metric because the normalized contract uses Celsius."
        )
    timeout_seconds = _positive_int(
        api_config.get("timeout_seconds", 30), "api.timeout_seconds"
    )
    requested_count = _positive_int(
        api_config.get("max_forecast_records", MAX_PROVIDER_RECORDS),
        "api.max_forecast_records",
        maximum=MAX_PROVIDER_RECORDS,
    )
    api_key = get_api_key(config)
    response = request_get(
        endpoint,
        params={
            "lat": binding["weather_proxy_latitude"],
            "lon": binding["weather_proxy_longitude"],
            "appid": api_key,
            "units": units,
            "cnt": requested_count,
        },
        timeout=timeout_seconds,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise OpenWeatherForecastError("OpenWeather response must be a JSON object.")
    _validate_provider_snapshot(
        payload,
        binding=binding,
        requested_count=requested_count,
    )

    retrieved_at = _utc_timestamp(retrieved_at_utc)
    enriched = attach_pipeline_metadata(
        payload,
        dataset_name="forecast_weather",
        binding=binding,
    )
    metadata = enriched["_pipeline_metadata"]
    metadata.update(
        {
            "provider": PROVIDER,
            "product": PRODUCT,
            "forecast_issue_basis": ISSUE_BASIS,
            "retrieved_at_utc": retrieved_at.isoformat(),
            "endpoint": endpoint,
            "units": units,
            "requested_record_count": requested_count,
            "returned_record_count": len(payload["list"]),
        }
    )
    metadata["raw_snapshot_id"] = _snapshot_id(
        payload,
        binding=binding,
        retrieved_at_utc=retrieved_at,
    )
    validate_payload(enriched, RAW_CONTRACT_PATH, "OpenWeather forecast")
    return enriched


def normalize_openweather_forecast(
    raw_payload: dict[str, Any],
) -> list[dict[str, Any]]:
    """Normalize future forecast slots into the provider-neutral contract."""
    validate_payload(raw_payload, RAW_CONTRACT_PATH, "OpenWeather forecast")
    metadata = raw_payload.get("_pipeline_metadata")
    if not isinstance(metadata, dict):
        raise OpenWeatherForecastError(
            "Raw OpenWeather forecast is missing pipeline metadata."
        )
    if metadata.get("forecast_issue_basis") != ISSUE_BASIS:
        raise OpenWeatherForecastError(
            "OpenWeather forecast must declare retrieval_time_surrogate issue basis."
        )
    try:
        retrieved_at = pd.Timestamp(metadata["retrieved_at_utc"])
    except (KeyError, TypeError, ValueError) as exc:
        raise OpenWeatherForecastError(
            "Raw OpenWeather forecast has invalid retrieved_at_utc metadata."
        ) from exc
    if retrieved_at.tzinfo is None:
        raise OpenWeatherForecastError(
            "Raw OpenWeather retrieved_at_utc must be timezone-aware."
        )
    retrieved_at = retrieved_at.tz_convert("UTC")

    city = raw_payload["city"]
    city_identity = str(metadata["weather_proxy_city"]).rsplit(",", 1)[0]
    records: list[dict[str, Any]] = []
    for provider_record in raw_payload["list"]:
        valid_at = pd.Timestamp(provider_record["dt"], unit="s", tz="UTC")
        if valid_at <= retrieved_at:
            continue
        normalized = {
            "source_area": metadata["source_area"],
            "city": city_identity,
            "forecast_issued_at_utc": retrieved_at.isoformat(),
            "forecast_ingested_at_utc": retrieved_at.isoformat(),
            "forecast_valid_at_utc": valid_at.isoformat(),
            "forecast_temperature_c": float(provider_record["main"]["temp"]),
            "forecast_humidity_pct": float(provider_record["main"]["humidity"]),
            "forecast_provider": PROVIDER,
            "forecast_model": MODEL,
            "forecast_issue_basis": ISSUE_BASIS,
            "forecast_retrieved_at_utc": retrieved_at.isoformat(),
            "forecast_provider_record_id": str(provider_record["dt"]),
            "forecast_provider_location_name": str(city["name"]),
            "forecast_latitude": float(city["coord"]["lat"]),
            "forecast_longitude": float(city["coord"]["lon"]),
            "raw_snapshot_id": metadata["raw_snapshot_id"],
        }
        validate_payload(
            normalized,
            NORMALIZED_CONTRACT_PATH,
            "normalized forecast weather",
        )
        records.append(normalized)
    if not records:
        raise OpenWeatherForecastError(
            "OpenWeather snapshot contains no forecast slots after retrieval time."
        )
    return records


def _output_root(
    config: dict[str, Any],
    name: str,
    default: str,
) -> Path:
    output = config.get("output", {})
    value = str(output.get(name, default)).strip()
    if not value:
        raise ValueError(f"output.{name} must be a non-empty path.")
    return Path(value)


def save_raw_snapshot(
    raw_payload: dict[str, Any],
    *,
    output_root: Path,
) -> Path:
    metadata = raw_payload["_pipeline_metadata"]
    retrieved_at = pd.Timestamp(metadata["retrieved_at_utc"])
    partition = output_root / f"ingestion_date={retrieved_at:%Y-%m-%d}"
    partition.mkdir(parents=True, exist_ok=True)
    file_path = partition / (
        f"openweather_forecast_{retrieved_at:%Y%m%d_%H%M%S}_"
        f"{metadata['raw_snapshot_id'][:12]}.json"
    )
    with file_path.open("x", encoding="utf-8") as file_handle:
        json.dump(raw_payload, file_handle, indent=2, sort_keys=True)
    return file_path


def save_normalized_forecast(
    records: list[dict[str, Any]],
    *,
    output_root: Path,
) -> Path:
    if not records:
        raise OpenWeatherForecastError("No normalized forecast records to save.")
    retrieved_at = pd.Timestamp(records[0]["forecast_retrieved_at_utc"])
    snapshot_id = str(records[0]["raw_snapshot_id"])
    if any(str(record["raw_snapshot_id"]) != snapshot_id for record in records):
        raise OpenWeatherForecastError(
            "Normalized records must belong to one raw snapshot."
        )
    partition = output_root / f"ingestion_date={retrieved_at:%Y-%m-%d}"
    partition.mkdir(parents=True, exist_ok=True)
    file_path = partition / (
        f"forecast_weather_{retrieved_at:%Y%m%d_%H%M%S}_{snapshot_id[:12]}.parquet"
    )
    if file_path.exists():
        raise FileExistsError(f"Refusing to overwrite {file_path}.")
    temp_path = file_path.with_suffix(".tmp.parquet")
    if temp_path.exists():
        raise FileExistsError(f"Temporary output already exists: {temp_path}.")
    frame = pd.DataFrame(records)
    timestamp_columns = [
        "forecast_issued_at_utc",
        "forecast_ingested_at_utc",
        "forecast_valid_at_utc",
        "forecast_retrieved_at_utc",
    ]
    for column in timestamp_columns:
        frame[column] = pd.to_datetime(frame[column], utc=True, errors="raise")
    frame.to_parquet(temp_path, index=False)
    temp_path.replace(file_path)
    return file_path


def main() -> None:
    config = load_config()
    raw_payload = fetch_openweather_forecast(config)
    normalized = normalize_openweather_forecast(raw_payload)
    raw_path = save_raw_snapshot(
        raw_payload,
        output_root=_output_root(
            config,
            "raw_root",
            "data/raw/forecast_weather/openweather",
        ),
    )
    normalized_path = save_normalized_forecast(
        normalized,
        output_root=_output_root(
            config,
            "normalized_root",
            "data/normalized/forecast_weather/openweather",
        ),
    )
    print(f"Saved raw OpenWeather forecast to {raw_path}")
    print(f"Saved normalized forecast weather to {normalized_path}")


if __name__ == "__main__":
    main()

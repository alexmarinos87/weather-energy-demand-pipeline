import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from ingestion.common.contract_validator import validate_payload
from ingestion.common.source_area import attach_pipeline_metadata, validate_source_binding

WEATHER_CONTRACT_PATH = PROJECT_ROOT / "data-contracts" / "weather_schema.json"


def load_config(config_path: Path | None = None):
    """Load API configuration from YAML file."""
    if config_path is None:
        config_path = Path(__file__).parent / "config.yaml"
    if not config_path.exists():
        template_path = config_path.with_name("config.example.yaml")
        raise FileNotFoundError(
            f"Missing {config_path}. Create it from {template_path}."
        )
    with config_path.open("r", encoding="utf-8") as file_handle:
        return yaml.safe_load(file_handle)


def get_api_key(config):
    """Read API key from environment variable configured in YAML."""
    api_config = config.get("api", {})
    api_key_env = api_config.get("api_key_env")
    if not api_key_env:
        raise ValueError("Missing api.api_key_env in config.yaml.")
    api_key = os.getenv(api_key_env)
    if not api_key:
        raise EnvironmentError(f"Environment variable {api_key_env} is not set.")
    return api_key


def resolve_binding(config: dict) -> dict[str, str]:
    api_config = config.get("api", {})
    return validate_source_binding(
        config.get("source_area"),
        weather_city=api_config.get("city"),
    )


def fetch_weather(config):
    """Fetch current weather for the configured licence-area proxy city."""
    binding = resolve_binding(config)
    url = f"{config['api']['base_url'].rstrip('/')}/weather"
    api_key = get_api_key(config)

    params = {
        "q": config["api"]["city"],
        "appid": api_key,
        "units": config["api"]["units"],
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    payload = response.json()
    validate_payload(payload, WEATHER_CONTRACT_PATH, "weather")
    return attach_pipeline_metadata(payload, dataset_name="weather", binding=binding)


def save_raw_data(data):
    """Save raw weather JSON to a timestamped file."""
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_dir = Path("data/raw/weather")
    output_dir.mkdir(parents=True, exist_ok=True)
    file_path = output_dir / f"weather_{timestamp}.json"

    with file_path.open("w", encoding="utf-8") as file_handle:
        json.dump(data, file_handle, indent=2)

    print(f"Saved raw weather data to {file_path}")


def main():
    config = load_config()
    weather_data = fetch_weather(config)
    validate_payload(weather_data, WEATHER_CONTRACT_PATH, "weather")
    save_raw_data(weather_data)


if __name__ == "__main__":
    main()

import json
import os
import sys
from datetime import datetime
from pathlib import Path

import requests
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from ingestion.common.contract_validator import validate_payload

WEATHER_CONTRACT_PATH = PROJECT_ROOT / "data-contracts" / "weather_schema.json"


def load_config():
    """Load API configuration from YAML file."""
    config_path = Path(__file__).parent / "config.yaml"
    if not config_path.exists():
        template_path = config_path.with_name("config.example.yaml")
        raise FileNotFoundError(
            f"Missing {config_path}. Create it from {template_path}."
        )
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def get_api_key(config):
    """Read API key from environment variable configured in YAML."""
    api_config = config.get("api", {})
    api_key_env = api_config.get("api_key_env")
    if not api_key_env:
        raise ValueError("Missing api.api_key_env in config.yaml.")
    api_key = os.getenv(api_key_env)
    if not api_key:
        raise EnvironmentError(
            f"Environment variable {api_key_env} is not set."
        )
    return api_key


def fetch_weather(config):
    """Fetch current weather data from OpenWeather API."""
    url = f"{config['api']['base_url']}/weather"
    api_key = get_api_key(config)

    params = {
        "q": config["api"]["city"],
        "appid": api_key,
        "units": config["api"]["units"],
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()  # fail fast if API call breaks

    return response.json()


def save_raw_data(data):
    """Save raw weather JSON to a timestamped file."""
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    output_dir = Path("data/raw/weather")
    output_dir.mkdir(parents=True, exist_ok=True)

    file_path = output_dir / f"weather_{timestamp}.json"

    with open(file_path, "w") as f:
        json.dump(data, f, indent=2)

    print(f"Saved raw weather data to {file_path}")


def main():
    config = load_config()
    weather_data = fetch_weather(config)
    validate_payload(weather_data, WEATHER_CONTRACT_PATH, "weather")
    save_raw_data(weather_data)


if __name__ == "__main__":
    main()

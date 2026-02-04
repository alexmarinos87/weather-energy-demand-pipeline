import json
import os
from datetime import datetime
from pathlib import Path

import requests
import yaml


def load_config():
    """Load API configuration from YAML file."""
    config_path = Path(__file__).parent / "config.yaml"
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def fetch_weather(config):
    """Fetch current weather data from OpenWeather API."""
    api_config = config["api"]
    base_url = api_config["base_url"].rstrip("/")
    url = f"{base_url}/weather"

    api_key_env = api_config.get("api_key_env")
    api_key_value = os.getenv(api_key_env) if api_key_env else None
    api_key = api_key_value or api_config.get("api_key")
    if not api_key:
        raise ValueError(
            "Missing OpenWeather API key. "
            "Set OPENWEATHER_API_KEY or provide api.api_key in config.yaml."
        )

    params = {
        "q": api_config["city"],
        "appid": api_key,
        "units": api_config.get("units", "metric"),
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
    save_raw_data(weather_data)


if __name__ == "__main__":
    main()

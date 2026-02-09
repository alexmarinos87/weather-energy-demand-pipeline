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


def build_headers(api_config: dict) -> dict:
    """Build headers for API requests, including optional API keys."""
    headers = {}
    api_key_env = api_config.get("api_key_env")
    api_key_header = api_config.get("api_key_header")
    if api_key_env and api_key_header:
        api_key_value = os.getenv(api_key_env)
        if api_key_value:
            headers[api_key_header] = api_key_value
    return headers


def fetch_energy(config: dict) -> dict:
    """Fetch electricity demand data from the UK National Grid ESO API."""
    api_config = config["api"]
    base_url = api_config["base_url"].rstrip("/")
    endpoint = api_config["endpoint"].lstrip("/")
    url = f"{base_url}/{endpoint}"
    params = api_config.get("params", {})
    headers = build_headers(api_config)
    timeout_seconds = api_config.get("timeout_seconds", 30)

    response = requests.get(
        url,
        params=params,
        headers=headers,
        timeout=timeout_seconds,
    )
    response.raise_for_status()
    return response.json()


def save_raw_data(data: dict):
    """Save raw energy JSON to a timestamped file."""
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    output_dir = Path("data/raw/energy")
    output_dir.mkdir(parents=True, exist_ok=True)
    file_path = output_dir / f"energy_{timestamp}.json"

    with open(file_path, "w") as f:
        json.dump(data, f, indent=2)

    print(f"Saved raw energy data to {file_path}")


def main():
    config = load_config()
    energy_data = fetch_energy(config)
    save_raw_data(energy_data)


if __name__ == "__main__":
    main()

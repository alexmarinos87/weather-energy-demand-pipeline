import requests
import yaml
import json
from datetime import datetime
from pathlib import Path


def load_config():
    """Load API configuration from YAML file."""
    config_path = Path(__file__).parent / "config.yaml"
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def fetch_weather(config):
    """Fetch current weather data from OpenWeather API."""
    url = f"{config['api']['base_url']}/weather"

    params = {
        "q": config["api"]["city"],
        "appid": config["api"]["api_key"],
        "units": config["api"]["units"],
    }

    response = requests.get(url, params=params)
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

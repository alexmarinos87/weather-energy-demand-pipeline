import json
from datetime import datetime
from pathlib import Path
import os

import pandas as pd


RAW_DIR = Path("data/raw/weather")
SILVER_DIR = Path("data/silver/weather")


def extract_fields(raw_json: dict) -> dict:
    """Flatten and clean fields from raw OpenWeather API response."""
    return {
        "timestamp_utc": datetime.utcfromtimestamp(raw_json["dt"]).isoformat(),
        "city": raw_json.get("name", ""),
        "temperature": raw_json["main"].get("temp"),
        "feels_like": raw_json["main"].get("feels_like"),
        "humidity": raw_json["main"].get("humidity"),
        "wind_speed": raw_json.get("wind", {}).get("speed"),
        "cloud_cover": raw_json.get("clouds", {}).get("all")
    }


def process_file(filepath: Path) -> dict:
    """Load raw JSON from a file and extract selected fields."""
    with open(filepath, "r") as f:
        raw_data = json.load(f)
    return extract_fields(raw_data)


def save_clean_data(df: pd.DataFrame, output_path: Path):
    """Save cleaned DataFrame to partitioned Silver path."""
    if df.empty:
        print("No valid records to write.")
        return

    dt_partition = datetime.utcnow().strftime("dt=%Y-%m-%d")
    output_dir = output_path / dt_partition
    output_dir.mkdir(parents=True, exist_ok=True)

    filename = f"weather_clean_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.parquet"
    df.to_parquet(output_dir / filename, index=False)
    print(f"Saved cleaned data to {output_dir / filename}")


def main():
    raw_files = list(RAW_DIR.glob("*.json"))
    if not raw_files:
        print("No raw weather data files found.")
        return

    records = []
    for file in raw_files:
        try:
            record = process_file(file)
            records.append(record)
        except Exception as e:
            print(f"Failed to process {file.name}: {e}")

    df = pd.DataFrame(records)
    save_clean_data(df, SILVER_DIR)


if __name__ == "__main__":
    main()

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


RAW_DIR = Path("data/raw/weather")
SILVER_DIR = Path("data/silver/weather")

WEATHER_CANONICAL_COLUMNS = [
    "source_dataset",
    "source_file",
    "source_record_id",
    "event_timestamp_utc",
    "ingestion_timestamp_utc",
    "event_date_utc",
    "city",
    "country_code",
    "latitude",
    "longitude",
    "temperature_c",
    "feels_like_c",
    "humidity_pct",
    "pressure_hpa",
    "cloud_cover_pct",
    "wind_speed_mps",
    "weather_main",
    "weather_description",
]


def _parse_ingestion_timestamp(filepath: Path) -> datetime:
    """Parse ingestion timestamp from filename; fallback to file mtime in UTC."""
    try:
        timestamp_text = filepath.stem.split("_", maxsplit=1)[1]
        parsed = datetime.strptime(timestamp_text, "%Y%m%d_%H%M%S")
        return parsed.replace(tzinfo=timezone.utc)
    except (IndexError, ValueError):
        return datetime.fromtimestamp(filepath.stat().st_mtime, tz=timezone.utc)


def _build_record(raw_json: dict[str, Any], source_file: str, ingestion_ts: datetime) -> dict[str, Any]:
    weather_summary = raw_json.get("weather", [{}])[0] or {}
    event_ts = pd.to_datetime(raw_json["dt"], unit="s", utc=True)

    return {
        "source_dataset": "weather",
        "source_file": source_file,
        "source_record_id": raw_json.get("id"),
        "event_timestamp_utc": event_ts,
        "ingestion_timestamp_utc": pd.Timestamp(ingestion_ts),
        "event_date_utc": event_ts.strftime("%Y-%m-%d"),
        "city": raw_json.get("name"),
        "country_code": raw_json.get("sys", {}).get("country"),
        "latitude": raw_json.get("coord", {}).get("lat"),
        "longitude": raw_json.get("coord", {}).get("lon"),
        "temperature_c": raw_json.get("main", {}).get("temp"),
        "feels_like_c": raw_json.get("main", {}).get("feels_like"),
        "humidity_pct": raw_json.get("main", {}).get("humidity"),
        "pressure_hpa": raw_json.get("main", {}).get("pressure"),
        "cloud_cover_pct": raw_json.get("clouds", {}).get("all"),
        "wind_speed_mps": raw_json.get("wind", {}).get("speed"),
        "weather_main": weather_summary.get("main"),
        "weather_description": weather_summary.get("description"),
    }


def transform_weather_files(raw_dir: Path = RAW_DIR) -> pd.DataFrame:
    """Transform weather raw JSON files to canonical silver schema."""
    records: list[dict[str, Any]] = []
    for filepath in sorted(raw_dir.glob("*.json")):
        try:
            with filepath.open("r") as f:
                raw_data = json.load(f)
            records.append(
                _build_record(
                    raw_json=raw_data,
                    source_file=filepath.name,
                    ingestion_ts=_parse_ingestion_timestamp(filepath),
                )
            )
        except Exception as exc:
            print(f"Failed to process {filepath.name}: {exc}")

    if not records:
        return pd.DataFrame(columns=WEATHER_CANONICAL_COLUMNS)

    df = pd.DataFrame(records)[WEATHER_CANONICAL_COLUMNS]
    df = df.sort_values("ingestion_timestamp_utc")
    df = df.drop_duplicates(subset=["city", "event_timestamp_utc"], keep="last")
    return df.reset_index(drop=True)


def save_clean_data(df: pd.DataFrame, output_path: Path = SILVER_DIR):
    """Write silver weather records partitioned by event_date_utc."""
    if df.empty:
        print("No valid weather records to write.")
        return

    run_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    for event_date, partition_df in df.groupby("event_date_utc", sort=True):
        output_dir = output_path / f"dt={event_date}"
        output_dir.mkdir(parents=True, exist_ok=True)

        output_file = output_dir / f"weather_clean_{run_timestamp}.parquet"
        partition_df.to_parquet(output_file, index=False)
        print(f"Saved cleaned weather data to {output_file}")


def main():
    transformed_df = transform_weather_files()
    save_clean_data(transformed_df)


if __name__ == "__main__":
    main()

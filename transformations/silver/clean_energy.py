import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


RAW_DIR = Path("data/raw/energy")
SILVER_DIR = Path("data/silver/energy")

ENERGY_CANONICAL_COLUMNS = [
    "source_dataset",
    "source_file",
    "resource_id",
    "source_record_id",
    "event_timestamp_utc",
    "ingestion_timestamp_utc",
    "event_date_utc",
    "demand_mw",
    "generation_mw",
    "import_mw",
    "solar_mw",
    "wind_mw",
    "stor_mw",
    "other_mw",
]


def _parse_ingestion_timestamp(filepath: Path) -> datetime:
    """Parse ingestion timestamp from filename; fallback to file mtime in UTC."""
    try:
        timestamp_text = filepath.stem.split("_", maxsplit=1)[1]
        parsed = datetime.strptime(timestamp_text, "%Y%m%d_%H%M%S")
        return parsed.replace(tzinfo=timezone.utc)
    except (IndexError, ValueError):
        return datetime.fromtimestamp(filepath.stat().st_mtime, tz=timezone.utc)


def _parse_event_timestamp(value: Any) -> pd.Timestamp:
    parsed = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(parsed):
        raise ValueError(f"Invalid event timestamp: {value!r}")
    return parsed


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def _build_records(raw_json: dict[str, Any], source_file: str, ingestion_ts: datetime) -> list[dict[str, Any]]:
    result = raw_json.get("result", {})
    resource_id = result.get("resource_id")
    records = result.get("records", [])

    cleaned: list[dict[str, Any]] = []
    for record in records:
        event_ts = _parse_event_timestamp(record.get("Timestamp"))
        cleaned.append(
            {
                "source_dataset": "energy",
                "source_file": source_file,
                "resource_id": resource_id,
                "source_record_id": record.get("_id"),
                "event_timestamp_utc": event_ts,
                "ingestion_timestamp_utc": pd.Timestamp(ingestion_ts),
                "event_date_utc": event_ts.strftime("%Y-%m-%d"),
                "demand_mw": _to_float(record.get("Demand")),
                "generation_mw": _to_float(record.get("Generation")),
                "import_mw": _to_float(record.get("Import")),
                "solar_mw": _to_float(record.get("Solar")),
                "wind_mw": _to_float(record.get("Wind")),
                "stor_mw": _to_float(record.get("STOR")),
                "other_mw": _to_float(record.get("Other")),
            }
        )

    return cleaned


def transform_energy_files(raw_dir: Path = RAW_DIR) -> pd.DataFrame:
    """Transform energy raw JSON files to canonical silver schema."""
    records: list[dict[str, Any]] = []
    for filepath in sorted(raw_dir.glob("*.json")):
        try:
            with filepath.open("r") as f:
                raw_data = json.load(f)
            records.extend(
                _build_records(
                    raw_json=raw_data,
                    source_file=filepath.name,
                    ingestion_ts=_parse_ingestion_timestamp(filepath),
                )
            )
        except Exception as exc:
            print(f"Failed to process {filepath.name}: {exc}")

    if not records:
        return pd.DataFrame(columns=ENERGY_CANONICAL_COLUMNS)

    df = pd.DataFrame(records)[ENERGY_CANONICAL_COLUMNS]
    df = df.sort_values("ingestion_timestamp_utc")
    df = df.drop_duplicates(
        subset=["resource_id", "source_record_id", "event_timestamp_utc"],
        keep="last",
    )
    return df.reset_index(drop=True)


def save_clean_data(df: pd.DataFrame, output_path: Path = SILVER_DIR):
    """Write silver energy records partitioned by event_date_utc."""
    if df.empty:
        print("No valid energy records to write.")
        return

    run_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    for event_date, partition_df in df.groupby("event_date_utc", sort=True):
        output_dir = output_path / f"dt={event_date}"
        output_dir.mkdir(parents=True, exist_ok=True)

        output_file = output_dir / f"energy_clean_{run_timestamp}.parquet"
        partition_df.to_parquet(output_file, index=False)
        print(f"Saved cleaned energy data to {output_file}")


def main():
    transformed_df = transform_energy_files()
    save_clean_data(transformed_df)


if __name__ == "__main__":
    main()

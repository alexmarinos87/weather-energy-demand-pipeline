import json
from datetime import datetime, timezone
from pathlib import Path
from stat import S_ISDIR
from typing import Any

import pandas as pd

if __package__:
    from .deduplication import select_latest_records
    from .publication import write_silver_partitions
else:  # Preserve direct-script execution as well as python -m.
    from deduplication import select_latest_records
    from publication import write_silver_partitions


RAW_DIR = Path("data/raw/energy")
SILVER_DIR = Path("data/silver/energy")

ENERGY_CANONICAL_COLUMNS = [
    "source_dataset",
    "source_file",
    "resource_id",
    "source_record_id",
    "source_area",
    "source_area_name",
    "metadata_contract_version",
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


def _build_records(
    raw_json: dict[str, Any],
    source_file: str,
    ingestion_ts: datetime,
) -> list[dict[str, Any]]:
    if not isinstance(raw_json, dict):
        raise ValueError("Energy raw input must be a JSON object.")
    result = raw_json.get("result")
    if not isinstance(result, dict) or not isinstance(result.get("records"), list):
        raise ValueError("Energy raw input must contain result.records as a list.")
    metadata = raw_json.get("_pipeline_metadata") or {}
    resource_id = result.get("resource_id")
    records = result["records"]

    cleaned: list[dict[str, Any]] = []
    for record in records:
        event_ts = _parse_event_timestamp(record.get("Timestamp"))
        cleaned.append(
            {
                "source_dataset": "energy",
                "source_file": source_file,
                "resource_id": resource_id,
                "source_record_id": record.get("_id"),
                "source_area": metadata.get("source_area"),
                "source_area_name": metadata.get("source_area_name"),
                "metadata_contract_version": metadata.get("contract_version"),
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
    """Transform all selected raw files, or raise without returning a partial batch."""
    raw_dir = Path(raw_dir)
    if not S_ISDIR(raw_dir.stat().st_mode):
        raise NotADirectoryError(f"Raw energy input must be a directory: {raw_dir}")
    # Enumerate explicitly so a missing/unreadable directory is not an empty batch.
    inputs = sorted(path for path in raw_dir.iterdir() if path.name.endswith(".json"))
    records: list[dict[str, Any]] = []
    for filepath in inputs:
        try:
            with filepath.open("r", encoding="utf-8") as file_handle:
                raw_data = json.load(file_handle)
            records.extend(
                _build_records(
                    raw_json=raw_data,
                    source_file=filepath.name,
                    ingestion_ts=_parse_ingestion_timestamp(filepath),
                )
            )
        except Exception as exc:
            raise ValueError(f"Failed to process {filepath.name}: {exc}") from exc

    if not records:
        return pd.DataFrame(columns=ENERGY_CANONICAL_COLUMNS)

    df = pd.DataFrame(records)[ENERGY_CANONICAL_COLUMNS]
    return select_latest_records(
        df,
        keys=[
            "source_area",
            "resource_id",
            "source_record_id",
            "event_timestamp_utc",
        ],
    )


def save_clean_data(df: pd.DataFrame, output_path: Path = SILVER_DIR):
    """Publish complete silver energy partitions without replacing existing files."""
    if df.empty:
        print("No valid energy records to write.")
        return

    run_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_files = write_silver_partitions(
        df, output_path, dataset="energy", run_timestamp=run_timestamp
    )
    for output_file in output_files:
        print(f"Saved cleaned energy data to {output_file}")


def main():
    transformed_df = transform_energy_files()
    save_clean_data(transformed_df)


if __name__ == "__main__":
    main()

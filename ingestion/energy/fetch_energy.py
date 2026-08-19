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

from ingestion.common.api_client import fetch_ckan_resource
from ingestion.common.contract_validator import validate_payload
from ingestion.common.source_area import attach_pipeline_metadata, validate_source_binding

ENERGY_CONTRACT_PATH = PROJECT_ROOT / "data-contracts" / "energy_schema.json"


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


def build_headers(api_config: dict) -> dict:
    """Build headers for API requests, including optional API keys."""
    headers = {}
    api_key_env = api_config.get("api_key_env")
    api_key_header = api_config.get("api_key_header")
    if api_key_env or api_key_header:
        if not (api_key_env and api_key_header):
            raise ValueError(
                "Both api.api_key_env and api.api_key_header must be set together."
            )
        api_key_value = os.getenv(api_key_env)
        if not api_key_value:
            raise EnvironmentError(f"Environment variable {api_key_env} is not set.")
        headers[api_key_header] = api_key_value
    return headers


def resolve_binding(config: dict) -> dict[str, str]:
    params = config.get("api", {}).get("params", {})
    resource_id = params.get("resource_id")
    if not resource_id:
        raise ValueError("Missing api.params.resource_id in config.yaml.")
    return validate_source_binding(
        config.get("source_area"),
        nged_resource_id=resource_id,
    )


def fetch_energy(config: dict) -> dict:
    """Fetch one complete, bounded NGED electricity-demand snapshot."""
    binding = resolve_binding(config)
    api_config = config["api"]
    base_url = api_config["base_url"].rstrip("/")
    endpoint = api_config["endpoint"].lstrip("/")
    url = f"{base_url}/{endpoint}"
    params = dict(api_config.get("params", {}))
    legacy_limit = params.pop("limit", None)
    headers = build_headers(api_config)
    timeout_seconds = api_config.get("timeout_seconds", 30)
    page_size = api_config.get("page_size", legacy_limit or 1000)
    max_records = api_config.get("max_records", 50_000)

    payload = fetch_ckan_resource(
        url=url,
        params=params,
        headers=headers,
        timeout_seconds=timeout_seconds,
        page_size=page_size,
        max_records=max_records,
        validate_page=lambda page: validate_payload(
            page,
            ENERGY_CONTRACT_PATH,
            "energy",
        ),
        request_get=requests.get,
    )
    return attach_pipeline_metadata(payload, dataset_name="energy", binding=binding)


def save_raw_data(data: dict):
    """Save raw energy JSON to a timestamped file."""
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_dir = Path("data/raw/energy")
    output_dir.mkdir(parents=True, exist_ok=True)
    file_path = output_dir / f"energy_{timestamp}.json"

    with file_path.open("w", encoding="utf-8") as file_handle:
        json.dump(data, file_handle, indent=2)

    print(f"Saved raw energy data to {file_path}")


def main():
    config = load_config()
    energy_data = fetch_energy(config)
    validate_payload(energy_data, ENERGY_CONTRACT_PATH, "energy")
    save_raw_data(energy_data)


if __name__ == "__main__":
    main()

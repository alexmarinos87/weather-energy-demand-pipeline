import json
from copy import deepcopy
from functools import lru_cache
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SOURCE_AREAS_CONTRACT_PATH = PROJECT_ROOT / "data-contracts" / "source_areas.json"


class SourceAreaError(ValueError):
    """Raised when weather and NGED sources are not bound to the same area."""


def normalize_source_area(value: Any) -> str:
    if value is None:
        raise SourceAreaError("Missing source_area configuration.")
    normalized = str(value).strip().lower().replace("-", "_").replace(" ", "_")
    if not normalized:
        raise SourceAreaError("Missing source_area configuration.")
    return normalized


@lru_cache(maxsize=8)
def _load_contract(contract_path: str) -> dict[str, Any]:
    path = Path(contract_path)
    with path.open("r", encoding="utf-8") as file_handle:
        contract = json.load(file_handle)

    version = contract.get("contract_version")
    areas = contract.get("areas")
    if not isinstance(version, str) or not version.strip():
        raise SourceAreaError("source_areas.json must define contract_version.")
    if not isinstance(areas, dict) or not areas:
        raise SourceAreaError("source_areas.json must define at least one area.")

    resource_ids: set[str] = set()
    for area_key, area in areas.items():
        if normalize_source_area(area_key) != area_key:
            raise SourceAreaError(
                f"Source-area key {area_key!r} must already be normalized."
            )
        if not isinstance(area, dict):
            raise SourceAreaError(f"Source-area {area_key!r} must be an object.")
        required = {"display_name", "nged_resource_id", "weather_proxy_city"}
        missing = sorted(required - set(area))
        if missing:
            raise SourceAreaError(
                f"Source-area {area_key!r} is missing: {', '.join(missing)}."
            )
        resource_id = str(area["nged_resource_id"]).strip()
        if resource_id in resource_ids:
            raise SourceAreaError(
                f"NGED resource ID {resource_id!r} is assigned more than once."
            )
        resource_ids.add(resource_id)

    return contract


def load_source_area_contract(
    contract_path: Path = SOURCE_AREAS_CONTRACT_PATH,
) -> dict[str, Any]:
    """Load and validate the source-area mapping contract."""
    return deepcopy(_load_contract(str(contract_path.resolve())))


def resolve_source_area(
    source_area: Any,
    contract_path: Path = SOURCE_AREAS_CONTRACT_PATH,
) -> dict[str, str]:
    """Resolve a normalized source-area key to its canonical binding."""
    normalized = normalize_source_area(source_area)
    contract = load_source_area_contract(contract_path)
    area = contract["areas"].get(normalized)
    if area is None:
        supported = ", ".join(sorted(contract["areas"]))
        raise SourceAreaError(
            f"Unsupported source_area={normalized!r}. Supported values: {supported}."
        )
    return {
        "contract_version": contract["contract_version"],
        "source_area": normalized,
        "source_area_name": str(area["display_name"]),
        "nged_resource_id": str(area["nged_resource_id"]),
        "weather_proxy_city": str(area["weather_proxy_city"]),
    }


def validate_source_binding(
    source_area: Any,
    *,
    nged_resource_id: str | None = None,
    weather_city: str | None = None,
    contract_path: Path = SOURCE_AREAS_CONTRACT_PATH,
) -> dict[str, str]:
    """Reject cross-area source combinations before any network request."""
    binding = resolve_source_area(source_area, contract_path)

    if nged_resource_id is not None:
        actual_resource_id = str(nged_resource_id).strip()
        if actual_resource_id != binding["nged_resource_id"]:
            raise SourceAreaError(
                f"source_area={binding['source_area']!r} requires NGED resource "
                f"{binding['nged_resource_id']!r}, got {actual_resource_id!r}."
            )

    if weather_city is not None:
        actual_city = str(weather_city).strip()
        if actual_city.casefold() != binding["weather_proxy_city"].casefold():
            raise SourceAreaError(
                f"source_area={binding['source_area']!r} requires weather proxy "
                f"{binding['weather_proxy_city']!r}, got {actual_city!r}."
            )

    return binding


def attach_pipeline_metadata(
    payload: dict[str, Any],
    *,
    dataset_name: str,
    binding: dict[str, str],
) -> dict[str, Any]:
    """Attach source-area provenance without mutating the API response object."""
    if dataset_name not in {"weather", "energy"}:
        raise ValueError("dataset_name must be either 'weather' or 'energy'.")

    enriched = deepcopy(payload)
    enriched["_pipeline_metadata"] = {
        "contract_version": binding["contract_version"],
        "dataset": dataset_name,
        "source_area": binding["source_area"],
        "source_area_name": binding["source_area_name"],
        "nged_resource_id": binding["nged_resource_id"],
        "weather_proxy_city": binding["weather_proxy_city"],
    }
    return enriched

import json

import pytest

from ingestion.common.source_area import (
    SourceAreaError,
    attach_pipeline_metadata,
    load_source_area_contract,
    resolve_source_area,
    validate_source_binding,
)


def test_contract_has_four_unique_nged_licence_area_resources():
    contract = load_source_area_contract()

    assert set(contract["areas"]) == {
        "east_midlands",
        "south_wales",
        "south_west",
        "west_midlands",
    }
    resource_ids = [area["nged_resource_id"] for area in contract["areas"].values()]
    assert len(resource_ids) == len(set(resource_ids)) == 4


def test_binding_accepts_canonical_area_sources():
    binding = validate_source_binding(
        "East Midlands",
        nged_resource_id="92d3431c-15d7-4aa6-ad34-2335596a026c",
        weather_city="nottingham,gb",
    )

    assert binding["source_area"] == "east_midlands"
    assert binding["source_area_name"] == "East Midlands"


def test_binding_rejects_cross_area_resource_and_weather_proxy():
    with pytest.raises(SourceAreaError, match="requires NGED resource"):
        validate_source_binding(
            "east_midlands",
            nged_resource_id="38b81427-a2df-42f2-befa-4d6fe9b54c98",
        )

    with pytest.raises(SourceAreaError, match="requires weather proxy"):
        validate_source_binding("east_midlands", weather_city="London,GB")


def test_unsupported_area_reports_supported_values():
    with pytest.raises(SourceAreaError, match="Supported values"):
        resolve_source_area("north_west")


def test_metadata_is_attached_without_mutating_source_payload():
    payload = {"name": "Nottingham"}
    binding = resolve_source_area("east_midlands")

    enriched = attach_pipeline_metadata(
        payload,
        dataset_name="weather",
        binding=binding,
    )

    assert "_pipeline_metadata" not in payload
    assert enriched["_pipeline_metadata"]["source_area"] == "east_midlands"
    assert enriched["_pipeline_metadata"]["weather_proxy_city"] == "Nottingham,GB"
    json.dumps(enriched)

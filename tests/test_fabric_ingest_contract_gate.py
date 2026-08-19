import json
import runpy
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK_PATH = PROJECT_ROOT / "fabric" / "notebooks" / "01_ingest_api_to_bronze.py"


def _load_notebook_namespace() -> dict:
    return runpy.run_path(str(NOTEBOOK_PATH), run_name="fabric_ingest_notebook")


def _valid_weather_payload() -> dict:
    return {
        "dt": 1738800000,
        "name": "Nottingham",
        "cod": 200,
        "main": {"temp": 11.2, "feels_like": 9.8, "humidity": 82},
        "weather": [{"main": "Clouds", "description": "broken clouds"}],
        "wind": {"speed": 4.1},
        "clouds": {"all": 70},
    }


def _valid_energy_payload(records=None, total=1) -> dict:
    return {
        "help": "https://connecteddata.nationalgrid.co.uk/",
        "success": True,
        "result": {
            "resource_id": "92d3431c-15d7-4aa6-ad34-2335596a026c",
            "records": records if records is not None else [{"_id": 1}],
            "limit": 1000,
            "total": total,
        },
    }


def test_fabric_ingestion_validates_weather_with_json_contract():
    namespace = _load_notebook_namespace()
    payload = _valid_weather_payload()
    payload["cod"] = "200"

    with pytest.raises(ValueError, match="weather_schema.json"):
        namespace["_validate_payload"](payload, "weather")


def test_fabric_source_area_binding_rejects_cross_area_inputs():
    namespace = _load_notebook_namespace()

    with pytest.raises(namespace["SourceAreaError"], match="weather proxy"):
        namespace["_source_area_binding"](weather_city="London,GB")

    with pytest.raises(namespace["SourceAreaError"], match="requires NGED resource"):
        namespace["_source_area_binding"](
            resource_id="38b81427-a2df-42f2-befa-4d6fe9b54c98"
        )


def test_fabric_metadata_contains_same_area_provenance():
    namespace = _load_notebook_namespace()
    binding = namespace["_source_area_binding"](weather_city="Nottingham,GB")

    enriched = namespace["_attach_pipeline_metadata"](
        _valid_weather_payload(), "weather", binding
    )

    metadata = enriched["_pipeline_metadata"]
    assert metadata["source_area"] == "east_midlands"
    assert metadata["nged_resource_id"] == "92d3431c-15d7-4aa6-ad34-2335596a026c"


def test_fabric_ingestion_can_load_contracts_from_parameter(tmp_path):
    namespace = _load_notebook_namespace()
    contracts_root = tmp_path / "contracts"
    contracts_root.mkdir()
    contract_path = contracts_root / "weather_schema.json"
    contract_path.write_text(json.dumps({"type": "object"}), encoding="utf-8")
    namespace["_resolve_contract_path"].__globals__["CONTRACTS_ROOT"] = str(contracts_root)

    assert namespace["_resolve_contract_path"]("weather") == contract_path


def test_fabric_energy_fetch_paginates_to_starting_total():
    namespace = _load_notebook_namespace()
    resource_id = "92d3431c-15d7-4aa6-ad34-2335596a026c"
    pages = {
        0: _valid_energy_payload([{"_id": 1}, {"_id": 2}], total=3),
        2: _valid_energy_payload([{"_id": 3}], total=4),
    }
    calls = []

    class FakeResponse:
        def __init__(self, payload):
            self.payload = payload

        def raise_for_status(self):
            return None

        def json(self):
            return self.payload

    def fake_get(url, params, headers, timeout):
        calls.append(dict(params))
        return FakeResponse(pages[params["offset"]])

    result = namespace["_fetch_ckan_resource"](
        url="https://example.test/datastore_search",
        resource_id=resource_id,
        api_token="secret",
        page_size=2,
        max_records=10,
        validate_page=lambda payload: namespace["_validate_payload"](payload, "energy"),
        request_get=fake_get,
    )

    assert [record["_id"] for record in result["result"]["records"]] == [1, 2, 3]
    assert [call["offset"] for call in calls] == [0, 2]


def test_fabric_raw_writer_honours_files_root_parameter(tmp_path):
    namespace = _load_notebook_namespace()
    writer = namespace["_write_raw_json"]
    writer.__globals__["LAKEHOUSE_FILES_ROOT"] = str(tmp_path)

    written = Path(writer("weather", _valid_weather_payload()))

    assert written.is_file()
    assert written.is_relative_to(tmp_path / "raw" / "weather")

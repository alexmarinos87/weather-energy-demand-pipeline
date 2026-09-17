"""Public-boundary regressions for current source-area catalogue acceptance."""
from copy import deepcopy
import json
import os
from pathlib import Path

import pytest

from ingestion.common import source_area as source


def catalogue():
    return {"contract_version": "test-v1", "areas": {"east_midlands": {
        "display_name": "East Midlands", "nged_resource_id": "resource-01",
        "weather_proxy_city": "Nottingham,GB", "weather_proxy_latitude": 52.9548,
        "weather_proxy_longitude": -1.1581,
    }}}


def write(path, document):
    path.write_text(json.dumps(document), encoding="utf-8")
    return path


@pytest.fixture
def contract(tmp_path):
    return write(tmp_path / "areas.json", catalogue())


def test_changed_binding_is_seen_even_with_identical_size_and_mtime(contract):
    source.validate_source_binding("east_midlands", nged_resource_id="resource-01", contract_path=contract)
    before = contract.stat()
    changed = catalogue()
    changed["areas"]["east_midlands"]["nged_resource_id"] = "resource-02"
    write(contract, changed)
    os.utime(contract, ns=(before.st_atime_ns, before.st_mtime_ns))
    assert contract.stat().st_size == before.st_size
    assert contract.stat().st_mtime_ns == before.st_mtime_ns
    with pytest.raises(source.SourceAreaError, match="requires NGED resource"):
        source.validate_source_binding("east_midlands", nged_resource_id="resource-01", contract_path=contract)
    binding = source.validate_source_binding("east_midlands", nged_resource_id="resource-02", contract_path=contract)
    assert binding["nged_resource_id"] == "resource-02"


def test_changed_city_is_used_in_new_metadata_without_mutation(contract):
    source.load_source_area_contract(contract)
    changed = catalogue()
    changed["areas"]["east_midlands"]["weather_proxy_city"] = "New Proxy,GB"
    write(contract, changed)
    binding = source.resolve_source_area("East-Midlands", contract)
    payload = {"main": {"temp": 10.0}}
    original = deepcopy(payload)
    enriched = source.attach_pipeline_metadata(payload, dataset_name="weather", binding=binding)
    assert enriched["_pipeline_metadata"]["weather_proxy_city"] == "New Proxy,GB"
    assert payload == original


def test_deleted_contract_cannot_use_cached_binding(contract):
    source.load_source_area_contract(contract)
    contract.unlink()
    with pytest.raises(FileNotFoundError):
        source.resolve_source_area("east_midlands", contract)


def test_unreadable_contract_cannot_use_cached_binding(contract, monkeypatch):
    source.load_source_area_contract(contract)
    original = Path.open
    def deny(path, *args, **kwargs):
        if path == contract:
            raise PermissionError("injected contract read denial")
        return original(path, *args, **kwargs)
    monkeypatch.setattr(Path, "open", deny)
    with pytest.raises(PermissionError, match="injected contract"):
        source.load_source_area_contract(contract)


@pytest.mark.parametrize("replacement", [b"{", b"[]", b"null", b"\xff"])
def test_malformed_replacement_cannot_fall_back_to_valid_cache(contract, replacement):
    source.load_source_area_contract(contract)
    contract.write_bytes(replacement)
    with pytest.raises((source.SourceAreaError, UnicodeDecodeError)):
        source.load_source_area_contract(contract)


@pytest.mark.parametrize("field", ["display_name", "nged_resource_id", "weather_proxy_city"])
@pytest.mark.parametrize("invalid", [None, True, 12, "", " ", [], {}, " padded "])
def test_text_identity_fields_are_not_stringified(contract, field, invalid):
    document = catalogue()
    document["areas"]["east_midlands"][field] = invalid
    write(contract, document)
    with pytest.raises(source.SourceAreaError, match=field):
        source.resolve_source_area("east_midlands", contract)


@pytest.mark.parametrize("document", [[], None, "not-an-object", 1])
def test_root_type_is_a_domain_error(contract, document):
    write(contract, document)
    with pytest.raises(source.SourceAreaError, match="object"):
        source.load_source_area_contract(contract)


@pytest.mark.parametrize("level", ["root", "area"])
def test_duplicate_json_members_are_rejected(contract, level):
    text = json.dumps(catalogue())
    if level == "root":
        text = text.replace('"contract_version": "test-v1"', '"contract_version": "old", "contract_version": "test-v1"')
    else:
        text = text.replace('"nged_resource_id": "resource-01"', '"nged_resource_id": "wrong", "nged_resource_id": "resource-01"')
    contract.write_text(text, encoding="utf-8")
    with pytest.raises(source.SourceAreaError, match="Duplicate"):
        source.load_source_area_contract(contract)


@pytest.mark.parametrize("constant", ["NaN", "Infinity", "-Infinity", "1e999"])
def test_nonfinite_values_anywhere_in_catalogue_are_rejected(contract, constant):
    text = json.dumps(catalogue())[:-1] + ', "untrusted_extension": ' + constant + '}'
    contract.write_text(text, encoding="utf-8")
    with pytest.raises(source.SourceAreaError, match="finite"):
        source.load_source_area_contract(contract)


def test_returned_catalogue_and_binding_do_not_mutate_cache(contract):
    first = source.load_source_area_contract(contract)
    first["areas"]["east_midlands"]["nged_resource_id"] = "caller-mutation"
    binding = source.resolve_source_area("East Midlands", contract)
    binding["nged_resource_id"] = "another-mutation"
    assert source.resolve_source_area("east_midlands", contract)["nged_resource_id"] == "resource-01"


def test_removed_area_is_not_still_resolvable(contract):
    source.resolve_source_area("east_midlands", contract)
    document = catalogue()
    document["areas"]["south_wales"] = document["areas"].pop("east_midlands")
    write(contract, document)
    with pytest.raises(source.SourceAreaError, match="Unsupported source_area"):
        source.resolve_source_area("east_midlands", contract)
    assert source.resolve_source_area("south_wales", contract)["source_area"] == "south_wales"


def test_valid_finite_coordinates_and_duplicate_resource_checks_remain(contract):
    document = catalogue()
    document["areas"]["east_midlands"]["weather_proxy_latitude"] = "52.9548"
    write(contract, document)
    assert source.resolve_source_area("East Midlands", contract)["weather_proxy_latitude"] == 52.9548
    document["areas"]["south_wales"] = {**document["areas"]["east_midlands"], "weather_proxy_latitude": 51.4816}
    other = write(contract.with_name("duplicate-resource.json"), document)
    with pytest.raises(source.SourceAreaError, match="assigned more than once"):
        source.load_source_area_contract(other)

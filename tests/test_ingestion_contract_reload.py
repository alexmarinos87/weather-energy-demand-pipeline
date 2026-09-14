"""Schema content is authoritative even after an earlier cache hit."""
from copy import deepcopy
import json
import os

import pytest

from ingestion.common.contract_validator import (
    ContractValidationError, _get_validator, validate_payload,
)


def schema(minimum):
    return {"type": "object", "properties": {"value": {"type": "number", "minimum": minimum}},
            "required": ["value"]}


def write(path, value):
    path.write_text(json.dumps(value), encoding="utf-8")


def test_stricter_replacement_is_used_after_a_successful_cache_hit(tmp_path):
    path = tmp_path / "contract.json"
    write(path, schema(1))
    validate_payload({"value": 5}, path, "sample")
    write(path, schema(9))
    with pytest.raises(ContractValidationError, match="value"):
        validate_payload({"value": 5}, path, "sample")


def test_relaxed_replacement_is_not_blocked_by_stale_validator(tmp_path):
    path = tmp_path / "contract.json"
    write(path, schema(9))
    with pytest.raises(ContractValidationError):
        validate_payload({"value": 5}, path, "sample")
    write(path, schema(1))
    validate_payload({"value": 5}, path, "sample")


def test_content_not_size_or_mtime_controls_cache_identity(tmp_path):
    path = tmp_path / "contract.json"
    write(path, schema(1))
    validate_payload({"value": 5}, path, "sample")
    before = path.stat()
    write(path, schema(9))
    os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))
    assert path.stat().st_size == before.st_size
    assert path.stat().st_mtime_ns == before.st_mtime_ns
    with pytest.raises(ContractValidationError, match="value"):
        validate_payload({"value": 5}, path, "sample")


def test_deleted_contract_does_not_fall_back_to_cached_rules(tmp_path):
    path = tmp_path / "contract.json"
    write(path, schema(1))
    validate_payload({"value": 5}, path, "sample")
    path.unlink()
    with pytest.raises(FileNotFoundError):
        validate_payload({"value": 5}, path, "sample")


@pytest.mark.parametrize("replacement", ["", "{", '{"type": "not-a-json-schema-type"}',
                                          '{"type":"integer","type":"object"}'])
def test_malformed_replacement_cannot_reuse_previous_compilation(tmp_path, replacement):
    path = tmp_path / "contract.json"
    write(path, schema(1))
    validate_payload({"value": 5}, path, "sample")
    path.write_text(replacement, encoding="utf-8")
    before = path.read_bytes()
    with pytest.raises(ContractValidationError, match="contract"):
        validate_payload({"value": 5}, path, "sample")
    assert path.read_bytes() == before


@pytest.mark.parametrize("document", [
    '{"type":"integer","type":"object"}',
    '{"properties":{"value":{"minimum":9,"minimum":1}}}',
    '{"default":{"owner":"first","owner":"second"}}',
    '{"title":"first","ti\\u0074le":"second"}',
])
def test_duplicate_schema_members_are_rejected_at_any_depth(tmp_path, document):
    path = tmp_path / "contract.json"
    path.write_text(document, encoding="utf-8")
    with pytest.raises(ContractValidationError, match="contract"):
        validate_payload({"value": 5}, path, "sample")


@pytest.mark.parametrize("token", ["NaN", "Infinity", "-Infinity", "1e999"])
def test_nonfinite_schema_numbers_are_rejected_even_in_annotations(tmp_path, token):
    path = tmp_path / "contract.json"
    path.write_text('{"default":' + token + '}', encoding="utf-8")
    with pytest.raises(ContractValidationError, match="contract"):
        validate_payload({}, path, "sample")


def test_invalid_encoding_after_cache_hit_is_not_ignored(tmp_path):
    path = tmp_path / "contract.json"
    write(path, schema(1))
    validate_payload({"value": 5}, path, "sample")
    path.write_bytes(b"\xff\xfe\xff")
    with pytest.raises(ContractValidationError, match="contract"):
        validate_payload({"value": 5}, path, "sample")


def test_identical_content_still_reuses_compilation_without_mutation(tmp_path):
    path = tmp_path / "contract.json"
    write(path, schema(1))
    first = _get_validator(str(path))
    original = path.read_bytes()
    payload = {"value": 5, "metadata": {"label": "NA", "ids": ["0012"]}}
    before = deepcopy(payload)
    for _ in range(3):
        validate_payload(payload, path, "sample")
        assert _get_validator(str(path)) is first
    assert path.read_bytes() == original
    assert payload == before


@pytest.mark.parametrize("allowed", [True, False])
def test_boolean_schemas_remain_supported(tmp_path, allowed):
    path = tmp_path / "contract.json"
    write(path, allowed)
    if allowed:
        validate_payload({}, path, "sample")
    else:
        with pytest.raises(ContractValidationError):
            validate_payload({}, path, "sample")


def test_local_schema_references_and_existing_error_reporting_remain_usable(tmp_path):
    path = tmp_path / "contract.json"
    write(path, {"$defs": {"positive": {"type": "integer", "minimum": 1}},
                 "type": "object", "properties": {"value": {"$ref": "#/$defs/positive"}}})
    validate_payload({"value": 2}, path, "sample")
    with pytest.raises(ContractValidationError, match="value"):
        validate_payload({"value": -1}, path, "sample")


def test_invalid_payload_report_remains_bounded(tmp_path):
    path = tmp_path / "contract.json"
    write(path, {"type": "object", "required": [f"field-{n}" for n in range(8)]})
    with pytest.raises(ContractValidationError) as captured:
        validate_payload({}, path, "sample")
    message = str(captured.value)
    assert "with 8 issue(s)" in message
    assert "3 additional issue(s)" in message
    assert message.count("\n- ") == 6

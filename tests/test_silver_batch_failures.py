"""Exercise both local silver entry points without provider or storage services."""
from copy import deepcopy
import json
from pathlib import Path

import pandas as pd
import pytest

from transformations.silver import clean_energy, clean_weather


@pytest.fixture(params=["energy", "weather"])
def batch(request, tmp_path):
    dataset = request.param
    module = clean_energy if dataset == "energy" else clean_weather
    transform = getattr(module, f"transform_{dataset}_files")
    columns = getattr(module, f"{dataset.upper()}_CANONICAL_COLUMNS")
    if dataset == "energy":
        payload = {"result": {"resource_id": "resource-01", "records": [
            {"_id": 1, "Timestamp": "2026-01-01T00:00:00Z", "Demand": 900.0}
        ]}}
    else:
        payload = {"dt": 1767225600, "name": "Nottingham", "main": {"temp": 8.0},
                   "weather": [{"main": "Clouds", "description": "clouds"}]}
    root = tmp_path / dataset
    root.mkdir()
    good = root / f"{dataset}_20260101_010000.json"
    bad = root / f"{dataset}_20260101_020000.json"
    return dataset, module, transform, columns, payload, root, good, bad


def write(path, payload):
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_existing_empty_directory_has_canonical_empty_result(batch):
    _, _, transform, columns, _, root, _, _ = batch
    result = transform(root)
    assert result.empty
    assert list(result.columns) == columns


def test_valid_legacy_capture_preserves_provenance_and_bytes(batch):
    dataset, _, transform, columns, payload, root, good, _ = batch
    original_payload = deepcopy(payload)
    write(good, payload)
    original_bytes = good.read_bytes()
    result = transform(root)
    assert list(result.columns) == columns
    assert len(result) == 1
    assert result.loc[0, "source_file"] == good.name
    assert result.loc[0, "source_area"] is None
    assert result.loc[0, "event_timestamp_utc"] == pd.Timestamp("2026-01-01T00:00:00Z")
    assert result.loc[0, "ingestion_timestamp_utc"] == pd.Timestamp("2026-01-01T01:00:00Z")
    field = "demand_mw" if dataset == "energy" else "temperature_c"
    assert result.loc[0, field] == (900.0 if dataset == "energy" else 8.0)
    assert good.read_bytes() == original_bytes and payload == original_payload


def test_missing_input_root_is_not_an_empty_success(batch):
    _, _, transform, _, _, root, _, _ = batch
    with pytest.raises(FileNotFoundError):
        transform(root / "missing")


def test_file_used_as_input_root_is_rejected(batch):
    _, _, transform, _, _, root, _, _ = batch
    source = root / "not-a-directory"
    source.write_text("not a directory", encoding="utf-8")
    with pytest.raises(NotADirectoryError):
        transform(source)


@pytest.mark.parametrize("malformed", [b"{", b"\xff\xfe", b"[]", b"null"])
def test_one_bad_file_aborts_the_batch_with_original_cause(batch, malformed):
    _, _, transform, _, payload, root, good, bad = batch
    write(good, payload)
    bad.write_bytes(malformed)
    original = {path: path.read_bytes() for path in (good, bad)}
    with pytest.raises(ValueError, match=bad.name) as captured:
        transform(root)
    assert captured.value.__cause__ is not None
    if malformed == b"{":
        assert isinstance(captured.value.__cause__, json.JSONDecodeError)
    if malformed == b"\xff\xfe":
        assert isinstance(captured.value.__cause__, UnicodeDecodeError)
    assert {path: path.read_bytes() for path in (good, bad)} == original


def test_invalid_event_timestamp_does_not_silently_drop_a_file(batch):
    dataset, _, transform, _, payload, root, good, bad = batch
    write(good, payload)
    invalid = deepcopy(payload)
    if dataset == "energy":
        invalid["result"]["records"][0]["Timestamp"] = "not-a-time"
    else:
        invalid["dt"] = "not-an-epoch"
    write(bad, invalid)
    with pytest.raises(ValueError, match=bad.name):
        transform(root)


def test_unreadable_file_is_not_omitted_from_a_successful_batch(batch, monkeypatch):
    _, _, transform, _, payload, root, good, bad = batch
    write(good, payload)
    write(bad, payload)
    original_open = Path.open

    def deny(path, *args, **kwargs):
        if path == bad:
            raise PermissionError("injected read denial")
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", deny)
    with pytest.raises(ValueError, match=bad.name) as captured:
        transform(root)
    assert isinstance(captured.value.__cause__, PermissionError)


def test_main_never_calls_saver_after_an_input_failure(batch, monkeypatch):
    dataset, module, transform, _, payload, root, good, bad = batch
    write(good, payload)
    bad.write_text("{", encoding="utf-8")
    saved = []
    monkeypatch.setattr(module, f"transform_{dataset}_files", lambda: transform(root))
    monkeypatch.setattr(module, "save_clean_data", lambda frame: saved.append(frame))
    with pytest.raises(ValueError, match=bad.name):
        module.main()
    assert saved == []


def test_only_top_level_json_inputs_are_selected(batch):
    _, _, transform, _, payload, root, good, _ = batch
    write(good, payload)
    (root / "ignored.part").write_text("{", encoding="utf-8")
    nested = root / "nested"
    nested.mkdir()
    (nested / "ignored.json").write_text("{", encoding="utf-8")
    assert len(transform(root)) == 1


@pytest.mark.parametrize("envelope", [
    {}, {"result": {}}, {"result": None},
    {"result": {"records": {}}}, {"result": {"records": None}},
])
def test_malformed_energy_envelope_cannot_masquerade_as_empty(tmp_path, envelope):
    path = tmp_path / "energy_20260101_020000.json"
    write(path, envelope)
    with pytest.raises(ValueError, match=path.name):
        clean_energy.transform_energy_files(tmp_path)


def test_explicit_empty_energy_capture_remains_valid(tmp_path):
    path = tmp_path / "energy_20260101_020000.json"
    write(path, {"result": {"resource_id": "resource-01", "records": []}})
    result = clean_energy.transform_energy_files(tmp_path)
    assert result.empty
    assert list(result.columns) == clean_energy.ENERGY_CANONICAL_COLUMNS


def test_bad_later_energy_record_prevents_partial_file_or_batch(tmp_path):
    write(tmp_path / "energy_20260101_010000.json", {"result": {"records": [
        {"_id": 1, "Timestamp": "2026-01-01T00:00:00Z", "Demand": 900.0}
    ]}})
    path = tmp_path / "energy_20260101_020000.json"
    write(path, {"result": {"records": [
        {"_id": 2, "Timestamp": "2026-01-01T00:30:00Z", "Demand": 950.0},
        {"_id": 3, "Timestamp": "broken", "Demand": 1000.0},
    ]}})
    with pytest.raises(ValueError, match=path.name):
        clean_energy.transform_energy_files(tmp_path)

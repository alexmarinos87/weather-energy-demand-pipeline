"""Exercise conflict resolution through actual local silver transformations."""
from copy import deepcopy
import json
import os

import pytest

from transformations.silver import clean_energy, clean_weather

STAMP = 1767229200


def write(path, payload, *, age=0):
    path.write_text(json.dumps(payload), encoding="utf-8")
    instant = (STAMP + age) * 1_000_000_000
    os.utime(path, ns=(instant, instant))
    return path


@pytest.fixture(params=["energy", "weather"])
def batch(request, tmp_path):
    dataset = request.param
    metadata = {"source_area": "east_midlands", "source_area_name": "East Midlands", "contract_version": "1.0.0"}
    if dataset == "energy":
        module = clean_energy
        payload = {"result": {"resource_id": "resource-01", "records": [
            {"_id": 1, "Timestamp": "2026-01-01T00:00:00Z", "Demand": 900.0}
        ]}, "_pipeline_metadata": metadata}
        measure, column = "Demand", "demand_mw"
    else:
        module = clean_weather
        payload = {"id": 1, "dt": 1767225600, "name": "Nottingham", "main": {"temp": 10.0},
                   "weather": [{"main": "Clouds", "description": "cloudy"}], "_pipeline_metadata": metadata}
        measure, column = "temp", "temperature_c"
    transform = getattr(module, f"transform_{dataset}_files")
    return dataset, module, transform, payload, measure, column, tmp_path


def change_measure(dataset, payload, key, value):
    result = deepcopy(payload)
    target = result["result"]["records"][0] if dataset == "energy" else result["main"]
    target[key] = value
    return result


@pytest.mark.parametrize("difference", ["measurement", "metadata", "optional-null"])
def test_latest_ties_with_conflicting_canonical_values_are_rejected(batch, difference):
    dataset, _, transform, payload, measure, _, root = batch
    changed = deepcopy(payload)
    if difference == "measurement":
        changed = change_measure(dataset, payload, measure, 42.0)
    elif difference == "metadata":
        changed["_pipeline_metadata"]["source_area_name"] = "Contradictory label"
    elif dataset == "energy":
        changed["result"]["records"][0]["Generation"] = 100.0
    else:
        changed["main"]["pressure"] = 1000.0
    first = write(root / f"{dataset}_a.json", payload)
    second = write(root / f"{dataset}_z.json", changed)
    original = {p: p.read_bytes() for p in (first, second)}
    with pytest.raises(ValueError, match="Conflicting latest") as captured:
        transform(root)
    assert first.name in str(captured.value)
    assert second.name in str(captured.value)
    assert {p: p.read_bytes() for p in original} == original


@pytest.mark.parametrize("reverse_creation", [False, True])
def test_equal_latest_captures_choose_deterministic_filename_only_for_provenance(batch, reverse_creation):
    dataset, _, transform, payload, _, _, root = batch
    names = [f"{dataset}_a.json", f"{dataset}_z.json"]
    for name in reversed(names) if reverse_creation else names:
        write(root / name, payload)
    first = transform(root)
    second = transform(root)
    assert len(first) == 1
    assert first.loc[0, "source_file"] == names[-1]
    assert first.equals(second)


def test_strictly_newer_capture_supersedes_old_conflicts(batch):
    dataset, _, transform, payload, measure, column, root = batch
    write(root / f"{dataset}_x.json", payload)
    write(root / f"{dataset}_z.json", change_measure(dataset, payload, measure, 42.0))
    write(root / f"{dataset}_a.json", change_measure(dataset, payload, measure, 55.0), age=60)
    result = transform(root)
    assert len(result) == 1
    assert result.loc[0, column] == 55.0
    assert result.loc[0, "source_file"] == f"{dataset}_a.json"


@pytest.mark.parametrize("conflicting", [False, True])
def test_legacy_null_area_is_grouped_not_dropped(batch, conflicting):
    dataset, _, transform, payload, measure, _, root = batch
    payload.pop("_pipeline_metadata")
    changed = change_measure(dataset, payload, measure, 42.0) if conflicting else deepcopy(payload)
    write(root / f"{dataset}_a.json", payload)
    write(root / f"{dataset}_z.json", changed)
    if conflicting:
        with pytest.raises(ValueError, match="Conflicting latest"):
            transform(root)
    else:
        result = transform(root)
        assert len(result) == 1
        assert result.loc[0, "source_area"] is None


def test_distinct_area_keys_do_not_create_false_conflicts(batch):
    dataset, _, transform, payload, measure, _, root = batch
    changed = change_measure(dataset, payload, measure, 42.0)
    changed["_pipeline_metadata"]["source_area"] = "south_wales"
    write(root / f"{dataset}_a.json", payload)
    write(root / f"{dataset}_z.json", changed)
    assert set(transform(root)["source_area"]) == {"east_midlands", "south_wales"}


def test_main_does_not_save_a_conflicting_result(batch, monkeypatch):
    dataset, module, transform, payload, measure, _, root = batch
    write(root / f"{dataset}_a.json", payload)
    write(root / f"{dataset}_z.json", change_measure(dataset, payload, measure, 42.0))
    saved = []
    monkeypatch.setattr(module, f"transform_{dataset}_files", lambda: transform(root))
    monkeypatch.setattr(module, "save_clean_data", lambda frame: saved.append(frame))
    with pytest.raises(ValueError, match="Conflicting latest"):
        module.main()
    assert not saved


@pytest.mark.parametrize("conflicting", [False, True])
def test_energy_duplicate_records_in_one_capture(tmp_path, conflicting):
    row = {"_id": 1, "Timestamp": "2026-01-01T00:00:00Z", "Demand": 900.0}
    other = {**row, "Demand": 950.0 if conflicting else 900.0}
    path = write(tmp_path / "energy_20260101_010000.json", {"result": {"resource_id": "r1", "records": [row, other]}})
    if conflicting:
        with pytest.raises(ValueError, match="Conflicting latest") as captured:
            clean_energy.transform_energy_files(tmp_path)
        assert path.name in str(captured.value)
    else:
        result = clean_energy.transform_energy_files(tmp_path)
        assert len(result) == 1
        assert result.loc[0, "demand_mw"] == 900.0


def test_energy_record_and_resource_keys_remain_independent(tmp_path):
    for index, (resource, identifier) in enumerate([("r1", 1), ("r1", 2), ("r2", 1)]):
        write(tmp_path / f"energy_{index}.json", {"result": {"resource_id": resource, "records": [
            {"_id": identifier, "Timestamp": "2026-01-01T00:00:00Z", "Demand": 900.0 + index}
        ]}})
    assert len(clean_energy.transform_energy_files(tmp_path)) == 3


def test_weather_provider_id_disagreement_is_not_resolved_by_filename(tmp_path):
    for index in (1, 2):
        write(tmp_path / f"weather_{index}.json", {"id": index, "dt": 1767225600, "name": "Nottingham", "main": {"temp": 10.0}})
    with pytest.raises(ValueError, match="Conflicting latest"):
        clean_weather.transform_weather_files(tmp_path)

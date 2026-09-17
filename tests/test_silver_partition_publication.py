"""Test both real silver savers using canonical frames from their transformers."""
from datetime import datetime, timezone
import json
from pathlib import Path

import pandas as pd
import pytest

from transformations.silver import clean_energy, clean_weather

RUN_STAMP = "20260103_010000"
DAYS = ("2026-01-01", "2026-01-02")


class FrozenDatetime(datetime):
    @classmethod
    def now(cls, tz=None):
        return datetime(2026, 1, 3, 1, tzinfo=timezone.utc)


@pytest.fixture(params=["energy", "weather"])
def batch(request, tmp_path, monkeypatch):
    dataset = request.param
    module = clean_energy if dataset == "energy" else clean_weather
    raw = tmp_path / "raw"
    raw.mkdir()
    for index, day in enumerate(DAYS):
        if dataset == "energy":
            payload = {"result": {"resource_id": "resource-01", "records": [
                {"_id": index + 1, "Timestamp": f"{day}T00:00:00Z", "Demand": 900.0 + index}
            ]}}
        else:
            payload = {"id": 1, "dt": int(pd.Timestamp(day, tz="UTC").timestamp()),
                       "name": "Nottingham", "main": {"temp": 10.0 + index}}
        (raw / f"{dataset}_20260103_00000{index}.json").write_text(json.dumps(payload), encoding="utf-8")
    frame = getattr(module, f"transform_{dataset}_files")(raw)
    monkeypatch.setattr(module, "datetime", FrozenDatetime)
    output = tmp_path / "silver"
    paths = [output / f"dt={day}" / f"{dataset}_clean_{RUN_STAMP}.parquet" for day in DAYS]
    return module, frame, output, paths


def files(root):
    return {path for path in root.rglob("*") if path.is_file() or path.is_symlink()}


def test_success_keeps_existing_paths_values_and_input_frame(batch, capsys):
    module, frame, output, paths = batch
    original = frame.copy(deep=True)
    assert module.save_clean_data(frame, output) is None
    assert files(output) == set(paths)
    for day, path in zip(DAYS, paths):
        with path.open("rb") as handle:
            saved = pd.read_parquet(handle)
        expected = frame.loc[frame["event_date_utc"].eq(day)].reset_index(drop=True)
        pd.testing.assert_frame_equal(saved, expected)
    pd.testing.assert_frame_equal(frame, original)
    assert capsys.readouterr().out.count("Saved cleaned") == 2


@pytest.mark.parametrize("position", [0, 1])
def test_existing_partition_prevents_any_new_publication(batch, position, capsys):
    module, frame, output, paths = batch
    retained = paths[position]
    retained.parent.mkdir(parents=True)
    retained.write_bytes(b"previous-evidence")
    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        module.save_clean_data(frame, output)
    assert retained.read_bytes() == b"previous-evidence"
    assert files(output) == {retained}
    assert "Saved cleaned" not in capsys.readouterr().out


@pytest.mark.parametrize("position", [0, 1])
def test_dangling_partition_symlink_is_preserved(batch, position):
    module, frame, output, paths = batch
    retained = paths[position]
    retained.parent.mkdir(parents=True)
    outside = output.parent / "outside.parquet"
    retained.symlink_to(outside)
    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        module.save_clean_data(frame, output)
    assert retained.is_symlink()
    assert not outside.exists()
    assert files(output) == {retained}


def test_same_second_retry_does_not_replace_evidence(batch):
    module, frame, output, paths = batch
    module.save_clean_data(frame, output)
    before = {path: path.read_bytes() for path in paths}
    with pytest.raises(FileExistsError):
        module.save_clean_data(frame, output)
    assert {path: path.read_bytes() for path in paths} == before


def test_later_serialization_failure_publishes_nothing(batch, monkeypatch, capsys):
    module, frame, output, _ = batch
    original = pd.DataFrame.to_parquet
    def fail_second(partition, destination, *args, **kwargs):
        if partition["event_date_utc"].iloc[0] == DAYS[1]:
            if hasattr(destination, "write"):
                destination.write(b"partial-parquet")
            else:
                Path(destination).write_bytes(b"partial-parquet")
            raise OSError("injected serialization failure")
        return original(partition, destination, *args, **kwargs)
    monkeypatch.setattr(pd.DataFrame, "to_parquet", fail_second)
    with pytest.raises(OSError, match="injected serialization"):
        module.save_clean_data(frame, output)
    assert not files(output)
    assert not list(output.glob(".silver-stage-*"))
    assert "Saved cleaned" not in capsys.readouterr().out


def test_competing_later_file_is_preserved_and_own_files_rolled_back(batch, monkeypatch):
    module, frame, output, paths = batch
    original = pd.DataFrame.to_parquet
    def competitor(partition, *args, **kwargs):
        if partition["event_date_utc"].iloc[0] == DAYS[1]:
            paths[1].parent.mkdir(parents=True, exist_ok=True)
            paths[1].write_bytes(b"competing-evidence")
        return original(partition, *args, **kwargs)
    monkeypatch.setattr(pd.DataFrame, "to_parquet", competitor)
    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        module.save_clean_data(frame, output)
    assert paths[1].read_bytes() == b"competing-evidence"
    assert files(output) == {paths[1]}


@pytest.mark.parametrize("after_link", [False, True])
def test_interrupted_publication_rolls_back_even_after_link_creation(batch, monkeypatch, after_link, capsys):
    module, frame, output, paths = batch
    original = Path.hardlink_to
    def interrupt(destination, source):
        if destination == paths[1]:
            if after_link:
                original(destination, source)
            raise KeyboardInterrupt("injected publication interruption")
        return original(destination, source)
    monkeypatch.setattr(Path, "hardlink_to", interrupt)
    with pytest.raises(KeyboardInterrupt, match="injected publication"):
        module.save_clean_data(frame, output)
    assert not files(output)
    assert "Saved cleaned" not in capsys.readouterr().out


def test_rollback_preserves_replacement_owned_by_another_writer(batch, monkeypatch):
    module, frame, output, paths = batch
    original = Path.hardlink_to
    def replace_then_fail(destination, source):
        if destination == paths[1]:
            paths[0].unlink()
            paths[0].write_bytes(b"replacement-evidence")
            raise OSError("injected failure after replacement")
        return original(destination, source)
    monkeypatch.setattr(Path, "hardlink_to", replace_then_fail)
    with pytest.raises(OSError, match="injected failure"):
        module.save_clean_data(frame, output)
    assert paths[0].read_bytes() == b"replacement-evidence"
    assert files(output) == {paths[0]}


@pytest.mark.parametrize("invalid", [None, "", "2026-02-30", "20260101", "../../outside", 20260101])
def test_bad_partition_labels_fail_before_any_output(batch, invalid):
    module, frame, output, _ = batch
    frame.loc[frame.index[-1], "event_date_utc"] = invalid
    with pytest.raises(ValueError, match="event_date_utc"):
        module.save_clean_data(frame, output)
    assert not output.exists()


def test_empty_frame_remains_a_noop(batch):
    module, frame, output, _ = batch
    assert module.save_clean_data(frame.iloc[:0], output) is None
    assert not output.exists()

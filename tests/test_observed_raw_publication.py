"""Use the public observed-weather/energy savers with no provider calls."""
from copy import deepcopy
from datetime import datetime, timezone
import json
from pathlib import Path

import pytest

from ingestion.energy import fetch_energy as energy
from ingestion.weather import fetch_weather as weather


class FrozenClock(datetime):
    @classmethod
    def now(cls, tz=None):
        return cls(2026, 1, 20, 12, 0, tzinfo=timezone.utc)


@pytest.fixture(params=[("weather", weather), ("energy", energy)], ids=["weather", "energy"])
def saver(request, tmp_path, monkeypatch):
    dataset, adapter = request.param
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(adapter, "datetime", FrozenClock)
    root = Path("data/raw") / dataset
    root.mkdir(parents=True)
    return adapter.save_raw_data, root / f"{dataset}_20260120_120000.json"


def assert_files(destination, expected):
    assert set(destination.parent.iterdir()) == set(expected)


def test_success_preserves_existing_filename_content_and_input(saver, capsys):
    save, destination = saver
    payload = {"name": "Ath\u00e9na", "records": [{"id": "0012", "value": 5.5}], "flag": False}
    original = deepcopy(payload)
    assert save(payload) is None
    assert json.loads(destination.read_text(encoding="utf-8")) == original
    assert payload == original
    assert_files(destination, [destination])
    assert "Saved raw " in capsys.readouterr().out


def test_repeated_same_second_capture_cannot_replace_first(saver, capsys):
    save, destination = saver
    save({"capture": "first"})
    original = destination.read_bytes()
    capsys.readouterr()
    with pytest.raises(FileExistsError):
        save({"capture": "second"})
    assert destination.read_bytes() == original
    assert_files(destination, [destination])
    assert "Saved raw " not in capsys.readouterr().out


@pytest.mark.parametrize("kind", ["file", "symlink", "dangling-symlink"])
def test_existing_evidence_and_symlinks_are_preserved(saver, kind, capsys):
    save, destination = saver
    target = destination.parent.parent / "outside.json"
    if kind == "file":
        destination.write_bytes(b"retained-evidence")
    else:
        if kind == "symlink":
            target.write_bytes(b"outside-evidence")
        destination.symlink_to(target.resolve())
    with pytest.raises(FileExistsError):
        save({"replacement": True})
    if kind == "file":
        assert destination.read_bytes() == b"retained-evidence"
    else:
        assert destination.is_symlink()
        if kind == "symlink":
            assert target.read_bytes() == b"outside-evidence"
        else:
            assert not target.exists()
    assert_files(destination, [destination])
    assert "Saved raw " not in capsys.readouterr().out


def test_unserializable_value_leaves_no_partial_file(saver, capsys):
    save, destination = saver
    with pytest.raises(TypeError):
        save({"valid-prefix": 1, "invalid": object()})
    assert_files(destination, [])
    assert "Saved raw " not in capsys.readouterr().out


@pytest.mark.parametrize("value", [float("nan"), float("inf"), -float("inf")])
def test_nonfinite_numbers_cannot_be_published_as_json(saver, value):
    save, destination = saver
    with pytest.raises(ValueError):
        save({"observed": value})
    assert_files(destination, [])


@pytest.mark.parametrize("failure", [OSError, KeyboardInterrupt])
def test_serialization_interruption_removes_staging(saver, failure, monkeypatch, capsys):
    save, destination = saver
    unrelated = destination.parent / "keep.txt"
    unrelated.write_bytes(b"keep")

    def interrupt(payload, handle, **kwargs):
        handle.write('{"partial":')
        raise failure("injected serialization interruption")

    monkeypatch.setattr(json, "dump", interrupt)
    with pytest.raises(failure, match="injected serialization"):
        save({"value": 1})
    assert_files(destination, [unrelated])
    assert unrelated.read_bytes() == b"keep"
    assert "Saved raw " not in capsys.readouterr().out


def test_final_filename_is_not_visible_during_serialization(saver, monkeypatch):
    save, destination = saver
    original = json.dump

    def inspect(payload, handle, **kwargs):
        assert not destination.exists(), "Incomplete raw evidence is publicly visible"
        assert list(destination.parent.rglob("*.json")) == []
        return original(payload, handle, **kwargs)

    monkeypatch.setattr(json, "dump", inspect)
    save({"value": 1})
    assert_files(destination, [destination])


def test_competing_destination_is_not_overwritten_or_deleted(saver, monkeypatch):
    save, destination = saver
    original = json.dump

    def compete(payload, handle, **kwargs):
        destination.write_bytes(b"competing-evidence")
        return original(payload, handle, **kwargs)

    monkeypatch.setattr(json, "dump", compete)
    with pytest.raises(FileExistsError):
        save({"value": 1})
    assert destination.read_bytes() == b"competing-evidence"
    assert_files(destination, [destination])


def test_interrupt_after_link_creation_removes_only_own_output(saver, monkeypatch):
    save, destination = saver
    original = Path.hardlink_to

    def interrupt(path, source):
        original(path, source)
        raise KeyboardInterrupt("injected post-link interruption")

    monkeypatch.setattr(Path, "hardlink_to", interrupt)
    with pytest.raises(KeyboardInterrupt, match="post-link"):
        save({"value": 1})
    assert_files(destination, [])


def test_unsupported_hardlinks_fail_without_overwriting_fallback(saver, monkeypatch):
    save, destination = saver

    def unavailable(*args, **kwargs):
        raise OSError("hardlinks unavailable")

    monkeypatch.setattr(Path, "hardlink_to", unavailable)
    with pytest.raises(OSError, match="hardlinks unavailable"):
        save({"value": 1})
    assert_files(destination, [])

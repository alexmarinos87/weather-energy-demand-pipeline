"""Exercise the real forecast savers with offline adapter-produced evidence."""
from copy import deepcopy
from datetime import datetime, timezone
import json
from pathlib import Path

import pandas as pd
import pytest

from ingestion.forecast_weather import fetch_openweather_forecast as adapter
from test_openweather_forecast_ingestion import FakeResponse, _config, _payload


@pytest.fixture(params=["raw", "normalized"])
def output_case(request, monkeypatch, tmp_path):
    monkeypatch.setattr(adapter, "get_api_key", lambda config: "dummy-publication-key")
    raw = adapter.fetch_openweather_forecast(
        _config(), request_get=lambda *a, **k: FakeResponse(_payload()),
        retrieved_at_utc=datetime(2026, 1, 1, 0, 5, tzinfo=timezone.utc),
    )
    kind = request.param
    records = adapter.normalize_openweather_forecast(raw)
    data = raw if kind == "raw" else records
    root = tmp_path / kind
    snapshot = raw["_pipeline_metadata"]["raw_snapshot_id"][:12]
    name = (f"openweather_forecast_20260101_000500_{snapshot}.json" if kind == "raw"
            else f"forecast_weather_20260101_000500_{snapshot}.parquet")
    final = root / "ingestion_date=2026-01-01" / name
    saver = adapter.save_raw_snapshot if kind == "raw" else adapter.save_normalized_forecast
    return kind, data, root, final, saver


def files(root):
    return {path for path in root.rglob("*") if path.is_file() or path.is_symlink()}


def metadata(case):
    kind, data, *_ = case
    return [data["_pipeline_metadata"]] if kind == "raw" else data


def test_success_round_trip_preserves_payload_columns_and_return_path(output_case):
    kind, data, root, final, saver = output_case
    before = deepcopy(data)
    assert saver(data, output_root=root) == final
    assert files(root) == {final}
    assert data == before
    if kind == "raw":
        assert final.read_text(encoding="utf-8") == json.dumps(data, indent=2, sort_keys=True)
    else:
        expected = pd.DataFrame(data)
        for name in ("forecast_issued_at_utc", "forecast_ingested_at_utc",
                     "forecast_valid_at_utc", "forecast_retrieved_at_utc"):
            expected[name] = pd.to_datetime(expected[name], utc=True)
        with final.open("rb") as handle:
            pd.testing.assert_frame_equal(pd.read_parquet(handle), expected)


def test_existing_destination_is_never_replaced(output_case):
    _, data, root, final, saver = output_case
    final.parent.mkdir(parents=True)
    final.write_bytes(b"retained-evidence")
    with pytest.raises(FileExistsError):
        saver(data, output_root=root)
    assert final.read_bytes() == b"retained-evidence"
    assert files(root) == {final}


def test_dangling_final_symlink_is_preserved(output_case):
    _, data, root, final, saver = output_case
    final.parent.mkdir(parents=True)
    outside = root.parent / "unrelated"
    final.symlink_to(outside)
    with pytest.raises(FileExistsError):
        saver(data, output_root=root)
    assert final.is_symlink()
    assert not outside.exists()
    assert files(root) == {final}


def test_serialization_failure_leaves_no_published_or_staged_file(output_case, monkeypatch):
    kind, data, root, final, saver = output_case
    if kind == "raw":
        data["unserializable_test_value"] = object()
        error = TypeError
    else:
        def fail(frame, destination, *args, **kwargs):
            assert not final.exists(), "Final path must not expose a partial file"
            if hasattr(destination, "write"):
                destination.write(b"partial-parquet")
            else:
                Path(destination).write_bytes(b"partial-parquet")
            raise OSError("injected serialization failure")
        monkeypatch.setattr(pd.DataFrame, "to_parquet", fail)
        error = OSError
    with pytest.raises(error):
        saver(data, output_root=root)
    assert not files(root)


@pytest.mark.parametrize("after_link", [False, True])
def test_caught_interruption_before_or_after_publication_cleans_own_files(output_case, monkeypatch, after_link):
    _, data, root, final, saver = output_case
    original = Path.hardlink_to
    def interrupted(destination, source):
        if destination == final:
            if after_link:
                original(destination, source)
            raise KeyboardInterrupt("injected publication interruption")
        return original(destination, source)
    monkeypatch.setattr(Path, "hardlink_to", interrupted)
    with pytest.raises(KeyboardInterrupt, match="injected publication"):
        saver(data, output_root=root)
    assert not files(root)


def test_competing_file_created_at_publication_is_preserved(output_case, monkeypatch):
    _, data, root, final, saver = output_case
    original = Path.hardlink_to
    def compete(destination, source):
        if destination == final:
            destination.write_bytes(b"competing-evidence")
        return original(destination, source)
    monkeypatch.setattr(Path, "hardlink_to", compete)
    with pytest.raises(FileExistsError):
        saver(data, output_root=root)
    assert final.read_bytes() == b"competing-evidence"
    assert files(root) == {final}


def test_rollback_preserves_another_writers_replacement(output_case, monkeypatch):
    _, data, root, final, saver = output_case
    original = Path.hardlink_to
    def replaced(destination, source):
        original(destination, source)
        destination.unlink()
        destination.write_bytes(b"replacement-evidence")
        raise OSError("injected failure after replacement")
    monkeypatch.setattr(Path, "hardlink_to", replaced)
    with pytest.raises(OSError, match="injected failure"):
        saver(data, output_root=root)
    assert final.read_bytes() == b"replacement-evidence"
    assert files(root) == {final}


@pytest.mark.parametrize("invalid", [None, "", "a" * 12, "A" * 64, "../outside/../" + "a" * 64])
def test_invalid_snapshot_identity_fails_before_output(output_case, invalid):
    _, data, root, _, saver = output_case
    for item in metadata(output_case):
        item["raw_snapshot_id"] = invalid
    with pytest.raises(adapter.OpenWeatherForecastError, match="raw_snapshot_id"):
        saver(data, output_root=root)
    assert not root.exists()


@pytest.mark.parametrize("invalid", [None, "NaT", "invalid-time", "2026-01-01T00:05:00"])
def test_invalid_retrieval_time_fails_before_output(output_case, invalid):
    kind, data, root, _, saver = output_case
    key = "retrieved_at_utc" if kind == "raw" else "forecast_retrieved_at_utc"
    for item in metadata(output_case):
        item[key] = invalid
    with pytest.raises(adapter.OpenWeatherForecastError, match="retriev"):
        saver(data, output_root=root)
    assert not root.exists()


def test_equivalent_offset_uses_the_same_utc_partition_and_filename(output_case):
    kind, data, root, final, saver = output_case
    key = "retrieved_at_utc" if kind == "raw" else "forecast_retrieved_at_utc"
    for item in metadata(output_case):
        item[key] = "2026-01-01T01:05:00+01:00"
    assert saver(data, output_root=root) == final


def test_parquet_race_during_serialization_does_not_replace_competitor(output_case, monkeypatch):
    kind, data, root, final, saver = output_case
    if kind == "raw":
        # Raw's ordinary collision case is also checked here without skipping a case.
        final.parent.mkdir(parents=True)
        final.write_bytes(b"competing-evidence")
    else:
        original = pd.DataFrame.to_parquet
        def compete(frame, destination, *args, **kwargs):
            result = original(frame, destination, *args, **kwargs)
            final.write_bytes(b"competing-evidence")
            return result
        monkeypatch.setattr(pd.DataFrame, "to_parquet", compete)
    with pytest.raises(FileExistsError):
        saver(data, output_root=root)
    assert final.read_bytes() == b"competing-evidence"
    assert files(root) == {final}


def test_normalized_mixed_retrieval_metadata_is_rejected(monkeypatch, tmp_path):
    monkeypatch.setattr(adapter, "get_api_key", lambda config: "dummy-key")
    raw = adapter.fetch_openweather_forecast(
        _config(), request_get=lambda *a, **k: FakeResponse(_payload()),
        retrieved_at_utc=datetime(2026, 1, 1, 0, 5, tzinfo=timezone.utc),
    )
    records = adapter.normalize_openweather_forecast(raw)
    records[1]["forecast_retrieved_at_utc"] = "2026-01-01T00:06:00+00:00"
    root = tmp_path / "normalized"
    with pytest.raises(adapter.OpenWeatherForecastError, match="retriev"):
        adapter.save_normalized_forecast(records, output_root=root)
    assert not root.exists()

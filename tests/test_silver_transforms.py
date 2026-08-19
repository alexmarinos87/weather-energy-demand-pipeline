import json

from transformations.silver import clean_energy, clean_weather


def _write_json(path, payload):
    with path.open("w", encoding="utf-8") as file_handle:
        json.dump(payload, file_handle)


def _metadata(area="east_midlands"):
    return {
        "contract_version": "1.0.0",
        "dataset": "weather",
        "source_area": area,
        "source_area_name": "East Midlands",
        "nged_resource_id": "92d3431c-15d7-4aa6-ad34-2335596a026c",
        "weather_proxy_city": "Nottingham,GB",
    }


def test_weather_transform_propagates_area_metadata_and_utc(tmp_path):
    raw_dir = tmp_path / "raw_weather"
    raw_dir.mkdir()
    payload = {
        "id": 2641170,
        "dt": 1704067200,
        "name": "Nottingham",
        "coord": {"lat": 52.95, "lon": -1.15},
        "sys": {"country": "GB"},
        "main": {"temp": 9.32, "feels_like": 7.97, "humidity": 87, "pressure": 1005},
        "weather": [{"main": "Clouds", "description": "few clouds"}],
        "wind": {"speed": 2.57},
        "clouds": {"all": 20},
        "_pipeline_metadata": _metadata(),
    }
    _write_json(raw_dir / "weather_20260208_185041.json", payload)

    df = clean_weather.transform_weather_files(raw_dir)

    assert list(df.columns) == clean_weather.WEATHER_CANONICAL_COLUMNS
    assert df.loc[0, "source_area"] == "east_midlands"
    assert df.loc[0, "metadata_contract_version"] == "1.0.0"
    assert str(df["event_timestamp_utc"].dtype) == "datetime64[ns, UTC]"


def test_weather_transform_keeps_legacy_raw_with_null_area(tmp_path):
    raw_dir = tmp_path / "raw_weather"
    raw_dir.mkdir()
    payload = {
        "id": 2643743,
        "dt": 1704067200,
        "name": "London",
        "main": {"temp": 10.0, "feels_like": 9.0, "humidity": 80},
        "weather": [{"main": "Clouds", "description": "broken clouds"}],
        "wind": {"speed": 4.0},
        "clouds": {"all": 70},
    }
    _write_json(raw_dir / "weather_20260208_120000.json", payload)

    df = clean_weather.transform_weather_files(raw_dir)

    assert len(df) == 1
    assert df.loc[0, "source_area"] is None


def test_weather_deduplication_is_scoped_by_source_area(tmp_path):
    raw_dir = tmp_path / "raw_weather"
    raw_dir.mkdir()
    base = {
        "id": 1,
        "dt": 1704067200,
        "name": "Shared Proxy",
        "main": {"temp": 10.0, "feels_like": 9.0, "humidity": 80},
        "weather": [{"main": "Clouds", "description": "clouds"}],
        "wind": {"speed": 4.0},
        "clouds": {"all": 70},
    }
    first = dict(base)
    first["_pipeline_metadata"] = _metadata("east_midlands")
    second = dict(base)
    second["_pipeline_metadata"] = {
        **_metadata("south_wales"),
        "source_area_name": "South Wales",
    }
    _write_json(raw_dir / "weather_20260208_120000.json", first)
    _write_json(raw_dir / "weather_20260208_130000.json", second)

    df = clean_weather.transform_weather_files(raw_dir)

    assert len(df) == 2
    assert set(df["source_area"]) == {"east_midlands", "south_wales"}


def test_energy_transform_propagates_area_metadata_and_deduplicates(tmp_path):
    raw_dir = tmp_path / "raw_energy"
    raw_dir.mkdir()
    resource_id = "92d3431c-15d7-4aa6-ad34-2335596a026c"
    first_payload = {
        "help": "https://example.test",
        "success": True,
        "result": {
            "resource_id": resource_id,
            "records": [{"_id": 1, "Timestamp": "2025-08-23T22:50:00", "Demand": 2437.38}],
            "limit": 1000,
            "total": 1,
        },
        "_pipeline_metadata": {**_metadata(), "dataset": "energy"},
    }
    second_payload = {
        **first_payload,
        "result": {
            "resource_id": resource_id,
            "records": [{"_id": 1, "Timestamp": "2025-08-23T22:50:00", "Demand": 2500.0}],
            "limit": 1000,
            "total": 1,
        },
    }
    _write_json(raw_dir / "energy_20260208_120000.json", first_payload)
    _write_json(raw_dir / "energy_20260208_130000.json", second_payload)

    df = clean_energy.transform_energy_files(raw_dir)

    assert list(df.columns) == clean_energy.ENERGY_CANONICAL_COLUMNS
    assert len(df) == 1
    assert df.loc[0, "source_area"] == "east_midlands"
    assert df.loc[0, "demand_mw"] == 2500.0

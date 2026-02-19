import json

from transformations.silver import clean_energy, clean_weather


def _write_json(path, payload):
    with path.open("w") as f:
        json.dump(payload, f)


def test_weather_transform_canonical_schema_and_utc(tmp_path):
    raw_dir = tmp_path / "raw_weather"
    raw_dir.mkdir()

    payload = {
        "id": 2643743,
        "dt": 1704067200,
        "name": "London",
        "coord": {"lat": 51.5085, "lon": -0.1257},
        "sys": {"country": "GB"},
        "main": {"temp": 9.32, "feels_like": 7.97, "humidity": 87, "pressure": 1005},
        "weather": [{"main": "Clouds", "description": "few clouds"}],
        "wind": {"speed": 2.57},
        "clouds": {"all": 20},
    }

    _write_json(raw_dir / "weather_20260208_185041.json", payload)
    df = clean_weather.transform_weather_files(raw_dir)

    assert list(df.columns) == clean_weather.WEATHER_CANONICAL_COLUMNS
    assert len(df) == 1
    assert str(df["event_timestamp_utc"].dtype) == "datetime64[ns, UTC]"
    assert str(df["ingestion_timestamp_utc"].dtype) == "datetime64[ns, UTC]"
    assert df.loc[0, "event_date_utc"] == "2024-01-01"
    assert df.loc[0, "ingestion_timestamp_utc"].isoformat() == "2026-02-08T18:50:41+00:00"


def test_weather_transform_deduplicates_by_city_and_timestamp(tmp_path):
    raw_dir = tmp_path / "raw_weather"
    raw_dir.mkdir()

    base_payload = {
        "id": 2643743,
        "dt": 1704067200,
        "name": "London",
        "coord": {"lat": 51.5085, "lon": -0.1257},
        "sys": {"country": "GB"},
        "main": {"temp": 10.0, "feels_like": 9.0, "humidity": 80, "pressure": 1001},
        "weather": [{"main": "Clouds", "description": "broken clouds"}],
        "wind": {"speed": 4.0},
        "clouds": {"all": 70},
    }
    newer_payload = dict(base_payload)
    newer_payload["main"] = dict(base_payload["main"])
    newer_payload["main"]["temp"] = 11.0

    _write_json(raw_dir / "weather_20260208_120000.json", base_payload)
    _write_json(raw_dir / "weather_20260208_130000.json", newer_payload)

    df = clean_weather.transform_weather_files(raw_dir)

    assert len(df) == 1
    assert df.loc[0, "temperature_c"] == 11.0
    assert df.loc[0, "ingestion_timestamp_utc"].isoformat() == "2026-02-08T13:00:00+00:00"


def test_energy_transform_canonical_schema_utc_and_dedup(tmp_path):
    raw_dir = tmp_path / "raw_energy"
    raw_dir.mkdir()

    first_payload = {
        "help": "https://connecteddata.nationalgrid.co.uk/",
        "success": True,
        "result": {
            "resource_id": "resource-123",
            "records": [
                {
                    "_id": 1,
                    "Timestamp": "2025-08-23T22:50:00",
                    "Demand": 2437.38,
                    "Generation": 347.47,
                    "Import": 2090.0,
                    "Solar": 12.65,
                    "Wind": 48.12,
                    "STOR": 103.16,
                    "Other": 183.66,
                }
            ],
            "limit": 1000,
            "total": 1,
        },
    }
    second_payload = {
        "help": "https://connecteddata.nationalgrid.co.uk/",
        "success": True,
        "result": {
            "resource_id": "resource-123",
            "records": [
                {
                    "_id": 1,
                    "Timestamp": "2025-08-23T22:50:00",
                    "Demand": 2500.0,
                    "Generation": 347.47,
                    "Import": 2090.0,
                    "Solar": 12.65,
                    "Wind": 48.12,
                    "STOR": 103.16,
                    "Other": 183.66,
                },
                {
                    "_id": 2,
                    "Timestamp": "2025-08-23T22:55:00",
                    "Demand": 2422.62,
                    "Generation": 336.39,
                    "Import": 2086.33,
                    "Solar": 12.85,
                    "Wind": 48.49,
                    "STOR": 88.8,
                    "Other": 186.37,
                },
            ],
            "limit": 1000,
            "total": 2,
        },
    }

    _write_json(raw_dir / "energy_20260208_120000.json", first_payload)
    _write_json(raw_dir / "energy_20260208_130000.json", second_payload)

    df = clean_energy.transform_energy_files(raw_dir)

    assert list(df.columns) == clean_energy.ENERGY_CANONICAL_COLUMNS
    assert len(df) == 2
    assert str(df["event_timestamp_utc"].dtype) == "datetime64[ns, UTC]"
    assert str(df["ingestion_timestamp_utc"].dtype) == "datetime64[ns, UTC]"

    deduped_row = df.loc[df["source_record_id"] == 1].iloc[0]
    assert deduped_row["demand_mw"] == 2500.0
    assert deduped_row["event_date_utc"] == "2025-08-23"

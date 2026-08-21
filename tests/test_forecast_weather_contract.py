from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
from jsonschema import Draft202012Validator, FormatChecker

from forecasting.forecast_weather import (
    ForecastWeatherConfig,
    ForecastWeatherContractError,
    attach_target_forecast_weather,
    build_demo_forecast_weather_frame,
    prepare_forecast_weather_frame,
)


def _supervised(rows: int = 4, *, area: str = "east_midlands") -> pd.DataFrame:
    feature = pd.date_range("2026-01-01T00:00:00Z", periods=rows, freq="5min")
    return pd.DataFrame(
        {
            "source_area": area,
            "resource_id": "resource-1",
            "city": "Nottingham",
            "feature_timestamp_utc": feature,
            "target_timestamp_utc": feature + pd.Timedelta(minutes=30),
            "requested_horizon_minutes": 30,
            "demand_mw": [100.0 + index for index in range(rows)],
        }
    )


def _forecast_for(supervised: pd.DataFrame) -> pd.DataFrame:
    records = []
    for row in supervised.itertuples(index=False):
        records.append(
            {
                "source_area": row.source_area,
                "city": row.city,
                "forecast_issued_at_utc": row.feature_timestamp_utc
                - pd.Timedelta(minutes=10),
                "forecast_ingested_at_utc": row.feature_timestamp_utc
                - pd.Timedelta(minutes=9),
                "forecast_valid_at_utc": row.target_timestamp_utc,
                "forecast_temperature_c": 8.0,
                "forecast_humidity_pct": 61.0,
                "forecast_provider": "provider-a",
                "forecast_model": "model-v1",
            }
        )
    return pd.DataFrame(records)


def test_public_api_exports_forecast_weather_contract():
    from forecasting import (
        ForecastWeatherConfig as PublicForecastWeatherConfig,
        attach_target_forecast_weather as public_attach,
        prepare_feature_frame,
    )

    assert PublicForecastWeatherConfig is ForecastWeatherConfig
    assert public_attach is attach_target_forecast_weather
    assert callable(prepare_feature_frame)


def test_forecast_weather_normalizes_offsets_and_satisfies_schema():
    frame = pd.DataFrame(
        [
            {
                "source_area": "east_midlands",
                "city": "Nottingham",
                "forecast_issued_at_utc": "2026-01-01T00:00:00+01:00",
                "forecast_ingested_at_utc": "2026-01-01T00:01:00+01:00",
                "forecast_valid_at_utc": "2026-01-01T00:30:00+01:00",
                "forecast_temperature_c": 7.5,
                "forecast_humidity_pct": 72.0,
                "forecast_provider": "provider-a",
                "forecast_model": "model-v1",
            }
        ]
    )
    prepared = prepare_forecast_weather_frame(frame)

    assert str(prepared.loc[0, "forecast_issued_at_utc"]) == (
        "2025-12-31 23:00:00+00:00"
    )
    contract_path = (
        Path(__file__).resolve().parents[1]
        / "data-contracts"
        / "forecast_weather_schema.json"
    )
    contract = json.loads(contract_path.read_text(encoding="utf-8"))
    row = prepared.iloc[0].to_dict()
    for column in (
        "forecast_issued_at_utc",
        "forecast_ingested_at_utc",
        "forecast_valid_at_utc",
    ):
        row[column] = pd.Timestamp(row[column]).isoformat()
    assert list(
        Draft202012Validator(
            contract, format_checker=FormatChecker()
        ).iter_errors(row)
    ) == []


def test_naive_forecast_timestamps_are_rejected():
    frame = _forecast_for(_supervised(1))
    frame["forecast_issued_at_utc"] = frame[
        "forecast_issued_at_utc"
    ].astype(object)
    frame.loc[0, "forecast_issued_at_utc"] = "2026-01-01T00:00:00"

    with pytest.raises(ForecastWeatherContractError, match="timezone-aware"):
        prepare_forecast_weather_frame(frame)


def test_forecast_must_be_available_after_issue_and_before_valid_time():
    frame = _forecast_for(_supervised(1))
    frame.loc[0, "forecast_ingested_at_utc"] = (
        frame.loc[0, "forecast_issued_at_utc"] - pd.Timedelta(minutes=1)
    )
    with pytest.raises(ForecastWeatherContractError, match="before provider issuance"):
        prepare_forecast_weather_frame(frame)

    frame = _forecast_for(_supervised(1))
    frame.loc[0, "forecast_ingested_at_utc"] = frame.loc[
        0, "forecast_valid_at_utc"
    ]
    with pytest.raises(ForecastWeatherContractError, match="before their valid time"):
        prepare_forecast_weather_frame(frame)


def test_match_uses_latest_available_forecast_and_ignores_future_issue():
    supervised = _supervised(1)
    feature = supervised.loc[0, "feature_timestamp_utc"]
    target = supervised.loc[0, "target_timestamp_utc"]
    weather = pd.DataFrame(
        [
            {
                "source_area": "east_midlands",
                "city": "Nottingham",
                "forecast_issued_at_utc": feature - pd.Timedelta(minutes=20),
                "forecast_ingested_at_utc": feature - pd.Timedelta(minutes=19),
                "forecast_valid_at_utc": target,
                "forecast_temperature_c": 5.0,
                "forecast_humidity_pct": 60.0,
                "forecast_provider": "provider-a",
                "forecast_model": "older",
            },
            {
                "source_area": "east_midlands",
                "city": "Nottingham",
                "forecast_issued_at_utc": feature - pd.Timedelta(minutes=5),
                "forecast_ingested_at_utc": feature - pd.Timedelta(minutes=4),
                "forecast_valid_at_utc": target,
                "forecast_temperature_c": 8.0,
                "forecast_humidity_pct": 65.0,
                "forecast_provider": "provider-a",
                "forecast_model": "latest-available",
            },
            {
                "source_area": "east_midlands",
                "city": "Nottingham",
                "forecast_issued_at_utc": feature + pd.Timedelta(minutes=1),
                "forecast_ingested_at_utc": feature + pd.Timedelta(minutes=2),
                "forecast_valid_at_utc": target,
                "forecast_temperature_c": 99.0,
                "forecast_humidity_pct": 99.0,
                "forecast_provider": "provider-a",
                "forecast_model": "future-leak",
            },
        ]
    )

    matched = attach_target_forecast_weather(
        supervised,
        weather,
        config=ForecastWeatherConfig(min_coverage=1.0),
    )

    assert matched.loc[0, "target_weather_model"] == "latest-available"
    assert matched.loc[0, "target_weather_temperature_c"] == 8.0
    assert (
        matched.loc[0, "target_weather_forecast_ingested_at_utc"] <= feature
    )


def test_match_prioritizes_target_validity_before_recency():
    supervised = _supervised(1)
    feature = supervised.loc[0, "feature_timestamp_utc"]
    target = supervised.loc[0, "target_timestamp_utc"]
    weather = pd.DataFrame(
        [
            {
                "source_area": "east_midlands",
                "city": "Nottingham",
                "forecast_issued_at_utc": feature - pd.Timedelta(minutes=20),
                "forecast_ingested_at_utc": feature - pd.Timedelta(minutes=19),
                "forecast_valid_at_utc": target,
                "forecast_temperature_c": 6.0,
                "forecast_humidity_pct": 60.0,
                "forecast_provider": "provider-a",
                "forecast_model": "exact-valid-time",
            },
            {
                "source_area": "east_midlands",
                "city": "Nottingham",
                "forecast_issued_at_utc": feature - pd.Timedelta(minutes=5),
                "forecast_ingested_at_utc": feature - pd.Timedelta(minutes=4),
                "forecast_valid_at_utc": target + pd.Timedelta(minutes=5),
                "forecast_temperature_c": 8.0,
                "forecast_humidity_pct": 65.0,
                "forecast_provider": "provider-a",
                "forecast_model": "newer-but-offset",
            },
        ]
    )

    matched = attach_target_forecast_weather(
        supervised,
        weather,
        config=ForecastWeatherConfig(
            valid_time_tolerance_minutes=5,
            min_coverage=1.0,
        ),
    )

    assert matched.loc[0, "target_weather_model"] == "exact-valid-time"
    assert matched.loc[0, "target_weather_valid_delta_minutes"] == 0.0


def test_cross_area_weather_does_not_match():
    supervised = _supervised(2)
    weather = _forecast_for(supervised)
    weather["source_area"] = "south_west"

    with pytest.raises(ForecastWeatherContractError, match="matched 0/2"):
        attach_target_forecast_weather(
            supervised,
            weather,
            config=ForecastWeatherConfig(min_coverage=1.0),
        )


def test_low_weather_coverage_fails_instead_of_silently_dropping_rows():
    supervised = _supervised(4)
    weather = _forecast_for(supervised).iloc[:2].copy()

    with pytest.raises(ForecastWeatherContractError, match="minimum coverage"):
        attach_target_forecast_weather(
            supervised,
            weather,
            config=ForecastWeatherConfig(
                valid_time_tolerance_minutes=0,
                min_coverage=0.75,
            ),
        )


def test_match_records_causal_provenance_and_coverage():
    supervised = _supervised(3)
    matched = attach_target_forecast_weather(
        supervised,
        _forecast_for(supervised),
        config=ForecastWeatherConfig(min_coverage=1.0),
    )

    assert set(matched["weather_feature_mode"]) == {"target_forecast"}
    assert set(matched["forecast_weather_contract_version"]) == {
        "target-weather-v1"
    }
    assert set(matched["forecast_weather_coverage_pct"]) == {100.0}
    assert (
        matched["target_weather_forecast_ingested_at_utc"]
        <= matched["feature_timestamp_utc"]
    ).all()
    assert (
        matched["target_weather_forecast_valid_at_utc"]
        > matched["target_weather_forecast_issued_at_utc"]
    ).all()


def test_demo_forecast_weather_is_credential_free_and_target_valid():
    timestamps = pd.date_range(
        "2026-01-01T00:00:00Z", periods=20, freq="5min"
    )
    features = pd.DataFrame(
        {
            "source_area": "east_midlands",
            "city": "Nottingham",
            "event_timestamp_utc": timestamps,
            "temperature": [5.0 + index * 0.1 for index in range(20)],
            "humidity": [60.0 + index % 5 for index in range(20)],
        }
    )

    weather = build_demo_forecast_weather_frame(
        features, horizon_minutes=(30, 60)
    )

    assert not weather.empty
    assert set(weather["forecast_provider"]) == {"demo"}
    assert (
        weather["forecast_issued_at_utc"]
        == weather["forecast_ingested_at_utc"]
    ).all()
    assert (
        weather["forecast_valid_at_utc"]
        > weather["forecast_ingested_at_utc"]
    ).all()


def test_demo_forecasts_attach_to_exact_target_times():
    timestamps = pd.date_range(
        "2026-01-01T00:00:00Z", periods=20, freq="5min"
    )
    features = pd.DataFrame(
        {
            "source_area": "east_midlands",
            "city": "Nottingham",
            "event_timestamp_utc": timestamps,
            "temperature": [5.0 + index * 0.1 for index in range(20)],
            "humidity": [60.0 + index % 5 for index in range(20)],
        }
    )
    supervised = pd.DataFrame(
        {
            "source_area": "east_midlands",
            "resource_id": "resource-1",
            "city": "Nottingham",
            "feature_timestamp_utc": timestamps[:8],
            "target_timestamp_utc": timestamps[:8] + pd.Timedelta(minutes=30),
            "requested_horizon_minutes": 30,
        }
    )
    weather = build_demo_forecast_weather_frame(
        features, horizon_minutes=(30,)
    )

    matched = attach_target_forecast_weather(
        supervised,
        weather,
        config=ForecastWeatherConfig(
            valid_time_tolerance_minutes=0,
            min_coverage=1.0,
        ),
    )

    assert set(matched["target_weather_valid_delta_minutes"]) == {0.0}
    assert set(matched["forecast_weather_coverage_pct"]) == {100.0}


def test_matching_preserves_resource_identity_for_shared_weather_area():
    first = _supervised(2)
    second = first.copy()
    second["resource_id"] = "resource-2"
    supervised = pd.concat([first, second], ignore_index=True)
    weather = _forecast_for(first)

    matched = attach_target_forecast_weather(
        supervised,
        weather,
        config=ForecastWeatherConfig(min_coverage=1.0),
    )

    assert set(matched["resource_id"]) == {"resource-1", "resource-2"}
    assert len(matched) == len(supervised)
    assert set(matched["forecast_weather_coverage_pct"]) == {100.0}

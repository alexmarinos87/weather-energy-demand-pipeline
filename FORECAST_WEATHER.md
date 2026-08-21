# Target-valid forecast-weather contract

## Why this exists

The current demand baselines are leakage-safe, but their weather inputs are observations available at feature time. A real forward forecast also needs weather predictions that were already available when the demand forecast was made and that are valid for the future demand target.

This increment defines that boundary without claiming that a live weather-forecast provider, Fabric ingestion path, or model integration already exists.

## Normalized input contract

`data-contracts/forecast_weather_schema.json` defines one normalized provider record:

| Field | Meaning |
| --- | --- |
| `source_area` | Canonical project licence-area key |
| `city` | Contract-bound representative weather proxy |
| `forecast_issued_at_utc` | When the provider issued the forecast |
| `forecast_ingested_at_utc` | When the pipeline made it available |
| `forecast_valid_at_utc` | Weather time represented by the forecast |
| `forecast_temperature_c` | Forecast temperature in Celsius |
| `forecast_humidity_pct` | Forecast relative humidity |
| `forecast_provider` | Provider identity |
| `forecast_model` | Provider model or product identity |

`prepare_forecast_weather_frame` rejects:

- missing or blank source identity;
- timezone-naive timestamps;
- non-finite temperature or humidity;
- humidity outside 0–100%;
- ingestion before provider issuance;
- ingestion at or after forecast valid time; and
- duplicate provider issue/valid identities.

All accepted timestamps are normalized to UTC.

## Causal target matching

`attach_target_forecast_weather` operates on supervised demand rows that already contain feature time, future target time, source identity, and requested horizon.

A forecast-weather record is eligible only when:

```text
forecast_issued_at_utc
    <=
forecast_ingested_at_utc
    <=
feature_timestamp_utc
    <
target_timestamp_utc
```

The forecast valid time must also be after feature time and within the configured tolerance of the demand target. Matching is restricted to the same `source_area` and `city`.

When several records are eligible, the matcher selects:

1. the smallest absolute valid-time difference from the demand target;
2. the latest pipeline availability time;
3. the latest provider issue time; and
4. provider/model identity as a deterministic final tie-breaker.

This prevents a newer but less target-relevant forecast from replacing an exact target-valid record, and prevents forecasts issued or ingested after feature time from leaking into the model feature set.

## Coverage and evidence

Coverage is calculated independently for each source-area/resource/city/horizon group. The run fails rather than silently dropping rows when matched coverage falls below `ForecastWeatherConfig.min_coverage`.

Matched rows retain:

- provider issue, ingestion, and valid timestamps;
- provider and model identity;
- target-valid temperature and humidity;
- valid-time difference from the demand target;
- provider lead time;
- lead time from feature time;
- age of the forecast at feature time;
- eligible and matched counts;
- coverage percentage; and
- `forecast_weather_contract_version=target-weather-v1`.

## Credential-free demonstration

```python
from forecasting import (
    BacktestConfig,
    ForecastWeatherConfig,
    attach_target_forecast_weather,
    build_demo_feature_frame,
    build_demo_forecast_weather_frame,
    build_supervised_frame,
    prepare_feature_frame,
)

features = build_demo_feature_frame()
backtest = BacktestConfig(horizon_minutes=(30, 60))
supervised = build_supervised_frame(
    prepare_feature_frame(features, backtest),
    backtest,
)
forecast_weather = build_demo_forecast_weather_frame(
    features,
    horizon_minutes=(30, 60),
)
target_weather_features = attach_target_forecast_weather(
    supervised,
    forecast_weather,
    config=ForecastWeatherConfig(
        valid_time_tolerance_minutes=15,
        max_availability_age_minutes=180,
        min_coverage=0.90,
    ),
)
```

The deterministic generator uses no API credentials and exists only to exercise the contract and causal matching rules.

## Current boundary

This increment does not:

- call a live forecast-weather provider;
- add forecast-weather raw or silver ingestion;
- change the local ridge feature set;
- change the Fabric forecasting notebooks or Delta tables; or
- claim production forward-forecast readiness.

The next dependent increment is to add the normalized target-weather fields to the local model comparison while preserving the existing observed-weather baseline. Fabric parity should follow only after that model contract is validated locally.

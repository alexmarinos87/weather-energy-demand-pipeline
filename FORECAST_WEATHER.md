# Target-valid forecast-weather contract

## Why this exists

The ordinary demand baselines use weather observations available at feature time. A forward-looking model also needs weather predictions that were already available when the demand forecast was made and that are valid near the future demand target.

The repository now separates this into two local layers:

1. a provider-neutral target-weather contract and causal matcher; and
2. an explicit paired model comparison between observed-at-feature and target-valid forecast weather.

Neither layer claims that a live forecast provider or Fabric forecast-weather ingestion path already exists.

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

This prevents a future-issued forecast from leaking into the model and prevents a newer but less target-relevant forecast from replacing an exact target-valid record.

## Coverage and evidence

Coverage is calculated independently for each source-area/resource/city/horizon group. The matcher fails rather than silently dropping rows when coverage falls below `ForecastWeatherConfig.min_coverage`.

Matched rows retain:

- provider issue, ingestion, and valid timestamps;
- provider and model identity;
- target-valid temperature and humidity;
- valid-time difference from the demand target;
- provider and feature-time lead;
- age of the forecast at feature time;
- eligible and matched counts;
- coverage percentage; and
- `forecast_weather_contract_version=target-weather-v1`.

## Paired model comparison

`run_weather_model_comparison` creates one target-weather-covered supervised cohort and evaluates both ridge models on exactly the same rows:

```text
ridge_weather_lag
    demand + lag/rolling/calendar
    + temperature/humidity/weather age observed at feature time

ridge_target_weather
    identical demand + lag/rolling/calendar features
    + target-valid forecast temperature/humidity/availability age
```

The target-weather model replaces only these three default observed-weather columns:

| Observed model | Target-weather model |
| --- | --- |
| `temperature` | `target_weather_temperature_c` |
| `humidity` | `target_weather_humidity_pct` |
| `weather_age_minutes` | `target_weather_availability_age_minutes` |

The two models therefore use the same:

- source-area/resource/city identity;
- 30- or 60-minute demand targets;
- target-weather-covered rows;
- holdout or rolling-origin cutoffs;
- unavailable-label purge;
- demand, lag, rolling, and calendar inputs;
- training boundary; and
- evaluation timestamps.

A comparison run fails if the two model cohorts diverge. Prediction evidence uses `weather_comparison_contract_version=weather-model-comparison-v1` and `data-contracts/forecast_weather_model_comparison_schema.json`.

## Credential-free commands

The ordinary baseline remains the default:

```bash
python3 -m forecasting.run_baseline \
  --demo \
  --horizon-minutes 30 60 \
  --output-dir data/forecasting \
  --output-format csv
```

Run the paired holdout comparison explicitly:

```bash
python3 -m forecasting.run_baseline \
  --demo \
  --model-set weather-comparison \
  --horizon-minutes 30 60 \
  --forecast-valid-time-tolerance-minutes 15 \
  --forecast-max-availability-age-minutes 180 \
  --min-forecast-weather-coverage 0.90 \
  --output-dir data/forecasting \
  --output-format csv
```

Outputs:

- `data/forecasting/weather_comparison_predictions.csv`
- `data/forecasting/weather_comparison_metrics.csv`

Use the same paired cohort with rolling origins:

```bash
python3 -m forecasting.run_baseline \
  --demo \
  --model-set weather-comparison \
  --evaluation-mode rolling-origin \
  --rolling-origin-folds 3 \
  --horizon-minutes 30 60 \
  --output-dir data/forecasting \
  --output-format csv
```

Outputs:

- `data/forecasting/rolling_origin_weather_comparison_predictions.csv`
- `data/forecasting/rolling_origin_weather_comparison_metrics.csv`

For real local files, replace `--demo` with both inputs:

```bash
python3 -m forecasting.run_baseline \
  --input gold_feature_engineering.parquet \
  --forecast-weather-input normalized_forecast_weather.parquet \
  --model-set weather-comparison \
  --evaluation-mode rolling-origin \
  --horizon-minutes 30 60 \
  --output-dir data/forecasting \
  --output-format parquet
```

The deterministic demo forecast generator uses no API credentials.

## Current boundary

This increment does not:

- call a live forecast-weather provider;
- add forecast-weather raw or silver ingestion;
- change the ordinary baseline outputs or default CLI behaviour;
- change the Fabric forecasting notebooks or Delta tables; or
- claim production forward-forecast readiness.

The next dependent increment is Fabric ingestion and Spark parity for normalized forecast weather, but only after the local paired evidence demonstrates that the target-weather model adds value. Production readiness would additionally require provider selection, forecast-versus-actual weather reconciliation, drift monitoring, model registration, and promotion controls.

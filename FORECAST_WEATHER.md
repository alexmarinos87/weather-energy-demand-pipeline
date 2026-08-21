# Target-valid forecast weather

## Purpose

The ordinary demand baselines use weather observations available at feature time. A forward-looking model also needs weather predictions that were already available when the demand forecast was made and that are valid near the future demand target.

The repository now separates this into three local layers:

1. a provider-neutral forecast-weather contract and causal matcher;
2. a bounded OpenWeather 5-day / 3-hour ingestion adapter; and
3. a paired comparison between observed-at-feature and target-valid-weather ridge models.

The adapter is executable, but it is not scheduled or deployed in Microsoft Fabric.

## Normalized contract

`data-contracts/forecast_weather_schema.json` defines normalized forecast records:

| Field | Meaning |
| --- | --- |
| `source_area` | Canonical licence-area key |
| `city` | Contract-bound representative city |
| `forecast_issued_at_utc` | Provider issue time, or a declared conservative surrogate |
| `forecast_ingested_at_utc` | Time the pipeline made the forecast available |
| `forecast_valid_at_utc` | Future weather time represented by the forecast |
| `forecast_temperature_c` | Forecast temperature in Celsius |
| `forecast_humidity_pct` | Forecast relative humidity |
| `forecast_provider` | Provider identity |
| `forecast_model` | Provider product/model identity |

`prepare_forecast_weather_frame` rejects blank source identity, timezone-naive timestamps, non-finite values, humidity outside 0–100%, ingestion before issuance, ingestion at or after valid time, and duplicate provider issue/valid identities.

## OpenWeather adapter

`ingestion/forecast_weather/fetch_openweather_forecast.py` calls the coordinate-based OpenWeather 5-day / 3-hour endpoint. The endpoint, scheme, host, API path, metric units, timeout, and record bound are validated before the API key is read.

The project-owned coordinates are held with each city proxy in `data-contracts/source_areas.json`. The adapter rejects a response whose country or coordinates do not match the selected source area.

OpenWeather's forecast records expose their future valid timestamp, but this adapter does not claim an exact provider model-run timestamp. It therefore uses the completed retrieval timestamp as a conservative issue/availability boundary and records:

```text
forecast_issue_basis=retrieval_time_surrogate
forecast_issued_at_utc=forecast_ingested_at_utc=forecast_retrieved_at_utc
```

This is safe for causal evaluation because the forecast is treated as unavailable until the response has actually been retrieved. It is less precise than a provider-issued model-run timestamp and is retained explicitly in the evidence.

The adapter validates:

- the raw JSON Schema;
- `cnt` against the actual list length and configured maximum;
- unique, strictly increasing UTC forecast timestamps;
- GB country identity;
- response coordinates against the source-area proxy;
- future valid times after retrieval; and
- the normalized provider-neutral schema.

It writes immutable, content-addressed outputs:

```text
data/raw/forecast_weather/openweather/
  ingestion_date=YYYY-MM-DD/
    openweather_forecast_YYYYMMDD_HHMMSS_<snapshot>.json

data/normalized/forecast_weather/openweather/
  ingestion_date=YYYY-MM-DD/
    forecast_weather_YYYYMMDD_HHMMSS_<snapshot>.parquet
```

The raw snapshot identifier is a SHA-256 digest over the provider payload, source binding, coordinates, and retrieval time. Existing files are never overwritten.

### Run locally

```bash
cp ingestion/forecast_weather/config.example.yaml \
   ingestion/forecast_weather/config.yaml
export OPENWEATHER_API_KEY=...
python3 ingestion/forecast_weather/fetch_openweather_forecast.py
```

The default adapter is bounded to at most 40 records. No API key is persisted in raw or normalized evidence.

## Causal target matching

`attach_target_forecast_weather` operates on supervised demand rows containing feature time, future target time, source identity, and requested horizon. A forecast record is eligible only when:

```text
forecast_issued_at_utc
    <=
forecast_ingested_at_utc
    <=
feature_timestamp_utc
    <
target_timestamp_utc
```

Forecast valid time must occur after feature time and within the configured tolerance of the demand target. Matching is limited to the same `source_area` and `city`.

When several records are eligible, selection uses:

1. smallest absolute valid-time difference from the target;
2. latest pipeline availability time;
3. latest provider issue time; and
4. provider/model identity as a deterministic tie-breaker.

Coverage is calculated independently for each source-area/resource/city/horizon group. The matcher fails rather than silently dropping rows below `ForecastWeatherConfig.min_coverage`.

## Paired model comparison

`run_weather_model_comparison` evaluates both ridge models on one target-weather-covered cohort:

```text
ridge_weather_lag
    demand + lag/rolling/calendar
    + temperature/humidity/weather age observed at feature time

ridge_target_weather
    identical demand + lag/rolling/calendar
    + target-valid temperature/humidity/availability age
```

The only default substitutions are:

| Observed model | Target-weather model |
| --- | --- |
| `temperature` | `target_weather_temperature_c` |
| `humidity` | `target_weather_humidity_pct` |
| `weather_age_minutes` | `target_weather_availability_age_minutes` |

Targets, rows, cutoffs, label purging, training boundaries, and evaluation timestamps must be identical. Evidence is versioned as `weather-model-comparison-v1`.

### Compare using an adapter output

```bash
python3 -m forecasting.run_baseline \
  --input gold_feature_engineering.parquet \
  --forecast-weather-input \
    data/normalized/forecast_weather/openweather/ingestion_date=YYYY-MM-DD/forecast_weather_....parquet \
  --model-set weather-comparison \
  --evaluation-mode rolling-origin \
  --horizon-minutes 30 60 \
  --output-dir data/forecasting \
  --output-format parquet
```

## Credential-free comparison commands

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

## Current boundary

The adapter is local and on-demand. This increment does not:

- make a live request during tests or CI;
- schedule forecast retrieval;
- add forecast weather to Fabric bronze/silver tables;
- reconcile forecasts against later observed weather;
- change the ordinary baseline or Fabric model inputs; or
- claim production forward-forecast readiness.

The next dependent layer is forecast-versus-observed reconciliation and coverage monitoring on retained snapshots, followed by Fabric ingestion and Spark model parity if the real target-weather evidence is useful.

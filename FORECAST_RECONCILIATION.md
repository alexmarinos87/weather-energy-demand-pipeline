# Forecast-versus-observed weather reconciliation

## Purpose

The repository can retain normalized target-valid weather forecasts, but a retained forecast is not useful evidence until the later observed weather is matched back to it. This reconciliation layer measures forecast completeness and error before target-weather features are promoted into the canonical Fabric model path.

## Inputs

Forecast input is the normalized provider-neutral output produced by the OpenWeather adapter or another conforming provider:

```text
source_area
city
forecast_issued_at_utc
forecast_ingested_at_utc
forecast_valid_at_utc
forecast_temperature_c
forecast_humidity_pct
forecast_provider
forecast_model
```

Observed input is the existing silver weather schema:

```text
source_area
city
event_timestamp_utc
ingestion_timestamp_utc
temperature_c
humidity_pct
source_file
```

Both inputs must contain timezone-aware timestamps. Observed ingestion must not precede the observed event time.

## Mature forecast boundary

A forecast slot is reconciliation-eligible only when its valid timestamp lies within the retained observed-weather history for the same source area and city.

Forecasts beyond the latest observed timestamp are still pending and are excluded from the coverage denominator. Missing observations inside retained history remain eligible and lower coverage.

This distinction prevents future forecast slots from appearing as failures while ensuring internal observation gaps cannot disappear silently.

## Matching rule

For each mature forecast slot:

1. restrict observations to the same `source_area` and `city`;
2. retain observations within `observation_tolerance_minutes` of forecast valid time;
3. choose the smallest absolute time difference;
4. when equally distant, prefer the observation at or before valid time;
5. then prefer the latest ingested duplicate and a stable source-file order.

The default tolerance is 90 minutes. It is configurable because current-weather observation cadence may differ from the provider's three-hour forecast slots.

## Evidence

Every mature forecast produces a row with `reconciliation_status=matched` or `unmatched`. Matched rows retain:

- forecast issue, ingestion, and valid timestamps;
- forecast provider, model, issue basis, snapshot, and provider-record identity;
- observed event and ingestion timestamps plus source file;
- signed and absolute valid-time difference;
- observed ingestion lag;
- forecast temperature and humidity;
- observed temperature and humidity;
- signed, absolute, and squared errors; and
- `reconciliation_contract_version=forecast-observation-reconciliation-v1`.

Aggregate metrics are produced independently by source area, city, provider, model, issue basis, and lead-time bucket:

```text
00-06h
06-12h
12-24h
24-48h
48h+
```

Metrics include:

- eligible and matched forecast counts;
- forecast-versus-observed coverage;
- temperature MAE, RMSE, and bias;
- humidity MAE, RMSE, and bias;
- average and maximum observation-time offset;
- average and maximum observed ingestion lag; and
- average provider and pipeline lead time.

The run fails when any provider/model/lead bucket is below the configured minimum coverage.

## Run locally

```bash
python3 -m forecasting.run_weather_reconciliation \
  --forecast-input data/normalized/forecast_weather/openweather \
  --observed-input data/silver/weather \
  --observation-tolerance-minutes 90 \
  --min-coverage 0.80 \
  --output-dir data/reconciliation/forecast_weather \
  --output-format parquet
```

Inputs may be one CSV/Parquet file or a partitioned directory. Outputs are immutable, run-ID-qualified files:

```text
forecast_weather_reconciliation_<run-id>.parquet
forecast_weather_quality_metrics_<run-id>.parquet
```

The equivalent CSV output is available with `--output-format csv`.

## Contracts

- `data-contracts/forecast_weather_reconciliation_schema.json`
- `data-contracts/forecast_weather_quality_metrics_schema.json`

## Boundary

This is retrospective local evidence. It does not schedule forecast retrieval, alter the ordinary baseline, change Fabric tables, or promote `ridge_target_weather`. The next dependent layer is a reviewed promotion gate based on sufficient observation count, coverage, weather error, and paired demand-model performance.

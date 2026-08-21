# Weather–Energy Demand Pipeline

## Problem

Weather and electricity-demand data arrive from different systems, at different frequencies, and with different spatial meanings. Combining them without explicit source identity can produce untraceable analysis, cross-region joins, weather leakage, or model scores contaminated by labels that would not yet have been known when a forecast was made.

## Solution

This project implements a Microsoft Fabric medallion pipeline plus bounded local forecasting experiments that:

- ingest OpenWeather current observations and complete, bounded NGED Live Data snapshots;
- validate source responses against versioned contracts before writing raw data;
- bind each NGED licence-area resource to an explicit project weather proxy;
- propagate source-area and file-level provenance through silver tables;
- join only same-area weather at or before each demand timestamp;
- build causal lag, rolling, calendar, weather, and aggregation features;
- ingest bounded OpenWeather 5-day / 3-hour forecasts by contract-owned coordinates;
- write immutable raw forecast snapshots and normalized target-weather Parquet;
- compare observed-at-feature and target-valid-weather ridge models on paired rows;
- create bounded 30- and 60-minute future demand targets;
- purge training labels that would not yet be available at evaluation feature time;
- support both a fixed chronological holdout and optional rolling-origin backtesting;
- append prediction, target-coverage, evaluation-origin, and metric evidence to Delta tables; and
- record blocking and warning data-quality results in `dq_run_results`.

The active cloud target remains Microsoft Fabric: OneLake, Lakehouse Delta tables, Spark notebooks, Data Factory orchestration, the SQL analytics endpoint, and Power BI-ready outputs. The OpenWeather forecast adapter and target-weather comparison are local, explicit paths and are not yet scheduled or deployed in Fabric.

## Data sources and spatial contract

- **OpenWeather current weather API** — current weather observations.
- **OpenWeather 5-day / 3-hour forecast API** — bounded local forecast snapshots.
- **National Grid Electricity Distribution Connected Data Portal** — near-real-time demand, generation, and import data split by licence area.

`data-contracts/source_areas.json` defines the allowed source binding and the project-owned coordinate used for forecast requests:

| Source area | NGED resource | Weather proxy | Forecast coordinates |
| --- | --- | --- | --- |
| East Midlands | `92d3431c-15d7-4aa6-ad34-2335596a026c` | `Nottingham,GB` | `52.9548, -1.1581` |
| South Wales | `38b81427-a2df-42f2-befa-4d6fe9b54c98` | `Cardiff,GB` | `51.4816, -3.1791` |
| South West | `85aaa199-15df-40ec-845f-6c61cbedc20f` | `Bristol,GB` | `51.4545, -2.5879` |
| West Midlands | `1c3447df-37d7-4fb4-9f99-0e2a0d691dbe` | `Birmingham,GB` | `52.4862, -1.8904` |

The cities and coordinates are representative project proxies. They are not official NGED mappings and do not represent every weather condition across a licence area.

## Data and evaluation flow

```text
OpenWeather current + NGED Live Data
        ↓ contract and source-area validation
OneLake immutable raw JSON + provenance metadata
        ↓ typed parsing and deduplication
silver_weather + silver_energy
        ↓ same-area, past-only weather matching
gold_weather_demand_join
        ↓ prior-demand lags/rolling windows + causal weather/calendar features
gold_feature_engineering
        ↓ 30/60-minute bounded target matching
        ↓ fixed holdout OR expanding rolling origins
        ↓ unavailable-label purge at every evaluation cutoff
persistence_current_value + ridge_weather_lag
        ↓
forecast_baseline_predictions + forecast_baseline_metrics
        ↓
SQL endpoint / Power BI / model comparison
```

The local target-weather path is separate from the deployed Fabric path:

```text
OpenWeather 5-day / 3-hour forecast
        ↓ fixed HTTPS endpoint + contract coordinates
        ↓ raw schema, count/order/country/coordinate checks
immutable content-addressed raw JSON
        ↓ conservative retrieval-time availability boundary
normalized target-weather Parquet
        ↓ causal issue/ingestion/valid-time matching
paired supervised demand rows
        ↓ identical cutoffs and label purge
ridge_weather_lag ↔ ridge_target_weather
        ↓
local weather-comparison predictions and metrics
```

For each demand observation, the Fabric gold join selects the latest current-weather observation from the same `source_area` whose timestamp is no later than demand time and no more than six hours old. Demand rolling features exclude the current row.

Every demand prediction must satisfy:

```text
trained_through_utc < feature_timestamp_utc < event_timestamp_utc
```

Rolling-origin predictions additionally satisfy:

```text
trained_through_utc
    <
origin_cutoff_utc
    <=
feature_timestamp_utc
    <
event_timestamp_utc
```

## Time-horizon target contract

The local and Fabric demand runtimes use the same matching rules for every source-area/resource/city group and requested horizon:

1. Calculate the ideal target timestamp at feature time plus 30 or 60 minutes.
2. Exclude trailing feature rows whose ideal target lies beyond retained history.
3. Choose the first source observation at or after the ideal target timestamp.
4. Accept that target only when it is no more than `target_tolerance_minutes` late.
5. Require matched/eligible coverage of at least `min_target_coverage` for every configured group and horizon.
6. Fail when a configured group/horizon has no eligible history or insufficient in-history target coverage.

Forecast evidence records requested and actual elapsed horizons, target delay, observation distance, target coverage, feature time, future target time, current demand, actual future demand, predictions, errors, and the training-label boundary.

## Bounded OpenWeather forecast ingestion

The local adapter is:

```text
ingestion/forecast_weather/fetch_openweather_forecast.py
```

It validates the exact HTTPS OpenWeather host and API path, metric units, timeout, and record bound before reading `OPENWEATHER_API_KEY`. Requests use the source-area latitude and longitude from the project contract rather than accepting arbitrary runtime coordinates.

The raw response must satisfy `data-contracts/openweather_forecast_raw_schema.json`. The adapter also rejects:

- a `cnt` value that disagrees with the returned list;
- more records than the configured bound, capped at 40;
- duplicate or non-increasing forecast timestamps;
- a non-GB response for the configured proxy;
- response coordinates outside the configured tolerance; and
- a snapshot with no future forecast slots after retrieval.

The provider records expose future valid timestamps. The adapter does not claim an exact provider model-run timestamp. It therefore treats completed retrieval time as a conservative issue and availability boundary and records:

```text
forecast_issue_basis=retrieval_time_surrogate
forecast_issued_at_utc=forecast_ingested_at_utc=forecast_retrieved_at_utc
```

Raw and normalized outputs are immutable and content-addressed:

```text
data/raw/forecast_weather/openweather/
  ingestion_date=YYYY-MM-DD/
    openweather_forecast_YYYYMMDD_HHMMSS_<snapshot>.json

data/normalized/forecast_weather/openweather/
  ingestion_date=YYYY-MM-DD/
    forecast_weather_YYYYMMDD_HHMMSS_<snapshot>.parquet
```

No API key is written to either output.

### Run the forecast adapter locally

```bash
cp ingestion/forecast_weather/config.example.yaml \
   ingestion/forecast_weather/config.yaml
export OPENWEATHER_API_KEY=...
python3 ingestion/forecast_weather/fetch_openweather_forecast.py
```

The adapter is on-demand only. Tests and CI use mocked responses and perform no live OpenWeather request.

## Target-valid forecast-weather contract

`data-contracts/forecast_weather_schema.json` and `forecasting/forecast_weather.py` define the provider-neutral normalized contract. It separates provider issue or declared issue surrogate, pipeline availability, forecast valid time, and future demand target time.

A normalized forecast may match a supervised demand row only when it has the same area and city and satisfies:

```text
forecast_issued_at_utc
    <=
forecast_ingested_at_utc
    <=
feature_timestamp_utc
    <
target_timestamp_utc
```

Forecast valid time must occur after feature time and within the configured tolerance of the demand target. The closest target-valid record wins; ties use the latest available ingestion and issue times. Coverage is enforced independently per source-area/resource/city/horizon group, so missing forecast weather cannot be hidden by silently dropping rows.

## Paired target-weather model comparison

`run_weather_model_comparison` evaluates both ridge models on exactly the same target-weather-covered cohort:

- `ridge_weather_lag` uses observed temperature, humidity, and weather age at feature time;
- `ridge_target_weather` replaces those three fields with target-valid forecast temperature, humidity, and forecast availability age.

All demand, lag, rolling, calendar, target, split, rolling-origin, and label-purge semantics are identical. The comparison fails if evaluated timestamps or training boundaries diverge. Evidence is versioned as `weather-model-comparison-v1`.

See `FORECAST_WEATHER.md` for the complete adapter, contract, and comparison details.

## Evaluation modes

The fixed holdout remains the default in both local and Fabric demand runtimes:

```text
earliest 60% → training
next 20%     → validation
final 20%    → test
```

Select rolling-origin evaluation explicitly to partition validation history into repeated expanding-window origins while retaining the final 20% as one untouched test origin. Every origin independently purges labels that would not have been known at its cutoff.

Rolling-origin evidence records:

- `origin_fold`;
- `origin_count`;
- `origin_cutoff_utc`;
- `training_observation_count`; and
- `evaluation_contract_version=rolling-origin-v1`.

The evaluator rejects incomplete fold sequences, reused evaluation timestamps, decreasing origin cutoffs, decreasing available training history, invalid validation/test fold placement, or labels unavailable at an origin cutoff.

## Credential-free forecasting demos

Install dependencies once:

```bash
python3 -m pip install -r requirements.txt
```

Fixed baseline holdout:

```bash
python3 -m forecasting.run_baseline \
  --demo \
  --horizon-minutes 30 60 \
  --target-tolerance-minutes 5 \
  --min-target-coverage 0.90 \
  --output-dir data/forecasting \
  --output-format csv
```

Outputs:

- `data/forecasting/baseline_predictions.csv`
- `data/forecasting/baseline_metrics.csv`

Rolling-origin baseline:

```bash
python3 -m forecasting.run_baseline \
  --demo \
  --evaluation-mode rolling-origin \
  --rolling-origin-folds 3 \
  --horizon-minutes 30 60 \
  --output-dir data/forecasting \
  --output-format csv
```

Outputs:

- `data/forecasting/rolling_origin_predictions.csv`
- `data/forecasting/rolling_origin_metrics.csv`

Paired observed-versus-target-weather comparison:

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

Add `--evaluation-mode rolling-origin --rolling-origin-folds 3` to produce:

- `data/forecasting/rolling_origin_weather_comparison_predictions.csv`
- `data/forecasting/rolling_origin_weather_comparison_metrics.csv`

For real local files, provide both inputs:

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

## Local ingestion development

```bash
cp ingestion/weather/config.example.yaml ingestion/weather/config.yaml
cp ingestion/forecast_weather/config.example.yaml \
   ingestion/forecast_weather/config.yaml
cp ingestion/energy/config.example.yaml ingestion/energy/config.yaml
python3 -m pip install -r requirements.txt
```

Set credentials outside the repository:

```bash
export OPENWEATHER_API_KEY=...
export NATIONAL_GRID_API_TOKEN=...
```

Keep the same `source_area` across weather, forecast-weather, and energy configuration. Then run the required local paths explicitly:

```bash
python3 ingestion/weather/fetch_weather.py
python3 ingestion/forecast_weather/fetch_openweather_forecast.py
python3 ingestion/energy/fetch_energy.py
python3 transformations/silver/clean_weather.py
python3 transformations/silver/clean_energy.py
pytest -q
```

Energy ingestion requests CKAN pages in ascending `_id` order until the total reported by the first page is complete. It fails rather than silently publishing a partial snapshot when ordering, totals, resource identity, or the explicit record bound is violated.

## Fabric run order

1. Create and attach the `weather_energy_lakehouse` Lakehouse.
2. Upload all files in `data-contracts/` to `Files/data-contracts/`.
3. Import notebook sources `01` through `06` from `fabric/notebooks/`.
4. Create `weather_energy_demand_pipeline` from `fabric/pipelines/weather_energy_demand_pipeline.md`.
5. Supply secure credentials and a consistent `SOURCE_AREA`, `WEATHER_CITY`, and `NATIONAL_GRID_RESOURCE_ID`.
6. Configure `HORIZON_MINUTES`, `TARGET_TOLERANCE_MINUTES`, and `MIN_TARGET_COVERAGE`.
7. Keep `EVALUATION_MODE=holdout` for ordinary hourly runs, or select `rolling-origin` with a reviewed `ROLLING_ORIGIN_FOLDS`.
8. Run ingestion, silver, gold, source/gold quality, forecasting, and forecast quality in order.
9. Optionally create pass-through SQL views from `fabric/sql/gold_views_tsql.sql` and `fabric/sql/forecast_views_tsql.sql`.
10. Enable the schedule only after source quotas and Fabric capacity are confirmed.

The Fabric run order does not yet include forecast-weather ingestion.

## Forecasting boundary

The repository now has an executable, bounded local provider adapter that produces normalized forecast-weather evidence for the paired model comparison. It is not a production forecast service: retrieval is manual, provider issue time is represented by an explicit conservative retrieval-time surrogate, and no forecast-versus-observed reconciliation or provider-quality monitoring exists yet. The ordinary local baseline and Fabric notebooks continue to use observed weather.

The next useful increment is retained forecast-versus-observed reconciliation and target-time coverage monitoring. Fabric bronze/silver ingestion and Spark model parity should follow only after real snapshots demonstrate acceptable coverage and usefulness.

# Weather–Energy Demand Pipeline

## Problem

Weather and electricity-demand data arrive from different systems, at different frequencies, and with different spatial meanings. Combining them without explicit source identity can produce untraceable analysis, cross-region joins, weather leakage, or model scores contaminated by labels that would not yet have been known when a forecast was made.

## Solution

This project implements a Microsoft Fabric medallion pipeline that:

- ingests OpenWeather observations and complete, bounded NGED Live Data snapshots;
- validates source responses against versioned contracts before writing raw data;
- binds each NGED licence-area resource to an explicit project weather proxy;
- propagates source-area and file-level provenance through silver tables;
- joins only same-area weather at or before each demand timestamp;
- builds causal lag, rolling, calendar, weather, and aggregation features;
- creates explicit future demand targets and purges unavailable labels;
- evaluates persistence and regularised linear baselines with chronological train, validation, and test boundaries;
- appends prediction and metric evidence to Delta tables; and
- records blocking and warning data-quality results in `dq_run_results`.

The active cloud target is Microsoft Fabric: OneLake, Lakehouse Delta tables, Spark notebooks, Data Factory orchestration, the SQL analytics endpoint, and Power BI-ready outputs.

## Data sources and spatial contract

- **OpenWeather API** — current weather observations.
- **National Grid Electricity Distribution Connected Data Portal** — near-real-time demand, generation, and import data split by licence area.

`data-contracts/source_areas.json` defines the allowed bindings:

| Source area | NGED resource | Weather proxy |
| --- | --- | --- |
| East Midlands | `92d3431c-15d7-4aa6-ad34-2335596a026c` | `Nottingham,GB` |
| South Wales | `38b81427-a2df-42f2-befa-4d6fe9b54c98` | `Cardiff,GB` |
| South West | `85aaa199-15df-40ec-845f-6c61cbedc20f` | `Bristol,GB` |
| West Midlands | `1c3447df-37d7-4fb4-9f99-0e2a0d691dbe` | `Birmingham,GB` |

The cities are representative project proxies. They are not official NGED mappings and do not represent all weather conditions across a licence area.

## Data and evaluation flow

```text
OpenWeather + NGED Live Data
        ↓ contract and source-area validation
OneLake immutable raw JSON + provenance metadata
        ↓ typed parsing and deduplication
silver_weather + silver_energy
        ↓ same-area, past-only weather matching
gold_weather_demand_join
        ↓ prior-demand lags/rolling windows + causal weather/calendar features
gold_feature_engineering
        ↓ explicit future target + chronological split + label purge
persistence_current_value + ridge_weather_lag
        ↓
forecast_baseline_predictions + forecast_baseline_metrics
        ↓
SQL endpoint / Power BI / model comparison
```

For each demand observation, the gold join selects the latest weather observation from the same `source_area` whose timestamp is no later than demand time and no more than six hours old. Demand rolling features exclude the current row.

Every prediction must satisfy:

```text
trained_through_utc < feature_timestamp_utc < event_timestamp_utc
```

## Credential-free time-horizon demo

The local benchmark now evaluates the approved 30- and 60-minute service horizons in one run:

```bash
python3 -m pip install -r requirements.txt
python3 -m forecasting.run_baseline \
  --demo \
  --horizon-minutes 30 60 \
  --target-tolerance-minutes 5 \
  --min-target-coverage 0.90 \
  --output-dir data/forecasting \
  --output-format csv
```

For each feature timestamp, target selection chooses the first observation at or after the requested horizon. The observation must arrive within the configured tolerance. Trailing rows without enough future history are excluded from the coverage denominator; missing targets inside retained history reduce coverage and fail the run when the configured minimum is not met.

Forecast evidence distinguishes:

- `requested_horizon_minutes` — the 30- or 60-minute service target;
- `target_tolerance_minutes` — maximum permitted late match;
- `horizon_minutes` — actual elapsed time to the matched observation;
- `target_delay_minutes` — delay beyond the requested horizon;
- `horizon_steps` — actual ordered-observation distance, retained for diagnostics;
- `feature_timestamp_utc` and `event_timestamp_utc` — feature and target times;
- `trained_through_utc` — latest target label used for fitting.

Outputs:

- `data/forecasting/baseline_predictions.csv`
- `data/forecasting/baseline_metrics.csv`

The demo compares:

- `persistence_current_value` — demand at feature time carried forward;
- `ridge_weather_lag` — regularised linear demand model using information available at feature time.

For exported Fabric features, replace `--demo` with:

```bash
python3 -m forecasting.run_baseline \
  --input gold_feature_engineering.parquet \
  --horizon-minutes 30 60 \
  --output-dir data/forecasting \
  --output-format parquet
```

The local implementation is time-based. The current Fabric notebook still uses `HORIZON_STEPS`; a dependent parity increment replaces that parameter with the same bounded 30/60-minute matching contract.

## Local ingestion development

```bash
cp ingestion/weather/config.example.yaml ingestion/weather/config.yaml
cp ingestion/energy/config.example.yaml ingestion/energy/config.yaml
python3 -m pip install -r requirements.txt
```

Set credentials outside the repository:

```bash
export OPENWEATHER_API_KEY=...
export NATIONAL_GRID_API_TOKEN=...
```

Keep the same `source_area` in both config files and use the matching resource/city from the table above. Then run:

```bash
python3 ingestion/weather/fetch_weather.py
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
6. Run ingestion, silver, gold, source/gold quality, forecasting, and forecast quality in order.
7. Optionally create pass-through SQL views from `fabric/sql/gold_views_tsql.sql` and `fabric/sql/forecast_views_tsql.sql`.
8. Enable the schedule only after source quotas and Fabric capacity are confirmed.

## Forecasting boundary

The local benchmark now creates explicit 30- and 60-minute targets and handles missing or irregular intervals through bounded matching and coverage evidence. It still uses observed weather at feature time. A production forward forecast additionally requires forecast-weather inputs, rolling-origin evaluation, drift monitoring, and a model promotion process.

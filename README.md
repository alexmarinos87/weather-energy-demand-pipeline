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
- defines a versioned local contract for target-valid forecast weather;
- creates bounded 30- and 60-minute future demand targets;
- purges training labels that would not yet be available at evaluation feature time;
- supports both a fixed chronological holdout and optional rolling-origin backtesting;
- evaluates persistence and regularised linear baselines;
- appends prediction, target-coverage, evaluation-origin, and metric evidence to Delta tables; and
- records blocking and warning data-quality results in `dq_run_results`.

The active cloud target is Microsoft Fabric: OneLake, Lakehouse Delta tables, Spark notebooks, Data Factory orchestration, the SQL analytics endpoint, and Power BI-ready outputs.

## Data sources and spatial contract

- **OpenWeather API** — current weather observations.
- **Normalized forecast-weather input** — a provider-neutral local contract; no live forecast adapter is implemented yet.
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
        ↓ 30/60-minute bounded target matching
        ↓ fixed holdout OR expanding rolling origins
        ↓ unavailable-label purge at every evaluation cutoff
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

The local and Fabric runtimes use the same matching rules for every source-area/resource/city group and requested horizon:

1. Calculate the ideal target timestamp at feature time plus 30 or 60 minutes.
2. Exclude trailing feature rows whose ideal target lies beyond the retained history.
3. Choose the first source observation at or after the ideal target timestamp.
4. Accept that target only when it is no more than `target_tolerance_minutes` late.
5. Require matched/eligible coverage of at least `min_target_coverage` for every configured group and horizon.
6. Fail the run when a configured group/horizon has no eligible history or insufficient in-history target coverage.

Forecast evidence records:

- `requested_horizon_minutes` — the 30- or 60-minute service target;
- `target_tolerance_minutes` — maximum permitted late match;
- `horizon_minutes` — actual elapsed time to the matched observation;
- `target_delay_minutes` — delay beyond the requested horizon;
- `horizon_steps` — actual ordered-observation distance, retained for diagnostics;
- matched and eligible target counts plus target coverage;
- feature time, future target time, current demand, actual future demand, predictions, errors, and training-label boundary.

## Target-valid forecast-weather contract

`data-contracts/forecast_weather_schema.json` and `forecasting/forecast_weather.py` define a provider-neutral local contract for weather predictions that are valid near a future demand target. The contract separates:

- provider issue time;
- pipeline ingestion/availability time;
- forecast valid time; and
- future demand target time.

The matcher requires same-area/city identity and enforces:

```text
forecast_issued_at_utc
    <=
forecast_ingested_at_utc
    <=
feature_timestamp_utc
    <
target_timestamp_utc
```

Forecast valid time must be after feature time and within a configured tolerance of the demand target. The closest target-valid record wins; ties use the latest available issuance. Coverage is enforced independently per source-area/resource/city/horizon group, so missing forecast weather cannot be hidden by silently dropping rows.

Matched evidence retains provider/model identity, issue/ingestion/valid timestamps, target-valid temperature and humidity, valid-time difference, lead times, availability age, and coverage. See `FORECAST_WEATHER.md` for the complete contract and credential-free Python example.

This contract is not yet wired into the baseline models or Fabric notebooks. Existing model and cloud behaviour is unchanged.

## Evaluation modes

The fixed holdout remains the default in both local and Fabric runtimes:

```text
earliest 60% → training
next 20%     → validation
final 20%    → test
```

Select rolling-origin evaluation explicitly to partition the validation history into repeated expanding-window origins while retaining the final 20% as one untouched test origin. Every origin independently purges labels that would not have been known at its cutoff.

Rolling-origin evidence records:

- `origin_fold`;
- `origin_count`;
- `origin_cutoff_utc`;
- `training_observation_count`; and
- `evaluation_contract_version=rolling-origin-v1`.

The evaluator rejects incomplete fold sequences, reused evaluation timestamps, decreasing origin cutoffs, decreasing available training history, invalid validation/test fold placement, or labels unavailable at an origin cutoff.

## Credential-free forecasting demo

Fixed holdout:

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

Outputs:

- `data/forecasting/baseline_predictions.csv`
- `data/forecasting/baseline_metrics.csv`

Rolling-origin evaluation:

```bash
python3 -m forecasting.run_baseline \
  --demo \
  --evaluation-mode rolling-origin \
  --rolling-origin-folds 3 \
  --horizon-minutes 30 60 \
  --target-tolerance-minutes 5 \
  --min-target-coverage 0.90 \
  --output-dir data/forecasting \
  --output-format csv
```

Outputs:

- `data/forecasting/rolling_origin_predictions.csv`
- `data/forecasting/rolling_origin_metrics.csv`

The demo compares:

- `persistence_current_value` — demand at feature time carried forward;
- `ridge_weather_lag` — regularised linear demand model using information available at feature time.

For exported Fabric features, replace `--demo` with:

```bash
python3 -m forecasting.run_baseline \
  --input gold_feature_engineering.parquet \
  --evaluation-mode rolling-origin \
  --rolling-origin-folds 3 \
  --horizon-minutes 30 60 \
  --output-dir data/forecasting \
  --output-format parquet
```

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
6. Configure `HORIZON_MINUTES`, `TARGET_TOLERANCE_MINUTES`, and `MIN_TARGET_COVERAGE`.
7. Keep `EVALUATION_MODE=holdout` for ordinary hourly runs, or select `rolling-origin` with a reviewed `ROLLING_ORIGIN_FOLDS`.
8. Run ingestion, silver, gold, source/gold quality, forecasting, and forecast quality in order.
9. Optionally create pass-through SQL views from `fabric/sql/gold_views_tsql.sql` and `fabric/sql/forecast_views_tsql.sql`.
10. Enable the schedule only after source quotas and Fabric capacity are confirmed.

## Forecasting boundary

The benchmark creates explicit 30- and 60-minute targets, handles missing or irregular intervals through bounded matching, prevents overlapping-label leakage, and provides local/Fabric rolling-origin parity. A versioned local target-weather contract now exists, but the baseline models and Fabric notebooks still use weather observed at feature time. The next product step is local model integration against the normalized forecast-weather evidence, followed by Fabric parity, drift monitoring, model registration, and promotion controls.

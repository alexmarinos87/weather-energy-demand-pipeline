# Weather–Energy Demand Pipeline

A contract-first weather and electricity-demand data product for Microsoft Fabric, with reproducible local analytics, leakage-safe forecasting experiments, provider-quality evidence, and explicit human authority over pilots and model decisions.

The repository separates three concerns:

```text
product path
    ingestion → silver → causal gold features → analytics/forecast evidence

evaluation path
    bounded targets → holdout/rolling backtests → seasonal/calendar scorecards

control path
    reconciliation → health evidence → human review → bounded pilot records
```

No control artifact silently deploys, schedules, activates, or promotes anything.

## One-command credential-free demo

Install the supported Python 3.11/Linux dependency resolution:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install \
  -r requirements.txt \
  -c constraints/ci-python311-linux.txt
python -m pip check
```

Run the complete local product journey:

```bash
python -m forecasting.run_portfolio_demo \
  --output-root data/portfolio-demo \
  --output-format csv
```

Each run creates a new immutable directory:

```text
data/portfolio-demo/pdm-<24-hex>/
```

It contains:

- deterministic gold-like demand/weather features for all four source areas;
- deterministic target-valid forecast weather;
- 30- and 60-minute persistence and ridge baseline predictions/metrics;
- paired `ridge_weather_lag` and `ridge_target_weather` predictions/metrics;
- 12 days of 30-minute seasonal feature history;
- exact elapsed-time previous-day and previous-week evidence under both UTC and UK-local calendar contracts;
- a paired five-model area/horizon scorecard and persistence comparison;
- descriptive demand/weather tables and two Markdown reports; and
- `portfolio_demo_manifest.json` with file sizes, row counts, SHA-256 hashes, spatial/model/feature-contract identities, horizons, seasonal periods, and no-side-effect flags.

The demo reads no credential, performs no live source call, changes no Fabric workspace, activates no schedule, promotes no model, and publishes nothing externally. See `PORTFOLIO_DEMO.md`.

## Capability index

### Product and evaluation

| Capability | Main entry point | Detail |
| --- | --- | --- |
| One-command four-area product demo | `python -m forecasting.run_portfolio_demo` | `PORTFOLIO_DEMO.md` |
| Descriptive demand/weather analytics | `python -m forecasting.run_demand_weather_report` | `ANALYTICS_REPORT.md` |
| Current-weather ingestion | `ingestion/weather/fetch_weather.py` | `architecture/data_flow.md` |
| NGED energy ingestion | `ingestion/energy/fetch_energy.py` | `architecture/data_flow.md` |
| OpenWeather forecast ingestion | `ingestion/forecast_weather/fetch_openweather_forecast.py` | `FORECAST_WEATHER.md` |
| Fixed and rolling-origin baselines | `python -m forecasting.run_baseline` | `ROLLING_ORIGIN.md` |
| Paired observed/target-weather comparison | `--model-set weather-comparison` | `FORECAST_WEATHER.md` |
| Elapsed-time day/week baselines | `python -m forecasting.run_seasonal_baselines` | `SEASONAL_BASELINES.md` |
| UTC/UK-local model-family scorecard | `python -m forecasting.run_model_family_scorecard` | `MODEL_SCORECARD.md` |
| Calibration-only prediction intervals | `python -m forecasting.run_prediction_intervals` | `PREDICTION_INTERVALS.md` |
| Forecast-versus-observed reconciliation | `python -m forecasting.run_weather_reconciliation` | `FORECAST_RECONCILIATION.md` |
| Forecast-provider health/drift evidence | `python -m forecasting.run_provider_monitoring` | `PROVIDER_MONITORING.md` |
| Fabric core medallion path | `fabric/notebooks/01` through `06` | `fabric/README.md` |
| Optional Fabric forecast bronze/silver | `fabric/notebooks/01b`, `02b` | `fabric/FORECAST_WEATHER.md` |
| Optional Fabric paired comparison | `fabric/notebooks/05c`, `06c` | `fabric/pipelines/target_weather_model_comparison_pipeline.md` |
| Optional Fabric seasonal comparison | `fabric/notebooks/05d`, `06d` | `fabric/pipelines/seasonal_baseline_comparison_pipeline.md` |
| Optional Fabric prediction intervals | `fabric/notebooks/05e`, `06e` | `fabric/pipelines/prediction_interval_pipeline.md` |

### Evidence and human authority

| Capability | Boundary document |
| --- | --- |
| Target-weather promotion assessment | `PROMOTION_ASSESSMENT.md` |
| Immutable model-candidate registry | `MODEL_REGISTRY.md` |
| Evidence inventory/retention/quarantine/compaction | `EVIDENCE_LIFECYCLE.md`, `EVIDENCE_OPERATIONS.md`, `EVIDENCE_COMPACTION.md` |
| Candidate recovery bundle and restore | `RECOVERY_BUNDLE.md` |
| Non-production Fabric pilot planning | `FABRIC_PILOT.md` |
| Preflight and single-use authorization | `PILOT_AUTHORIZATION.md` |
| Operator-supplied pilot receipt/assessment | `PILOT_RUN_RECEIPT.md` |
| Named post-pilot decision | `POST_PILOT_DECISION.md` |
| CI action provenance | `CI_SUPPLY_CHAIN.md` |
| Python dependency resolution | `DEPENDENCY_REPRODUCIBILITY.md` |
| Dependency-ordered next work | `ROADMAP.md` |

## Architecture

### Core observed-weather path

```text
OpenWeather current + NGED Live Data
        ↓ source-area preflight and JSON contracts
immutable raw JSON + _pipeline_metadata
        ↓ typed parsing, provenance, deterministic deduplication
silver_weather + silver_energy
        ↓ same-area current weather at/before demand time
        ↓ maximum weather age six hours
gold_weather_demand_join
        ↓ prior-demand lags/rolling windows + calendar/weather features
gold_feature_engineering
        ↓ bounded 30/60-minute demand targets
        ↓ unavailable-label purge at every evaluation boundary
persistence_current_value + ridge_weather_lag
        ↓
forecast_baseline_predictions + forecast_baseline_metrics
```

### Target-weather evidence path

```text
OpenWeather 5-day / 3-hour forecast
        ↓ fixed HTTPS endpoint + contract-owned coordinates
        ↓ schema/count/order/country/coordinate checks
immutable content-addressed raw forecast snapshot
        ↓ conservative retrieval-time availability boundary
normalized target-weather evidence
        ↓ same-area, available-by-feature-time, target-valid matching
paired supervised demand cohort
        ↓ identical targets, cutoffs, origins, and label purge
ridge_weather_lag ↔ ridge_target_weather
        ↓
comparison + reconciliation + provider-health evidence
        ↓
human-review-only assessment and candidate records
```

### Seasonal, calendar, and uncertainty path

```text
retained causal demand features
        ↓ target-minus-1-day / target-minus-1-week elapsed UTC matching
persistence + previous-day + previous-week + ridge
        ↓ paired UTC and Europe/London calendar feature contracts
five-model area/horizon scorecard
        ↓ causally available validation residuals only
calibration-only test prediction intervals
```

Spark Delta tables are canonical for the Fabric path. SQL analytics endpoint files are pass-through views; they do not reimplement silver cleaning, target matching, model fitting, seasonal matching, calibration, or rolling-origin logic.

## Data sources and spatial contract

- **OpenWeather current weather API** supplies current observations.
- **OpenWeather 5-day / 3-hour forecast API** supplies bounded forecast snapshots.
- **National Grid Electricity Distribution Connected Data Portal** supplies licence-area demand, generation, and import data.

`data-contracts/source_areas.json` owns the allowed source bindings:

| Source area | NGED resource | Representative weather proxy | Forecast coordinates |
| --- | --- | --- | --- |
| East Midlands | `92d3431c-15d7-4aa6-ad34-2335596a026c` | `Nottingham,GB` | `52.9548, -1.1581` |
| South Wales | `38b81427-a2df-42f2-befa-4d6fe9b54c98` | `Cardiff,GB` | `51.4816, -3.1791` |
| South West | `85aaa199-15df-40ec-845f-6c61cbedc20f` | `Bristol,GB` | `51.4545, -2.5879` |
| West Midlands | `1c3447df-37d7-4fb4-9f99-0e2a0d691dbe` | `Birmingham,GB` | `52.4862, -1.8904` |

The cities and coordinates are project-owned representatives, not official NGED mappings and not complete descriptions of weather across each licence area.

## Core contracts

### Causal current-weather matching

A demand observation may use only current weather from the same non-null `source_area` whose timestamp is at or before demand time and no more than six hours old. The latest eligible observation wins deterministically.

Demand lag and rolling features exclude the current demand target row.

### Bounded demand targets

For every source-area/resource/city group and requested horizon:

1. calculate the ideal target at feature time plus 30 or 60 minutes;
2. exclude trailing feature rows whose ideal target lies beyond retained history;
3. choose the first demand observation at or after the ideal target;
4. accept it only inside the configured late-match tolerance; and
5. enforce matched/eligible coverage for every group and horizon.

Every prediction must satisfy:

```text
trained_through_utc < feature_timestamp_utc < event_timestamp_utc
```

Rolling-origin predictions additionally satisfy:

```text
trained_through_utc
    < origin_cutoff_utc
    <= feature_timestamp_utc
    < event_timestamp_utc
```

### Target-valid forecast weather

A normalized forecast can enter a demand feature set only when:

```text
forecast_issued_at_utc
    <= forecast_ingested_at_utc
    <= feature_timestamp_utc
    < target_timestamp_utc
```

It must have the same area/city identity, a valid time after feature time, a bounded difference from the demand target, and acceptable coverage. The OpenWeather adapter explicitly records retrieval time as a conservative issue/availability surrogate rather than inventing an unavailable provider model-run timestamp.

### Elapsed seasonal references and calendar features

Previous-day and previous-week baselines search for observations near:

```text
target_timestamp_utc - 1,440 minutes
target_timestamp_utc - 10,080 minutes
```

They do not use fixed row offsets. References must be available at feature time and satisfy the configured tolerance and coverage contract.

UTC remains the target, ordering, join, and audit identity. Europe/London fields are derived model features with a distinct feature-contract version. The scorecard requires identical controls, targets, split/origin evidence, and training boundaries before comparing UTC and UK-local ridge predictions.

### Calibration-only intervals

Interval width comes only from validation target labels available before the first test feature timestamp. Overlapping validation labels and all test labels are prohibited from choosing the radius. Empirical retained-test coverage is evidence, not an unconditional future guarantee.

## Local commands

### Descriptive analytics

```bash
python -m forecasting.run_demand_weather_report \
  --demo \
  --top-peak-count 10 \
  --temperature-bin-width-c 2.5 \
  --output-dir data/analytics/demand_weather \
  --output-format csv
```

The report is descriptive. Correlation and temperature-band averages do not establish causation.

### Fixed baseline

```bash
python -m forecasting.run_baseline \
  --demo \
  --horizon-minutes 30 60 \
  --target-tolerance-minutes 5 \
  --min-target-coverage 0.90 \
  --output-dir data/forecasting \
  --output-format csv
```

### Rolling-origin baseline

```bash
python -m forecasting.run_baseline \
  --demo \
  --evaluation-mode rolling-origin \
  --rolling-origin-folds 3 \
  --horizon-minutes 30 60 \
  --output-dir data/forecasting \
  --output-format csv
```

### Paired target-weather comparison

```bash
python -m forecasting.run_baseline \
  --demo \
  --model-set weather-comparison \
  --evaluation-mode rolling-origin \
  --rolling-origin-folds 3 \
  --horizon-minutes 30 60 \
  --output-dir data/forecasting \
  --output-format csv
```

For retained/exported data, provide both gold features and normalized forecast weather with `--input` and `--forecast-weather-input`.

### Seasonal baselines and calendar scorecard

Create UTC and UK-local seasonal runs over the same retained features, then compare them:

```bash
python -m forecasting.run_model_family_scorecard \
  --utc-predictions seasonal_comparison_predictions.parquet \
  --uk-local-predictions seasonal_comparison_uk_local_calendar_predictions.parquet \
  --output-dir data/forecasting/model_family_scorecard \
  --output-format parquet
```

See `SEASONAL_BASELINES.md`, `CALENDAR_FEATURES.md`, and `MODEL_SCORECARD.md`.

### Calibration-only prediction intervals

```bash
python -m forecasting.run_prediction_intervals \
  --predictions-input seasonal_comparison_predictions.parquet \
  --coverage-levels 0.80 0.90 0.95 \
  --min-calibration-rows 24 \
  --output-dir data/forecasting/prediction_intervals \
  --output-format parquet
```

## Optional live local ingestion

Copy the reviewed example configuration files, then keep credential values outside the repository:

```bash
cp ingestion/weather/config.example.yaml ingestion/weather/config.yaml
cp ingestion/energy/config.example.yaml ingestion/energy/config.yaml
cp ingestion/forecast_weather/config.example.yaml \
   ingestion/forecast_weather/config.yaml

export OPENWEATHER_API_KEY=...
export NATIONAL_GRID_API_TOKEN=...
```

Run only the source path required:

```bash
python ingestion/weather/fetch_weather.py
python ingestion/energy/fetch_energy.py
python ingestion/forecast_weather/fetch_openweather_forecast.py
```

Local raw and normalized evidence is written under ignored `data/` paths. Tests and CI mock source responses and perform no live request.

## Microsoft Fabric

The Fabric source files are implementation templates and contracts; this repository workflow does not deploy them automatically.

Core order:

```text
01_ingest_api_to_bronze
02_bronze_to_silver
03_build_gold_tables
04_data_quality_checks
05_baseline_forecasting
06_forecast_quality_checks
```

Optional manual evidence subflows:

```text
01b_ingest_forecast_weather_to_bronze
02b_forecast_weather_to_silver

05c_target_weather_model_comparison
06c_target_weather_comparison_quality_checks

05d_seasonal_baseline_comparison
06d_seasonal_baseline_quality_checks

05e_prediction_intervals
06e_prediction_interval_quality_checks
```

The optional subflows remain unscheduled. Successful execution does not change the active observed-weather baseline and does not authorize promotion.

## Test and CI contract

CI uses:

- Python 3.11;
- the complete `constraints/ci-python311-linux.txt` resolution;
- `python -m pip check`;
- commit-pinned Node 24 GitHub Actions;
- `contents: read` token permission;
- `persist-credentials: false` checkout;
- Python compilation; and
- the complete pytest suite.

Run the equivalent local gate:

```bash
python -m pip install \
  -r requirements.txt \
  -c constraints/ci-python311-linux.txt
python -m pip check
python -m compileall -q ingestion transformations forecasting fabric/notebooks tests
python -m pytest -q
```

## Human-authority boundary

The repository can produce evidence for review, but it cannot silently:

- approve or activate a model;
- authorize or execute another Fabric pilot;
- mutate the model registry from a post-pilot decision;
- schedule ingestion or forecasting;
- deploy a Fabric workspace;
- deliver an external alert;
- permanently delete retained evidence; or
- replace the observed-weather control model.

A live Fabric pilot remains an external, named human operation requiring a current plan, preflight, single-use authorization, bounded execution, immutable receipt, assessment, and a separate named post-pilot decision.

## Current boundary

The repository has source adapters, local and Fabric transformation logic, descriptive analytics, historical target-weather and seasonal evaluation, paired calendar scorecards, calibration-only interval evidence, provider-quality evidence, recovery controls, and manual pilot contracts. It does not claim that a live Fabric workspace has been deployed, that empirical interval coverage is guaranteed under shift, or that any candidate is production-approved.

See `ROADMAP.md` for the next dependency-ordered increment.

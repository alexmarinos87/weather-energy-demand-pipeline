# Microsoft Fabric implementation

## Workspace shape

- Lakehouse: `weather_energy_lakehouse`
- Environment: `weather_energy_env`
- Ordinary notebooks: `01_ingest_api_to_bronze`, `02_bronze_to_silver`, `03_build_gold_tables`, `04_data_quality_checks`, `05_baseline_forecasting`, `06_forecast_quality_checks`
- Optional/manual evidence notebooks: `05c_target_weather_model_comparison`, `06c_target_weather_comparison_quality_checks`, `05d_seasonal_baseline_comparison`, `06d_seasonal_baseline_quality_checks`, `05e_prediction_intervals`, `06e_prediction_interval_quality_checks`, `05f_prediction_interval_monitoring`, `06f_prediction_interval_monitoring_quality_checks`
- Data Factory pipeline: `weather_energy_demand_pipeline`
- Manual subflows: `seasonal_baseline_comparison_pipeline`, `prediction_interval_pipeline`, `prediction_interval_monitoring_pipeline`

Attach the Lakehouse and Environment to every notebook.

## OneLake layout

Raw captures:

- `Files/raw/weather/ingestion_date=YYYY-MM-DD/weather_YYYYMMDD_HHMMSS.json`
- `Files/raw/energy/ingestion_date=YYYY-MM-DD/energy_YYYYMMDD_HHMMSS.json`

Contracts uploaded to `Files/data-contracts/`:

- `weather_schema.json`
- `energy_schema.json`
- `source_areas.json`
- `gold_features_schema.json`
- `forecast_evaluation_schema.json`
- `rolling_origin_evaluation_schema.json`
- `prediction_interval_schema.json`
- `prediction_interval_metrics_schema.json`
- `prediction_interval_health_check_schema.json`
- `prediction_interval_health_summary_schema.json`

Ordinary tables:

- `silver_weather`
- `silver_energy`
- `gold_weather_demand_join`
- `gold_feature_engineering`
- `gold_demand_aggregation`
- `forecast_baseline_predictions`
- `forecast_baseline_metrics`
- `dq_run_results`

Optional/manual evidence tables:

- `forecast_weather_comparison_predictions`
- `forecast_weather_comparison_metrics`
- `forecast_seasonal_comparison_predictions`
- `forecast_seasonal_comparison_metrics`
- `forecast_prediction_intervals`
- `forecast_prediction_interval_metrics`
- `forecast_prediction_interval_health_checks`
- `forecast_prediction_interval_health_summary`

## Required source binding

Set `SOURCE_AREA`, `WEATHER_CITY`, and `NATIONAL_GRID_RESOURCE_ID` as one valid combination from `data-contracts/source_areas.json`. Ingestion preflights the combination before source I/O and records it in every raw payload.

## Runtime parameters

| Parameter | Default | Purpose |
| --- | --- | --- |
| `DATASET` | `all` | `all`, `weather`, or `energy` |
| `SOURCE_AREA` | `east_midlands` | Canonical licence-area key |
| `WEATHER_CITY` | `Nottingham,GB` | Contract-bound weather proxy |
| `NATIONAL_GRID_RESOURCE_ID` | empty | Contract-bound NGED resource UUID |
| `OPENWEATHER_API_KEY` | empty | Secure weather credential |
| `NATIONAL_GRID_API_TOKEN` | empty | Secure NGED credential |
| `ENERGY_PAGE_SIZE` | `1000` | Records per deterministic CKAN page |
| `ENERGY_MAX_RECORDS` | `50000` | Explicit complete-snapshot safety bound |
| `ENERGY_LIMIT` | empty | Deprecated alias for page size |
| `CONTRACTS_ROOT` | empty | Optional contracts folder override |
| `TRAIN_FRACTION` | `0.60` | Earliest chronological training share |
| `VALIDATION_FRACTION` | `0.20` | Chronological validation share |
| `MIN_TRAIN_ROWS` | `24` | Minimum training labels after overlap purging |
| `MIN_VALIDATION_ROWS` | `6` | Minimum rows in each validation origin |
| `MIN_TEST_ROWS` | `6` | Minimum final test target rows |
| `RIDGE_REG_PARAM` | `1.0` | L2 regularisation strength |
| `HORIZON_MINUTES` | `30,60` | Approved elapsed-time targets; only 30 and 60 are accepted |
| `TARGET_TOLERANCE_MINUTES` | `5` | Maximum permitted target delay |
| `MIN_TARGET_COVERAGE` | `0.90` | Minimum matched/eligible coverage per group and horizon |
| `EVALUATION_MODE` | `holdout` | `holdout` or explicit `rolling-origin` |
| `ROLLING_ORIGIN_FOLDS` | `3` | Total origins in rolling mode; all but the final origin use validation history |
| `MAX_EXPECTED_DATA_LAG_HOURS` | `3` | Freshness warning threshold |

Manual interval parameters are documented separately in
`fabric/pipelines/prediction_interval_pipeline.md`:

```text
POINT_PREDICTION_RUN_ID
COVERAGE_LEVELS=0.80,0.90,0.95
MIN_CALIBRATION_ROWS=24
```

Manual interval-monitoring parameters are documented in
`fabric/pipelines/prediction_interval_monitoring_pipeline.md`:

```text
AS_OF_UTC=<optional timezone-aware boundary>
RECENT_INTERVAL_RUN_COUNT=3
REFERENCE_INTERVAL_RUN_COUNT=6
MIN_RECENT_INTERVAL_RUNS=2
MIN_REFERENCE_INTERVAL_RUNS=3
MAX_INTERVAL_RUN_AGE_MINUTES=10080
MAX_EVALUATION_AGE_MINUTES=20160
MIN_CALIBRATION_OBSERVATION_COUNT=24
MAX_RECENT_COVERAGE_SHORTFALL_PCT_POINTS=5.0
MAX_COVERAGE_DROP_PCT_POINTS=5.0
MAX_AVERAGE_INTERVAL_WIDTH_INCREASE_PCT=25.0
MAX_CALIBRATION_HISTORY_DROP_PCT=25.0
```

## Bounded time-horizon forecasting

`05_baseline_forecasting` cross-joins each causal feature row with the configured 30/60-minute horizons, then selects the first same-group demand observation at or after the ideal target time. A target is retained only when it is within `TARGET_TOLERANCE_MINUTES`.

Trailing feature rows without enough retained future history are excluded from coverage. Missing targets within retained history remain eligible and lower coverage. Every source-area/resource/city and configured horizon must have eligible history and meet `MIN_TARGET_COVERAGE`; otherwise the run fails before model fitting.

Prediction evidence records requested and actual elapsed horizon, observation distance, target delay, target coverage, feature/target timestamps, current and future demand, model output, errors, and the latest label used for fitting.

## Fixed holdout and rolling-origin modes

The fixed holdout remains the default:

```text
first 60% → training
next 20%  → validation
final 20% → test
```

Set `EVALUATION_MODE=rolling-origin` to split the validation 20% across `ROLLING_ORIGIN_FOLDS - 1` sequential origins while retaining the final 20% as one untouched test origin. Training candidates expand after each validation origin. Before every fit, labels whose target timestamp is at or after the origin cutoff are purged.

Rolling prediction and metric evidence includes:

- `origin_fold`;
- `origin_count`;
- `origin_cutoff_utc`;
- `training_observation_count`;
- `evaluation_contract_version=rolling-origin-v1`.

Holdout evidence uses `evaluation_contract_version=fixed-holdout-v1` and null origin fields, allowing both modes to append safely to the same Delta tables.

The forecasting notebook rejects incomplete or non-monotonic origin evidence before writing. `06_forecast_quality_checks` repeats those checks on the newest durable run, including complete fold sequences, strictly increasing cutoffs, non-decreasing post-purge training history, correct validation/test placement, and no reused evaluation timestamps.

Prediction and metric tables append by run ID. Define a retention or compaction policy before increasing schedule frequency, fold count, or retained history.

## Calibration-only prediction interval parity

`05e_prediction_intervals` consumes one retained
`forecast_seasonal_comparison_predictions` run. It never fits or refits a point
model.

For each source area, resource, city, horizon, model, and feature contract, it:

1. finds the first final-test feature timestamp;
2. keeps only validation residuals whose target timestamp is earlier than that
   boundary;
3. requires the configured minimum calibration history;
4. applies the finite-sample rank `ceil((n + 1) * q)`, clipped to `[1, n]`; and
5. writes symmetric test intervals and empirical coverage/width metrics.

`06e_prediction_interval_quality_checks` independently validates the source-run
binding, exact four-model pairs, calibration causality, finite-sample rank,
fixed radius, coverage-level completeness, interval bounds, metrics, and final
test origin.

The interval subflow is manual. Empirical coverage is retained evidence and not
a guarantee after distribution shift.

## Advisory prediction-interval monitoring parity

`05f_prediction_interval_monitoring` consumes retained
`forecast_prediction_interval_metrics`. It applies the same
`prediction-interval-monitoring-policy-v1` contract as the local pandas monitor
without creating or recalibrating an interval.

For each exact source-area/resource/city/horizon/model/feature-contract/
coverage-level/interval-contract slice, it:

1. ranks retained interval runs newest-first;
2. bounds processing to the configured recent and immediately preceding
   reference windows;
3. weights empirical coverage and average interval width by retained evaluation
   row count;
4. checks recent history, run freshness, evaluation freshness, minimum causal
   calibration history, and recent coverage shortfall;
5. emits coverage, width, and calibration-history drift warnings only when
   sufficient reference history exists; and
6. appends health checks and one summary row to separate Delta tables.

`06f_prediction_interval_monitoring_quality_checks` independently validates
required fields, check identity and completeness, conditional drift checks,
comparator outcomes, binding to retained source metrics, summary counts/status,
contract versions, and the five no-automatic-action fields.

The monitoring subflow is manual and advisory. It does not deliver an alert,
change a radius, select or refit a model, activate a schedule, or alter registry,
promotion, deployment, or infrastructure state.

## Deployment

1. Create the Lakehouse and Environment.
2. Upload all versioned contracts.
3. Import the six ordinary notebooks and attach the Lakehouse.
4. Import optional evidence notebooks only when their dependent tables are required.
5. Configure secure credentials or connection-backed secret lookup.
6. Create the ordinary Data Factory pipeline from the repository specification.
7. Configure the source binding and time-horizon parameters.
8. Keep `EVALUATION_MODE=holdout` for the ordinary run, or select `rolling-origin` with a reviewed fold count.
9. Run ingestion, silver, gold, source/gold quality, forecasting, and forecast quality once manually.
10. Run the seasonal and interval subflows manually when reviewed evidence is required.
11. After several interval runs exist, run the interval-monitoring subflow manually and inspect its independent quality gate.
12. Inspect requested horizon, actual delay, target coverage, evaluation contract, origin evidence, model metrics, interval coverage/width, interval health, and `dq_run_results`.
13. Create SQL endpoint views only when stable analyst-facing names are useful.
14. Enable the ordinary schedule after quota and capacity validation; do not schedule the evidence subflows by default.

Spark Delta tables are canonical. SQL endpoint views are pass-through views,
avoiding a second implementation of joins, feature windows, target matching,
origin construction, calibration, interval construction, monitoring windows,
or health logic.

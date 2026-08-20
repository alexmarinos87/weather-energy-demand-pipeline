# Fabric Data Factory pipeline

Pipeline name: `weather_energy_demand_pipeline`

## Parameters

| Name | Required | Notes |
| --- | --- | --- |
| `DATASET` | Yes | Use `all` for scheduled runs. |
| `SOURCE_AREA` | Yes | Key from `source_areas.json`. |
| `WEATHER_CITY` | Yes | Must match the area's weather proxy. |
| `NATIONAL_GRID_RESOURCE_ID` | Yes | Must match the area's NGED resource. |
| `OPENWEATHER_API_KEY` | Yes | Secure parameter or connection-backed secret. |
| `NATIONAL_GRID_API_TOKEN` | Yes | Secure parameter or connection-backed secret. |
| `ENERGY_PAGE_SIZE` | No | Default `1000`. |
| `ENERGY_MAX_RECORDS` | No | Default `50000`; ingestion fails rather than truncates. |
| `CONTRACTS_ROOT` | No | Override only when contracts are not in `Files/data-contracts`. |
| `TRAIN_FRACTION` | No | Default `0.60`. |
| `VALIDATION_FRACTION` | No | Default `0.20`. |
| `MIN_TRAIN_ROWS` | No | Default `24` after horizon-overlap purging. |
| `MIN_VALIDATION_ROWS` | No | Default `6`. |
| `MIN_TEST_ROWS` | No | Default `6`. |
| `RIDGE_REG_PARAM` | No | Default `1.0`. |
| `HORIZON_STEPS` | No | Default `1`; ordered source observations from feature time to target time. |
| `MAX_EXPECTED_DATA_LAG_HOURS` | No | Default `3`. |

## Activities

1. `01_ingest_api_to_bronze`
   - Preflight the source-area/resource/city binding before source I/O.
   - Validate every API response page.
   - Write immutable raw JSON with provenance metadata.
2. `02_bronze_to_silver`
   - Depend on successful ingestion.
   - Build typed, deduplicated silver tables and retain source identity.
3. `03_build_gold_tables`
   - Depend on silver success.
   - Match only same-area, past weather no more than six hours old.
   - Ensure demand rolling features exclude the current row.
4. `04_data_quality_checks`
   - Depend on gold success.
   - Persist source and gold quality results and fail on blocking defects.
5. `05_baseline_forecasting`
   - Depend on source/gold quality success.
   - Shift each causal feature row to an explicit future demand target using `HORIZON_STEPS`.
   - Purge training labels whose target time is not earlier than the first evaluation feature time.
   - Append future-horizon prediction and metric evidence.
   - Fail on insufficient purged history, invalid horizons, invalid predictions, duplicate timestamps, or label-availability leakage.
6. `06_forecast_quality_checks`
   - Depend on forecasting success.
   - Validate feature/target ordering, positive horizons, label-availability boundaries, and metric windows for the newest run.
   - Append results to `dq_run_results` and fail on blocking defects.

Start hourly. Use two retries separated by at least five minutes. Do not enable a faster cadence until API quota, source latency, forecast-run growth, and Fabric capacity have been observed.

# Fabric Orchestration Schedule

The cloud pipeline should run as a Microsoft Fabric Data Factory pipeline.

## Pipeline

Name: `weather_energy_demand_pipeline`

Activities:

1. Notebook: `01_ingest_api_to_bronze`
   - Parameters:
     - `DATASET=all`
     - `WEATHER_CITY=London,GB`
     - `NATIONAL_GRID_RESOURCE_ID=<resource UUID>`
     - API keys supplied as secure pipeline parameters or through a Fabric connection.
2. Notebook: `02_bronze_to_silver`
   - Rebuilds typed Delta silver tables from raw files.
3. Notebook: `03_build_gold_tables`
   - Rebuilds weather-demand join, model features, and aggregates.
4. Notebook: `04_data_quality_checks`
   - Optional parameter:
     - `MAX_EXPECTED_DATA_LAG_HOURS=3`
   - Writes run results to `dq_run_results`.
   - Fails the pipeline if required checks fail.
   - Writes freshness warnings when silver or gold timestamps are older than expected.

## Recommended Cadence

Start with an hourly schedule:

- Frequency: every 1 hour
- Time zone: UTC
- Retry: 2 retries with at least 5 minutes between attempts
- Timeout: 30 minutes per notebook activity

Move to a 15 or 30 minute schedule only after confirming API quota, Fabric capacity headroom, and downstream dashboard latency needs.

## Failure Handling

- Ingestion failure should stop the run before silver or gold tables are rebuilt.
- Data quality failure should mark the pipeline failed and preserve the `dq_run_results` row for investigation.
- Re-run the full pipeline after fixing source or schema issues because silver and gold are rebuilt from immutable raw files.

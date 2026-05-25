# Fabric Data Factory Pipeline

Pipeline name: `weather_energy_demand_pipeline`

## Parameters

| Name | Required | Notes |
| --- | --- | --- |
| `DATASET` | Yes | Use `all` for scheduled runs. |
| `WEATHER_CITY` | Yes | Example: `London,GB`. |
| `NATIONAL_GRID_RESOURCE_ID` | Yes | Connected Data resource UUID. |
| `OPENWEATHER_API_KEY` | Yes | Mark as secure. |
| `NATIONAL_GRID_API_TOKEN` | Yes | Mark as secure. |
| `ENERGY_LIMIT` | No | Default `1000`. |

## Activities

1. Notebook activity: `01_ingest_api_to_bronze`
   - Pass all pipeline parameters.
   - Stop pipeline on failure.
2. Notebook activity: `02_bronze_to_silver`
   - Depends on ingestion success.
3. Notebook activity: `03_build_gold_tables`
   - Depends on silver success.
4. Notebook activity: `04_data_quality_checks`
   - Depends on gold success.
   - Any raised exception should fail the pipeline.

## Schedule

Use the cadence in `orchestration/schedules.md`. Start hourly until API quota and Fabric capacity usage are confirmed.

## Observability

Use the pipeline run history for activity failures and the Lakehouse table `dq_run_results` for data quality failures.

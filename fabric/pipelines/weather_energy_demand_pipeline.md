# Fabric Data Factory Pipeline

Pipeline name: `weather_energy_demand_pipeline`

## Parameters

| Name | Required | Notes |
| --- | --- | --- |
| `DATASET` | Yes | Use `all` for scheduled runs. |
| `WEATHER_CITY` | Yes | Example: `London,GB`. |
| `NATIONAL_GRID_RESOURCE_ID` | Yes | NGED Connected Data resource UUID. |
| `OPENWEATHER_API_KEY` | Yes | Mark as secure. |
| `NATIONAL_GRID_API_TOKEN` | Yes | Mark as secure. |
| `ENERGY_PAGE_SIZE` | No | Default `1000`; records per deterministic CKAN request. |
| `ENERGY_MAX_RECORDS` | No | Default `50000`; fail rather than publish a larger unreviewed snapshot. |
| `ENERGY_LIMIT` | No | Deprecated compatibility alias for `ENERGY_PAGE_SIZE`. |
| `CONTRACTS_ROOT` | No | Override only if contracts are not stored under `Files/data-contracts`. |
| `MAX_EXPECTED_DATA_LAG_HOURS` | No | Default `3`; passed to data quality checks as the freshness warning threshold. |

## Activities

1. Notebook activity: `01_ingest_api_to_bronze`
   - Pass all pipeline parameters.
   - Validate every API response page against the versioned JSON contracts.
   - Reassemble the complete energy snapshot in ascending CKAN `_id` order.
   - Stop the pipeline on contract failure, pagination inconsistency, or record-bound violation.
2. Notebook activity: `02_bronze_to_silver`
   - Depends on ingestion success.
3. Notebook activity: `03_build_gold_tables`
   - Depends on silver success.
4. Notebook activity: `04_data_quality_checks`
   - Depends on gold success.
   - Pass `MAX_EXPECTED_DATA_LAG_HOURS` when overriding the default freshness threshold.
   - Any raised exception should fail the pipeline.

## Schedule

Use the cadence in `orchestration/schedules.md`. Start hourly until API quota and Fabric capacity usage are confirmed.

## Observability

Use the pipeline run history for activity failures and the Lakehouse table `dq_run_results` for data quality failures. Raw energy files also expose pagination evidence under `result.pagination`.

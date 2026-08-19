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
4. `04_data_quality_checks`
   - Depend on gold success.
   - Persist all results and fail on blocking errors.

Start hourly. Use two retries separated by at least five minutes. Do not enable a faster cadence until API quota, source latency, and Fabric capacity have been observed.

# Microsoft Fabric implementation

## Workspace shape

- Lakehouse: `weather_energy_lakehouse`
- Environment: `weather_energy_env`
- Notebooks: `01_ingest_api_to_bronze`, `02_bronze_to_silver`, `03_build_gold_tables`, `04_data_quality_checks`
- Data Factory pipeline: `weather_energy_demand_pipeline`

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

Tables:

- `silver_weather`
- `silver_energy`
- `gold_weather_demand_join`
- `gold_feature_engineering`
- `gold_demand_aggregation`
- `dq_run_results`

## Required source binding

Set `SOURCE_AREA`, `WEATHER_CITY`, and `NATIONAL_GRID_RESOURCE_ID` as one valid combination from `data-contracts/source_areas.json`. The ingestion notebook preflights the complete combination before source I/O and records the binding in each raw payload.

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
| `MAX_EXPECTED_DATA_LAG_HOURS` | `3` | Freshness warning threshold |

## Deployment

1. Create the Lakehouse and Environment.
2. Upload all versioned contracts.
3. Import the four notebooks and attach the Lakehouse.
4. Configure secure credentials or connection-backed secret lookup.
5. Create the Data Factory pipeline from the repository specification.
6. Run once manually and inspect `dq_run_results`.
7. Create SQL endpoint views only when stable analyst-facing names are useful.
8. Enable the schedule after quota and capacity validation.

Spark Delta tables are canonical. SQL endpoint views are pass-through views, avoiding a second implementation of joins and feature windows.

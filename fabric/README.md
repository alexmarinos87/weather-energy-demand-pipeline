# Microsoft Fabric Migration

This folder contains the Fabric-native implementation for the weather and energy demand pipeline.

## Workspace Shape

Use a Fabric workspace with these items:

- Lakehouse: `weather_energy_lakehouse`
- Environment: `weather_energy_env`
- Notebooks:
  - `01_ingest_api_to_bronze`
  - `02_bronze_to_silver`
  - `03_build_gold_tables`
  - `04_data_quality_checks`
- Data Factory pipeline: `weather_energy_demand_pipeline`

Attach `weather_energy_lakehouse` as the default Lakehouse for every notebook.

## OneLake Layout

Raw API captures:

- `Files/raw/weather/ingestion_date=YYYY-MM-DD/weather_YYYYMMDD_HHMMSS.json`
- `Files/raw/energy/ingestion_date=YYYY-MM-DD/energy_YYYYMMDD_HHMMSS.json`

Versioned ingestion contracts:

- `Files/data-contracts/weather_schema.json`
- `Files/data-contracts/energy_schema.json`

Lakehouse tables:

- `silver_weather`
- `silver_energy`
- `gold_weather_demand_join`
- `gold_feature_engineering`
- `gold_demand_aggregation`
- `dq_run_results`

## Deployment Steps

1. Create the Lakehouse and Environment in Fabric.
2. Add the public Python libraries from `fabric/environment.yml` to the Environment.
3. Upload `data-contracts/weather_schema.json` and `data-contracts/energy_schema.json` to `Files/data-contracts/` in the Lakehouse.
4. Import each `.py` file in `fabric/notebooks/` as a Fabric notebook source.
5. Attach the Lakehouse and Environment to each notebook.
6. Create a Data Factory pipeline using `fabric/pipelines/weather_energy_demand_pipeline.md`.
7. Add secure parameters or connection-backed secrets for:
   - `OPENWEATHER_API_KEY`
   - `NATIONAL_GRID_API_TOKEN`
8. Run the notebooks in order once manually.
9. Add SQL endpoint views from `fabric/sql/gold_views_tsql.sql` if stable analyst-facing names are needed.
10. Enable the schedule from `orchestration/schedules.md`.

## Runtime Parameters

`01_ingest_api_to_bronze` accepts:

| Parameter | Default | Purpose |
| --- | --- | --- |
| `DATASET` | `all` | `all`, `weather`, or `energy` |
| `WEATHER_CITY` | `London,GB` | OpenWeather city query |
| `NATIONAL_GRID_RESOURCE_ID` | empty | Connected Data resource UUID |
| `OPENWEATHER_API_KEY` | empty | Secure weather API key |
| `NATIONAL_GRID_API_TOKEN` | empty | Secure energy API token |
| `ENERGY_LIMIT` | `1000` | Max records per energy pull |
| `CONTRACTS_ROOT` | empty | Optional override for the folder containing `weather_schema.json` and `energy_schema.json`; defaults to `Files/data-contracts` |

## Migration Notes

- The local Python scripts remain useful for quick development and tests.
- The Fabric notebooks are the production cloud path.
- Silver and gold tables are rebuilt from raw files. That is deliberate for this project scale and keeps lineage simple.
- For larger history, replace overwrite writes with Delta merge logic partitioned by `event_date_utc`.
- Use Spark notebooks to modify Lakehouse Delta tables. The SQL analytics endpoint is for T-SQL querying and reusable views over those tables.

## Microsoft References

- [What is Microsoft Fabric?](https://learn.microsoft.com/en-us/fabric/get-started/microsoft-fabric-overview)
- [Lakehouse and Delta Lake tables](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-and-delta-tables)
- [SQL analytics endpoint for a Lakehouse](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-sql-analytics-endpoint)
- [Notebook activity in Fabric Data Factory](https://learn.microsoft.com/en-us/fabric/data-factory/notebook-activity)
- [Fabric deployment pipelines](https://learn.microsoft.com/en-us/fabric/cicd/deployment-pipelines/intro-to-deployment-pipelines)

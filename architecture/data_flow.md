# Microsoft Fabric Data Flow

This project now targets Microsoft Fabric as the managed analytics platform.

## Target Architecture

```text
OpenWeather API             National Grid Connected Data API
       |                                  |
       | Fabric notebook HTTP ingestion   |
       v                                  v
OneLake Lakehouse Files: raw/weather and raw/energy
       |
       | Fabric Spark notebook validation, parsing, deduplication
       v
Lakehouse Delta tables: silver_weather and silver_energy
       |
       | Fabric Spark notebook joins, features, aggregates
       v
Lakehouse Delta tables: gold_weather_demand_join,
                       gold_feature_engineering,
                       gold_demand_aggregation
       |
       | SQL analytics endpoint / Power BI semantic model
       v
Analyst queries, dashboards, forecasting features
```

## Medallion Layout

Raw files are immutable API captures:

- `Files/raw/weather/ingestion_date=YYYY-MM-DD/weather_YYYYMMDD_HHMMSS.json`
- `Files/raw/energy/ingestion_date=YYYY-MM-DD/energy_YYYYMMDD_HHMMSS.json`

Silver tables are canonical, typed, and deduplicated:

- `silver_weather`
- `silver_energy`

Gold tables are analytics-ready:

- `gold_weather_demand_join`
- `gold_feature_engineering`
- `gold_demand_aggregation`

Data quality run history is stored in:

- `dq_run_results`

## AWS to Fabric Mapping

| Previous AWS concept | Microsoft Fabric target |
| --- | --- |
| S3 raw and curated buckets | OneLake Lakehouse Files and Tables |
| Glue catalog tables | Lakehouse Delta tables and SQL analytics endpoint metadata |
| Athena SQL views | Fabric Spark SQL tables and SQL endpoint T-SQL views |
| Step Functions schedules | Fabric Data Factory pipeline |
| CloudWatch checks and alarms | Fabric monitoring hub plus `dq_run_results` |
| IAM roles and secrets | Fabric workspace roles, connections, and secure pipeline parameters |

## Operational Notes

- Keep raw API responses immutable so downstream records remain traceable to source payloads.
- Recompute silver and gold tables from raw files for this project scale. Move to incremental merge logic when history becomes large.
- Use a Fabric deployment pipeline or Git integration for promotion between dev, test, and production workspaces.
- Keep API keys in Fabric connections, Azure Key Vault, or secure pipeline parameters. Do not commit secrets.

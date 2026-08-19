# Microsoft Fabric data flow

## Target architecture

```text
OpenWeather API                    NGED Connected Data API
       |                                     |
       | source-area preflight + contracts   |
       v                                     v
OneLake Files/raw/weather and Files/raw/energy
       | immutable JSON with _pipeline_metadata
       v
Fabric Spark: parse, type, deduplicate, retain provenance
       v
silver_weather -------------------- silver_energy
       | same source_area; weather <= demand; age <= 6 hours
       v
gold_weather_demand_join
       |
       | lag, rolling, calendar and weather-degree features
       v
gold_feature_engineering
       |
       v
gold_demand_aggregation
       |
       v
SQL endpoint pass-through views / Power BI / model experiments
```

## Source identity

`data-contracts/source_areas.json` is the canonical project mapping between an NGED Live Data resource and a representative OpenWeather city. Ingestion writes the contract version, source-area key, display name, NGED resource ID, and weather proxy city into `_pipeline_metadata`.

Silver retains this provenance. Historical raw files without metadata remain readable and receive a null `source_area`; data-quality checks warn about them, and SQL null semantics prevent them from joining into scoped gold features.

## Causal matching rule

A demand record may match only a weather record that:

1. has the same non-null `source_area`;
2. occurred at or before the demand timestamp; and
3. is no more than six hours old.

The latest eligible observation wins, with ingestion timestamp as a deterministic tie-breaker. `weather_age_minutes` remains in gold for observability.

## Canonical implementation

Spark Delta tables are canonical. The SQL analytics endpoint exposes pass-through views and does not reimplement joins or windows in T-SQL. This prevents logic drift between two SQL dialects.

## Scale boundary

Silver and gold are currently rebuilt from immutable raw files. Replace overwrite writes with partition-aware Delta merge logic when history or Fabric capacity makes full rebuilds inappropriate.

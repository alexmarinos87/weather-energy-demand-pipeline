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
       | prior-demand lag/rolling + calendar/weather features
       v
gold_feature_engineering ----------------> gold_demand_aggregation
       |
       | chronological train / validation / test
       | persistence and ridge baselines
       v
forecast_baseline_predictions + forecast_baseline_metrics
       |
       v
SQL endpoint pass-through views / Power BI / model comparison
```

## Source identity

`data-contracts/source_areas.json` is the canonical project mapping between an NGED Live Data resource and a representative OpenWeather city. Ingestion writes the contract version, source-area key, display name, NGED resource ID, and weather proxy city into `_pipeline_metadata`.

Silver retains this provenance. Historical raw files without metadata remain readable and receive a null `source_area`; data-quality checks warn about them, and SQL null semantics prevent them from joining into scoped gold features.

## Causal matching and feature rule

A demand record may match only a weather record that:

1. has the same non-null `source_area`;
2. occurred at or before the demand timestamp; and
3. is no more than six hours old.

The latest eligible observation wins, with ingestion timestamp as a deterministic tie-breaker. `weather_age_minutes` remains in gold for observability.

Target-derived features use prior demand only. `demand_lag_1` uses the immediately preceding record and `demand_rolling_mean_12` uses rows 12 through 1 before the target. The current `demand_mw` value is never included in its own input features.

## Chronological evaluation contract

Within each `source_area`, `resource_id`, and `city` group:

1. rows are sorted by `event_timestamp_utc`;
2. the first 60% form training history;
3. the next 20% form validation history;
4. the final 20% form test history;
5. validation models fit only training rows;
6. test models fit only training plus validation rows; and
7. every prediction must satisfy `trained_through_utc < event_timestamp_utc`.

The benchmark compares one-step persistence with ridge regression over causal features. Each run appends versioned prediction and metric evidence rather than overwriting prior evaluations.

## Canonical implementation

Spark Delta tables are canonical. The SQL analytics endpoint exposes pass-through views and does not reimplement joins, feature windows, or model logic in T-SQL. This prevents logic drift between execution paths.

The local pandas implementation exercises the same evaluation contract in CI and provides a credential-free demonstration. The Fabric notebook uses Spark ML for Lakehouse-scale execution.

## Scale boundary

Silver and gold are currently rebuilt from immutable raw files. Forecasting loops over the small set of licence-area groups and appends run evidence. Replace full rebuilds and driver-side group enumeration with partition-aware Delta merge and distributed model orchestration when history, area count, or Fabric capacity makes this inappropriate.

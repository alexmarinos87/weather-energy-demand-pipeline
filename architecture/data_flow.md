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
       | feature time --HORIZON_STEPS--> future target time
       | chronological split + overlapping-label purge
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

A demand record may match only a weather record that has the same non-null `source_area`, occurred at or before demand time, and is no more than six hours old. The latest eligible observation wins, with ingestion timestamp as a deterministic tie-breaker.

Gold demand features are causal: `demand_lag_1` uses the preceding observation and `demand_rolling_mean_12` uses rows 12 through 1 before the current observation.

## Future-horizon evaluation contract

Within each `source_area`, `resource_id`, and `city` group:

1. rows are ordered by source `event_timestamp_utc`;
2. feature time is the current row timestamp;
3. target time and target demand come from `HORIZON_STEPS` later in that same group;
4. target time must be strictly after feature time;
5. the supervised rows are split chronologically into training, validation, and test;
6. before validation fitting, training labels with target time at or after the first validation feature time are purged;
7. before test fitting, candidate labels with target time at or after the first test feature time are purged; and
8. every prediction must satisfy `trained_through_utc < feature_timestamp_utc < event_timestamp_utc`.

The persistence forecast uses current demand for the future target. Ridge regression may use current demand because it is known at feature time, together with prior demand, rolling demand, weather, and calendar inputs.

Prediction evidence stores the feature timestamp, future target timestamp, ordered horizon, observed horizon minutes, current demand, actual future demand, prediction, errors, and training boundary. Metrics retain both feature-window and target-window timestamps.

## Canonical implementation

Spark Delta tables are canonical. The SQL analytics endpoint exposes pass-through views and does not reimplement joins, feature windows, or model logic in T-SQL.

The local pandas implementation exercises the same future-horizon and label-purge contract in CI. The Fabric notebook uses Spark ML for Lakehouse-scale execution.

## Scale boundary

Silver and gold are currently rebuilt from immutable raw files. Forecasting loops over the small set of licence-area groups and appends run evidence. Replace full rebuilds and driver-side group enumeration with partition-aware Delta merge and distributed model orchestration when history, area count, or Fabric capacity makes this inappropriate.

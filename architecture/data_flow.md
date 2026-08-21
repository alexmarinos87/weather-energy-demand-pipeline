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
       | bounded 30/60-minute target matching
       | chronological split + unavailable-label purge
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

## Time-horizon evaluation contract

Within each source-area/resource/city group:

1. Feature rows are ordered by source `event_timestamp_utc`.
2. The ideal target time is feature time plus the configured 30- or 60-minute service horizon.
3. A row is coverage-eligible only when its ideal target lies within retained history; trailing rows are excluded.
4. The target is the first same-group observation at or after the ideal time.
5. The match is retained only when delay is within `TARGET_TOLERANCE_MINUTES`.
6. Every configured source group and horizon must have eligible history and meet `MIN_TARGET_COVERAGE`.
7. Matched rows are split chronologically into training, validation, and test.
8. Before validation and test fitting, labels whose target time is not earlier than the first evaluation feature time are purged.
9. Every prediction must satisfy `trained_through_utc < feature_timestamp_utc < event_timestamp_utc`.

This separates service horizon from source cadence. `requested_horizon_minutes` expresses the product requirement; `horizon_minutes` and `target_delay_minutes` expose the source observation actually used; `horizon_steps` remains diagnostic only.

The persistence forecast uses current demand for the future target. Ridge regression may use current demand because it is known at feature time, together with prior demand, rolling demand, weather, and calendar inputs.

## Canonical implementation

Spark Delta tables are canonical. The SQL analytics endpoint exposes pass-through views and does not reimplement joins, feature windows, target matching, or model logic in T-SQL.

The local pandas and Fabric Spark implementations use the same horizon, tolerance, coverage, and label-purge contract. Static parity tests prevent either path from reverting to observation-count targeting.

## Scale boundary

Silver and gold are currently rebuilt from immutable raw files. Forecasting enumerates the small set of source group/horizon combinations and appends run evidence. Replace full rebuilds, self-join target matching, and driver-side model enumeration with partition-aware Delta merge and distributed model orchestration when history, area count, or Fabric capacity makes this inappropriate.

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
       | fixed holdout OR expanding rolling origins
       | unavailable-label purge at every cutoff
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
7. Before every model fit, labels whose target time is not earlier than the evaluation cutoff are purged.
8. Every prediction must satisfy `trained_through_utc < feature_timestamp_utc < event_timestamp_utc`.

This separates service horizon from source cadence. `requested_horizon_minutes` expresses the product requirement; `horizon_minutes` and `target_delay_minutes` expose the source observation actually used; `horizon_steps` remains diagnostic only.

The persistence forecast uses current demand for the future target. Ridge regression may use current demand because it is known at feature time, together with prior demand, rolling demand, weather, and calendar inputs.

## Evaluation contracts

`EVALUATION_MODE=holdout` preserves the existing 60/20/20 chronological split:

```text
training → validation → untouched test
```

`EVALUATION_MODE=rolling-origin` keeps the same first 60% as initial history, partitions the validation 20% across `ROLLING_ORIGIN_FOLDS - 1` sequential validation origins, then evaluates the final 20% once as the untouched final origin.

For every source identity, horizon, and model, rolling-origin evidence must contain:

- folds `1..origin_count`;
- strictly increasing `origin_cutoff_utc`;
- non-decreasing `training_observation_count`;
- `split=validation` for all non-final folds;
- `split=test` for the final fold; and
- no feature timestamp evaluated in more than one origin.

Every rolling-origin prediction satisfies:

```text
trained_through_utc
    <
origin_cutoff_utc
    <=
feature_timestamp_utc
    <
event_timestamp_utc
```

Holdout rows use `evaluation_contract_version=fixed-holdout-v1` and null origin fields. Rolling rows use `evaluation_contract_version=rolling-origin-v1` and complete origin evidence. Both modes append to the same canonical Delta tables with schema evolution.

## Canonical implementation

Spark Delta tables are canonical. The SQL analytics endpoint exposes pass-through views and does not reimplement joins, feature windows, target matching, origin construction, or model logic in T-SQL.

The local pandas and Fabric Spark implementations now use the same time horizon, tolerance, target coverage, unavailable-label purge, fixed-holdout default, and optional rolling-origin contract. Static parity tests prevent either path from reverting to observation-count targeting or silently replacing the default evaluation mode.

## Scale boundary

Silver and gold are currently rebuilt from immutable raw files. Forecasting enumerates the small set of source group/horizon combinations and fits both models once per evaluation origin. Replace full rebuilds, self-join target matching, driver-side group/origin enumeration, and repeated model fitting with partition-aware Delta merge and distributed model orchestration when history, area count, fold count, or Fabric capacity makes this inappropriate.

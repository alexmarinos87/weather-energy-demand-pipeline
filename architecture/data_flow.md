# Microsoft Fabric data flow

## Deployed Fabric path

```text
OpenWeather current weather          NGED Connected Data API
          |                                      |
          | source-area preflight + contracts    |
          v                                      v
OneLake Files/raw/weather and Files/raw/energy
          | immutable JSON with provenance
          v
Fabric Spark: parse, type, deduplicate
          v
silver_weather ---------------------- silver_energy
          | same source_area; weather <= demand; age <= 6 hours
          v
gold_weather_demand_join
          |
          | prior-demand lag/rolling + calendar/weather features
          v
gold_feature_engineering ------------> gold_demand_aggregation
          |
          | bounded 30/60-minute target matching
          | fixed holdout OR expanding rolling origins
          | unavailable-label purge at every cutoff
          v
forecast_baseline_predictions + forecast_baseline_metrics
          |
          v
SQL endpoint pass-through views / Power BI
```

## Local forecast-weather path

```text
OpenWeather 5-day / 3-hour forecast
          | fixed HTTPS host/path + source-area coordinates
          | raw contract, count/order/location checks
          v
immutable raw JSON + SHA-256 snapshot identity
          |
          | retrieval-time availability boundary
          | future valid-time normalization
          v
normalized forecast-weather Parquet
          |
          | causal issue/ingestion/valid-time matching
          v
paired supervised demand rows
          | identical target rows, cutoffs, and label purge
          v
ridge_weather_lag -------- ridge_target_weather
          |
          v
local weather-comparison predictions + metrics
```

The local adapter is not yet part of the canonical Fabric pipeline.

## Source identity

`data-contracts/source_areas.json` is the canonical mapping between each NGED licence-area resource and a representative OpenWeather city and coordinate pair. Local ingestion writes the contract version, source-area key, display name, NGED resource ID, proxy city, and proxy coordinates into `_pipeline_metadata`.

The cities and coordinates are project proxies, not claims that one point represents every weather condition across a licence area.

## Current-weather causality

A demand record may match only a weather observation with the same non-null `source_area`, a timestamp at or before demand time, and an age no greater than six hours. The latest eligible observation wins. Gold demand features exclude the current demand row from lag and rolling inputs.

## Demand target contract

Within each source-area/resource/city group:

1. Order rows by source event time.
2. Calculate the ideal target at feature time plus 30 or 60 minutes.
3. Exclude trailing rows whose ideal target is beyond retained history.
4. Select the first observation at or after the ideal time.
5. Retain it only inside the target-delay tolerance.
6. Require eligible/matched coverage for every group and horizon.
7. Purge labels unavailable at each evaluation cutoff.
8. Require `trained_through_utc < feature_timestamp_utc < event_timestamp_utc`.

## OpenWeather forecast availability contract

The OpenWeather 5-day / 3-hour response supplies forecast valid timestamps. The adapter records the completed retrieval time as a conservative issue and ingestion surrogate because it does not claim a provider model-run timestamp:

```text
forecast_issue_basis=retrieval_time_surrogate
forecast_issued_at_utc
    = forecast_ingested_at_utc
    = forecast_retrieved_at_utc
```

This makes the forecast unavailable before it has actually arrived. Raw snapshots are bounded to 40 records, validated against their declared count, required to have unique increasing valid times, and checked against the contract-bound GB coordinates.

For demand matching, a normalized forecast must satisfy:

```text
forecast_issued_at_utc
    <=
forecast_ingested_at_utc
    <=
feature_timestamp_utc
    <
target_timestamp_utc
```

The valid time must be after feature time and close enough to the future demand target. Coverage is enforced per source-area/resource/city/horizon group.

## Paired model contract

The local comparison uses one target-weather-covered cohort. Both ridge models share all demand, lag, rolling, calendar, target, split, rolling-origin, and label-purge semantics. Only observed weather is substituted with target-valid forecast weather.

The evaluator rejects divergent row identities or training boundaries. `weather_feature_mode` distinguishes `observed_at_feature` from `target_forecast` evidence.

## Evaluation contracts

`EVALUATION_MODE=holdout` preserves the chronological 60/20/20 split:

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

Holdout rows use `evaluation_contract_version=fixed-holdout-v1` and null origin fields. Rolling rows use `evaluation_contract_version=rolling-origin-v1` and complete origin evidence.

## Canonical implementation and boundary

Spark Delta tables remain canonical for the deployed Fabric path. SQL endpoint views do not reimplement feature, target, origin, or model logic.

The local pandas and Fabric Spark paths continue to share the time-horizon, target-tolerance, target-coverage, unavailable-label purge, fixed-holdout default, and optional rolling-origin contracts. The OpenWeather adapter and target-weather model comparison deliberately remain outside the canonical Fabric path until retained evidence justifies provider ingestion and Spark parity.

## Scale boundary

Silver and gold are currently rebuilt from immutable raw files. Forecasting enumerates the small set of source group/horizon combinations and fits models once per evaluation origin. The local forecast adapter stores one bounded provider response and one normalized Parquet snapshot per invocation.

Replace full rebuilds, self-join target matching, driver-side group/origin enumeration, repeated model fitting, and per-run local forecast files with partition-aware Delta merge, distributed orchestration, retention, and compaction when history, area count, fold count, snapshot frequency, or Fabric capacity makes the current design inappropriate.

Before Fabric parity, retained forecasts should be reconciled against later observations to measure provider bias, availability, target-time coverage, and snapshot growth.

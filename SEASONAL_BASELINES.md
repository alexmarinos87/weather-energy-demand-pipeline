# Elapsed-time seasonal demand baselines

## Purpose

Current-value persistence is a useful control, but electricity demand also has strong daily and weekly structure. This contract adds previous-day and previous-week baselines without assuming that every source emits at a fixed row cadence.

The comparison is opt-in. Existing baseline and target-weather commands are unchanged.

## Target and reference semantics

For every already-supervised demand row:

```text
feature_timestamp_utc < target_timestamp_utc
```

The seasonal reference ideals are:

```text
previous day  = target_timestamp_utc - 1,440 minutes
previous week = target_timestamp_utc - 10,080 minutes
```

The source match must belong to the same `source_area`, `resource_id`, and `city`; be available no later than feature time; fall within the configured tolerance; and minimize absolute elapsed-time difference. When two observations are equally close, the observation at or before the ideal time wins.

The implementation searches timestamps. It does not use `shift(288)`, `shift(2016)`, or another row count as a proxy for a day or week.

## Coverage boundary

A reference is eligible only when its ideal timestamp lies inside retained source history. Rows whose ideal predates retained history are excluded from that period's denominator.

A missing observation inside retained history remains eligible and lowers coverage. Coverage is checked independently for every source group, demand horizon, and reference period.

The comparison cohort contains only rows with both previous-day and previous-week references. All models then use exactly that same cohort.

## Paired models

```text
persistence_current_value
seasonal_previous_day
seasonal_previous_week
ridge_weather_lag
```

For each holdout or rolling origin, one unavailable-label purge establishes a shared `trained_through_utc` boundary. Only the ridge requires fitting, but all four models record the same comparison boundary and evaluation rows.

Every seasonal prediction retains ideal and selected source timestamps, selected demand, signed and absolute offset, source age at feature time, tolerance, day/week coverage, and `seasonal_baseline_contract_version=elapsed-seasonal-v1`.

## UTC and daylight saving

Seasonal periods are elapsed UTC time. Around a Europe/London daylight-saving transition, an exact 1,440-minute source may have a different local civil hour. That is deliberate: this baseline measures an elapsed previous day. UK-local calendar ridge features remain a separate model contract.

## Credential-free demo

```bash
python -m forecasting.run_seasonal_baselines \
  --demo \
  --demo-days 12 \
  --horizon-minutes 30 60 \
  --seasonal-reference-tolerance-minutes 15 \
  --min-seasonal-reference-coverage 0.90 \
  --output-dir data/forecasting \
  --output-format csv
```

Outputs:

```text
seasonal_comparison_predictions.csv
seasonal_comparison_metrics.csv
```

Add `--evaluation-mode rolling-origin --rolling-origin-folds 3` for repeated cutoffs. Add `--calendar-mode uk-local` to use the UK-local ridge control while retaining UTC seasonal matching; output names gain `_uk_local_calendar`.

For real data:

```bash
python -m forecasting.run_seasonal_baselines \
  --input gold_feature_engineering.parquet \
  --evaluation-mode rolling-origin \
  --horizon-minutes 30 60 \
  --output-dir data/forecasting \
  --output-format parquet
```

At least a week of retained source history is required before a previous-week cohort can exist.

## Contracts

- `data-contracts/seasonal_baseline_prediction_schema.json`
- `data-contracts/seasonal_baseline_metrics_schema.json`

## Boundary

This is a local historical comparison. It makes no source call, changes no Fabric table, activates no schedule, and promotes no model. Fabric parity is a separate dependent increment.

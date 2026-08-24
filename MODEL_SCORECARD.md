# Paired model-family scorecard

## Purpose

The repository can produce elapsed-time seasonal evidence twice over the same
demand history:

- `time-horizon-v1`, using UTC calendar features for the ridge model; and
- `time-horizon-uk-calendar-v1`, using derived Europe/London calendar features.

A direct comparison is valid only when both retained runs evaluate exactly the
same source-area/resource/city, 30/60-minute target, split, rolling origin,
actual demand, training boundary, and training-row count.

`forecasting.model_family_scorecard` verifies that pairing and then compares:

```text
persistence_current_value
seasonal_previous_day
seasonal_previous_week
ridge_weather_lag_utc
ridge_weather_lag_uk_local
```

The output is evidence for review. It does not approve, promote, register, or
activate a model.

## Pairing contract

Each input must contain one complete seasonal run with the four original models.
The three controls must be identical across the UTC and UK-local runs:

```text
persistence_current_value
seasonal_previous_day
seasonal_previous_week
```

The scorecard rejects:

- different target identities;
- missing or duplicate model rows;
- different actual demand values;
- changed control predictions;
- different training boundaries or row counts;
- different holdout/rolling-origin evidence; or
- feature-contract identities that do not match the selected input.

Only the ridge prediction is allowed to differ between calendar modes.

## Evidence

The scorecard records metrics by:

```text
source_area
resource_id
city
requested_horizon_minutes
split
origin_fold, when present
model_name
```

Metrics include:

- paired observation count;
- SHA-256 of the paired target identities;
- MAE, RMSE, MAPE, and bias;
- median absolute error; and
- p95 absolute error.

Pairwise evidence compares every candidate with
`persistence_current_value` on exactly the same rows. It retains candidate and
reference MAE/RMSE, absolute MAE improvement, and win/tie/loss counts.

The contract version is:

```text
paired-model-family-scorecard-v1
```

## Run

Create UTC and UK-local seasonal prediction files over the same input and
configuration, then run:

```bash
python -m forecasting.run_model_family_scorecard \
  --utc-predictions data/forecasting/seasonal_comparison_predictions.parquet \
  --uk-local-predictions data/forecasting/seasonal_comparison_uk_local_calendar_predictions.parquet \
  --output-dir data/forecasting/model_family_scorecard \
  --output-format parquet
```

When an input directory contains several runs, select each explicitly:

```text
--utc-run-id <run-id>
--uk-local-run-id <run-id>
```

Outputs are immutable and scorecard-run-qualified:

```text
model_family_scorecard_<run-id>.parquet
model_family_pairwise_metrics_<run-id>.parquet
model_family_summary_<run-id>.md
```

## Interpretation

Comparisons remain partitioned by source area and demand horizon. A model that
wins in one area or horizon is not presented as universally best. UTC remains
the canonical target and ordering identity; Europe/London fields remain derived
features identified by their feature-contract version.

The Markdown summary shows retained test error and improvement over persistence,
but deliberately contains no approval or promotion status.

## Contracts

- `data-contracts/model_family_scorecard_schema.json`
- `data-contracts/model_family_pairwise_metrics_schema.json`

## Boundary

The scorecard performs no source request, model fit, model refit, Fabric
operation, schedule activation, registration, promotion, deployment,
infrastructure action, or publication. Portfolio-demo integration remains a
separate reviewed increment.

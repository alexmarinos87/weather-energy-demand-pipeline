# Calibration-only prediction intervals

## Purpose

Point forecasts do not express uncertainty. This layer converts retained validation and test point-prediction evidence into symmetric prediction intervals without refitting the underlying model and without using test labels to choose interval width.

The interval evidence is retrospective evaluation. It is not a probabilistic guarantee for arbitrary future data and does not promote or deploy a model.

## Causal calibration boundary

For every source area, resource, city, demand horizon, model, and feature contract:

1. identify the first test feature timestamp;
2. retain only validation rows whose target label timestamp is strictly earlier than that test feature timestamp;
3. require at least `min_calibration_rows` causally available residuals;
4. calculate absolute validation residuals; and
5. apply one fixed radius to every test prediction in that slice.

The key constraint is:

```text
calibration_label_available_through_utc
    <
evaluation_feature_start_utc
```

Validation labels that overlap the test feature boundary are excluded even when their validation feature timestamps are earlier. Test labels never set the radius.

## Finite-sample quantile

For `n` sorted absolute calibration errors and target coverage `q`, the radius uses the conservative one-indexed rank:

```text
rank = ceil((n + 1) * q)
rank = min(max(rank, 1), n)
radius = sorted_absolute_errors[rank - 1]
```

The interval is:

```text
point_prediction_mw - radius
    <= future demand <=
point_prediction_mw + radius
```

The implementation supports several coverage levels in one run. The default levels are 80%, 90%, and 95%.

## Evidence

Each test row records:

- point-prediction run identity;
- interval run identity and timestamp;
- source area/resource/city, horizon, model, and feature contract;
- actual and point-predicted demand;
- target coverage level;
- lower and upper bounds, width, and coverage outcome;
- calibration row count, finite-sample rank, and radius;
- calibration feature range and latest available label timestamp;
- test-origin identity when rolling-origin point evidence is supplied; and
- `interval_contract_version=split-conformal-absolute-residual-v1`.

Metrics retain empirical test coverage and interval-width evidence. Empirical coverage is an evaluation result, not a claim that the same coverage must hold after distribution shift.

## Run locally

First create point predictions with an existing fixed-holdout or rolling-origin command. Then run:

```bash
python -m forecasting.run_prediction_intervals \
  --predictions-input data/forecasting/seasonal_comparison_predictions.parquet \
  --coverage-levels 0.80 0.90 0.95 \
  --min-calibration-rows 24 \
  --output-dir data/forecasting/prediction_intervals \
  --output-format parquet
```

A directory may be supplied. When it contains more than one `run_id`, select one explicitly:

```text
--run-id <point-prediction-run-id>
```

Outputs are immutable and interval-run-qualified:

```text
prediction_intervals_<run-id>.parquet
prediction_interval_metrics_<run-id>.parquet
```

## Rolling-origin input

Existing rolling-origin evidence uses sequential validation origins followed by one final test origin. All validation labels available before the final test feature boundary may form the calibration set. The final test labels remain evaluation-only.

## Contracts

- `data-contracts/prediction_interval_schema.json`
- `data-contracts/prediction_interval_metrics_schema.json`

## Boundary

This increment performs no source call, model fit, Fabric operation, scheduling, registration, promotion, or publication. Fabric interval parity and portfolio-demo integration remain separate reviewed layers.

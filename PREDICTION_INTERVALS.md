# Calibration-only prediction intervals

## Purpose

Point forecasts do not express uncertainty. This layer converts retained
validation and test point-prediction evidence into symmetric prediction
intervals without refitting the underlying model and without using test labels
to choose interval width.

The same contract now exists in two deliberately separate runtimes:

- the local pandas CLI; and
- an optional/manual Microsoft Fabric subflow over retained seasonal-comparison
  Delta evidence.

Interval evidence is retrospective evaluation. It is not a probabilistic
guarantee for arbitrary future data and it does not promote or deploy a model.

## Causal calibration boundary

For every source area, resource, city, demand horizon, model, and feature
contract:

1. identify the first test feature timestamp;
2. retain only validation rows whose target-label timestamp is strictly earlier
   than that test feature timestamp;
3. require at least `min_calibration_rows` causally available residuals;
4. calculate absolute validation residuals; and
5. apply one fixed radius to every test prediction in that slice.

The key constraint is:

```text
calibration_label_available_through_utc
    <
evaluation_feature_start_utc
```

Validation labels that overlap the test feature boundary are excluded even when
their validation feature timestamps are earlier. Test labels never set the
radius.

## Finite-sample quantile

For `n` sorted absolute calibration errors and target coverage `q`, the radius
uses the conservative one-indexed rank:

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

The default target levels are 80%, 90%, and 95%.

## Evidence

Each test row records:

- source point-prediction run identity and interval-run identity;
- source area/resource/city, horizon, model, and feature contract;
- actual and point-predicted demand;
- nominal coverage level;
- lower and upper bounds, width, and coverage outcome;
- calibration row count, finite-sample rank, and radius;
- calibration feature range and latest available label timestamp;
- final test-origin identity when rolling-origin point evidence is supplied; and
- `interval_contract_version=split-conformal-absolute-residual-v1`.

Metrics retain empirical test coverage and interval-width evidence. Empirical
coverage is an evaluation result, not a claim that the same coverage must hold
after distribution shift.

## Run locally

First create point predictions with an existing fixed-holdout or rolling-origin
command. Then run:

```bash
python -m forecasting.run_prediction_intervals \
  --predictions-input data/forecasting/seasonal_comparison_predictions.parquet \
  --coverage-levels 0.80 0.90 0.95 \
  --min-calibration-rows 24 \
  --output-dir data/forecasting/prediction_intervals \
  --output-format parquet
```

A directory may be supplied. When it contains more than one `run_id`, select one
explicitly:

```text
--run-id <point-prediction-run-id>
```

Outputs are immutable and interval-run-qualified:

```text
prediction_intervals_<run-id>.parquet
prediction_interval_metrics_<run-id>.parquet
```

## Fabric parity

The optional manual subflow consists of:

```text
05d_seasonal_baseline_comparison
        ↓ retained four-model validation/test evidence
05e_prediction_intervals
        ↓ calibration-only interval and metric Delta tables
06e_prediction_interval_quality_checks
        ↓ blocking evidence in dq_run_results
```

The interval notebook reads one exact
`forecast_seasonal_comparison_predictions` run. It does not fit or refit
persistence, ridge, previous-day, or previous-week models.

Parameters:

```text
POINT_PREDICTION_RUN_ID=<seasonal_comparison_run_id>
COVERAGE_LEVELS=0.80,0.90,0.95
MIN_CALIBRATION_ROWS=24
```

Tables:

```text
forecast_prediction_intervals
forecast_prediction_interval_metrics
```

The quality notebook independently verifies:

- exactly one retained source run;
- exact four-model target pairs;
- calibration-label availability before final-test feature time;
- the finite-sample `ceil((n + 1) * q)` rank;
- one fixed radius per calibration slice and nominal level;
- complete coverage-level sets;
- valid symmetric interval bounds;
- row-to-metric coverage and width consistency; and
- final-test time and origin identity.

The SQL analytics endpoint exposes pass-through views only. Calibration,
quantile ranking, and interval construction remain in the canonical pandas or
Spark implementation rather than being reimplemented in T-SQL.

Run this Fabric subflow manually from
`fabric/pipelines/prediction_interval_pipeline.md`. Do not add it to the
ordinary scheduled pipeline until duration, table growth, calibration history,
and coverage/width behaviour have been reviewed.

## Rolling-origin input

Existing rolling-origin point evidence uses sequential validation origins
followed by one final test origin. All validation labels available before the
final test feature boundary may form the calibration set. Final test labels
remain evaluation-only.

Fabric evidence retains the final test fold, total origin count, origin cutoff,
and evaluation-contract version in addition to the local
`evaluation_origin_fold` field.

## Contracts

- `data-contracts/prediction_interval_schema.json`
- `data-contracts/prediction_interval_metrics_schema.json`

Both schemas allow additional Fabric lineage and origin fields while preserving
the same required causal interval contract.

## Boundary

Neither implementation performs a source call, model fit, model refit,
registration, promotion, schedule activation, deployment, or publication.
Portfolio-demo integration and interval coverage/width monitoring remain
separate reviewed layers.

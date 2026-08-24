# Fabric calibration-only prediction interval subflow

Pipeline name: `prediction_interval_pipeline`

This subflow is **manual and evidence-only**. It consumes one retained seasonal
comparison prediction run, calibrates intervals from causally available
validation residuals, and validates the resulting Delta evidence.

It does not replace `05_baseline_forecasting`, `05d_seasonal_baseline_comparison`,
or the ordinary six-stage pipeline. No model fitting or refitting occurs.

## Parameters

| Name | Required | Notes |
| --- | --- | --- |
| `POINT_PREDICTION_RUN_ID` | Recommended | Exact `seasonal_comparison_run_id`. When omitted, the newest retained run is selected. |
| `COVERAGE_LEVELS` | No | Default `0.80,0.90,0.95`; finite values strictly between 0 and 1. |
| `MIN_CALIBRATION_ROWS` | No | Default `24` causally available validation residuals per area/resource/city/horizon/model/feature-contract slice. |

## Activities

1. `05e_prediction_intervals`
   - Read one retained run from `forecast_seasonal_comparison_predictions`.
   - Require exact persistence, ridge, previous-day, and previous-week pairs.
   - Identify the first final-test feature timestamp for every model slice.
   - Retain only validation rows whose target timestamp is strictly earlier than
     that test feature timestamp.
   - Compute the finite-sample absolute-residual rank
     `ceil((n + 1) * coverage)` with clipping to `[1, n]`.
   - Apply one fixed symmetric radius to the test predictions in that slice.
   - Append row evidence to `forecast_prediction_intervals`.
   - Append coverage and width metrics to
     `forecast_prediction_interval_metrics`.
2. `06e_prediction_interval_quality_checks`
   - Depend on interval creation success.
   - Validate one source run, exact four-model pairing, calibration causality,
     finite-sample ranks, fixed radii, complete coverage levels, interval
     bounds, metric consistency, and final-test time boundaries.
   - Append blocking results to `dq_run_results`.

## Safety boundary

```text
model_fit_performed=false
model_refit_performed=false
test_labels_used_for_calibration=false
schedule_activation_allowed=false
model_promotion_allowed=false
deployment_authorized=false
```

Do not enable a trigger. Run this subflow manually after a reviewed
`05d_seasonal_baseline_comparison` run, inspect empirical coverage and width by
source area, horizon, model, and nominal level, then retain the evidence for
comparison. Empirical coverage is retrospective evidence rather than a future
coverage guarantee.

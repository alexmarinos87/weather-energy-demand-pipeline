# Optional Fabric elapsed-time seasonal comparison subflow

This subflow is manual and evidence-only. It does not replace `05_baseline_forecasting`, modify the ordinary six-stage pipeline, or retrain the retained ridge model.

## Dependencies

- `gold_feature_engineering` with at least one week of retained same-group demand history.
- One successful `forecast_baseline_predictions` run containing exact persistence/ridge validation and test pairs.
- Sufficient previous-day and previous-week source coverage after elapsed-time matching.

## Parameters

| Name | Default | Rule |
| --- | ---: | --- |
| `POINT_PREDICTION_RUN_ID` | latest compatible run | Optional explicit retained baseline run |
| `REFERENCE_TOLERANCE_MINUTES` | `15` | Maximum absolute source-time offset |
| `MIN_REFERENCE_COVERAGE` | `0.90` | Per source group, demand horizon, and reference period |

## Activities

1. `05d_seasonal_baseline_comparison`
   - Verify one exact persistence/ridge point-prediction run.
   - Build previous-day and previous-week ideals from the future demand target timestamp.
   - Match same-area/resource/city demand by elapsed UTC time, not source row position.
   - Exclude ideals before retained history from the denominator and retain internal gaps as coverage failures.
   - Restrict all four models to the cohort containing both references.
   - Reuse the retained ridge and persistence predictions; no model fitting occurs.
   - Append versioned comparison predictions and metrics to separate Delta tables.
2. `06d_seasonal_baseline_quality_checks`
   - Depend on comparison success.
   - Validate exact four-model pairs, one actual target, one training boundary, causal references, reference periods, coverage, horizons, metric windows, and rolling-origin sequence.
   - Append blocking results to `dq_run_results`.

## Tables

```text
forecast_seasonal_comparison_predictions
forecast_seasonal_comparison_metrics
```

The tables remain separate from `forecast_baseline_predictions` and `forecast_baseline_metrics`.

## Operating mode

Run manually after inspecting the chosen point-prediction run and retained demand history. Do not enable a trigger or change the active model merely because this subflow succeeds.

## Safety boundary

The subflow makes no source request, accesses no credentials, retrains no model, performs no promotion, and does not alter the ordinary pipeline schedule.

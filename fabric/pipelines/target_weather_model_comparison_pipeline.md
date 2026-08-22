# Optional Fabric target-weather comparison subflow

This subflow is manual and evidence-only. It does not replace `05_baseline_forecasting`, alter the ordinary six-stage pipeline, or promote a model.

## Dependencies

- `gold_feature_engineering` from the established observed-weather pipeline.
- `silver_forecast_weather` from the optional forecast-weather bronze/silver subflow.
- Sufficient target coverage, forecast-weather coverage, and post-purge training history.

## Parameters

| Name | Default | Rule |
| --- | ---: | --- |
| `TRAIN_FRACTION` | `0.60` | Earliest chronological training share |
| `VALIDATION_FRACTION` | `0.20` | Validation share; must leave a test split |
| `MIN_TRAIN_ROWS` | `24` | After unavailable-label purging |
| `MIN_VALIDATION_ROWS` | `6` | Per validation window |
| `MIN_TEST_ROWS` | `6` | Untouched final window |
| `RIDGE_REG_PARAM` | `1.0` | Positive L2 regularisation |
| `HORIZON_MINUTES` | `30,60` | Only 30 and 60 are accepted |
| `TARGET_TOLERANCE_MINUTES` | `5` | Maximum late demand-target match |
| `MIN_TARGET_COVERAGE` | `0.90` | Per source group and horizon |
| `EVALUATION_MODE` | `holdout` | `holdout` or `rolling-origin` |
| `ROLLING_ORIGIN_FOLDS` | `3` | At least 2 in rolling mode |
| `FORECAST_VALID_TOLERANCE_MINUTES` | `90` | Maximum forecast-valid difference from demand target |
| `MAX_FORECAST_AVAILABILITY_AGE_MINUTES` | `360` | Maximum age at feature time |
| `MIN_FORECAST_WEATHER_COVERAGE` | `0.90` | Per source group and demand horizon |

## Activities

1. `05c_target_weather_model_comparison`
   - Build the same bounded 30/60-minute future demand targets used by the baseline.
   - Attach only same-area/city forecast weather issued and ingested by feature time.
   - Choose the closest target-valid forecast, then latest available issue.
   - Build one target-weather-covered cohort.
   - Apply identical holdout or rolling-origin windows and one unavailable-label purge.
   - Fit `ridge_weather_lag` and `ridge_target_weather` on the same rows and training boundary.
   - Append predictions and metrics to separate comparison Delta tables.
2. `06c_target_weather_comparison_quality_checks`
   - Depend on comparison success.
   - Validate exact model pairs, actual targets, training boundaries, causal weather availability, coverage, model modes, contracts, metrics, and rolling-origin sequence.
   - Append blocking results to `dq_run_results`.

## Tables

```text
forecast_weather_comparison_predictions
forecast_weather_comparison_metrics
```

These tables are separate from the ordinary baseline evidence and are not used by the scheduled pipeline.

## Operating mode

Run manually after inspecting `silver_forecast_weather`. Feed exported comparison evidence and reconciliation metrics into the human-review-only promotion assessment. Do not enable a trigger or change the active model based solely on successful notebook execution.

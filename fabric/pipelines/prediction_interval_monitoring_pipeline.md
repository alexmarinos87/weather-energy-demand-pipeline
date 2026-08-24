# Fabric advisory prediction-interval monitoring subflow

Pipeline name: `prediction_interval_monitoring_pipeline`

This subflow is **manual, advisory, and evidence-only**. It reads retained
`forecast_prediction_interval_metrics`, compares recent and immediately
preceding history for each exact interval slice, writes versioned health
evidence, and independently validates that evidence.

It does not replace `05e_prediction_intervals`, does not create a new interval,
and does not fit, refit, recalibrate, select, register, promote, deploy, or
schedule a model.

## Parameters

| Name | Required | Default | Notes |
| --- | --- | ---: | --- |
| `AS_OF_UTC` | No | current UTC | Optional timezone-aware monitoring boundary for reproducible manual review. |
| `RECENT_INTERVAL_RUN_COUNT` | No | `3` | Newest retained runs in each exact monitoring slice. |
| `REFERENCE_INTERVAL_RUN_COUNT` | No | `6` | Immediately preceding runs eligible for the reference window. |
| `MIN_RECENT_INTERVAL_RUNS` | No | `2` | Hard minimum recent history. |
| `MIN_REFERENCE_INTERVAL_RUNS` | No | `3` | Minimum reference history before drift checks are emitted. |
| `MAX_INTERVAL_RUN_AGE_MINUTES` | No | `10080` | Hard freshness threshold for the newest interval run. |
| `MAX_EVALUATION_AGE_MINUTES` | No | `20160` | Hard freshness threshold for the newest retained evaluation end. |
| `MIN_CALIBRATION_OBSERVATION_COUNT` | No | `24` | Hard minimum causal calibration history across recent runs. |
| `MAX_RECENT_COVERAGE_SHORTFALL_PCT_POINTS` | No | `5.0` | Maximum nominal-minus-recent empirical coverage shortfall. |
| `MAX_COVERAGE_DROP_PCT_POINTS` | No | `5.0` | Advisory maximum reference-minus-recent coverage drop. |
| `MAX_AVERAGE_INTERVAL_WIDTH_INCREASE_PCT` | No | `25.0` | Advisory maximum weighted average-width increase. |
| `MAX_CALIBRATION_HISTORY_DROP_PCT` | No | `25.0` | Advisory maximum decline in mean calibration rows. |
| `MONITOR_RUN_ID` | Quality gate only | newest | Exact monitoring run to validate; omit to select the newest retained run. |

## Activities

1. `05f_prediction_interval_monitoring`
   - Read `forecast_prediction_interval_metrics`.
   - Reject missing, blank, duplicated, non-finite, non-positive, misordered,
     or otherwise malformed interval metric evidence.
   - Preserve exact source-area/resource/city/horizon/model/feature-contract/
     coverage-level/interval-contract slices.
   - Rank retained runs newest-first and bound processing to the configured
     recent plus reference windows.
   - Weight empirical coverage and average interval width by
     `evaluation_observation_count`.
   - Emit hard checks for recent history, run freshness, evaluation freshness,
     causal calibration history, and recent empirical-coverage shortfall.
   - Emit drift warnings only when the configured reference history exists.
   - Append checks to `forecast_prediction_interval_health_checks`.
   - Append one run summary to
     `forecast_prediction_interval_health_summary`.
2. `06f_prediction_interval_monitoring_quality_checks`
   - Depend on monitoring success.
   - Validate required fields, one check identity per slice/name, complete base
     checks, conditional drift-check presence, comparator results, binding to
     retained source metric rows, summary counts/status, contract versions,
     and the no-automatic-action authority boundary.
   - Append blocking validation results to `dq_run_results`.

## Status contract

```text
failed
    -> at least one hard error check failed

warning
    -> no hard error failed, but at least one advisory warning failed

healthy
    -> all emitted checks passed
```

Insufficient reference history is a warning. It is never treated as healthy and
no drift value is fabricated.

## Safety boundary

```text
automatic_remediation_allowed=false
automatic_recalibration_allowed=false
automatic_model_change_allowed=false
automatic_schedule_change_allowed=false
automatic_promotion_allowed=false

model_fit_performed=false
model_refit_performed=false
interval_recalibration_performed=false
schedule_activation_allowed=false
deployment_authorized=false
```

Do not enable a trigger. Run this subflow manually after several independently
validated interval runs exist, inspect failures and warnings by exact monitoring
slice, and retain the resulting evidence for human review. Empirical coverage
and width remain retrospective evidence rather than permission to change
forecasting state.

# Target-weather promotion assessment

## Purpose

This layer combines two independent evidence streams before the target-weather model is considered for Fabric implementation:

1. paired demand-model predictions from `ridge_weather_lag` and `ridge_target_weather`; and
2. forecast-versus-observed weather reconciliation metrics.

The result is advisory only. Its status is either:

```text
blocked
eligible_for_human_review
```

`automatic_promotion_allowed` is always `false`. The command does not deploy, register, schedule, or switch a model.

## Fair comparison contract

Each demand-model slice is grouped by source area, resource, city, requested demand horizon, evaluation split, rolling origin, forecast provider/model, and weather lead-time bucket.

Before scoring policy checks, the assessor verifies that the observed-weather and target-weather ridge models contain identical:

- feature and demand-target timestamps;
- actual demand values;
- provider/model weather identity;
- evaluation slice; and
- training-label boundary.

Malformed or unpaired evidence is rejected rather than interpreted as a failed promotion check.

## Default policy

| Check | Default |
| --- | ---: |
| Paired demand-model observations per slice | at least 24 |
| Forecast-weather target coverage in model cohort | at least 90% |
| Candidate MAE improvement | at least 0% |
| Candidate RMSE improvement | at least 0% |
| Absolute bias regression | no increase |
| Required demand horizons | 30 and 60 minutes |
| Untouched test split | required |
| Reconciled mature forecast observations | at least 24 |
| Forecast-versus-observed coverage | at least 90% |
| Temperature MAE | no more than 2.5°C |
| Humidity MAE | no more than 15 percentage points |

Weather-quality checks are matched to the source area, city, provider, model, and forecast lead-time buckets actually used by the candidate demand-model rows. Optional explicit lead buckets can make the policy stricter.

The defaults are review thresholds, not universal scientific claims. They are versioned as `target-weather-promotion-policy-v1` and should be changed only through a reviewed repository change or explicit command parameters.

## Run locally

```bash
python3 -m forecasting.run_promotion_assessment \
  --comparison-predictions data/forecasting/weather_comparison_predictions.parquet \
  --reconciliation-metrics data/reconciliation/forecast_weather/forecast_weather_quality_metrics_<run>.parquet \
  --min-model-observations 24 \
  --min-reconciliation-observations 24 \
  --min-model-forecast-coverage-pct 90 \
  --min-reconciliation-coverage-pct 90 \
  --max-temperature-mae-c 2.5 \
  --max-humidity-mae-pct 15 \
  --required-horizons 30 60 \
  --output-dir data/promotion/target_weather \
  --output-format parquet
```

When a directory contains multiple evidence runs, select one explicitly:

```text
--comparison-run-id <run-id>
--reconciliation-run-id <run-id>
```

Use `--require-eligible` when a reviewed workflow should return exit code 2 for a blocked assessment. Evidence is written before that exit code is returned.

Outputs are immutable, assessment-ID-qualified files:

```text
target_weather_promotion_checks_<assessment-id>.parquet
target_weather_promotion_summary_<assessment-id>.parquet
```

## Contracts

- `data-contracts/target_weather_promotion_check_schema.json`
- `data-contracts/target_weather_promotion_summary_schema.json`

## Human authority boundary

An eligible result means only that the supplied evidence met the configured policy. A human review must still examine:

- whether the evidence window is representative;
- performance by area, demand horizon, split, origin, and weather lead;
- provider limitations such as retrieval-time issue surrogates;
- operational cost, latency, retention, and failure behaviour; and
- whether Fabric implementation is proportionate.

No code path in this increment changes the active observed-weather baseline.

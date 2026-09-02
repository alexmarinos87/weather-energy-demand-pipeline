# Retained interval-policy compatibility

## Purpose

This layer compares the previous five-percentage-point and reviewed
three-percentage-point recent coverage-shortfall limits over the same immutable
prediction-interval health checks.

It answers a narrow question: which retained slice and overall conclusions would
change under the reviewed threshold?

## Input

The input is one retained local or exported Fabric health-check dataset using:

```text
policy_version=prediction-interval-monitoring-policy-v1
monitoring_contract_version=prediction-interval-monitoring-v1
```

Each exact source-area/resource/city/horizon/model/coverage slice must contain
the complete base check set. The evaluator verifies retained comparator results
before producing counterfactual evidence.

## Comparison

Only this reviewed policy difference is replayed:

```text
previous maximum recent coverage shortfall = 5.0 percentage points
current maximum recent coverage shortfall  = 3.0 percentage points
```

All other retained check outcomes are held constant. Status precedence remains:

```text
failed > warning > healthy
```

The output records exact slice transitions, changed check names, newly failed
slices, and overall monitor-run status changes.

## Run

```bash
python3 -m forecasting.run_interval_policy_compatibility \
  --health-checks prediction_interval_health_checks_<run>.parquet \
  --output-dir data/interval-policy-compatibility \
  --output-format parquet
```

Outputs are immutable and run-qualified:

```text
interval_policy_compatibility_slices_<run>.parquet
interval_policy_compatibility_summary_<run>.parquet
interval_policy_compatibility_report_<run>.md
```

## Authority boundary

The assessment does not rerun monitoring, rewrite retained health checks or
historical statuses, activate a policy, recalibrate intervals, alter models,
execute Fabric, change a schedule, deliver alerts, deploy, or publish
externally. A changed outcome is evidence requiring separate human review.

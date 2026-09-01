# Retained interval-policy compatibility assessment

## Purpose

G38 compares the previous five-percentage-point recent coverage-shortfall limit
with the reviewed three-point limit over one complete retained
`interval-health-trend-v1` dataset.

The assessment exists because G36 and G37 changed checked-in local and
optional/manual Fabric defaults without rewriting historical monitoring rows.
A retained status may therefore reflect the previous policy even though future
runs use the reviewed policy.

## Input

Supply one CSV or Parquet exact-slice trend dataset produced by the G25a trend
builder. Every row must preserve:

```text
scenario
source area
resource
city
forecast horizon
model
feature contract
target coverage
interval contract
```

The retained dataset must contain one trend run, valid timezone-aware evidence,
complete drift values only where reference history is sufficient, one canonical
monitor status per scenario, and no duplicate exact-slice identities.

## Policies compared

The implementation builds both policies from the checked-in
`PredictionIntervalMonitoringConfig` and fails closed unless the current hard
limit is exactly three percentage points.

```text
previous-five-point
    max_recent_coverage_shortfall_pct_points = 5.0

reviewed-three-point
    max_recent_coverage_shortfall_pct_points = 3.0
```

Every other freshness, history, calibration, coverage-drift, width, and
calibration-history threshold must be identical. A second policy difference is
rejected.

## Evaluation

Both candidates use the same rule semantics as the canonical policy-sensitivity
evaluator:

- minimum recent history;
- interval-run freshness;
- evaluation freshness;
- minimum causal calibration history;
- recent empirical-coverage shortfall;
- minimum reference history; and
- conditional coverage, width, and calibration-history drift warnings.

The assessment retains the original `monitor_status` as historical evidence and
calculates separate previous-policy and current-policy outcomes. It records:

```text
fully_compatible
slice_change_without_scenario_change
scenario_status_escalation
```

It also states whether the retained scenario status matches both policies, only
the previous policy, only the current policy, or neither policy.

A three-point policy cannot de-escalate an outcome relative to the otherwise
identical five-point policy. Any such result fails the assessment.

## Outputs

```text
interval_policy_retained_compatibility_slices_<run-id>.csv|parquet
interval_policy_retained_compatibility_summary_<run-id>.csv|parquet
interval_policy_retained_compatibility_report_<run-id>.md
interval_policy_retained_compatibility_manifest_<run-id>.json
```

The manifest binds the exact bytes of the slices, summary, and report, the
summary digest, both complete policy snapshots, the retained trend-run identity,
and the assessment timestamp. Existing outputs are never overwritten.

Run locally:

```bash
python3 -m forecasting.run_interval_policy_retained_compatibility \
  --slice-trends data/interval-health-trends/slice_trends.csv \
  --compatibility-run-timestamp 2026-09-01T13:30:00Z \
  --output-dir data/interval-policy-retained-compatibility
```

## Authority boundary

Every slice, summary, and manifest fixes the following to `false`:

```text
historical_statuses_rewritten
retained_evidence_mutated
monitoring_rerun_performed
threshold_activation_performed
interval_recalibration_performed
model_change_performed
fabric_execution_performed
schedule_change_performed
promotion_change_performed
alert_delivery_performed
deployment_performed
external_publication_performed
```

The assessment does not update a historical status, run local or Fabric
monitoring, activate a trigger, recalibrate an interval, change a model, deliver
an alert, deploy, or publish externally.

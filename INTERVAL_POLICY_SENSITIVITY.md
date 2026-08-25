# Interval-monitoring policy sensitivity

## Purpose

This local layer compares a bounded set of reviewed monitoring-policy candidates against retained `interval-health-trend-v1` slice evidence. It is counterfactual review evidence only.

It does not rerun the canonical monitor, reread raw interval metrics, recalculate an interval radius, update the active policy, mutate retained evidence, recalibrate an interval, change a model or schedule, promote a candidate, or deliver an alert.

## Candidates

The default comparison contains:

- `active-reference`: the exact checked-in `prediction-interval-monitoring-policy-v1` configuration;
- `stricter-review`: tighter freshness, calibration, coverage, and drift thresholds;
- `tolerant-review`: wider thresholds that expose conclusions dependent on permissive policy choices.

The evaluator requires two to five candidates, unique IDs, and exactly one active reference. Any active-reference threshold that differs from the checked-in policy is rejected.

## Input

```text
interval_health_slice_trends_<trend-run-id>.csv
interval_health_slice_trends_<trend-run-id>.parquet
```

The input must bind to one trend run and preserve exact source-area, resource, city, demand-horizon, model, feature-contract, interval-coverage, and interval-contract identity.

## Evaluation

Each candidate evaluates retained observations for recent run history, interval-run and evaluation freshness, minimum causal calibration history, recent empirical-coverage shortfall, reference-history sufficiency, coverage decline, interval-width growth, and calibration-history decline.

Error failures produce `failed`; otherwise warning failures produce `warning`; otherwise the slice is `healthy`. Where reference history is insufficient, drift outcomes remain null rather than fabricating no drift.

The active candidate must reproduce the retained canonical scenario status. Any mismatch fails closed.

## Outputs

```text
interval_policy_sensitivity_slices_<sensitivity-run-id>
interval_policy_sensitivity_summary_<sensitivity-run-id>
interval_policy_sensitivity_report_<sensitivity-run-id>.md
```

All rows use `interval-policy-sensitivity-v1` and retain every candidate threshold, observed value, exact slice identity, rule outcome, candidate status, active-reference status, and no-authority fields.

## Run locally

```bash
python3 -m forecasting.run_interval_policy_sensitivity \
  --slice-trends data/interval-health-trends/interval_health_slice_trends_<run>.parquet \
  --output-dir data/interval-policy-sensitivity \
  --output-format parquet
```

A reviewed JSON candidate list can be supplied with `--candidate-config`. The file is evidence input, not an active-policy update.

## Authority boundary

```text
active_policy_updated=false
retained_evidence_mutated=false
interval_recalibration_performed=false
model_change_performed=false
schedule_change_performed=false
promotion_change_performed=false
alert_delivery_performed=false
```

Empirical coverage remains retrospective evidence and is not an unconditional future guarantee. A separate immutable named human decision is required before any policy proposal can be accepted or rejected, and even that decision has no threshold-activation authority.

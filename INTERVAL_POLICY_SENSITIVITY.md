# Human-reviewed interval-monitoring policy sensitivity

## Purpose

This layer compares retained `interval-health-trend-v1` exact-slice evidence under a bounded set of reviewed monitoring-policy candidates. It exists to show whether retained `healthy`, `warning`, and `failed` conclusions are robust to reasonable threshold choices.

The implementation does not call `monitor_prediction_interval_health`. It does **not** rerun canonical interval monitoring, rewrite retained statuses, update the active policy, construct or recalibrate an interval, refit a model, change a schedule, promote a candidate, or deliver an alert.

## Input

```text
interval_health_slice_trends_<trend-run-id>.csv
or
interval_health_slice_trends_<trend-run-id>.parquet
```

The input must contain exactly one trend run and preserve the full monitoring slice:

```text
scenario
source_area
resource_id
city
requested_horizon_minutes
model_name
feature_contract_version
target_coverage_level
interval_contract_version
```

The evaluator rejects duplicate identities, timezone-naive timestamps, impossible event ordering, inconsistent retained scenario status, and reference-sufficiency flags that disagree with retained drift values.

## Reviewed candidates

The default review set contains:

```text
active-reference
    exact checked-in prediction-interval-monitoring-policy-v1

stricter-review
    tighter freshness, calibration, coverage, and drift limits

tolerant-review
    wider limits used only to expose de-escalation sensitivity
```

A custom JSON candidate list may contain between two and five policies and must contain exactly one `active_reference`. The active reference must exactly match the checked-in `PredictionIntervalMonitoringConfig`; a relabelled or altered active policy is rejected.

## Evaluation rules

Error-severity rules cover:

- minimum recent interval-run history;
- latest interval-run age;
- latest evaluation age;
- minimum causal calibration history; and
- recent empirical-coverage shortfall.

Warning-severity rules cover:

- minimum reference history;
- empirical-coverage decline;
- average interval-width increase; and
- calibration-history decline.

Status precedence remains:

```text
failed > warning > healthy
```

Where reference history is insufficient, drift-rule results remain null. The evaluator does not fabricate a zero-drift result.

## Fail-closed active reproduction

The active-reference candidate is aggregated across every exact slice in a scenario. Its resulting status must exactly reproduce the retained canonical monitor status. Any disagreement fails the sensitivity run rather than presenting a competing status as canonical evidence.

## Outputs

```text
interval_policy_sensitivity_slices_<sensitivity-run-id>
interval_policy_sensitivity_summary_<sensitivity-run-id>
interval_policy_sensitivity_report_<sensitivity-run-id>.md
```

All rows use:

```text
sensitivity_contract_version=interval-policy-sensitivity-v1
```

The exact-slice output retains every candidate threshold, observed value, individual rule result, candidate status, active-reference slice status, and changed-slice indicator.

The summary records candidate status by scenario, status counts, changed slices, and one classification:

```text
active_reference
status_robust
status_sensitive
```

## Run locally

```bash
python3 -m forecasting.run_interval_policy_sensitivity \
  --slice-trends \
    data/interval-health-trends/interval_health_slice_trends_<run>.parquet \
  --output-dir data/interval-policy-sensitivity \
  --output-format parquet
```

For deterministic review:

```bash
python3 -m forecasting.run_interval_policy_sensitivity \
  --slice-trends interval_health_slice_trends_iht-111111111111111111111111.csv \
  --sensitivity-run-id ips-222222222222222222222222 \
  --sensitivity-run-timestamp 2026-01-20T01:00:00Z \
  --output-dir data/interval-policy-sensitivity \
  --output-format csv
```

Existing outputs are not overwritten.

## Authority boundary

Every retained row fixes these fields to `false`:

```text
active_policy_updated=false
candidate_thresholds_activated=false
retained_evidence_mutated=false
interval_recalibration_performed=false
model_change_performed=false
schedule_change_performed=false
promotion_change_performed=false
alert_delivery_performed=false
```

Candidate outcomes are counterfactual review evidence only. Empirical coverage remains retrospective evidence and is not an unconditional future guarantee.

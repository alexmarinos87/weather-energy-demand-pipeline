# Interval-health trend datasets

## Purpose

This layer converts retained prediction-interval health history into two
reproducible analyst-facing datasets without changing any forecasting state.

It consumes the repeated interval metric history and retained scenario summaries
created by the portfolio interval-health demonstration or equivalent exported
evidence. It does not call a live source, fit or refit a model, or change any
forecasting state. In particular, it does not recalculate an interval radius,
change a monitoring threshold, activate a schedule, promote a candidate, or
deliver an alert. Automatic recalibration is not performed; the lower-case
contract phrase is explicit: automatic recalibration remains outside this
reporting layer.

## Input contract

The history input retains one row per:

```text
scenario
/ interval run
/ source area
/ resource
/ city
/ demand horizon
/ model
/ feature contract
/ nominal interval coverage
/ interval contract
```

Every run in a scenario must contain exactly the same slice set. Run sequences
must be contiguous from one, timestamps must be timezone-aware, evaluation
evidence may not end after its interval run, and duplicate run/slice identities
are rejected.

The health-summary input contains one retained monitor outcome per scenario.
Automatic remediation, recalibration, model change, schedule change, and
promotion authority fields must all remain false.

## Run-level trend dataset

`interval_health_run_trends` retains every historical metric row and adds:

```text
nominal_coverage_pct
coverage_shortfall_pct_points
slice_run_sequence
slice_run_count
monitoring_window
is_latest_interval_run

previous_interval_run_id
previous_interval_run_timestamp_utc
previous_empirical_coverage_pct
previous_average_interval_width_mw
previous_calibration_observation_count

coverage_change_from_previous_pct_points
interval_width_change_from_previous_pct
calibration_history_change_from_previous_pct

monitor_run_id
monitor_status
failed_error_check_count
failed_warning_check_count
trend_contract_version
```

The first retained row in a slice has null previous-run changes. Those values
remain unknown; the implementation does not invent a zero change.

With the default policy, the newest three rows are labelled `recent`, the
preceding six are labelled `reference`, and older retained history is labelled
`older`.

## Exact-slice trend dataset

`interval_health_slice_trends` contains one row per exact monitoring slice and
scenario. It exposes:

```text
latest interval-run identity and timestamps
latest coverage shortfall
latest width and calibration history

recent and reference run counts
reference-history sufficiency
evaluation-row weighted recent coverage
evaluation-row weighted reference coverage
evaluation-row weighted recent and reference width

coverage drop
average interval-width increase
calibration-history decline
retained monitor status
attention_required
```

Coverage and width are evaluation-row weighted. Calibration history uses the
mean count for longitudinal comparison and preserves the recent minimum so a
short calibration run is not hidden by stronger neighbouring runs.

When reference history is insufficient, reference and drift fields remain null.
The dataset does not fabricate a no-drift result.

## Run locally

```bash
python3 -m forecasting.run_interval_health_trends \
  --history \
    data/portfolio-interval-health/<run>/repeated_interval_metric_history.parquet \
  --health-summary \
    data/portfolio-interval-health/<run>/prediction_interval_health_summary.parquet \
  --output-dir data/interval-health-trends \
  --output-format parquet
```

For deterministic review:

```bash
python3 -m forecasting.run_interval_health_trends \
  --history repeated_interval_metric_history.csv \
  --health-summary prediction_interval_health_summary.csv \
  --trend-run-id iht-111111111111111111111111 \
  --trend-run-timestamp 2026-01-20T00:00:00Z \
  --output-dir data/interval-health-trends \
  --output-format csv
```

Outputs are immutable and trend-run-qualified:

```text
interval_health_run_trends_<trend-run-id>.csv
interval_health_slice_trends_<trend-run-id>.csv
```

The same names use `.parquet` when Parquet output is selected. Existing files
are not overwritten.

## Versioned contracts

- `data-contracts/interval_health_run_trend_schema.json`
- `data-contracts/interval_health_slice_trend_schema.json`

Both use:

```text
trend_contract_version=interval-health-trend-v1
```

## Authority boundary

This increment is descriptive retained evidence only.

```text
automatic_remediation_performed=false
automatic_recalibration_performed=false
automatic_model_change_performed=false
automatic_schedule_change_performed=false
automatic_promotion_performed=false
alert_delivery_performed=false
```

A warning or failed retained monitor status is surfaced for review. It is not an
instruction to modify a model, radius, threshold, registry, schedule, or
deployment.

The dependent next layer is a thin-client report that reads only these retained
trend datasets and contains no independent monitoring calculation.

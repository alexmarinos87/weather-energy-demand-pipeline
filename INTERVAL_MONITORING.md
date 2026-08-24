# Prediction-interval health monitoring

## Purpose

This layer turns retained prediction-interval metric runs into longitudinal
health evidence. It monitors freshness, causal calibration history, empirical
coverage, and interval width without recalibrating a radius, refitting or
changing a model, activating a schedule, or promoting anything.

The same advisory contract now exists in two deliberately separate runtimes:

- the local pandas CLI over CSV/Parquet evidence; and
- an optional/manual Microsoft Fabric subflow over retained Delta metrics.

A failed check is evidence for human review, not permission to mutate
forecasting state.

## Input

Supply one CSV/Parquet metric file, a directory containing immutable
`prediction_interval_metrics_<run-id>` outputs, or retained rows from the Fabric
`forecast_prediction_interval_metrics` table.

Every row must retain one exact monitoring slice:

```text
source area / resource / city / horizon / model /
feature contract / target coverage / interval contract
```

Runs are ordered by `interval_run_timestamp_utc`. Duplicate run/slice rows,
timezone-naive local timestamps, non-positive observation counts, invalid
coverage, and evaluation evidence ending after its interval run are rejected.

## Windows

For each exact slice, the newest three runs form the recent window. Up to six
immediately preceding runs form the reference window.

The default policy requires at least two recent runs. At least three reference
runs are required before longitudinal drift is calculated. Insufficient
reference history is emitted as a warning rather than being treated as healthy
or converted into a fabricated drift value.

## Hard checks

The monitor reports an error when any default hard limit fails:

```text
recent interval runs >= 2
latest interval-run age <= 10,080 minutes
latest evaluation age <= 20,160 minutes
minimum recent causal calibration rows >= 24
recent empirical coverage shortfall <= 5 percentage points
```

Recent empirical coverage is weighted by each run's retained evaluation row
count. Coverage shortfall is:

```text
max(0, nominal coverage - recent weighted empirical coverage)
```

This remains retrospective evaluation evidence. Passing the check does not
provide an unconditional future coverage guarantee.

## Drift warnings

When sufficient reference history exists, the monitor compares recent and
reference windows and warns when:

```text
empirical coverage drop > 5 percentage points
average interval-width increase > 25%
mean causal calibration-history drop > 25%
```

Coverage and width are evaluation-row weighted. Calibration history uses the
mean retained causal calibration count per run, while the hard calibration
check retains the minimum recent count so one under-calibrated run cannot be
hidden by averaging.

## Status and authority boundary

Check severity is `error` or `warning`. Summary status is:

```text
failed   -> at least one error check failed
warning  -> no error failed, but at least one warning failed
healthy  -> every emitted check passed
```

Every local and Fabric summary records all of the following as `false`:

```text
automatic_remediation_allowed
automatic_recalibration_allowed
automatic_model_change_allowed
automatic_schedule_change_allowed
automatic_promotion_allowed
```

## Run locally

```bash
python3 -m forecasting.run_interval_monitoring \
  --interval-metrics data/forecasting/prediction_intervals \
  --as-of-utc 2026-08-24T16:00:00Z \
  --output-dir data/monitoring/prediction_intervals \
  --output-format parquet
```

Use `--fail-on-error` to return exit code 2 after writing evidence when status is
`failed`. Use `--fail-on-warning` to return exit code 3 for a warning-only
result.

Outputs are immutable and monitor-run-qualified:

```text
prediction_interval_health_checks_<run-id>.parquet
prediction_interval_health_summary_<run-id>.parquet
```

## Optional/manual Fabric parity

The Fabric subflow is:

```text
forecast_prediction_interval_metrics
        ↓
05f_prediction_interval_monitoring
        ↓
forecast_prediction_interval_health_checks
forecast_prediction_interval_health_summary
        ↓
06f_prediction_interval_monitoring_quality_checks
        ↓
dq_run_results
```

`05f_prediction_interval_monitoring` applies the same recent/reference windows,
freshness limits, calibration minimums, weighted coverage/width calculations,
drift thresholds, statuses, policy version, and no-automatic-action fields as
the local monitor. Processing is bounded to the configured recent plus
reference run counts for every exact slice.

`06f_prediction_interval_monitoring_quality_checks` independently validates:

- required fields and one check identity per slice/name;
- complete hard/base checks;
- drift checks only when reference history is sufficient;
- comparator and persisted pass/fail consistency;
- binding of every latest interval run to retained source metrics;
- summary counts and status;
- policy and monitoring contract versions; and
- all five authority fields remaining false.

Run it manually from
`fabric/pipelines/prediction_interval_monitoring_pipeline.md`. No alert delivery
or trigger is configured.

## Contracts

- `data-contracts/prediction_interval_health_check_schema.json`
- `data-contracts/prediction_interval_health_summary_schema.json`

Both runtimes use:

```text
policy_version=prediction-interval-monitoring-policy-v1
monitoring_contract_version=prediction-interval-monitoring-v1
```

## Boundary

This increment does not call a live source, fit or refit a model, alter a
calibration radius, modify retained interval evidence, deliver an alert,
activate a schedule, register or promote a candidate, deploy infrastructure, or
publish externally. Fabric outputs are optional/manual retained evidence only.

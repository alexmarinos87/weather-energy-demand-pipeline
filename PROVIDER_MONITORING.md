# Forecast-provider health monitoring

## Purpose

This layer turns retained forecast snapshots and forecast-versus-observed reconciliation metrics into longitudinal health evidence. It is advisory and local: it does not deliver alerts, restart ingestion, change thresholds, or promote a model.

## Inputs

1. Normalized forecast-weather rows from local ingestion or an export of `silver_forecast_weather`.
2. Reconciliation metrics produced by `forecasting.run_weather_reconciliation`.

Inputs may be one CSV/Parquet file or a partitioned directory.

## Forecast snapshot checks

For every source area, city, provider, and model, the monitor evaluates:

- age of the latest ingested snapshot;
- number of forecast slots in the latest snapshot;
- future horizon represented by the latest snapshot;
- whether at least two snapshots exist for cadence evidence; and
- maximum gap between retained snapshot ingestions.

The default policy requires:

```text
latest ingestion age <= 240 minutes
latest snapshot slots >= 8
latest snapshot horizon >= 1,440 minutes
maximum snapshot gap <= 360 minutes
```

Snapshot cadence cannot be measured from one snapshot. That condition is emitted as a warning rather than silently treated as healthy.

## Reconciliation checks

For every source area, city, provider, model, issue basis, and lead-time bucket, reconciliation runs are ordered by their run timestamp.

The newest three runs form the recent window. Up to six immediately preceding runs form the reference window.

Hard checks require:

```text
recent runs >= 2
latest reconciliation age <= 1,440 minutes
weighted mature-forecast coverage >= 90%
weighted temperature MAE <= 2.5 C
weighted humidity MAE <= 15 percentage points
```

When at least three reference runs exist, drift warnings compare recent and reference windows:

```text
coverage drop <= 5 percentage points
temperature MAE increase <= 0.5 C
humidity MAE increase <= 3 percentage points
```

Insufficient reference history is a warning, not a fabricated drift result.

## Status

Check severity is `error` or `warning`. The summary status is:

```text
failed   -> at least one error check failed
warning  -> no error failed, but at least one warning failed
healthy  -> all emitted checks passed
```

`automatic_remediation_allowed` is always `false`.

## Run locally

```bash
python3 -m forecasting.run_provider_monitoring \
  --forecast-input data/normalized/forecast_weather/openweather \
  --reconciliation-metrics data/reconciliation/forecast_weather \
  --as-of-utc 2026-08-22T15:00:00Z \
  --output-dir data/monitoring/forecast_provider \
  --output-format parquet
```

Use `--fail-on-error` to return exit code 2 after writing evidence when the summary is `failed`. Use `--fail-on-warning` to return exit code 3 for a warning-only result.

Outputs are immutable and monitor-run-qualified:

```text
forecast_provider_health_checks_<run-id>.parquet
forecast_provider_health_summary_<run-id>.parquet
```

## Contracts

- `data-contracts/forecast_provider_health_check_schema.json`
- `data-contracts/forecast_provider_health_summary_schema.json`

## Boundary

This increment provides evidence, not alert delivery. A later operations layer may publish selected failures to reviewed destinations, but must preserve human authority and avoid unattended model or infrastructure changes.

# Rolling-origin demand backtesting

The fixed validation/test holdout remains the default in both local and Microsoft Fabric execution. Rolling-origin evaluation is an explicit mode for checking the 30- and 60-minute baselines at repeated historical cutoffs.

## Contract

With `rolling-origin` mode and the default three origins:

1. the earliest 60% remains initial training history;
2. the next 20% is partitioned into two sequential validation origins;
3. the training candidate window expands after each validation origin;
4. future target labels that are not known at the next origin cutoff are purged;
5. the final 20% is retained as one untouched test origin.

Every emitted rolling-origin prediction must satisfy:

```text
trained_through_utc
    <
origin_cutoff_utc
    <=
feature_timestamp_utc
    <
event_timestamp_utc
```

Each prediction and metric records:

- `origin_fold`;
- `origin_count`;
- `origin_cutoff_utc`;
- `training_observation_count`; and
- `evaluation_contract_version=rolling-origin-v1`.

The run rejects incomplete fold sequences, reused evaluation timestamps, decreasing cutoffs, decreasing available training history, validation rows in the final origin, test rows before the final origin, or labels unavailable at an origin cutoff.

## Run locally

Fixed holdout:

```bash
python3 -m forecasting.run_baseline \
  --demo \
  --horizon-minutes 30 60 \
  --output-dir data/forecasting \
  --output-format csv
```

This writes `baseline_predictions.csv` and `baseline_metrics.csv`.

Rolling-origin mode:

```bash
python3 -m forecasting.run_baseline \
  --demo \
  --evaluation-mode rolling-origin \
  --horizon-minutes 30 60 \
  --rolling-origin-folds 3 \
  --output-dir data/forecasting \
  --output-format csv
```

This writes `rolling_origin_predictions.csv` and `rolling_origin_metrics.csv`, so it cannot silently overwrite fixed-holdout evidence.

## Run in Fabric

Pass these optional parameters to `05_baseline_forecasting`:

```text
EVALUATION_MODE=rolling-origin
ROLLING_ORIGIN_FOLDS=3
```

The notebook keeps the same bounded 30/60-minute target matching, target-tolerance, coverage, feature, model, and label-purge contracts as the local runner. It appends origin fields to `forecast_baseline_predictions` and `forecast_baseline_metrics` using Delta schema evolution.

`06_forecast_quality_checks` and `monitoring/forecast_quality_checks.sql` validate:

- complete origin sequences for every source group, horizon, and model;
- strictly increasing origin cutoffs;
- non-decreasing post-purge training history;
- one untouched final test origin;
- validation-only earlier origins;
- no evaluation timestamp reused across origins; and
- label availability at every origin cutoff.

`data-contracts/rolling_origin_evaluation_schema.json` defines the additional origin evidence.

## Scheduling boundary

Use `EVALUATION_MODE=holdout` for the ordinary hourly pipeline until Fabric duration and table growth have been observed. Run rolling-origin evaluation manually or on a lower-frequency reviewed schedule because every additional fold fits both baselines for every source group and horizon.

This remains historical backtesting with weather observed at feature time. Production forecasting still requires forecast-weather inputs, drift monitoring, model registration, and promotion controls.

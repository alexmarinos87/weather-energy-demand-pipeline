# Rolling-origin demand backtesting

The local forecasting runner now evaluates the 30- and 60-minute baselines at repeated historical cutoffs instead of relying on one validation window and one test window.

## Contract

The default `--rolling-origin-folds 3` produces:

1. an expanding-window validation origin;
2. a later expanding-window validation origin; and
3. one final untouched test origin.

The original 60/20/20 chronological boundary remains meaningful. The validation 20% is partitioned across all non-final origins, while the final 20% remains the test window. Before every model fit, target labels whose timestamps are not earlier than the origin cutoff are purged.

Each prediction and metric records:

- `origin_fold`;
- `origin_count`;
- `origin_cutoff_utc`;
- `training_observation_count`; and
- `evaluation_contract_version=rolling-origin-v1`.

The run rejects incomplete fold sequences, decreasing cutoffs, decreasing available training history, validation rows in the final origin, test rows before the final origin, or labels unavailable at an origin cutoff.

## Run locally

```bash
python3 -m forecasting.run_baseline \
  --demo \
  --horizon-minutes 30 60 \
  --rolling-origin-folds 3 \
  --output-dir data/forecasting \
  --output-format csv
```

`data-contracts/rolling_origin_evaluation_schema.json` defines the additional origin evidence. Prediction rows continue to satisfy the existing time-horizon evidence contract as well.

## Current boundary

This increment updates the credential-free local/pandas evaluation path. The canonical Fabric notebook still uses its existing fixed chronological validation/test split. A dependent Fabric increment should add the same repeated cutoffs before the rolling-origin evidence is treated as cloud-runtime parity.

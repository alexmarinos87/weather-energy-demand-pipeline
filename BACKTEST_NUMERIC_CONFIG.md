# Backtest numeric configuration

`BacktestConfig.validate()` rejects invalid numeric settings before the normal
backtest path prepares features, builds targets or fits a model. It raises
`ForecastingContractError` naming the setting; it does not silently coerce strings,
truncate counts or turn nonfinite values into defaults.

`train_fraction`, `validation_fraction`, `ridge_alpha` and `min_target_coverage`
must be finite, float-representable real numbers (`numbers.Real`), excluding
booleans. Built-in integer/float and compatible NumPy real scalars are supported.
Numeric strings, missing values, NaN and infinities are rejected. Other numeric
types such as Decimal are outside this explicitly real-valued configuration
contract; callers should supply an intentional supported value.

`min_train_rows`, `min_validation_rows` and `min_test_rows` must be positive
`numbers.Integral` values, excluding booleans. Compatible NumPy integer scalars
remain supported. Fractional and whole-valued floats, strings and nonfinite values
are not row counts and are rejected.

Existing ranges remain unchanged: train and validation fractions lie strictly
between zero and one and together leave a nonempty test fraction; ridge alpha is
positive; target coverage is greater than zero and at most one. Default values,
approved horizons, target tolerance, feature contracts, chronological splits,
label-availability purging and model arithmetic are unchanged. Validation does not
mutate the frozen configuration or input data.

The CLI still parses its arguments using argparse and loads/generates the input
frame before entering the backtest. This change does not promise preflight before
all input I/O: it prevents invalid settings from reaching feature preparation or
fitting through the established backtest entry points. Direct low-level helpers
and arbitrary user-defined numeric classes are not newly validated here.

```bash
python -m pytest -q tests/test_backtest_numeric_config.py
```

Regressions cover configuration acceptance, unchanged valid/default values,
rejection before feature preparation and fitting, input immutability and the real
CLI failing before output creation. Full unchanged constrained CI remains required.
No model is activated, no threshold is changed, and no source data, historical
result, Fabric workspace, dependency or workflow is rewritten by this change.

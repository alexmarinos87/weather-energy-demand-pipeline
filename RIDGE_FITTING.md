# Ridge fitting acceptance

The standalone `fit_ridge_model` now validates the selected training inputs and
computed numeric state before returning a `RidgeModel`. Invalid inputs raise
`ForecastingContractError` with the setting, source column/row or fitting stage.

## Inputs

Alpha must be a positive finite float-representable `numbers.Real` value. Built-in
integers/floats and compatible NumPy real scalars are accepted. Booleans, numeric
strings, Decimal values outside that real-number contract, missing values and
nonfinite or unrepresentable values are rejected, without coercion to defaults.

Feature selection must be a nonempty ordered iterable of unique nonblank string
column names. A bare string, bytes, mapping or set is not a column sequence.
Selected features and the target column must exist, and the DataFrame must not
have duplicate column names. Names are checked, not silently trimmed or renamed.
Callers must provide finite iterables; arbitrary custom containers are not a new
supported interface. Intercept-only fitting is not added; separately constructed
intercept-only models retain their scoring contract.

All selected feature and target values are converted to finite real float scalars
before training arithmetic or a solver call. Finite numeric strings retain the
existing conversion; booleans, complex values, nonscalar containers, missing
values, infinity and unrepresentable numbers are rejected. The source DataFrame
is not mutated, and invalid observations are neither removed nor imputed.

## Computation

The fitter validates means, scales, standardized design values, the completed
normal equations and solved coefficients. Overflow during fitting raises a
contextual domain error with the arithmetic exception as its cause where an
exception was raised. A nonfinite solver result cannot become a returned model.

The existing mean, sample-scale and pivoting solver functions are unchanged.
Regularization is still applied to feature coefficients and not the intercept.
Constant/near-constant feature handling and the order of ordinary float arithmetic
remain unchanged. Validated column lists are used in their original positional
row order when building the design; inputs are not reread with different values.

This is rejection of invalid computation, not a new numerical method. It does not
repair ill-conditioning, change precision, tune alpha, clip measurements, fit a
replacement model or prove numerical accuracy merely because values are finite.
The low-level solver remains an internal helper, not an independently validated
public API. Scoring and downstream evaluation have separate validation boundaries.
No feature/label availability policy or source authenticity check is introduced.

## Regression gate

```bash
python -m pytest -q tests/test_ridge_fit_validation.py
```

Tests exercise direct fitter calls, malformed configuration/columns, invalid
source values never reaching the solver, actual mean/variance/system overflow,
an injected nonfinite solver result, source immutability and valid numeric strings.
A real fit for x=[0,1,2], y=[2,4,6], alpha=1 retains coefficients (4,4/3) and
predictions [8/3,4,16/3]. Full constrained repository CI and validation against
other candidates are required after source or base changes.

This change leaves `RidgeModel.predict`, saved models, input/output writers,
forecasting defaults, schemas, dependencies and CI workflows unchanged. It does
not activate a model, deploy anything, rewrite retained evidence or claim Fabric
runtime parity.

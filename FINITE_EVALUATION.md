# Finite evaluation results

The shared Python helpers in `forecasting/evaluation.py` reject invalid numeric
prediction evidence instead of returning NaN/infinite results or silently
reducing the denominator by skipping missing observations.

`_prediction_rows` requires finite float-representable current demand, actual
future demand and predictions. It also checks the computed residual and squared
error. A finite forecast can still overflow when subtracted from its target or
when its residual is squared; both cases raise `ForecastingContractError` with
model/split/row/field context before a prediction table is returned.

`_metric_row` rejects empty groups and validates actual/predicted inputs before
pandas aggregation. It checks residuals, squared errors, MAE, RMSE, bias and MAPE.
No invalid observation is dropped. In particular, an aggregate can overflow even
when individual squared errors are finite. That is an explicit failure, not a
successful infinite score. Existing numerical warnings may accompany a caught
reduction overflow; the implementation does not globally suppress warnings.

Ordinary float arithmetic and metric definitions remain unchanged. MAPE still
excludes actual values whose absolute magnitude is at most `1e-12`; when all
actuals meet that condition MAPE remains `None`, not zero or a fabricated number.
Other error metrics and observation_count still include those rows. Valid numeric
strings retain float conversion, while booleans, complex values, nonscalar
containers, missing values, infinities and unrepresentable numbers are rejected.
Input DataFrames are not mutated.

The scope is the shared Python evaluation path and its callers, not every report
module or arbitrary externally supplied metric metadata. It does not independently
validate all provenance/count/time fields in direct helper calls, replace the
linear solver, change split/purge logic, stabilize extreme-value arithmetic,
clip predictions, impute values, authenticate evidence, or implement Spark parity.
The existing CLI fails before its output writes when these helpers reject; output
writers and their overwrite/transaction semantics are unchanged.

```bash
python -m pytest -q tests/test_evaluation_finite_results.py
```

Regressions exercise malformed measurements, missing-value averaging, residual,
squared-error and reduction overflow, unchanged hand-computable metrics and MAPE,
input immutability and the real baseline CLI with an injected extreme prediction.
Full unchanged constrained CI and candidate-composition validation remain required.
No historical evidence, model, policy, dependency or workflow is rewritten.

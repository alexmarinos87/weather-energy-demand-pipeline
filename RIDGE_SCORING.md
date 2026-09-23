# Ridge scoring contract

`RidgeModel.predict` validates its scoring vectors before consuming prediction
rows. Means and scales must have one entry per feature, coefficients must include
one intercept plus one entry per feature, and every numeric entry must be finite
and float-representable. Scales must be strictly positive. Invalid state raises
`ForecastingContractError`, including when the prediction batch is empty.

Each prediction row must contain exactly the feature width. Raw string, bytes,
bytearray and mapping containers are rejected rather than iterated as feature
values or dictionary keys. Valid finite scalar values, including numeric strings
inside a proper row, retain the existing float conversion. Booleans, missing
values, nonfinite values and unrepresentable numbers raise a contextual error
identifying the zero-based row and feature. Rows and batches may be generators;
callers must supply a finite iterable in the model's positional feature order.

Standardized feature values and final predictions are also checked for finiteness.
Even finite inputs can overflow during subtraction, division, multiplication or
summation; the method fails instead of returning NaN or infinity. It does not
clamp, impute, retry, fit a replacement model or return a partially successful list.
Already-consumed generator values cannot be rolled back after an error.

For normal fitted float state the existing standardization and weighted-sum
arithmetic is unchanged. Empty prediction batches and valid intercept-only models
remain supported. `fit_ridge_model`, the sample-scale calculations and the linear
solver are not changed. A regression uses x=[0,1,2], y=[2,4,6], alpha=1 to verify
the hand-computable predictions [8/3,4,16/3] after a real fit.

These checks do not infer feature names from arbitrary row containers or detect a
same-width permutation. Callers remain responsible for ordering columns exactly
as `feature_columns`. This is not a complete model-artifact validator or proof of
training provenance. The scoring check does not validate the unused alpha field,
repair numerical instability during fitting, or guarantee finite downstream
squared-error/aggregate metrics for arbitrarily extreme but finite predictions.
Those boundaries remain separate responsibilities.

```bash
python -m pytest -q tests/test_ridge_scoring_contract.py
```

The regression suite checks dimensions, model-vector state, contextual numeric
rejections, scoring overflow, valid iterators and a real fitted model. An injected
invalid fitted state also exercises the actual baseline CLI, which must stop
before writing predictions/metrics. Full unchanged constrained CI and combined
candidate validation remain required. No data, saved model, policy, schema,
Fabric workspace, dependency or workflow is changed by this implementation.

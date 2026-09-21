# Finite forecast measurements

The local OpenWeather adapter applies `_require_finite_forecast_measurements`
after raw structural/schema validation at fetch acceptance and normalization.
It checks every forecast slot's `main.temp` and `main.humidity`, plus the provider
location's `city.coord.lat` and `city.coord.lon`. Checks precede coordinate delta
comparisons and expiration filtering; an expired slot cannot hide an invalid
numeric measurement. One shared helper serves both entry points.

NaN, positive infinity and negative infinity are rejected. Numbers that cannot be
represented by the existing float-based normalization (for example extremely
large integers) receive a contextual `OpenWeatherForecastError` with the original
exception as cause. Existing schema type and range checks still run first, so
some invalid inputs continue to raise `ContractValidationError` instead.

This does not clamp, replace or impute values, add physical temperature bounds,
coerce numeric strings, or change legitimate finite values. Fetch request settings,
provider count/order checks, proxy tolerance, retained timestamps, snapshot IDs and
normalized output shape are unchanged. Replay stays offline and input objects are
not mutated. No models, policies, schemas, dependencies or CI settings are changed.

The scope is deliberately limited to these normalized measurements and provider
coordinates. It does not scan arbitrary extra provider fields or pipeline metadata,
authenticate sources, verify snapshot digests or establish physical plausibility.
Saved metadata retains its separate validation rules. This does not implement
Fabric/Spark parity or fix file-publication behavior.

Regression command:

```bash
python -m pytest -q tests/test_forecast_finite_measurements.py
```

The 36 cases exercise the real adapter with injected dummy credentials and HTTP
responses, then offline replay. They cover nonfinite values, decoded overflow,
unrepresentable integers, unchanged finite values, expired invalid slots, input
immutability and the main entry point stopping before either saver. Full constrained
CI and separate combined validation remain necessary after integration changes.

# Forecast request preflight

`fetch_openweather_forecast` validates configured request bounds and any explicitly
supplied retrieval time before looking up the API key or making the HTTP request.
The existing source binding, endpoint allowlist and metric-units checks remain.

## Integer bounds

`api.timeout_seconds` and `api.max_forecast_records` accept positive Python
integers or decimal integer strings with optional surrounding whitespace and a
leading plus sign. Floating-point values, including `2.0`, are rejected instead
of truncated; booleans, non-finite values and fractional strings are also rejected.
The existing maximum of 40 forecast records is unchanged. Invalid inputs raise a
`ValueError` naming the field. Use YAML integer values, for example:

```yaml
api:
  timeout_seconds: 20
  max_forecast_records: 40
```

This fragment supplements the existing configuration; it is not a complete config.
No new request parameter or retry is introduced.

## Retrieval-time semantics

An explicit `retrieved_at_utc` must be a non-missing timezone-aware `datetime`.
It must have both timezone information and a defined UTC offset. Strings, naive
datetimes, empty values, booleans, zero and `NaT` are rejected before key lookup
or HTTP. Valid aware datetimes are converted to UTC without modifying the input.

Omitting the argument (or using `None`) still captures current UTC time **after
the response and provider validation**, not at request start. This preserves the
existing conservative availability-time convention. Do not move the default
clock into request preflight: the forecast has not been received at that point.
The API does not authenticate a caller-supplied time or convert the retrieval-time
surrogate into the provider's true model issue time.

## Scope and tests

Provider payload validation, normalization, source metadata, output filenames,
raw/normalized writers, schemas and dependencies are unchanged. No live provider
call or actual credential read is needed for the tests in
`tests/test_forecast_request_preflight.py`; key lookup and HTTP are injected.
They cover rejection before side effects, valid bounds and the 40-record cap,
offset conversion, unchanged inputs/request parameters, exclusion of the dummy
key from evidence, and response-before-clock ordering for the default timestamp.

Datetime awareness follows the [Python datetime contract](https://docs.python.org/3.11/library/datetime.html#determining-if-an-object-is-aware-or-naive).

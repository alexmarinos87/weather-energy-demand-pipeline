# Current-weather unit boundary

The local current-weather adapter requires an explicit metric request. Silver
copies provider `main.temp`, `main.feels_like` and `wind.speed` into
`temperature_c`, `feels_like_c` and `wind_speed_mps` without unit conversion.
Accepting arbitrary request units would let those labels misrepresent the values.

OpenWeather defines metric temperature in Celsius, standard temperature in Kelvin,
and imperial temperature in Fahrenheit. Metric wind speed is metres per second;
imperial wind speed is miles per hour. See the provider's
[unit table](https://openweathermap.org/api/weather-data) and
[current-weather API documentation](https://docs.openweather.co.uk/current).

## Request contract

`api.units` must be a string whose stripped, lowercase value is `metric`.
Case and whitespace variants are accepted, but the HTTP request always sends
canonical `units=metric`. Missing, blank, non-string, `standard` and `imperial`
settings raise `ValueError` naming `api.units` before credential lookup or HTTP.
Source-area/city binding validation still runs first. Use this field in the
existing configuration:

```yaml
api:
  units: metric
```

The fragment supplements the existing example config; it is not a complete
configuration. There is no implicit default, conversion mode or fallback request.
The URL, city parameter, timeout, provider-schema gate and successful metadata
shape are unchanged. Input dictionaries are not modified.

## Evidence and scope limits

This establishes the units requested from the provider. It does not authenticate
a provider response, prove a historical capture's units or convert old raw data.
The raw payload has no newly added unit marker, and callers supplying legacy raw
files directly to silver retain responsibility for their unit provenance. Do not
relabel or rewrite historical evidence based on this request-side correction.

The separate forecast adapter, raw-file saver, silver transformation rules,
Fabric notebooks, JSON schemas and forecasting calculations are not changed by
this increment. Raw-publication PR #113 changes the same adapter's saver rather
than its request code; integration must preserve both independent edits.

`tests/test_current_weather_units.py` uses the real current-weather adapter with
injected key/HTTP functions and sends accepted evidence through the real silver
transformer. It covers rejection before side effects, canonical metric requests,
Celsius/feels-like/wind columns, source/configuration immutability, dummy-key
exclusion, existing source-binding/provider errors and transport propagation.
No live request or real credential is needed.

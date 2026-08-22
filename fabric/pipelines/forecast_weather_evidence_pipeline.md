# Optional Fabric forecast-weather evidence subflow

This is not part of the ordinary hourly `weather_energy_demand_pipeline`.

## Parameters

| Name | Required | Default / rule |
| --- | --- | --- |
| `SOURCE_AREA` | Yes | Key from `source_areas.json` |
| `OPENWEATHER_API_KEY` | Yes | Secure parameter or connection-backed secret |
| `FORECAST_MAX_RECORDS` | No | `40`; values above 40 are rejected |
| `REQUEST_TIMEOUT_SECONDS` | No | `30`; positive integer |
| `COORDINATE_TOLERANCE_DEGREES` | No | `0.25`; non-negative |
| `LAKEHOUSE_FILES_ROOT` | No | `/lakehouse/default/Files` |
| `CONTRACTS_ROOT` | No | Optional contract-folder override |

## Activities

1. `01b_ingest_forecast_weather_to_bronze`
   - Validate source-area coordinates and bounded configuration before reading the secret.
   - Fetch the fixed OpenWeather 5-day/3-hour endpoint.
   - Validate schema, counts, ordering, country, and coordinates.
   - Write one immutable content-addressed raw snapshot.
2. `02b_forecast_weather_to_silver`
   - Depend on bronze success.
   - Normalize issue/availability/valid-time evidence.
   - Enforce the retrieval-time-surrogate contract.
   - Deduplicate deterministically.
   - Overwrite the canonical `silver_forecast_weather` table partitioned by valid date.

## Operating mode

Run manually during evidence collection. Do not add a trigger until quota, capacity, retention, forecast-versus-observed coverage, and target-weather usefulness have been reviewed. This subflow does not run forecasting and cannot promote a model.

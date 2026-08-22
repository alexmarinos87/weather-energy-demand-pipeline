# Fabric forecast-weather bronze and silver path

## Boundary

Forecast-weather evidence is an optional/manual Fabric subflow. It does not alter the existing six-stage `weather_energy_demand_pipeline`, activate a schedule, or add target weather to the active demand model.

The subflow consists of:

```text
01b_ingest_forecast_weather_to_bronze
        ↓ immutable provider raw JSON
02b_forecast_weather_to_silver
        ↓ canonical silver_forecast_weather Delta table
```

## Bronze ingestion

`01b_ingest_forecast_weather_to_bronze` uses the project-owned latitude and longitude from `data-contracts/source_areas.json`. Runtime users select only `SOURCE_AREA`; arbitrary coordinates and provider URLs are not accepted.

The notebook validates before source I/O:

- the source-area contract and proxy coordinates;
- a positive timeout;
- a forecast record bound no greater than 40; and
- availability of the secure `OPENWEATHER_API_KEY` only after non-secret preflight passes.

It calls the fixed HTTPS endpoint:

```text
https://api.openweathermap.org/data/2.5/forecast
```

with metric units and coordinate parameters. The response is checked against `openweather_forecast_raw_schema.json`, then checked for count consistency, strictly increasing valid timestamps, country identity, and coordinate agreement.

Provider model-run time is not invented. The completed retrieval time is recorded as the conservative issue and availability surrogate:

```text
forecast_issue_basis=retrieval_time_surrogate
forecast_issued_at_utc
    = forecast_ingested_at_utc
    = forecast_retrieved_at_utc
```

Raw files are content-addressed and created exclusively under:

```text
Files/raw/forecast_weather/ingestion_date=YYYY-MM-DD/
  openweather_forecast_<timestamp>_<snapshot>.json
```

Credentials are never written to the payload.

## Silver normalization

`02b_forecast_weather_to_silver` explodes provider forecast slots and builds `silver_forecast_weather` with:

- source-area and city identity;
- issue, ingestion, retrieval, and valid timestamps;
- temperature and humidity forecasts;
- provider, model, issue basis, and provider record identity;
- provider location and coordinates;
- raw snapshot and source-file lineage; and
- valid-date partitioning.

Rows are rejected when required fields are null, humidity is invalid, issue/availability timestamps differ from the declared retrieval surrogate, valid time is not future-facing, provider identity is wrong, country is not GB, or the snapshot digest is malformed.

Duplicates are resolved deterministically by source identity, provider/model, issue time, and valid time, preferring the latest available evidence and stable lineage.

## Manual execution

1. Upload all `data-contracts/` files to `Files/data-contracts/`.
2. Import and attach both optional notebooks to `weather_energy_lakehouse`.
3. Supply `SOURCE_AREA` and `OPENWEATHER_API_KEY` securely.
4. Run `01b_ingest_forecast_weather_to_bronze`.
5. Run `02b_forecast_weather_to_silver`.
6. Inspect `silver_forecast_weather` before using it in reconciliation or model experiments.

Do not schedule this subflow until retention, API quota, Fabric capacity, reconciliation coverage, and provider quality have been reviewed.

## Next dependency

The next product layer is Fabric paired-model execution using `silver_forecast_weather`, while preserving `ridge_weather_lag` as the control and keeping promotion human-reviewed.

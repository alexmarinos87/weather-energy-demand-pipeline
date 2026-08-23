# Reproducible demand/weather analytics report

## Purpose

The repository now exposes a credential-free, testable analysis surface for the demand and weather feature data. It replaces the former empty notebook placeholders with valid notebooks backed by one reusable Python implementation.

The analysis is descriptive. It does not claim that temperature or humidity causes demand, and it preserves the repository's source-area and representative-city-proxy terminology.

## Inputs

The report accepts the existing gold feature shape:

```text
source_area
resource_id
city
event_timestamp_utc
demand_mw
temperature
humidity
weather_age_minutes
```

CSV and Parquet files are supported. A partitioned directory is read recursively. All timestamps must be timezone-aware; source identities must be non-empty; numeric values must be finite; humidity must be between 0 and 100; weather age must be non-negative; and one source group cannot contain duplicate event timestamps.

## Outputs

For each source-area/resource/city group, the report produces:

- an overview with time coverage, observation cadence, demand and weather distributions, and Pearson correlations;
- an hourly UTC load profile;
- a fixed-width temperature-band demand profile;
- the highest-demand events with contemporaneous weather evidence; and
- a Markdown summary that states the descriptive interpretation boundary.

Every run writes immutable, run-ID-qualified files:

```text
demand_weather_overview_<run-id>.csv|parquet
hourly_load_profile_<run-id>.csv|parquet
temperature_demand_profile_<run-id>.csv|parquet
peak_demand_events_<run-id>.csv|parquet
demand_weather_report_<run-id>.md
```

The overview rows are versioned by `demand-weather-analytics-v1` and validated by `data-contracts/demand_weather_analysis_summary_schema.json`.

## Credential-free demo

```bash
python3 -m forecasting.run_demand_weather_report \
  --demo \
  --top-peak-count 10 \
  --temperature-bin-width-c 5 \
  --output-dir data/analytics/demand_weather \
  --output-format csv
```

## Analyze exported gold features

```bash
python3 -m forecasting.run_demand_weather_report \
  --input gold_feature_engineering.parquet \
  --top-peak-count 20 \
  --temperature-bin-width-c 2.5 \
  --output-dir data/analytics/demand_weather \
  --output-format parquet
```

## Notebooks

- `notebooks/exploratory_analysis.ipynb` demonstrates the overview, hourly profile, and peak events.
- `notebooks/demand_vs_temperature.ipynb` demonstrates the temperature-band profile and its interpretation boundary.

The notebooks contain no embedded credentials, no live API calls, and no committed execution output. They call the same tested functions as the CLI rather than reimplementing analysis logic.

## Boundary

The report summarizes retained data only. It does not fetch a source, train or promote a model, modify Fabric, create a schedule, deploy infrastructure, or publish an external dashboard.

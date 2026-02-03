## Problem

Energy demand is highly sensitive to weather conditions, yet weather and energy data are often siloed, updated at different frequencies, and consumed manually. This makes it difficult to analyse demand patterns consistently and at scale.

Manual data extraction introduces **auditability and traceability issues**, as results can vary depending on when and how the data is pulled. Over time, this leads to inconsistencies and reduced trust in analysis.

Additionally, the lack of integrated, near real-time data can create **operational blind spots**, forcing grid operators and analysts to react to changes in demand rather than anticipate them.

---

## Solution

This project implements a cloud-based data pipeline using **Amazon Web Services (AWS)** to ingest live weather data and electricity demand data, transform it into analytics-ready formats, and produce features suitable for demand analysis and forecasting.

The pipeline addresses auditability and traceability challenges by:
- Automating data ingestion
- Storing immutable raw data
- Applying version-controlled transformations
- Producing consistent, reproducible outputs

By combining weather data with energy demand data, the pipeline enables detection of **demand sensitivity shifts** and comparison of forecast versus actual demand. This supports more proactive decision-making for grid operators, analysts, and energy market participants.

---

## Data Sources

- **OpenWeather API** – live and historical weather data (temperature, humidity, wind, precipitation)
- **UK National Grid ESO** – electricity demand data

---

## Skills Demonstrated

- Data engineering fundamentals (API ingestion, layered data modeling)
- Cloud-native architecture (AWS-based design)
- SQL and data modeling
- Domain-aware feature engineering (weather–demand relationships)
- Data quality and reliability
- Automation and reproducibility
- Technical documentation

---

## Plan

1) Ingestion (Raw Layer)

- Weather ingestion: run OpenWeather pulls on a schedule; store immutable raw JSON.
- Energy ingestion: run UK National Grid ESO pulls; store immutable raw data.
- Secrets/config: define how API keys and endpoints are managed (env vars, config files).

2) Silver Layer (Cleaning / Standardization)

- Data cleaning: normalise schemas, parse timestamps, handle missing fields.
- Deduplication: apply deterministic deduping rules.
- Partitioning strategy: define dt partitioning for query performance.

3) Gold Layer (Analytics / Features)

- Weather–demand join: define keying strategy (timestamp + location).
- Feature engineering: derive weather sensitivity metrics, rolling stats, etc.
- Aggregations: align with forecasting or reporting requirements.

4) Orchestration & Scheduling

- Workflow orchestration: define the order (ingest → silver → gold).
- Schedules: document cadence (hourly, daily).
- Retries & alerting: error handling and failure notifications.

5) Data Quality & Monitoring

- Validation checks: schema validation, anomaly detection.
- SLAs: data freshness & completeness checks.
- Observability: logging, metrics, and alerting.

6) Documentation & Usage

- Quickstart: prerequisites, how to run ingestion & transformations.
- Architecture diagram: refer to the data flow and AWS components.
- Example analysis: demonstrate a simple demand vs weather sensitivity study.

---

## Quickstart (Local)

1) Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

2) Set API keys:

   ```bash
   export OPENWEATHER_API_KEY="your_openweather_key"
   export NATIONAL_GRID_API_KEY="your_national_grid_key"
   ```

3) Configure ingestion:

   - Weather: copy `ingestion/weather/config.example.yaml` to `ingestion/weather/config.yaml` and update your city/units.
   - Energy: update `ingestion/energy/config.yaml` with the ESO `resource_id` for the dataset you want.

4) Run ingestion (raw layer):

   ```bash
   python ingestion/weather/fetch_weather.py
   python ingestion/energy/fetch_energy.py
   ```

5) Run cleaning (silver layer):

   ```bash
   python transformations/silver/clean_weather.py
   ```

6) Continue with gold-layer SQL once your energy schema is finalized.

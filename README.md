## Problem

Energy demand is highly sensitive to weather conditions, yet weather and energy data are often siloed, updated at different frequencies, and consumed manually. This makes it difficult to analyse demand patterns consistently and at scale.

Manual data extraction introduces **auditability and traceability issues**, as results can vary depending on when and how the data is pulled. Over time, this leads to inconsistencies and reduced trust in analysis.

Additionally, the lack of integrated, near real-time data can create **operational blind spots**, forcing grid operators and analysts to react to changes in demand rather than anticipate them.

---

## Solution

This project implements a cloud-based data pipeline using **Microsoft Fabric** to ingest live weather data and electricity demand data, transform it into analytics-ready Delta tables, and produce features suitable for demand analysis and forecasting.

The pipeline addresses auditability and traceability challenges by:
- Automating data ingestion
- Storing immutable raw data in OneLake
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
- Cloud-native architecture with Microsoft Fabric, OneLake, Lakehouse, Data Factory, and notebooks
- SQL, PySpark, and Delta Lake data modeling
- Domain-aware feature engineering (weather–demand relationships)
- Data quality and reliability
- Automation and reproducibility
- Technical documentation

---

## Microsoft Fabric Migration

The active cloud target is now Microsoft Fabric.

- Storage: OneLake Lakehouse files and Delta tables
- Orchestration: Fabric Data Factory pipeline schedule
- Compute: Fabric notebooks using Spark
- Serving: Lakehouse SQL analytics endpoint and Power BI-ready gold tables
- Monitoring: Fabric monitoring hub plus data quality checks written to Delta

Fabric migration assets are in:

- `fabric/README.md`
- `fabric/notebooks/`
- `fabric/sql/`
- `fabric/pipelines/`
- `architecture/data_flow.md`
- `orchestration/schedules.md`
- `monitoring/data_quality_checks.sql`

---

## Local Run (Development Quickstart)

The local Python entry points are retained for development and contract testing. Fabric notebooks are the canonical cloud runtime.

### Prereqs

- Python 3.10+
- `pip` installed (`python3 -m pip --version`)

### Setup

1. Create your weather config:

```bash
cp ingestion/weather/config.example.yaml ingestion/weather/config.yaml
```

2. Create your energy config:

```bash
cp ingestion/energy/config.example.yaml ingestion/energy/config.yaml
```

3. Set API keys in `.env`:

```
OPENWEATHER_API_KEY=your_key
NATIONAL_GRID_API_TOKEN=your_token
```

4. Set the energy `resource_id` in `ingestion/energy/config.yaml`.
   This repo currently uses the NGED **Live Data** dataset, which is regional.
   Pick one resource (East Midlands, South Wales, South West, West Midlands).

5. Install dependencies:

```bash
python3 -m pip install -r requirements.txt
```

### Run

```bash
set -a
source .env
set +a

python3 ingestion/weather/fetch_weather.py
python3 ingestion/energy/fetch_energy.py
```

Raw outputs are written to:

- `data/raw/weather/`
- `data/raw/energy/`

### Quick Win Implemented: Contract Gate on Ingestion

Local and Fabric ingestion jobs now validate API payloads against versioned contracts before writing raw files:

- `data-contracts/weather_schema.json`
- `data-contracts/energy_schema.json`

If a payload drifts (missing required fields or invalid types), ingestion fails fast and no raw file is written.

For Fabric runs, upload these files to `Files/data-contracts/` in the Lakehouse or pass `CONTRACTS_ROOT` to the ingestion notebook.

Run tests for this gate:

```bash
pytest -q
```

---

## Fabric Run Order

1. Create a Fabric workspace and Lakehouse named `weather_energy_lakehouse`.
2. Create a Fabric Environment from `fabric/environment.yml`.
3. Import the notebook sources in `fabric/notebooks/` and attach the Lakehouse.
4. Create a Fabric Data Factory pipeline following `fabric/pipelines/weather_energy_demand_pipeline.md`.
5. Add the SQL endpoint views from `fabric/sql/gold_views_tsql.sql` if analysts need stable SQL view names.
6. Schedule the pipeline using `orchestration/schedules.md`.

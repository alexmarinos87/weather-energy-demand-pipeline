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

## Local Run (Quickstart)

### Prereqs

- Python 3.10+
- `pip` installed (`python3 -m pip --version`)

### Setup

1. Create your weather config:

```bash
cp ingestion/weather/config.example.yaml ingestion/weather/config.yaml
```

2. Set API keys in `.env`:

```
OPENWEATHER_API_KEY=your_key
NATIONAL_GRID_API_TOKEN=your_token
```

3. Set the energy `resource_id` in `ingestion/energy/config.yaml`.
   This repo currently uses the NGED **Live Data** dataset, which is regional.
   Pick one resource (East Midlands, South Wales, South West, West Midlands).

4. Install dependencies:

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

Both ingestion jobs now validate API payloads against versioned contracts before writing raw files:

- `data-contracts/weather_schema.json`
- `data-contracts/energy_schema.json`

If a payload drifts (missing required fields or invalid types), ingestion fails fast and no raw file is written.

Run tests for this gate:

```bash
pytest -q
```

---

## Plan (Current)

1. Ingestion: verify API access and stable raw capture.
2. Silver: implement energy cleaning and align weather schema.
3. Gold: build join + feature engineering SQL.
4. Orchestration: wire Step Functions and schedules.
5. Monitoring: add CloudWatch alarms and data quality checks.

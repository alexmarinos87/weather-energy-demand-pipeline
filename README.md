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

# Fabric orchestration schedule

Run `weather_energy_demand_pipeline` hourly in UTC initially.

Required parameters must form one valid source binding, for example:

```text
SOURCE_AREA=east_midlands
WEATHER_CITY=Nottingham,GB
NATIONAL_GRID_RESOURCE_ID=92d3431c-15d7-4aa6-ad34-2335596a026c
DATASET=all
```

Recommended controls:

- two retries with at least five minutes between attempts;
- 30-minute timeout per ingestion, silver, gold, and quality activity;
- a separate reviewed timeout for baseline forecasting after observing group sizes;
- secure credential parameters or Fabric connections;
- ingestion failure stops all downstream work;
- forecasting runs only after causal gold features are rebuilt;
- final quality errors fail the run after results are written;
- warning results remain visible for freshness, unmatched weather, and legacy unscoped history.

Each scheduled forecasting run appends prediction and metric evidence. Define a retention/compaction policy before increasing cadence or retained history.

Move to a 15- or 30-minute cadence only after confirming API quotas, source update behaviour, Fabric capacity headroom, forecast-table growth, and downstream latency needs.

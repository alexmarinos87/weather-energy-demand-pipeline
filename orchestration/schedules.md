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
- 30-minute timeout per notebook activity;
- secure credential parameters or Fabric connections;
- ingestion failure stops silver and gold rebuilds;
- data-quality errors fail the run after results are written;
- warning results remain visible for freshness, unmatched weather, and legacy unscoped history.

Move to a 15- or 30-minute cadence only after confirming API quotas, source update behaviour, Fabric capacity headroom, and downstream latency needs.

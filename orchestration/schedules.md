# Fabric orchestration schedule

Run `weather_energy_demand_pipeline` hourly in UTC initially with the fixed holdout evaluation mode.

Required parameters must form one valid source binding, for example:

```text
SOURCE_AREA=east_midlands
WEATHER_CITY=Nottingham,GB
NATIONAL_GRID_RESOURCE_ID=92d3431c-15d7-4aa6-ad34-2335596a026c
DATASET=all
HORIZON_MINUTES=30,60
TARGET_TOLERANCE_MINUTES=5
MIN_TARGET_COVERAGE=0.90
EVALUATION_MODE=holdout
ROLLING_ORIGIN_FOLDS=3
```

`ROLLING_ORIGIN_FOLDS` is ignored by forecasting when `EVALUATION_MODE=holdout`, but keeping an explicit reviewed value makes a later rolling run reproducible.

Recommended controls:

- two retries with at least five minutes between attempts;
- 30-minute timeout per ingestion, silver, gold, and quality activity;
- a separate reviewed timeout for forecasting after observing target-match, model-group, and rolling-fold sizes;
- secure credential parameters or Fabric connections;
- ingestion failure stops all downstream work;
- forecasting runs only after causal gold features are rebuilt;
- zero eligible horizon history or low target coverage fails forecasting;
- incomplete or non-monotonic rolling-origin evidence fails forecasting and final quality checks;
- final quality errors fail the run after results are written;
- warning results remain visible for freshness, unmatched weather, and legacy unscoped history.

Each scheduled forecasting run appends prediction and metric evidence. Define a retention/compaction policy before increasing cadence, retained history, or rolling fold count.

Use `EVALUATION_MODE=rolling-origin` manually or on a lower-frequency reviewed schedule first. Each additional origin fits both persistence evidence and ridge regression for every source group and horizon, increasing driver-side model orchestration and Delta-table growth.

Move the ordinary holdout pipeline to a 15- or 30-minute schedule only after confirming API quotas, source update behaviour, source interval regularity, target coverage, Fabric capacity headroom, forecast-table growth, and downstream latency needs. Increase rolling-origin frequency only after its separate runtime and storage profile is understood.

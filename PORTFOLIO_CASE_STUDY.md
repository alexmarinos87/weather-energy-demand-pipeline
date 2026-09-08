# Portfolio Case Study: Weather–Energy Demand Forecasting

## Executive summary

This repository demonstrates a contract-first Microsoft Fabric data product that combines weather and electricity-demand data while protecting forecasting evaluation from temporal leakage. It includes deterministic local evidence, Fabric implementation templates, provider-quality monitoring and explicit human authority over model and pilot decisions.

The central engineering question is not simply whether a model can produce a prediction. It is whether every feature, target, split, forecast snapshot and promotion decision can be shown to have been available at the correct time and tied to the correct source area.

## The engineering problem

Electricity demand is time-dependent and weather-sensitive, but naïve joins and evaluation logic can leak future information into training or scoring. A trustworthy data product must also account for source-area identity, delayed observations, provider snapshots, calendar behaviour and the difference between retrospective evidence and production approval.

The project addresses those concerns across four representative NGED licence-area/weather-proxy bindings:

- East Midlands / Nottingham;
- South Wales / Cardiff;
- South West / Bristol; and
- West Midlands / Birmingham.

The cities and coordinates are project-owned proxies rather than claims that one point completely represents weather across an entire licence area.

## Architecture

```text
OpenWeather observations + NGED demand + bounded forecast snapshots
                               ↓
       source-area contracts · immutable raw evidence · provenance
                               ↓
                typed and deduplicated Silver datasets
                               ↓
    same-area causal joins · lag/rolling · calendar/weather features
                               ↓
       bounded 30/60-minute targets · unavailable-label purge
                               ↓
 persistence · seasonal · observed-weather and target-weather ridge models
                               ↓
 scorecards · calibration-only intervals · reconciliation · provider health
                               ↓
          immutable review evidence and explicit human decisions
```

The canonical Fabric path uses Spark Delta tables for current-weather and demand ingestion, Silver transformation, causal Gold features, forecasting evidence and pass-through reporting views. The local target-weather comparison remains deliberately separate until retained evidence justifies provider ingestion and Spark parity.

## Key engineering decisions

### 1. Bind every row to a source-area contract

`data-contracts/source_areas.json` owns the relationship between each NGED resource and its representative weather identity. Area, resource, city, coordinates and contract version travel with the evidence so rows from different spatial groups cannot be mixed silently.

### 2. Join weather only when it was causally available

A demand row may use current weather only from the same non-null source area, with a weather timestamp at or before demand time and no more than six hours old. Lag and rolling demand features exclude the current demand row.

### 3. Make target construction explicit

For each source-area/resource/city group, the evaluator forms an ideal target 30 or 60 minutes after feature time, finds the first acceptable observation inside the configured tolerance and enforces matched coverage. Labels unavailable at a training or evaluation cutoff are purged.

Every retained prediction preserves the ordering:

```text
trained_through_utc < feature_timestamp_utc < event_timestamp_utc
```

Rolling-origin evidence additionally records the origin cutoff and non-overlapping evaluation folds.

### 4. Treat forecast weather as versioned availability evidence

The OpenWeather adapter records retrieval time as a conservative issue/availability surrogate rather than inventing a provider model-run timestamp. A forecast can enter a feature set only after ingestion, before feature time and for a valid time close enough to the future demand target.

Observed-weather and target-weather models are compared on the same target-covered cohort with identical targets, splits, origins and label-purge rules.

### 5. Keep UTC as the audit identity

UTC owns ordering, target, join and audit identity. Europe/London calendar fields are versioned model features. Paired scorecards require identical target and training evidence before comparing the two calendar contracts.

### 6. Calibrate intervals without test-label leakage

Prediction-interval radii are chosen only from validation residuals whose target labels were available before the first test feature timestamp. Retained test labels measure empirical coverage; they do not select the interval width.

### 7. Separate evidence generation from promotion authority

The repository can generate comparisons, health evidence, candidate records and pilot artifacts. Those outputs cannot silently deploy Fabric, activate a schedule, promote a model, authorize a pilot or rewrite historical evidence.

## Five-minute walkthrough

### 1. Run the credential-free product journey

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install \
  -r requirements.txt \
  -c constraints/ci-python311-linux.txt
python -m pip check

python -m forecasting.run_portfolio_demo \
  --output-root data/portfolio-demo \
  --output-format csv
```

Each run creates a new immutable `pdm-<id>` directory rather than overwriting an earlier evidence package.

### 2. Inspect the manifest

Open `portfolio_demo_manifest.json`. It binds the four source areas, both demand horizons, model identities, UTC and UK-local feature contracts, interval levels and every artifact's path, size, row count and SHA-256 digest.

The package includes deterministic features, persistence and ridge baselines, target-weather comparisons, seasonal previous-day and previous-week baselines, a five-model scorecard, 80/90/95% calibration-only intervals and descriptive reports.

### 3. Trace one causal boundary

Use [`architecture/data_flow.md`](architecture/data_flow.md) to follow:

```text
weather available by feature time
        + prior demand only
        + target available after feature time
        + label available before evaluation cutoff
```

This is the core explanation for why the demo is more than a conventional random train/test split.

### 4. Show the human-authority boundary

Use the model registry, promotion assessment and pilot documents to explain that evidence may support a named decision but cannot activate one automatically.

## Evidence map

| Question | Repository evidence |
| --- | --- |
| What does the complete local demo produce? | [`PORTFOLIO_DEMO.md`](PORTFOLIO_DEMO.md) |
| How do source, Silver, Gold and evaluation stages connect? | [`architecture/data_flow.md`](architecture/data_flow.md) |
| How is target-valid forecast weather handled? | [`FORECAST_WEATHER.md`](FORECAST_WEATHER.md) |
| How are rolling origins constructed? | [`ROLLING_ORIGIN.md`](ROLLING_ORIGIN.md) |
| How are seasonal and calendar models compared? | [`MODEL_SCORECARD.md`](MODEL_SCORECARD.md) |
| How are prediction intervals calibrated? | [`PREDICTION_INTERVALS.md`](PREDICTION_INTERVALS.md) |
| How is forecast-provider quality monitored? | [`PROVIDER_MONITORING.md`](PROVIDER_MONITORING.md) |
| What would run in Fabric? | [`fabric/README.md`](fabric/README.md) |
| What work remains dependency-ordered? | [`ROADMAP.md`](ROADMAP.md) |
| Where is the complete capability reference? | [`PROJECT_REFERENCE.md`](PROJECT_REFERENCE.md) |

## Trade-offs and boundaries

- The local demo is deterministic and credential-free; it is not evidence that a live Fabric workspace has been deployed.
- Representative cities are spatial proxies, not complete meteorological descriptions of their licence areas.
- The current design rebuilds small Silver and Gold histories and enumerates a bounded number of groups and origins; larger scale would require partition-aware Delta operations and distributed orchestration.
- Historical backtests and empirical interval coverage do not guarantee future performance under distribution shift.
- Provider monitoring and promotion records are evidence for review, not authority to activate a model or schedule.

## Interview discussion prompts

- Where can leakage enter a weather-and-demand forecasting pipeline?
- Why is retrieval time used as a conservative forecast-availability boundary?
- How do fixed holdout and rolling-origin evaluation answer different questions?
- Why keep UTC as the audit identity while offering UK-local calendar features?
- How should interval calibration change when data availability is delayed?
- What evidence would be required before adding forecast-weather ingestion to the canonical Fabric path?

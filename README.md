# Weather–Energy Demand Pipeline

A contract-first Microsoft Fabric data product that combines weather and electricity-demand data with causal feature engineering, leakage-safe forecasting evaluation, provider-quality evidence and explicit human control over model promotion.

**Python · PySpark · Microsoft Fabric · Delta Lake · Time Series · Data Quality · CI**

## At a glance

| Path | Implementation focus |
| --- | --- |
| Product | Current weather and electricity-demand ingestion, typed Silver data and causal Gold features |
| Evaluation | Bounded 30/60-minute targets, holdout and rolling-origin backtests, seasonal baselines and paired calendar scorecards |
| Forecast evidence | Observed-weather and target-valid forecast-weather comparisons with calibration-only prediction intervals |
| Operations | Source reconciliation, provider health and drift evidence, immutable manifests and recovery controls |
| Governance | Human-reviewed candidate records, bounded pilot contracts and no silent scheduling, activation or promotion |

## One-command credential-free demo

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install \
  -r requirements.txt \
  -c constraints/ci-python311-linux.txt

python -m forecasting.run_portfolio_demo \
  --output-root data/portfolio-demo \
  --output-format csv
```

The demo uses deterministic local inputs, performs no live provider request, changes no Fabric workspace and writes an immutable evidence bundle for all four configured source areas. Its `portfolio_demo_manifest.json` records artifact hashes, row counts, contract identities and no-side-effect flags.

## Architecture

```text
OpenWeather current + forecast · NGED demand data
                         ↓
source contracts · immutable raw evidence · typed Silver
                         ↓
causal same-area joins + lag, calendar and weather features
                         ↓
bounded targets + unavailable-label purge
                         ↓
persistence · seasonal · ridge evaluation
                         ↓
scorecards · reconciliation · provider-health evidence
                         ↓
human review and bounded pilot records
```

## Engineering guarantees

- Weather and demand are joined only within the same source area and within explicit timestamp and age boundaries.
- Demand lags and rolling features exclude the current target row.
- Target labels must be available inside the declared horizon tolerance, and unavailable labels are purged at every evaluation boundary.
- Forecast weather must have been available by feature time and valid for the demand target window.
- UTC remains the ordering and audit identity; Europe/London calendar fields are versioned model features rather than replacements for UTC.
- Prediction intervals use calibration evidence only; retained test labels do not choose the interval radius.
- Generated evidence can inform a review but cannot silently deploy a workspace, schedule a pipeline, promote a model or authorize a pilot.

## Capability index

| Topic | Starting point |
| --- | --- |
| Credential-free portfolio demo | [`PORTFOLIO_DEMO.md`](PORTFOLIO_DEMO.md) |
| Source and transformation flow | [`architecture/data_flow.md`](architecture/data_flow.md) |
| Forecast-weather contract | [`FORECAST_WEATHER.md`](FORECAST_WEATHER.md) |
| Rolling-origin evaluation | [`ROLLING_ORIGIN.md`](ROLLING_ORIGIN.md) |
| Seasonal and calendar comparisons | [`MODEL_SCORECARD.md`](MODEL_SCORECARD.md) |
| Prediction intervals | [`PREDICTION_INTERVALS.md`](PREDICTION_INTERVALS.md) |
| Provider monitoring | [`PROVIDER_MONITORING.md`](PROVIDER_MONITORING.md) |
| Fabric implementation templates | [`fabric/README.md`](fabric/README.md) |
| Dependency-ordered roadmap | [`ROADMAP.md`](ROADMAP.md) |
| Complete capability and command reference | [`PROJECT_REFERENCE.md`](PROJECT_REFERENCE.md) |

## Project boundary

The repository contains executable local behaviour, Fabric implementation templates and governance contracts. It does not claim that a live Fabric workspace has been deployed, that historical model performance guarantees future performance, or that any model candidate is production-approved.

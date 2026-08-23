# One-command local portfolio demo

## Purpose

The repository has many independently testable components. The portfolio demo assembles the most visible local product path into one credential-free command without weakening their individual contracts.

It runs:

```text
deterministic gold-like feature data
        ↓
fixed 30/60-minute persistence and ridge baselines
        ↓
deterministic target-valid weather evidence
        ↓
paired observed-weather vs target-weather ridge comparison
        ↓
descriptive demand/weather analytics report
        ↓
hash-verified immutable artifact manifest
```

## Install the supported CI environment

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install \
  -r requirements.txt \
  -c constraints/ci-python311-linux.txt
python -m pip check
```

On Windows PowerShell, activate with:

```powershell
.venv\Scripts\Activate.ps1
```

## Run

```bash
python -m forecasting.run_portfolio_demo \
  --output-root data/portfolio-demo \
  --output-format csv
```

Parquet output is available with `--output-format parquet`.

## Output

Each invocation creates a new immutable directory:

```text
data/portfolio-demo/pdm-<24-hex>/
```

It contains:

- deterministic demo features;
- deterministic target-valid forecast weather;
- baseline predictions and metrics;
- paired weather-comparison predictions and metrics;
- demand/weather overview, hourly profile, temperature-band profile, and peak events;
- a descriptive Markdown report; and
- `portfolio_demo_manifest.json`.

The manifest records every artifact's role, relative path, content type, size, row count, and SHA-256 hash. It also records the exact horizons and model identities and fixes these safety statements:

```text
credential_free=true
live_source_calls_performed=false
fabric_operations_performed=false
schedule_activation_performed=false
model_promotion_performed=false
external_publication_performed=false
```

The complete staged directory is renamed into place only after the manifest and every artifact have been verified.

## Contract

- `data-contracts/portfolio_demo_manifest_schema.json`
- `forecasting/portfolio_demo.py`
- `forecasting/run_portfolio_demo.py`

## Boundary

The demo is a deterministic product walkthrough, not a live-source or production run. It performs no OpenWeather or NGED request, reads no credential, changes no Fabric workspace, creates no schedule, promotes no model, and publishes nothing externally.

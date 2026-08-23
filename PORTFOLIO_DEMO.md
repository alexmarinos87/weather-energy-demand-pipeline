# One-command local portfolio demo

## Purpose

The repository has many independently testable components. The portfolio demo assembles the most visible local product path into one credential-free command without weakening their individual contracts.

It now proves the same product flow independently for all four contracted NGED licence areas:

```text
East Midlands / Nottingham
South Wales / Cardiff
South West / Bristol
West Midlands / Birmingham
```

The cities remain representative project weather proxies, not official NGED mappings or complete descriptions of weather across a licence area.

The command runs:

```text
four deterministic, spatially isolated gold-like feature groups
        ↓
fixed 30/60-minute persistence and ridge baselines per group
        ↓
deterministic target-valid weather evidence per area/city
        ↓
paired observed-weather vs target-weather ridge comparison
        ↓
descriptive demand/weather analytics for every area
        ↓
hash-verified immutable artifact manifest and source bindings
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

- `demo_features` with one unique resource/city/timestamp group for every contracted area;
- `demo_source_area_summary` with counts, ranges, and descriptive demand/weather values for each area;
- deterministic target-valid forecast weather;
- baseline predictions and metrics;
- paired weather-comparison predictions and metrics;
- demand/weather overview, hourly profile, temperature-band profile, and peak events;
- a descriptive Markdown report with one section per area; and
- `portfolio_demo_manifest.json`.

The manifest records:

- the source-area contract version;
- exactly four source bindings with area, NGED resource ID, and city;
- every artifact's role, relative path, content type, size, row count, and SHA-256 hash;
- the exact demand horizons and model identities; and
- explicit no-side-effect flags.

Verification reopens `demo_features` and `demo_source_area_summary` and rejects:

- a missing, duplicated, or unexpected source area;
- resource/city identities that differ from the manifest;
- duplicate group/timestamp identities;
- invalid per-area observation counts; or
- any artifact size, hash, or row-count change.

The fixed safety statements remain:

```text
credential_free=true
live_source_calls_performed=false
fabric_operations_performed=false
schedule_activation_performed=false
model_promotion_performed=false
external_publication_performed=false
```

The complete staged directory is renamed into place only after the manifest, spatial bindings, and every artifact have been verified.

## Contract

- `data-contracts/portfolio_demo_manifest_schema.json`
- `forecasting/demo.py`
- `forecasting/portfolio_demo.py`
- `forecasting/run_portfolio_demo.py`

The current manifest version is `portfolio-demo-manifest-v2`.

## Boundary

The demo is a deterministic product walkthrough, not a live-source or production run. It performs no OpenWeather or NGED request, reads no credential, changes no Fabric workspace, creates no schedule, promotes no model, and publishes nothing externally.

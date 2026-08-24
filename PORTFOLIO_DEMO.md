# One-command local portfolio demo

## Purpose

The portfolio demo assembles the repository's visible local product path into
one credential-free command while preserving every independently tested
contract.

It proves the same journey for all four contracted NGED licence areas:

```text
East Midlands / Nottingham
South Wales / Cardiff
South West / Bristol
West Midlands / Birmingham
```

The cities remain representative project weather proxies rather than official
NGED mappings or complete descriptions of weather across a licence area.

The command now runs:

```text
four deterministic, spatially isolated gold-like feature groups
        ↓
fixed 30/60-minute persistence and ridge baselines
        ↓
deterministic target-valid weather evidence
        ↓
paired observed-weather vs target-weather ridge comparison
        ↓
12-day, 30-minute seasonal feature history for every area
        ↓
exact elapsed-time previous-day and previous-week baselines
        ↓
paired UTC-calendar and UK-local-calendar ridge scorecard
        ↓
80/90/95% calibration-only intervals from UTC seasonal validation residuals
        ↓
descriptive analytics and retrospective interval coverage/width summaries
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

It contains the original product artifacts:

- `demo_features`;
- `demo_source_area_summary`;
- `demo_forecast_weather`;
- baseline predictions and metrics;
- paired target-weather predictions and metrics;
- descriptive overview, hourly profile, temperature-band profile, peaks, and
  Markdown report.

It also contains the independently validated seasonal layer:

- `seasonal_demo_features` at a fixed 30-minute cadence over 12 days;
- `seasonal_utc_predictions` and `seasonal_utc_metrics`;
- `seasonal_uk_local_predictions` and `seasonal_uk_local_metrics`;
- `model_family_scorecard`;
- `model_family_pairwise_metrics`; and
- `model_family_summary.md`.

The elapsed-time seasonal models are:

```text
persistence_current_value
seasonal_previous_day
seasonal_previous_week
ridge_weather_lag
```

The five-model scorecard contains:

```text
persistence_current_value
seasonal_previous_day
seasonal_previous_week
ridge_weather_lag_utc
ridge_weather_lag_uk_local
```

UTC and UK-local runs must use identical target identities, actual demand,
training boundaries, training counts, persistence predictions, and seasonal
predictions. Only the ridge prediction may differ between calendar feature
contracts.

## Calibration-only interval evidence

The portfolio calibrates intervals from the UTC seasonal run because that run is
the established control point-prediction contract. For every source area,
resource, city, 30/60-minute horizon, and four-model identity:

1. find the first test feature timestamp;
2. retain only validation residuals whose target labels were available before
   that timestamp;
3. require a minimum calibration history of 24 rows;
4. calculate absolute residuals;
5. use the finite-sample rank `ceil((n + 1) * coverage)` clipped to `[1, n]`;
6. apply one symmetric radius to the test predictions; and
7. retain empirical test coverage and interval-width evidence.

The nominal coverage levels are:

```text
0.80
0.90
0.95
```

New artifacts are:

```text
prediction_intervals
prediction_interval_metrics
interval_coverage_summary
prediction_interval_summary.md
```

The area/horizon summary retains four models, rows per model, mean/minimum/
maximum empirical coverage, mean/median width, minimum calibration history, and
maximum calibration radius.

The key causal boundary is:

```text
calibration_label_available_through_utc
    <
evaluation_feature_start_utc
```

Test labels never choose the radius. Empirical coverage is retrospective
evidence and is not an unconditional future guarantee under distribution
shift.

## Manifest contract

`portfolio_demo_manifest.json` records:

- the source-area contract version;
- exactly four source bindings with area, NGED resource ID, and city;
- 30- and 60-minute demand horizons;
- baseline, target-weather, seasonal, scorecard, and interval model identities;
- UTC and UK-local feature-contract versions;
- previous-day and previous-week elapsed periods (`1440` and `10080` minutes);
- the 30-minute seasonal source cadence and 12-day retained demo history;
- interval levels, minimum calibration rows, interval contract, and UTC point
  source contract;
- every artifact's role, relative path, content type, size, row count, and
  SHA-256 hash; and
- explicit no-side-effect flags.

The current manifest version is:

```text
portfolio-demo-manifest-v4
```

The manifest binds 24 artifacts.

Verification reopens every tabular artifact and rejects:

- a missing, duplicated, or unexpected source area;
- resource/city identities that differ from the manifest;
- duplicate group/timestamp identities;
- missing 30- or 60-minute horizons;
- missing seasonal, scorecard, or interval models;
- UTC evidence labelled as UK-local or vice versa;
- seasonal reference timestamps after feature time;
- non-zero elapsed reference offsets in the deterministic demo;
- scorecard models that do not share one paired target digest and observation
  count;
- calibration labels at or after an interval evaluation feature timestamp;
- incorrect finite-sample ranks;
- invalid bounds, coverage flags, levels, or calibration counts; or
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

The complete staged directory is renamed into place only after the manifest,
spatial bindings, seasonal evidence, scorecard evidence, interval evidence, and
every artifact have been verified.

## Interpretation boundary

The scorecard and interval summaries keep every result partitioned by source
area and horizon. Lower retained error is comparison evidence only; it is not
model approval or promotion. UTC remains the target and ordering identity.
Europe/London fields are derived features that retain explicit
feature-contract identity.

Previous-day and previous-week matches use target-minus-period elapsed UTC time,
not fixed row offsets.

No radius, model, schedule, registry, or promotion state changes as a result of
the interval evidence.

## Contracts

- `data-contracts/portfolio_demo_manifest_schema.json`
- `forecasting/demo.py`
- `forecasting/portfolio_demo.py`
- `forecasting/portfolio_seasonal.py`
- `forecasting/portfolio_intervals.py`
- `forecasting/run_portfolio_demo.py`

## Boundary

The demo is a deterministic product walkthrough, not a live-source or
production run. It performs no OpenWeather or NGED request, reads no credential,
changes no Fabric workspace, creates no schedule, promotes no model, and
publishes nothing externally.

Interval coverage/width monitoring across repeated retained runs remains a
separate reviewed increment.

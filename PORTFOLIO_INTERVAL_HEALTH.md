# Repeated portfolio interval-health demonstration

## Purpose

This credential-free extension demonstrates the advisory prediction-interval
monitor over repeated retained histories for all four contracted source areas.
It creates deterministic healthy, warning, and failed review scenarios and
binds the input history, row-level checks, summaries, and operator report into
one immutable manifest.

The scenarios are synthetic product evidence. They are not live alerts and they
do not authorise recalibration, model changes, schedule changes, promotion, or
remediation.

## Scope

Every retained monitoring slice preserves:

```text
source_area
resource_id
city
requested_horizon_minutes
model_name
feature_contract_version
target_coverage_level
interval_contract_version
```

The demonstration covers:

- East Midlands, South Wales, South West, and West Midlands;
- 30- and 60-minute demand horizons;
- persistence, previous-day, previous-week, and ridge point models;
- 80%, 90%, and 95% interval levels; and
- nine interval metric runs per scenario.

Each run contains the complete 96-slice portfolio:

```text
4 areas × 2 horizons × 4 models × 3 coverage levels
```

## Deterministic scenarios

### Healthy

Recent and reference windows retain stable interval width, adequate calibration
history, fresh evidence, and empirical coverage within the configured
shortfall. Every emitted check passes.

### Warning

Hard requirements still pass, but recent average interval width is 35% higher
than the reference window. The outcome is `warning`, providing an operator
triage path without fabricating a hard failure.

### Failed

Recent interval evidence has fewer than 24 calibration observations and
empirical coverage ten percentage points below nominal. Error-severity checks
fail and the outcome is `failed`.

## Run locally

```bash
python3 -m forecasting.run_portfolio_interval_health_demo \
  --output-root data/portfolio-interval-health \
  --output-format csv
```

Parquet output is also supported:

```bash
python3 -m forecasting.run_portfolio_interval_health_demo \
  --output-root data/portfolio-interval-health \
  --output-format parquet
```

A deterministic run identity and timestamp may be supplied for reproducible
review:

```bash
python3 -m forecasting.run_portfolio_interval_health_demo \
  --run-id pih-222222222222222222222222 \
  --run-timestamp 2026-01-20T00:00:00Z
```

## Outputs

Each immutable run directory contains:

```text
repeated_interval_metric_history.<csv|parquet>
prediction_interval_health_checks.<csv|parquet>
prediction_interval_health_summary.<csv|parquet>
interval_health_operator_report.md
portfolio_interval_health_manifest.json
```

The manifest is versioned as:

```text
portfolio-interval-health-manifest-v1
```

It records source bindings, scenarios, expected statuses, run counts, model and
horizon contracts, coverage levels, monitoring policy versions, artifact hashes,
row counts, and no-side-effect flags.

## Verification

`verify_portfolio_interval_health_manifest` reopens every artifact and verifies:

- artifact path, size, SHA-256, and row count;
- all four source-area/resource/city identities;
- complete 30/60-minute, four-model, and 80/90/95% slices;
- exactly nine complete interval runs per scenario;
- retained healthy, warning, and failed outcomes;
- no-automatic-action fields on every summary;
- reproducibility of monitor status and check identities from the retained
  history; and
- an operator report containing every area, every scenario, and the human
  authority boundary.

The versioned manifest schema is:

```text
data-contracts/portfolio_interval_health_manifest_schema.json
```

## Authority boundary

The demonstration records all of the following as false:

```text
live_source_calls_performed
fabric_operations_performed
schedule_activation_performed
automatic_remediation_performed
automatic_recalibration_performed
automatic_model_change_performed
automatic_promotion_performed
alert_delivery_performed
external_publication_performed
```

Empirical coverage remains retrospective evidence and is not an unconditional
future guarantee under distribution shift.

# Interval-health thin-client reporting

## Purpose

This layer reads only the retained trend datasets and renders
`interval-health-trend-v1` evidence into analyst-facing tables, Markdown, HTML,
and a thin Jupyter client.

The report does not recalculate monitor status. It does not reread raw interval
metrics, construct prediction intervals, change a calibration radius, or apply
an independent threshold policy. The retained trend datasets remain the
canonical reporting input.

## Inputs

```text
interval_health_run_trends_<trend-run-id>
interval_health_slice_trends_<trend-run-id>
```

Both inputs must bind to exactly one shared `trend_run_id`, retain the same
scenario and slice identities, and use:

```text
trend_contract_version=interval-health-trend-v1
```

The report rejects duplicate slice identities, inconsistent retained statuses,
missing source-area dimensions, or mismatched run/slice trend cohorts.

## Report datasets

### Scenario overview

`interval_health_report_overview` contains one row per retained scenario with:

```text
monitor status
source-area, horizon, model, coverage, and slice counts
attention slice count and percentage
failed error and warning counts
maximum latest coverage shortfall
maximum coverage drop
maximum interval-width increase
maximum calibration-history decline
minimum recent calibration history
reference-history coverage
```

### Area and horizon triage

`interval_health_report_area_horizon` retains:

```text
scenario
source_area
resource_id
city
requested_horizon_minutes
```

It never collapses the four contracted source areas into one anonymous
portfolio series.

### Attention queue

`interval_health_report_attention_queue` retains exact:

```text
area / resource / city / horizon / model /
feature contract / coverage / interval contract
```

identity for warning and failed slices. Ordering is presentation-only:
`failed` rows precede `warning` rows. The queue does not create a remediation
instruction.

## Markdown and HTML

Both reports render the same retained tables and state explicitly that:

- empirical coverage is retrospective evidence rather than an unconditional
  future guarantee;
- monitor status is retained, not recalculated;
- automatic recalibration, model changes, schedule changes, promotion,
  remediation, alert delivery, deployment, and publication are not performed.

## Run locally

```bash
python3 -m forecasting.run_interval_health_report \
  --run-trends \
    data/interval-health-trends/interval_health_run_trends_<run>.parquet \
  --slice-trends \
    data/interval-health-trends/interval_health_slice_trends_<run>.parquet \
  --output-dir data/interval-health-report \
  --output-format parquet
```

For deterministic review:

```bash
python3 -m forecasting.run_interval_health_report \
  --run-trends interval_health_run_trends_iht-111111111111111111111111.csv \
  --slice-trends interval_health_slice_trends_iht-111111111111111111111111.csv \
  --report-run-id ihr-222222222222222222222222 \
  --report-run-timestamp 2026-01-20T00:00:00Z \
  --output-dir data/interval-health-report \
  --output-format csv
```

Outputs are immutable and report-run-qualified:

```text
interval_health_report_overview_<report-run-id>
interval_health_report_area_horizon_<report-run-id>
interval_health_report_attention_queue_<report-run-id>
interval_health_report_<report-run-id>.md
interval_health_report_<report-run-id>.html
```

Existing outputs are not overwritten.

## Thin notebook

`notebooks/interval_health_trends.ipynb` is intentionally a thin client. It:

1. loads the two retained trend datasets;
2. calls `build_interval_health_report`;
3. displays the canonical report tables and Markdown.

The notebook does not implement windowing, drift calculations, status
precedence, or threshold checks.

## Versioned contracts

- `data-contracts/interval_health_report_overview_schema.json`
- `data-contracts/interval_health_report_area_horizon_schema.json`
- `data-contracts/interval_health_report_attention_schema.json`

All retained report rows use:

```text
report_contract_version=interval-health-report-v1
```

## Authority boundary

```text
automatic_remediation_performed=false
automatic_recalibration_performed=false
automatic_model_change_performed=false
automatic_schedule_change_performed=false
automatic_promotion_performed=false
alert_delivery_performed=false
deployment_performed=false
external_publication_performed=false
```

This is a reporting surface only. Human reviewers decide whether any separate
investigation or change is warranted.

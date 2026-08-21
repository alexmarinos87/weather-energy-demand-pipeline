# Fabric monitoring runbook

## Daily checks

- Review the latest Data Factory pipeline status.
- Query `dq_run_results` for errors and warnings.
- Confirm expected `source_area` values in silver and gold.
- Confirm no cross-area or future-weather match failures.
- Confirm no forecast training-boundary, time-horizon, tolerance, or target-coverage failures.
- Compare validation and test MAE/RMSE by `requested_horizon_minutes` for persistence and ridge.
- Review actual target delay and matched/eligible coverage by source area.
- Review unmatched-weather and freshness warnings.
- Confirm complete-snapshot pagination metadata on recent energy raw files.

## Useful SQL

```sql
SELECT TOP (100)
    run_timestamp_utc,
    check_name,
    severity,
    failed_rows,
    status
FROM dbo.dq_run_results
ORDER BY run_timestamp_utc DESC, severity, check_name;
```

```sql
SELECT TOP (100)
    run_timestamp_utc,
    source_area,
    requested_horizon_minutes,
    target_tolerance_minutes,
    split,
    model_name,
    observation_count,
    eligible_target_count,
    matched_target_count,
    target_coverage_pct,
    minimum_target_coverage_pct,
    horizon_minutes_avg,
    target_delay_minutes_avg,
    target_delay_minutes_max,
    mae_mw,
    rmse_mw,
    mape_pct,
    bias_mw,
    trained_through_utc,
    evaluation_feature_start_utc,
    evaluation_feature_end_utc,
    evaluation_start_utc,
    evaluation_end_utc
FROM dbo.forecast_baseline_metrics
ORDER BY run_timestamp_utc DESC, source_area, requested_horizon_minutes, split, model_name;
```

```sql
SELECT TOP (100)
    source_area,
    feature_timestamp_utc,
    event_timestamp_utc AS target_timestamp_utc,
    requested_horizon_minutes,
    target_tolerance_minutes,
    horizon_steps,
    horizon_minutes,
    target_delay_minutes,
    current_demand_mw,
    actual_demand_mw,
    predicted_demand_mw,
    trained_through_utc
FROM dbo.forecast_baseline_predictions
ORDER BY run_timestamp_utc DESC, source_area, requested_horizon_minutes, event_timestamp_utc;
```

## Triage

- Source-binding failures mean `SOURCE_AREA`, `WEATHER_CITY`, and the NGED resource disagree.
- Ingestion failures usually indicate credentials, quota, source availability, contract drift, or incomplete CKAN pagination.
- Unscoped-source warnings indicate legacy raw files without `_pipeline_metadata`; retain them for lineage, but do not use them in scoped gold features.
- Unsupported-horizon, delay-tolerance, low-coverage, or label-availability failures are blocking correctness defects.
- Zero eligible targets usually mean retained history is shorter than the configured horizon for a source group.
- Low matched/eligible coverage usually means source gaps exceeded `TARGET_TOLERANCE_MINUTES`; investigate source cadence before relaxing the control.
- Insufficient-history failures may occur after target matching and overlap purging; do not bypass them with random splitting or overlapping labels.
- A ridge model underperforming persistence is not a pipeline failure, but it is evidence not to promote the model.

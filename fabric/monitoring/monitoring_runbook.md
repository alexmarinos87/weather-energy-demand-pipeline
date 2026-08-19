# Fabric monitoring runbook

## Daily checks

- Review the latest Data Factory pipeline status.
- Query `dq_run_results` for errors and warnings.
- Confirm expected `source_area` values in silver and gold.
- Confirm no cross-area or future-weather match failures.
- Confirm no forecast training-boundary failures.
- Compare validation and test MAE/RMSE for persistence and ridge baselines.
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
    split,
    model_name,
    observation_count,
    mae_mw,
    rmse_mw,
    mape_pct,
    bias_mw,
    trained_through_utc,
    evaluation_start_utc,
    evaluation_end_utc
FROM dbo.forecast_baseline_metrics
ORDER BY run_timestamp_utc DESC, source_area, split, model_name;
```

## Triage

- Source-binding failures mean `SOURCE_AREA`, `WEATHER_CITY`, and the NGED resource disagree.
- Ingestion failures usually indicate credentials, quota, source availability, contract drift, or incomplete CKAN pagination.
- Unscoped-source warnings indicate legacy raw files without `_pipeline_metadata`; retain them for lineage, but do not use them in scoped gold features.
- Future-weather, cross-area, target-window, or training-boundary failures are blocking correctness defects.
- Insufficient-history failures mean a group cannot support the configured chronological split; do not bypass them with random splitting.
- A ridge model underperforming persistence is not a pipeline failure, but it is evidence not to promote the model.

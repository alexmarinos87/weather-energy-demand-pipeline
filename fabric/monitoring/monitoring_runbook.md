# Fabric monitoring runbook

## Daily checks

- Review the latest Data Factory pipeline status.
- Query `dq_run_results` for errors and warnings.
- Confirm expected `source_area` values in silver and gold.
- Confirm no cross-area or future-weather match failures.
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
SELECT
    source_area,
    MAX(event_timestamp_utc) AS latest_feature_timestamp_utc,
    COUNT_BIG(*) AS feature_rows
FROM dbo.gold_feature_engineering
GROUP BY source_area;
```

## Triage

- Source-binding failures mean `SOURCE_AREA`, `WEATHER_CITY`, and the NGED resource disagree.
- Ingestion failures usually indicate credentials, quota, source availability, contract drift, or incomplete CKAN pagination.
- Unscoped-source warnings indicate legacy raw files without `_pipeline_metadata`; retain them for lineage, but do not use them in scoped gold features.
- Future-weather or cross-area failures are blocking correctness defects.
- Unmatched-weather warnings usually indicate source timing gaps or a paused weather feed.

# Fabric Monitoring Runbook

## Daily Checks

- Review the latest Fabric Data Factory pipeline run status.
- Query `dq_run_results` for failed checks.
- Confirm `gold_feature_engineering` contains fresh rows for the expected energy resource.
- Review freshness warnings for silver and gold tables.
- Confirm source API failures did not repeat across retries.

## Useful SQL

```sql
SELECT TOP (50)
    run_timestamp_utc,
    check_name,
    severity,
    failed_rows,
    status
FROM dbo.dq_run_results
ORDER BY run_timestamp_utc DESC, check_name;
```

```sql
SELECT
    MAX(event_timestamp_utc) AS latest_energy_timestamp_utc,
    COUNT_BIG(*) AS feature_rows
FROM dbo.gold_feature_engineering;
```

## Triage

- Ingestion failures usually mean API credentials, quota, or source availability changed.
- Silver failures usually mean raw payload shape changed and the contract gate needs review.
- Gold failures usually mean required silver fields are missing or timestamp alignment has drifted.
- Freshness warnings usually mean the source API is delayed, the pipeline schedule paused, or no new records matched the expected resource.
- Data quality failures should be resolved before refreshing downstream reports.

-- Microsoft Fabric SQL analytics endpoint data quality checks.
-- These checks return a failed row count per rule. The Fabric notebook
-- fabric/notebooks/04_data_quality_checks.py runs equivalent checks and fails
-- the orchestrated pipeline when a required rule fails.

SELECT
    'silver_weather_required_fields' AS check_name,
    COUNT_BIG(*) AS failed_rows
FROM dbo.silver_weather
WHERE event_timestamp_utc IS NULL
   OR city IS NULL
   OR temperature_c IS NULL
   OR humidity_pct IS NULL

UNION ALL

SELECT
    'silver_energy_required_fields' AS check_name,
    COUNT_BIG(*) AS failed_rows
FROM dbo.silver_energy
WHERE event_timestamp_utc IS NULL
   OR resource_id IS NULL
   OR demand_mw IS NULL

UNION ALL

SELECT
    'silver_weather_duplicates' AS check_name,
    COUNT_BIG(*) AS failed_rows
FROM (
    SELECT city, event_timestamp_utc
    FROM dbo.silver_weather
    GROUP BY city, event_timestamp_utc
    HAVING COUNT_BIG(*) > 1
) duplicates

UNION ALL

SELECT
    'silver_energy_duplicates' AS check_name,
    COUNT_BIG(*) AS failed_rows
FROM (
    SELECT resource_id, source_record_id, event_timestamp_utc
    FROM dbo.silver_energy
    GROUP BY resource_id, source_record_id, event_timestamp_utc
    HAVING COUNT_BIG(*) > 1
) duplicates

UNION ALL

SELECT
    'gold_feature_required_fields' AS check_name,
    COUNT_BIG(*) AS failed_rows
FROM dbo.gold_feature_engineering
WHERE event_timestamp_utc IS NULL
   OR city IS NULL
   OR temperature IS NULL
   OR humidity IS NULL
   OR demand_mw IS NULL

UNION ALL

SELECT
    'weather_match_outside_expected_window' AS check_name,
    COUNT_BIG(*) AS failed_rows
FROM dbo.gold_weather_demand_join
WHERE weather_event_timestamp_utc IS NOT NULL
  AND ABS(weather_time_delta_minutes) > 360;

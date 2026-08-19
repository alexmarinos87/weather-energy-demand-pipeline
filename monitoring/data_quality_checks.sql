-- Microsoft Fabric SQL analytics endpoint data quality checks.
-- The notebook equivalent records severity and fails the pipeline for errors.

SELECT 'silver_weather_required_fields' AS check_name, COUNT_BIG(*) AS failed_rows
FROM dbo.silver_weather
WHERE event_timestamp_utc IS NULL
   OR city IS NULL
   OR temperature_c IS NULL
   OR humidity_pct IS NULL

UNION ALL
SELECT 'silver_energy_required_fields', COUNT_BIG(*)
FROM dbo.silver_energy
WHERE event_timestamp_utc IS NULL
   OR resource_id IS NULL
   OR demand_mw IS NULL

UNION ALL
SELECT 'silver_weather_unscoped_source_area', COUNT_BIG(*)
FROM dbo.silver_weather
WHERE source_area IS NULL

UNION ALL
SELECT 'silver_energy_unscoped_source_area', COUNT_BIG(*)
FROM dbo.silver_energy
WHERE source_area IS NULL

UNION ALL
SELECT 'silver_weather_duplicates', COUNT_BIG(*)
FROM (
    SELECT source_area, city, event_timestamp_utc
    FROM dbo.silver_weather
    GROUP BY source_area, city, event_timestamp_utc
    HAVING COUNT_BIG(*) > 1
) duplicates

UNION ALL
SELECT 'silver_energy_duplicates', COUNT_BIG(*)
FROM (
    SELECT source_area, resource_id, source_record_id, event_timestamp_utc
    FROM dbo.silver_energy
    GROUP BY source_area, resource_id, source_record_id, event_timestamp_utc
    HAVING COUNT_BIG(*) > 1
) duplicates

UNION ALL
SELECT 'gold_weather_cross_area_match', COUNT_BIG(*)
FROM dbo.gold_weather_demand_join
WHERE weather_source_area IS NOT NULL
  AND weather_source_area <> source_area

UNION ALL
SELECT 'gold_weather_future_match', COUNT_BIG(*)
FROM dbo.gold_weather_demand_join
WHERE weather_event_timestamp_utc > event_timestamp_utc
   OR weather_age_minutes < 0

UNION ALL
SELECT 'gold_weather_unmatched', COUNT_BIG(*)
FROM dbo.gold_weather_demand_join
WHERE source_area IS NOT NULL
  AND weather_event_timestamp_utc IS NULL

UNION ALL
SELECT 'gold_feature_required_fields', COUNT_BIG(*)
FROM dbo.gold_feature_engineering
WHERE event_timestamp_utc IS NULL
   OR source_area IS NULL
   OR city IS NULL
   OR temperature IS NULL
   OR humidity IS NULL
   OR demand_mw IS NULL

UNION ALL
SELECT 'weather_match_outside_expected_window', COUNT_BIG(*)
FROM dbo.gold_weather_demand_join
WHERE weather_event_timestamp_utc IS NOT NULL
  AND weather_age_minutes > 360;

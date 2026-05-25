-- Gold Step 1: join weather signals onto each energy observation.
-- Engine target: Microsoft Fabric Spark SQL.
-- Expected Lakehouse tables:
--   silver_energy
--   silver_weather

CREATE OR REPLACE TABLE gold_weather_demand_join
USING DELTA
AS
WITH candidate_pairs AS (
    SELECT
        e.resource_id,
        e.source_record_id,
        e.event_timestamp_utc,
        e.event_date_utc,
        e.demand_mw,
        e.generation_mw,
        e.import_mw,
        e.solar_mw,
        e.wind_mw,
        e.stor_mw,
        e.other_mw,
        e.ingestion_timestamp_utc AS energy_ingestion_timestamp_utc,
        w.city,
        w.country_code,
        w.event_timestamp_utc AS weather_event_timestamp_utc,
        w.temperature_c,
        w.feels_like_c,
        w.humidity_pct,
        w.pressure_hpa,
        w.cloud_cover_pct,
        w.wind_speed_mps,
        w.weather_main,
        w.weather_description,
        w.ingestion_timestamp_utc AS weather_ingestion_timestamp_utc,
        CAST((unix_timestamp(e.event_timestamp_utc) - unix_timestamp(w.event_timestamp_utc)) / 60 AS INT) AS weather_age_minutes,
        CAST(ABS(unix_timestamp(e.event_timestamp_utc) - unix_timestamp(w.event_timestamp_utc)) / 60 AS INT) AS weather_time_delta_minutes,
        ROW_NUMBER() OVER (
            PARTITION BY e.resource_id, e.source_record_id, e.event_timestamp_utc
            ORDER BY
                ABS(unix_timestamp(e.event_timestamp_utc) - unix_timestamp(w.event_timestamp_utc)),
                w.event_timestamp_utc DESC
        ) AS match_rank
    FROM silver_energy e
    LEFT JOIN silver_weather w
        ON w.event_timestamp_utc BETWEEN e.event_timestamp_utc - INTERVAL 6 HOURS
                                     AND e.event_timestamp_utc + INTERVAL 1 HOUR
)
SELECT
    resource_id,
    source_record_id,
    event_timestamp_utc,
    event_date_utc,
    city,
    country_code,
    demand_mw,
    generation_mw,
    import_mw,
    solar_mw,
    wind_mw,
    stor_mw,
    other_mw,
    weather_event_timestamp_utc,
    weather_age_minutes,
    weather_time_delta_minutes,
    temperature_c,
    feels_like_c,
    humidity_pct,
    pressure_hpa,
    cloud_cover_pct,
    wind_speed_mps,
    weather_main,
    weather_description,
    energy_ingestion_timestamp_utc,
    weather_ingestion_timestamp_utc
FROM candidate_pairs
WHERE match_rank = 1;

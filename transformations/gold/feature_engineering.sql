-- Gold Step 2: model-ready features from the weather-demand join.
-- Engine target: Microsoft Fabric Spark SQL.
-- This table is contract-aligned with required fields:
--   event_timestamp_utc, city, temperature, humidity, demand_mw

CREATE OR REPLACE TABLE gold_feature_engineering
USING DELTA
AS
WITH base AS (
    SELECT
        event_timestamp_utc,
        event_date_utc,
        city,
        country_code,
        resource_id,
        demand_mw,
        generation_mw,
        import_mw,
        solar_mw,
        wind_mw,
        stor_mw,
        other_mw,
        COALESCE(temperature_c, feels_like_c) AS temperature,
        humidity_pct AS humidity,
        pressure_hpa,
        cloud_cover_pct,
        wind_speed_mps,
        weather_main,
        weather_description,
        weather_age_minutes
    FROM gold_weather_demand_join
    WHERE demand_mw IS NOT NULL
      AND city IS NOT NULL
      AND COALESCE(temperature_c, feels_like_c) IS NOT NULL
      AND humidity_pct IS NOT NULL
),
features AS (
    SELECT
        event_timestamp_utc,
        event_date_utc,
        city,
        country_code,
        resource_id,
        temperature,
        humidity,
        demand_mw,
        generation_mw,
        import_mw,
        solar_mw,
        wind_mw,
        stor_mw,
        other_mw,
        pressure_hpa,
        cloud_cover_pct,
        wind_speed_mps,
        weather_main,
        weather_description,
        weather_age_minutes,
        HOUR(event_timestamp_utc) AS hour_of_day_utc,
        DAYOFWEEK(event_timestamp_utc) AS day_of_week_utc,
        CASE
            WHEN DAYOFWEEK(event_timestamp_utc) IN (1, 7) THEN 1
            ELSE 0
        END AS is_weekend_utc,
        temperature * temperature AS temperature_sq,
        LAG(demand_mw, 1) OVER (
            PARTITION BY resource_id, city
            ORDER BY event_timestamp_utc
        ) AS demand_lag_1,
        LAG(temperature, 1) OVER (
            PARTITION BY resource_id, city
            ORDER BY event_timestamp_utc
        ) AS temperature_lag_1,
        AVG(demand_mw) OVER (
            PARTITION BY resource_id, city
            ORDER BY event_timestamp_utc
            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
        ) AS demand_rolling_mean_12,
        AVG(temperature) OVER (
            PARTITION BY resource_id, city
            ORDER BY event_timestamp_utc
            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
        ) AS temperature_rolling_mean_12
    FROM base
)
SELECT
    event_timestamp_utc,
    event_date_utc,
    city,
    country_code,
    resource_id,
    temperature,
    humidity,
    demand_mw,
    generation_mw,
    import_mw,
    solar_mw,
    wind_mw,
    stor_mw,
    other_mw,
    pressure_hpa,
    cloud_cover_pct,
    wind_speed_mps,
    weather_main,
    weather_description,
    weather_age_minutes,
    hour_of_day_utc,
    day_of_week_utc,
    is_weekend_utc,
    temperature_sq,
    demand_lag_1,
    temperature_lag_1,
    demand_mw - demand_lag_1 AS demand_delta_1,
    temperature - temperature_lag_1 AS temperature_delta_1,
    demand_rolling_mean_12,
    temperature_rolling_mean_12
FROM features;

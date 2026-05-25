-- Gold Step 3: analytical aggregates for monitoring and trend analysis.
-- Engine target: Microsoft Fabric Spark SQL.
-- Source: gold_feature_engineering

CREATE OR REPLACE TABLE gold_demand_aggregation
USING DELTA
AS
WITH base AS (
    SELECT
        event_timestamp_utc,
        DATE_TRUNC('hour', event_timestamp_utc) AS hour_bucket_utc,
        DATE_TRUNC('day', event_timestamp_utc) AS day_bucket_utc,
        city,
        country_code,
        resource_id,
        demand_mw,
        generation_mw,
        import_mw,
        solar_mw,
        wind_mw,
        temperature,
        humidity,
        wind_speed_mps,
        cloud_cover_pct,
        weather_main,
        CASE
            WHEN temperature < 15 THEN 15 - temperature
            ELSE 0
        END AS heating_degree_c,
        CASE
            WHEN temperature > 18 THEN temperature - 18
            ELSE 0
        END AS cooling_degree_c,
        CASE
            WHEN generation_mw > 0
                THEN (COALESCE(solar_mw, 0) + COALESCE(wind_mw, 0)) / generation_mw
            ELSE NULL
        END AS renewable_share
    FROM gold_feature_engineering
    WHERE city IS NOT NULL
      AND resource_id IS NOT NULL
      AND demand_mw IS NOT NULL
),
hourly AS (
    SELECT
        'hourly' AS aggregation_level,
        hour_bucket_utc AS bucket_start_utc,
        hour_bucket_utc + INTERVAL 1 HOUR AS bucket_end_utc,
        CAST(hour_bucket_utc AS DATE) AS event_date_utc,
        city,
        country_code,
        resource_id,
        COUNT(*) AS sample_count,
        AVG(demand_mw) AS demand_avg_mw,
        MIN(demand_mw) AS demand_min_mw,
        MAX(demand_mw) AS demand_max_mw,
        percentile_approx(demand_mw, 0.95) AS demand_p95_mw,
        stddev_samp(demand_mw) AS demand_stddev_mw,
        AVG(generation_mw) AS generation_avg_mw,
        AVG(import_mw) AS import_avg_mw,
        AVG(solar_mw) AS solar_avg_mw,
        AVG(wind_mw) AS wind_avg_mw,
        AVG(renewable_share) AS renewable_share_avg,
        AVG(temperature) AS temperature_avg_c,
        MIN(temperature) AS temperature_min_c,
        MAX(temperature) AS temperature_max_c,
        AVG(humidity) AS humidity_avg_pct,
        AVG(wind_speed_mps) AS wind_speed_avg_mps,
        AVG(cloud_cover_pct) AS cloud_cover_avg_pct,
        AVG(heating_degree_c) AS heating_degree_avg_c,
        AVG(cooling_degree_c) AS cooling_degree_avg_c,
        max_by(weather_main, event_timestamp_utc) AS latest_weather_main
    FROM base
    GROUP BY hour_bucket_utc, city, country_code, resource_id
),
daily AS (
    SELECT
        'daily' AS aggregation_level,
        day_bucket_utc AS bucket_start_utc,
        day_bucket_utc + INTERVAL 1 DAY AS bucket_end_utc,
        CAST(day_bucket_utc AS DATE) AS event_date_utc,
        city,
        country_code,
        resource_id,
        COUNT(*) AS sample_count,
        AVG(demand_mw) AS demand_avg_mw,
        MIN(demand_mw) AS demand_min_mw,
        MAX(demand_mw) AS demand_max_mw,
        percentile_approx(demand_mw, 0.95) AS demand_p95_mw,
        stddev_samp(demand_mw) AS demand_stddev_mw,
        AVG(generation_mw) AS generation_avg_mw,
        AVG(import_mw) AS import_avg_mw,
        AVG(solar_mw) AS solar_avg_mw,
        AVG(wind_mw) AS wind_avg_mw,
        AVG(renewable_share) AS renewable_share_avg,
        AVG(temperature) AS temperature_avg_c,
        MIN(temperature) AS temperature_min_c,
        MAX(temperature) AS temperature_max_c,
        AVG(humidity) AS humidity_avg_pct,
        AVG(wind_speed_mps) AS wind_speed_avg_mps,
        AVG(cloud_cover_pct) AS cloud_cover_avg_pct,
        AVG(heating_degree_c) AS heating_degree_avg_c,
        AVG(cooling_degree_c) AS cooling_degree_avg_c,
        max_by(weather_main, event_timestamp_utc) AS latest_weather_main
    FROM base
    GROUP BY day_bucket_utc, city, country_code, resource_id
)
SELECT * FROM hourly
UNION ALL
SELECT * FROM daily;

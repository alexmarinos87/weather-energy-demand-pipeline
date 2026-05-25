-- Microsoft Fabric SQL analytics endpoint views.
-- Run these in the SQL analytics endpoint for the Lakehouse after the Spark
-- notebooks have created silver_weather and silver_energy.

CREATE OR ALTER VIEW dbo.gold_weather_demand_join_v AS
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
        DATEDIFF(minute, w.event_timestamp_utc, e.event_timestamp_utc) AS weather_age_minutes,
        ABS(DATEDIFF(minute, w.event_timestamp_utc, e.event_timestamp_utc)) AS weather_time_delta_minutes,
        ROW_NUMBER() OVER (
            PARTITION BY e.resource_id, e.source_record_id, e.event_timestamp_utc
            ORDER BY
                ABS(DATEDIFF(minute, w.event_timestamp_utc, e.event_timestamp_utc)),
                w.event_timestamp_utc DESC
        ) AS match_rank
    FROM dbo.silver_energy AS e
    LEFT JOIN dbo.silver_weather AS w
        ON w.event_timestamp_utc BETWEEN DATEADD(hour, -6, e.event_timestamp_utc)
                                     AND DATEADD(hour, 1, e.event_timestamp_utc)
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
GO

CREATE OR ALTER VIEW dbo.gold_feature_engineering_v AS
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
    FROM dbo.gold_weather_demand_join_v
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
        DATEPART(hour, event_timestamp_utc) AS hour_of_day_utc,
        CASE
            WHEN DATEDIFF(day, '19000101', CAST(event_timestamp_utc AS date)) % 7 IN (5, 6) THEN 1
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
    DATEDIFF(day, '19000101', CAST(event_timestamp_utc AS date)) % 7 + 1 AS day_of_week_utc,
    is_weekend_utc,
    temperature_sq,
    demand_lag_1,
    temperature_lag_1,
    demand_mw - demand_lag_1 AS demand_delta_1,
    temperature - temperature_lag_1 AS temperature_delta_1,
    demand_rolling_mean_12,
    temperature_rolling_mean_12
FROM features;
GO

CREATE OR ALTER VIEW dbo.gold_demand_aggregation_v AS
WITH base AS (
    SELECT
        event_timestamp_utc,
        DATEADD(hour, DATEDIFF(hour, '19000101', event_timestamp_utc), '19000101') AS hour_bucket_utc,
        DATEADD(day, DATEDIFF(day, '19000101', event_timestamp_utc), '19000101') AS day_bucket_utc,
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
    FROM dbo.gold_feature_engineering_v
    WHERE city IS NOT NULL
      AND resource_id IS NOT NULL
      AND demand_mw IS NOT NULL
),
hourly_ranked AS (
    SELECT
        base.*,
        FIRST_VALUE(weather_main) OVER (
            PARTITION BY hour_bucket_utc, city, country_code, resource_id
            ORDER BY event_timestamp_utc DESC
        ) AS latest_weather_main
    FROM base
),
daily_ranked AS (
    SELECT
        base.*,
        FIRST_VALUE(weather_main) OVER (
            PARTITION BY day_bucket_utc, city, country_code, resource_id
            ORDER BY event_timestamp_utc DESC
        ) AS latest_weather_main
    FROM base
),
hourly AS (
    SELECT
        'hourly' AS aggregation_level,
        hour_bucket_utc AS bucket_start_utc,
        DATEADD(hour, 1, hour_bucket_utc) AS bucket_end_utc,
        CAST(hour_bucket_utc AS date) AS event_date_utc,
        city,
        country_code,
        resource_id,
        COUNT_BIG(*) AS sample_count,
        AVG(demand_mw) AS demand_avg_mw,
        MIN(demand_mw) AS demand_min_mw,
        MAX(demand_mw) AS demand_max_mw,
        CAST(NULL AS float) AS demand_p95_mw,
        STDEV(demand_mw) AS demand_stddev_mw,
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
        MAX(latest_weather_main) AS latest_weather_main
    FROM hourly_ranked
    GROUP BY hour_bucket_utc, city, country_code, resource_id
),
daily AS (
    SELECT
        'daily' AS aggregation_level,
        day_bucket_utc AS bucket_start_utc,
        DATEADD(day, 1, day_bucket_utc) AS bucket_end_utc,
        CAST(day_bucket_utc AS date) AS event_date_utc,
        city,
        country_code,
        resource_id,
        COUNT_BIG(*) AS sample_count,
        AVG(demand_mw) AS demand_avg_mw,
        MIN(demand_mw) AS demand_min_mw,
        MAX(demand_mw) AS demand_max_mw,
        CAST(NULL AS float) AS demand_p95_mw,
        STDEV(demand_mw) AS demand_stddev_mw,
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
        MAX(latest_weather_main) AS latest_weather_main
    FROM daily_ranked
    GROUP BY day_bucket_utc, city, country_code, resource_id
)
SELECT * FROM hourly
UNION ALL
SELECT * FROM daily;
GO

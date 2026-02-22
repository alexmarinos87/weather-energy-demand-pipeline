-- Gold Step 2: analytical aggregates for monitoring and trend analysis.
-- Engine target: Athena/Trino SQL.
-- Source: gold.feature_engineering

CREATE OR REPLACE VIEW gold.demand_aggregation AS
WITH base AS (
    SELECT
        event_timestamp_utc,
        date_trunc('hour', event_timestamp_utc) AS hour_bucket_utc,
        date_trunc('day', event_timestamp_utc) AS day_bucket_utc,
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
            WHEN generation_mw > 0 THEN (COALESCE(solar_mw, 0) + COALESCE(wind_mw, 0)) / generation_mw
            ELSE NULL
        END AS renewable_share
    FROM gold.feature_engineering
    WHERE city IS NOT NULL
      AND resource_id IS NOT NULL
      AND demand_mw IS NOT NULL
),
hourly AS (
    SELECT
        'hourly' AS aggregation_level,
        hour_bucket_utc AS bucket_start_utc,
        hour_bucket_utc + INTERVAL '1' HOUR AS bucket_end_utc,
        CAST(hour_bucket_utc AS date) AS event_date_utc,
        city,
        country_code,
        resource_id,
        count(*) AS sample_count,
        avg(demand_mw) AS demand_avg_mw,
        min(demand_mw) AS demand_min_mw,
        max(demand_mw) AS demand_max_mw,
        approx_percentile(demand_mw, 0.95) AS demand_p95_mw,
        stddev_samp(demand_mw) AS demand_stddev_mw,
        avg(generation_mw) AS generation_avg_mw,
        avg(import_mw) AS import_avg_mw,
        avg(solar_mw) AS solar_avg_mw,
        avg(wind_mw) AS wind_avg_mw,
        avg(renewable_share) AS renewable_share_avg,
        avg(temperature) AS temperature_avg_c,
        min(temperature) AS temperature_min_c,
        max(temperature) AS temperature_max_c,
        avg(humidity) AS humidity_avg_pct,
        avg(wind_speed_mps) AS wind_speed_avg_mps,
        avg(cloud_cover_pct) AS cloud_cover_avg_pct,
        avg(heating_degree_c) AS heating_degree_avg_c,
        avg(cooling_degree_c) AS cooling_degree_avg_c,
        max_by(weather_main, event_timestamp_utc) AS latest_weather_main
    FROM base
    GROUP BY
        hour_bucket_utc,
        city,
        country_code,
        resource_id
),
daily AS (
    SELECT
        'daily' AS aggregation_level,
        day_bucket_utc AS bucket_start_utc,
        day_bucket_utc + INTERVAL '1' DAY AS bucket_end_utc,
        CAST(day_bucket_utc AS date) AS event_date_utc,
        city,
        country_code,
        resource_id,
        count(*) AS sample_count,
        avg(demand_mw) AS demand_avg_mw,
        min(demand_mw) AS demand_min_mw,
        max(demand_mw) AS demand_max_mw,
        approx_percentile(demand_mw, 0.95) AS demand_p95_mw,
        stddev_samp(demand_mw) AS demand_stddev_mw,
        avg(generation_mw) AS generation_avg_mw,
        avg(import_mw) AS import_avg_mw,
        avg(solar_mw) AS solar_avg_mw,
        avg(wind_mw) AS wind_avg_mw,
        avg(renewable_share) AS renewable_share_avg,
        avg(temperature) AS temperature_avg_c,
        min(temperature) AS temperature_min_c,
        max(temperature) AS temperature_max_c,
        avg(humidity) AS humidity_avg_pct,
        avg(wind_speed_mps) AS wind_speed_avg_mps,
        avg(cloud_cover_pct) AS cloud_cover_avg_pct,
        avg(heating_degree_c) AS heating_degree_avg_c,
        avg(cooling_degree_c) AS cooling_degree_avg_c,
        max_by(weather_main, event_timestamp_utc) AS latest_weather_main
    FROM base
    GROUP BY
        day_bucket_utc,
        city,
        country_code,
        resource_id
)
SELECT
    aggregation_level,
    bucket_start_utc,
    bucket_end_utc,
    event_date_utc,
    city,
    country_code,
    resource_id,
    sample_count,
    demand_avg_mw,
    demand_min_mw,
    demand_max_mw,
    demand_p95_mw,
    demand_stddev_mw,
    generation_avg_mw,
    import_avg_mw,
    solar_avg_mw,
    wind_avg_mw,
    renewable_share_avg,
    temperature_avg_c,
    temperature_min_c,
    temperature_max_c,
    humidity_avg_pct,
    wind_speed_avg_mps,
    cloud_cover_avg_pct,
    heating_degree_avg_c,
    cooling_degree_avg_c,
    latest_weather_main
FROM hourly
UNION ALL
SELECT
    aggregation_level,
    bucket_start_utc,
    bucket_end_utc,
    event_date_utc,
    city,
    country_code,
    resource_id,
    sample_count,
    demand_avg_mw,
    demand_min_mw,
    demand_max_mw,
    demand_p95_mw,
    demand_stddev_mw,
    generation_avg_mw,
    import_avg_mw,
    solar_avg_mw,
    wind_avg_mw,
    renewable_share_avg,
    temperature_avg_c,
    temperature_min_c,
    temperature_max_c,
    humidity_avg_pct,
    wind_speed_avg_mps,
    cloud_cover_avg_pct,
    heating_degree_avg_c,
    cooling_degree_avg_c,
    latest_weather_main
FROM daily;

# Fabric notebook source: 03_build_gold_tables
#
# Rebuilds gold Delta tables from the canonical silver tables.


spark.conf.set("spark.sql.session.timeZone", "UTC")


spark.sql(
    """
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
    WHERE match_rank = 1
    """
)


spark.sql(
    """
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
    FROM features
    """
)


spark.sql(
    """
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
    SELECT * FROM daily
    """
)


for table_name in [
    "gold_weather_demand_join",
    "gold_feature_engineering",
    "gold_demand_aggregation",
]:
    row_count = spark.table(table_name).count()
    print({"table": table_name, "rows": row_count})

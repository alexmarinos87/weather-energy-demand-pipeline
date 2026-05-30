# Fabric notebook source: 04_data_quality_checks
#
# Runs required data quality checks and fails the pipeline when a required
# check has failed rows.

from datetime import datetime, timezone
from typing import Any


MAX_EXPECTED_DATA_LAG_HOURS = 3


def _get_parameter(name: str, default: Any) -> Any:
    return globals().get(name, default)


def _max_expected_data_lag_hours(value: int | None = None) -> int:
    lag_hours = int(
        value
        if value is not None
        else _get_parameter("MAX_EXPECTED_DATA_LAG_HOURS", MAX_EXPECTED_DATA_LAG_HOURS)
    )
    if lag_hours < 1:
        raise ValueError("MAX_EXPECTED_DATA_LAG_HOURS must be at least 1.")
    return lag_hours


def _freshness_sql(table_name: str, timestamp_column: str, lag_hours: int) -> str:
    return f"""
            SELECT CASE
                WHEN MAX({timestamp_column}) IS NULL THEN 1
                WHEN MAX({timestamp_column}) < CURRENT_TIMESTAMP() - INTERVAL {lag_hours} HOURS
                    THEN 1
                ELSE 0
            END AS failed_rows
            FROM {table_name}
        """


def build_checks(max_expected_data_lag_hours: int | None = None) -> list[dict[str, str]]:
    lag_hours = _max_expected_data_lag_hours(max_expected_data_lag_hours)
    return [
        {
            "check_name": "silver_weather_not_empty",
            "severity": "error",
            "sql": (
                "SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS failed_rows "
                "FROM silver_weather"
            ),
        },
        {
            "check_name": "silver_energy_not_empty",
            "severity": "error",
            "sql": (
                "SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS failed_rows "
                "FROM silver_energy"
            ),
        },
        {
            "check_name": "silver_weather_required_fields",
            "severity": "error",
            "sql": """
                SELECT COUNT(*) AS failed_rows
                FROM silver_weather
                WHERE event_timestamp_utc IS NULL
                   OR city IS NULL
                   OR temperature_c IS NULL
                   OR humidity_pct IS NULL
            """,
        },
        {
            "check_name": "silver_energy_required_fields",
            "severity": "error",
            "sql": """
                SELECT COUNT(*) AS failed_rows
                FROM silver_energy
                WHERE event_timestamp_utc IS NULL
                   OR resource_id IS NULL
                   OR demand_mw IS NULL
            """,
        },
        {
            "check_name": "silver_weather_duplicates",
            "severity": "error",
            "sql": """
                SELECT COUNT(*) AS failed_rows
                FROM (
                    SELECT city, event_timestamp_utc
                    FROM silver_weather
                    GROUP BY city, event_timestamp_utc
                    HAVING COUNT(*) > 1
                ) duplicates
            """,
        },
        {
            "check_name": "silver_energy_duplicates",
            "severity": "error",
            "sql": """
                SELECT COUNT(*) AS failed_rows
                FROM (
                    SELECT resource_id, source_record_id, event_timestamp_utc
                    FROM silver_energy
                    GROUP BY resource_id, source_record_id, event_timestamp_utc
                    HAVING COUNT(*) > 1
                ) duplicates
            """,
        },
        {
            "check_name": "silver_weather_freshness",
            "severity": "warn",
            "sql": _freshness_sql("silver_weather", "event_timestamp_utc", lag_hours),
        },
        {
            "check_name": "silver_energy_freshness",
            "severity": "warn",
            "sql": _freshness_sql("silver_energy", "event_timestamp_utc", lag_hours),
        },
        {
            "check_name": "gold_feature_required_fields",
            "severity": "error",
            "sql": """
                SELECT COUNT(*) AS failed_rows
                FROM gold_feature_engineering
                WHERE event_timestamp_utc IS NULL
                   OR city IS NULL
                   OR temperature IS NULL
                   OR humidity IS NULL
                   OR demand_mw IS NULL
            """,
        },
        {
            "check_name": "gold_feature_freshness",
            "severity": "warn",
            "sql": _freshness_sql(
                "gold_feature_engineering",
                "event_timestamp_utc",
                lag_hours,
            ),
        },
        {
            "check_name": "weather_match_outside_expected_window",
            "severity": "warn",
            "sql": """
                SELECT COUNT(*) AS failed_rows
                FROM gold_weather_demand_join
                WHERE weather_event_timestamp_utc IS NOT NULL
                  AND ABS(weather_time_delta_minutes) > 360
            """,
        },
    ]


def run_checks(spark_session) -> list[dict[str, Any]]:
    run_timestamp_utc = datetime.now(timezone.utc)
    results = []

    for check in build_checks():
        failed_rows = int(spark_session.sql(check["sql"]).collect()[0]["failed_rows"])
        status = "passed" if failed_rows == 0 else "failed"
        results.append(
            {
                "run_timestamp_utc": run_timestamp_utc,
                "check_name": check["check_name"],
                "severity": check["severity"],
                "failed_rows": failed_rows,
                "status": status,
            }
        )

    results_df = spark_session.createDataFrame(results)
    (
        results_df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable("dq_run_results")
    )

    results_df.orderBy("severity", "check_name").show(truncate=False)

    blocking_failures = [
        result for result in results
        if result["severity"] == "error" and result["failed_rows"] > 0
    ]

    if blocking_failures:
        failure_text = ", ".join(
            f"{result['check_name']}={result['failed_rows']}"
            for result in blocking_failures
        )
        raise ValueError(f"Data quality checks failed: {failure_text}")

    print({"status": "passed", "checks": len(results)})
    return results


if __name__ == "__main__":
    run_checks(spark)

# Fabric notebook source: 06c_target_weather_comparison_quality_checks
# Optional/manual quality gate for 05c_target_weather_model_comparison.

from datetime import datetime, timezone

from pyspark.sql import Window
from pyspark.sql import functions as F

PREDICTIONS_TABLE = "forecast_weather_comparison_predictions"
METRICS_TABLE = "forecast_weather_comparison_metrics"
RESULTS_TABLE = "dq_run_results"
BASELINE_MODEL = "ridge_weather_lag"
CANDIDATE_MODEL = "ridge_target_weather"
WEATHER_COMPARISON_CONTRACT_VERSION = "weather-model-comparison-v1"
FORECAST_WEATHER_CONTRACT_VERSION = "target-weather-v1"
SUPPORTED_HORIZONS = (30, 60)
SUPPORTED_LEAD_BUCKETS = ("00-06h", "06-12h", "12-24h", "24-48h", "48h+")


def _latest_run_id(spark_session) -> str:
    row = (
        spark_session.table(PREDICTIONS_TABLE)
        .orderBy(F.col("run_timestamp_utc").desc(), F.col("run_id").desc())
        .select("run_id")
        .first()
    )
    if row is None:
        raise ValueError("No Fabric target-weather comparison run is available.")
    return str(row["run_id"])


def _pair_failures(predictions) -> int:
    keys = [
        "source_area",
        "resource_id",
        "city",
        "requested_horizon_minutes",
        "split",
        "origin_fold",
        "feature_timestamp_utc",
        "event_timestamp_utc",
        "target_weather_provider",
        "target_weather_model",
    ]
    pairs = predictions.groupBy(*keys).agg(
        F.count(F.lit(1)).alias("row_count"),
        F.countDistinct("model_name").alias("model_count"),
        F.countDistinct("actual_demand_mw").alias("actual_count"),
        F.countDistinct("trained_through_utc").alias("boundary_count"),
    )
    return pairs.where(
        (F.col("row_count") != 2)
        | (F.col("model_count") != 2)
        | (F.col("actual_count") != 1)
        | (F.col("boundary_count") != 1)
    ).count()


def _rolling_sequence_failures(metrics) -> int:
    rolling = metrics.where(F.col("origin_count").isNotNull())
    if rolling.limit(1).count() == 0:
        return 0
    identity = [
        "source_area",
        "resource_id",
        "city",
        "requested_horizon_minutes",
        "model_name",
        "target_weather_provider",
        "target_weather_model",
    ]
    sequence = rolling.groupBy(*identity).agg(
        F.countDistinct("origin_fold").alias("fold_count"),
        F.min("origin_fold").alias("min_fold"),
        F.max("origin_fold").alias("max_fold"),
        F.min("origin_count").alias("min_origin_count"),
        F.max("origin_count").alias("max_origin_count"),
    )
    count_failures = sequence.where(
        (F.col("min_fold") != 1)
        | (F.col("max_fold") != F.col("max_origin_count"))
        | (F.col("fold_count") != F.col("max_origin_count"))
        | (F.col("min_origin_count") != F.col("max_origin_count"))
    ).count()
    order = Window.partitionBy(*identity).orderBy("origin_fold")
    ordered = (
        rolling.withColumn("previous_cutoff", F.lag("origin_cutoff_utc").over(order))
        .withColumn(
            "previous_training_count",
            F.lag("training_observation_count").over(order),
        )
    )
    order_failures = ordered.where(
        (
            F.col("previous_cutoff").isNotNull()
            & (F.col("origin_cutoff_utc") <= F.col("previous_cutoff"))
        )
        | (
            F.col("previous_training_count").isNotNull()
            & (
                F.col("training_observation_count")
                < F.col("previous_training_count")
            )
        )
        | (
            (F.col("origin_fold") < F.col("origin_count"))
            & (F.col("split") != "validation")
        )
        | (
            (F.col("origin_fold") == F.col("origin_count"))
            & (F.col("split") != "test")
        )
    ).count()
    return count_failures + order_failures


def run_checks(spark_session):
    run_id = _latest_run_id(spark_session)
    predictions = spark_session.table(PREDICTIONS_TABLE).where(F.col("run_id") == run_id)
    metrics = spark_session.table(METRICS_TABLE).where(F.col("run_id") == run_id)
    required_prediction_fields = [
        "source_area",
        "resource_id",
        "city",
        "feature_timestamp_utc",
        "event_timestamp_utc",
        "requested_horizon_minutes",
        "model_name",
        "weather_feature_mode",
        "actual_demand_mw",
        "predicted_demand_mw",
        "trained_through_utc",
        "target_weather_forecast_issued_at_utc",
        "target_weather_forecast_ingested_at_utc",
        "target_weather_forecast_valid_at_utc",
        "target_weather_temperature_c",
        "target_weather_humidity_pct",
        "target_weather_provider",
        "target_weather_model",
        "target_weather_issue_basis",
        "forecast_weather_coverage_pct",
        "minimum_forecast_weather_coverage_pct",
        "forecast_lead_time_bucket",
        "forecast_weather_contract_version",
        "weather_comparison_contract_version",
    ]
    null_condition = F.lit(False)
    for column in required_prediction_fields:
        null_condition = null_condition | F.col(column).isNull()
    checks = [
        {
            "check_name": "target_weather_comparison_predictions_not_empty",
            "failed_rows": int(predictions.limit(1).count() == 0),
        },
        {
            "check_name": "target_weather_comparison_metrics_not_empty",
            "failed_rows": int(metrics.limit(1).count() == 0),
        },
        {
            "check_name": "target_weather_comparison_required_fields",
            "failed_rows": predictions.where(null_condition).count(),
        },
        {
            "check_name": "target_weather_comparison_exact_pairs",
            "failed_rows": _pair_failures(predictions),
        },
        {
            "check_name": "target_weather_comparison_causal_boundaries",
            "failed_rows": predictions.where(
                (F.col("trained_through_utc") >= F.col("feature_timestamp_utc"))
                | (F.col("feature_timestamp_utc") >= F.col("event_timestamp_utc"))
                | (
                    F.col("target_weather_forecast_issued_at_utc")
                    > F.col("feature_timestamp_utc")
                )
                | (
                    F.col("target_weather_forecast_ingested_at_utc")
                    > F.col("feature_timestamp_utc")
                )
                | (
                    F.col("target_weather_forecast_valid_at_utc")
                    <= F.col("feature_timestamp_utc")
                )
                | (F.col("target_weather_valid_delta_minutes") < 0)
                | (F.col("target_weather_availability_age_minutes") < 0)
            ).count(),
        },
        {
            "check_name": "target_weather_comparison_model_modes",
            "failed_rows": predictions.where(
                (~F.col("model_name").isin(BASELINE_MODEL, CANDIDATE_MODEL))
                | (
                    (F.col("model_name") == BASELINE_MODEL)
                    & (F.col("weather_feature_mode") != "observed_at_feature")
                )
                | (
                    (F.col("model_name") == CANDIDATE_MODEL)
                    & (F.col("weather_feature_mode") != "target_forecast")
                )
            ).count(),
        },
        {
            "check_name": "target_weather_comparison_contract_versions",
            "failed_rows": predictions.where(
                (
                    F.col("weather_comparison_contract_version")
                    != WEATHER_COMPARISON_CONTRACT_VERSION
                )
                | (
                    F.col("forecast_weather_contract_version")
                    != FORECAST_WEATHER_CONTRACT_VERSION
                )
            ).count(),
        },
        {
            "check_name": "target_weather_comparison_coverage",
            "failed_rows": predictions.where(
                (F.col("forecast_weather_eligible_count") <= 0)
                | (F.col("forecast_weather_matched_count") <= 0)
                | (
                    F.col("forecast_weather_matched_count")
                    > F.col("forecast_weather_eligible_count")
                )
                | (
                    F.col("forecast_weather_coverage_pct")
                    < F.col("minimum_forecast_weather_coverage_pct")
                )
                | (
                    ~F.col("requested_horizon_minutes").isin(
                        *SUPPORTED_HORIZONS
                    )
                )
                | (~F.col("forecast_lead_time_bucket").isin(*SUPPORTED_LEAD_BUCKETS))
            ).count(),
        },
        {
            "check_name": "target_weather_comparison_metrics_valid",
            "failed_rows": metrics.where(
                (F.col("observation_count") <= 0)
                | (F.col("mae_mw") < 0)
                | (F.col("rmse_mw") < 0)
                | (
                    F.col("evaluation_feature_start_utc")
                    <= F.col("trained_through_utc")
                )
                | (
                    F.col("evaluation_feature_end_utc")
                    < F.col("evaluation_feature_start_utc")
                )
                | (
                    F.col("evaluation_start_utc")
                    <= F.col("evaluation_feature_start_utc")
                )
                | (
                    F.col("evaluation_end_utc")
                    < F.col("evaluation_start_utc")
                )
            ).count(),
        },
        {
            "check_name": "target_weather_comparison_rolling_sequence",
            "failed_rows": _rolling_sequence_failures(metrics),
        },
    ]
    now = datetime.now(timezone.utc)
    results = [
        {
            "run_timestamp_utc": now,
            "comparison_run_id": run_id,
            "check_name": check["check_name"],
            "severity": "error",
            "failed_rows": int(check["failed_rows"]),
            "status": "passed" if int(check["failed_rows"]) == 0 else "failed",
        }
        for check in checks
    ]
    result_frame = spark_session.createDataFrame(results)
    (
        result_frame.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(RESULTS_TABLE)
    )
    result_frame.orderBy("check_name").show(truncate=False)
    failures = [row for row in results if row["failed_rows"] > 0]
    if failures:
        summary = ", ".join(
            f"{row['check_name']}={row['failed_rows']}" for row in failures
        )
        raise ValueError(f"Target-weather comparison checks failed: {summary}")
    return results


if __name__ == "__main__":
    run_checks(spark)

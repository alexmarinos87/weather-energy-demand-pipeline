# Fabric notebook source: 06d_seasonal_baseline_quality_checks
# Optional/manual blocking checks for 05d_seasonal_baseline_comparison.

from datetime import datetime, timezone

from pyspark.sql import Window
from pyspark.sql import functions as F

PREDICTIONS_TABLE = "forecast_seasonal_comparison_predictions"
METRICS_TABLE = "forecast_seasonal_comparison_metrics"
RESULTS_TABLE = "dq_run_results"
GROUP_COLUMNS = ["source_area", "resource_id", "city"]
HORIZON_COLUMN = "requested_horizon_minutes"
FEATURE_TIMESTAMP_COLUMN = "feature_timestamp_utc"
TARGET_TIMESTAMP_COLUMN = "event_timestamp_utc"
ORIGIN_FOLD_COLUMN = "origin_fold"
ALL_MODELS = (
    "persistence_current_value",
    "ridge_weather_lag",
    "seasonal_previous_day",
    "seasonal_previous_week",
)
SEASONAL_MODELS = ("seasonal_previous_day", "seasonal_previous_week")
CONTRACT_VERSION = "elapsed-seasonal-v1"
SUPPORTED_HORIZONS = (30, 60)


def _latest_run_id(spark_session) -> str:
    row = (
        spark_session.table(PREDICTIONS_TABLE)
        .orderBy(
            F.col("seasonal_comparison_run_timestamp_utc").desc(),
            F.col("seasonal_comparison_run_id").desc(),
        )
        .select("seasonal_comparison_run_id")
        .first()
    )
    if row is None:
        raise ValueError("No Fabric seasonal comparison run is available.")
    return str(row["seasonal_comparison_run_id"])


def _pair_failures(predictions) -> int:
    keys = [
        *GROUP_COLUMNS,
        HORIZON_COLUMN,
        "split",
        ORIGIN_FOLD_COLUMN,
        FEATURE_TIMESTAMP_COLUMN,
        TARGET_TIMESTAMP_COLUMN,
    ]
    return (
        predictions.groupBy(*keys)
        .agg(
            F.count(F.lit(1)).alias("row_count"),
            F.countDistinct("model_name").alias("model_count"),
            F.countDistinct("actual_demand_mw").alias("actual_count"),
            F.countDistinct("trained_through_utc").alias("boundary_count"),
        )
        .where(
            (F.col("row_count") != len(ALL_MODELS))
            | (F.col("model_count") != len(ALL_MODELS))
            | (F.col("actual_count") != 1)
            | (F.col("boundary_count") != 1)
        )
        .count()
    )


def _rolling_sequence_failures(metrics) -> int:
    rolling = metrics.where(F.col("origin_count").isNotNull())
    if rolling.limit(1).count() == 0:
        return 0
    identity = [*GROUP_COLUMNS, HORIZON_COLUMN, "model_name"]
    sequence = rolling.groupBy(*identity).agg(
        F.countDistinct(ORIGIN_FOLD_COLUMN).alias("fold_count"),
        F.min(ORIGIN_FOLD_COLUMN).alias("minimum_fold"),
        F.max(ORIGIN_FOLD_COLUMN).alias("maximum_fold"),
        F.min("origin_count").alias("minimum_origin_count"),
        F.max("origin_count").alias("maximum_origin_count"),
    )
    count_failures = sequence.where(
        (F.col("minimum_fold") != 1)
        | (F.col("maximum_fold") != F.col("maximum_origin_count"))
        | (F.col("fold_count") != F.col("maximum_origin_count"))
        | (F.col("minimum_origin_count") != F.col("maximum_origin_count"))
    ).count()
    order = Window.partitionBy(*identity).orderBy(ORIGIN_FOLD_COLUMN)
    ordered = (
        rolling.withColumn(
            "previous_cutoff", F.lag("origin_cutoff_utc").over(order)
        )
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
            (F.col(ORIGIN_FOLD_COLUMN) < F.col("origin_count"))
            & (F.col("split") != "validation")
        )
        | (
            (F.col(ORIGIN_FOLD_COLUMN) == F.col("origin_count"))
            & (F.col("split") != "test")
        )
    ).count()
    return count_failures + order_failures


def run_checks(spark_session):
    run_id = _latest_run_id(spark_session)
    predictions = spark_session.table(PREDICTIONS_TABLE).where(
        F.col("seasonal_comparison_run_id") == run_id
    )
    metrics = spark_session.table(METRICS_TABLE).where(
        F.col("seasonal_comparison_run_id") == run_id
    )
    required_prediction_fields = [
        "point_prediction_run_id",
        *GROUP_COLUMNS,
        FEATURE_TIMESTAMP_COLUMN,
        TARGET_TIMESTAMP_COLUMN,
        HORIZON_COLUMN,
        "split",
        "model_name",
        "model_fit_required",
        "actual_demand_mw",
        "predicted_demand_mw",
        "trained_through_utc",
        "previous_day_eligible_count",
        "previous_day_matched_count",
        "previous_day_coverage_pct",
        "previous_week_eligible_count",
        "previous_week_matched_count",
        "previous_week_coverage_pct",
        "seasonal_reference_tolerance_minutes",
        "seasonal_baseline_contract_version",
    ]
    null_condition = F.lit(False)
    for column in required_prediction_fields:
        null_condition = null_condition | F.col(column).isNull()
    seasonal = predictions.where(F.col("model_name").isin(*SEASONAL_MODELS))
    checks = [
        {
            "check_name": "seasonal_comparison_predictions_not_empty",
            "failed_rows": int(predictions.limit(1).count() == 0),
        },
        {
            "check_name": "seasonal_comparison_metrics_not_empty",
            "failed_rows": int(metrics.limit(1).count() == 0),
        },
        {
            "check_name": "seasonal_comparison_required_fields",
            "failed_rows": predictions.where(null_condition).count(),
        },
        {
            "check_name": "seasonal_comparison_exact_pairs",
            "failed_rows": _pair_failures(predictions),
        },
        {
            "check_name": "seasonal_comparison_model_contract",
            "failed_rows": predictions.where(
                (~F.col("model_name").isin(*ALL_MODELS))
                | (
                    (F.col("model_name") == "ridge_weather_lag")
                    & (~F.col("model_fit_required"))
                )
                | (
                    (F.col("model_name") != "ridge_weather_lag")
                    & F.col("model_fit_required")
                )
                | (
                    F.col("seasonal_baseline_contract_version")
                    != CONTRACT_VERSION
                )
            ).count(),
        },
        {
            "check_name": "seasonal_comparison_reference_causality",
            "failed_rows": seasonal.where(
                F.col("seasonal_reference_timestamp_utc").isNull()
                | (
                    F.col("seasonal_reference_timestamp_utc")
                    > F.col(FEATURE_TIMESTAMP_COLUMN)
                )
                | (F.col("seasonal_reference_source_age_minutes") < 0)
                | (
                    F.col("seasonal_reference_absolute_offset_minutes")
                    > F.col("seasonal_reference_tolerance_minutes")
                )
                | (
                    (F.col("model_name") == "seasonal_previous_day")
                    & (F.col("seasonal_reference_period_minutes") != 1440)
                )
                | (
                    (F.col("model_name") == "seasonal_previous_week")
                    & (F.col("seasonal_reference_period_minutes") != 10080)
                )
            ).count(),
        },
        {
            "check_name": "seasonal_comparison_coverage",
            "failed_rows": predictions.where(
                (F.col("previous_day_eligible_count") <= 0)
                | (F.col("previous_day_matched_count") <= 0)
                | (
                    F.col("previous_day_matched_count")
                    > F.col("previous_day_eligible_count")
                )
                | (F.col("previous_week_eligible_count") <= 0)
                | (F.col("previous_week_matched_count") <= 0)
                | (
                    F.col("previous_week_matched_count")
                    > F.col("previous_week_eligible_count")
                )
                | (~F.col(HORIZON_COLUMN).isin(*SUPPORTED_HORIZONS))
            ).count(),
        },
        {
            "check_name": "seasonal_comparison_time_boundaries",
            "failed_rows": predictions.where(
                (F.col("trained_through_utc") >= F.col(FEATURE_TIMESTAMP_COLUMN))
                | (
                    F.col(FEATURE_TIMESTAMP_COLUMN)
                    >= F.col(TARGET_TIMESTAMP_COLUMN)
                )
            ).count(),
        },
        {
            "check_name": "seasonal_comparison_metrics_valid",
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
            "check_name": "seasonal_comparison_rolling_sequence",
            "failed_rows": _rolling_sequence_failures(metrics),
        },
    ]
    now = datetime.now(timezone.utc)
    results = [
        {
            "run_timestamp_utc": now,
            "seasonal_comparison_run_id": run_id,
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
        raise ValueError(f"Seasonal comparison checks failed: {summary}")
    return results


if __name__ == "__main__":
    run_checks(spark)

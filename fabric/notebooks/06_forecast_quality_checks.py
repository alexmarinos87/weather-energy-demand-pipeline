# Fabric notebook source: 06_forecast_quality_checks
# Validates the newest fixed-holdout or rolling-origin 30/60-minute run.

from datetime import datetime, timezone

from pyspark.sql import Window
from pyspark.sql import functions as F


PREDICTIONS_TABLE = "forecast_baseline_predictions"
METRICS_TABLE = "forecast_baseline_metrics"
RESULTS_TABLE = "dq_run_results"
GROUP_COLUMNS = ["source_area", "resource_id", "city"]
MODEL_GROUP_COLUMNS = [
    *GROUP_COLUMNS,
    "requested_horizon_minutes",
    "model_name",
]
SUPPORTED_HORIZON_MINUTES = (30, 60)
FEATURE_CONTRACT_VERSION = "time-horizon-v1"
HOLDOUT_EVALUATION_CONTRACT_VERSION = "fixed-holdout-v1"
ROLLING_ORIGIN_EVALUATION_CONTRACT_VERSION = "rolling-origin-v1"
SUPPORTED_EVALUATION_CONTRACT_VERSIONS = (
    HOLDOUT_EVALUATION_CONTRACT_VERSION,
    ROLLING_ORIGIN_EVALUATION_CONTRACT_VERSION,
)


def _latest_run_id(spark_session) -> str:
    row = (
        spark_session.table(PREDICTIONS_TABLE)
        .orderBy(F.col("run_timestamp_utc").desc(), F.col("run_id").desc())
        .select("run_id")
        .first()
    )
    if row is None:
        raise ValueError("No baseline prediction run is available for validation.")
    return str(row["run_id"])


def _rolling_origin_sequence_failures(predictions, metrics) -> int:
    rolling_predictions = predictions.where(
        F.col("evaluation_contract_version")
        == ROLLING_ORIGIN_EVALUATION_CONTRACT_VERSION
    )
    rolling_metrics = metrics.where(
        F.col("evaluation_contract_version")
        == ROLLING_ORIGIN_EVALUATION_CONTRACT_VERSION
    )
    if rolling_predictions.limit(1).count() == 0:
        return 0

    groups = rolling_metrics.groupBy(*MODEL_GROUP_COLUMNS).agg(
        F.countDistinct("origin_fold").alias("_fold_count"),
        F.min("origin_fold").alias("_min_fold"),
        F.max("origin_fold").alias("_max_fold"),
        F.min("origin_count").alias("_min_origin_count"),
        F.max("origin_count").alias("_max_origin_count"),
    )
    incomplete_groups = groups.where(
        (F.col("_min_fold") != 1)
        | (F.col("_max_fold") != F.col("_max_origin_count"))
        | (F.col("_fold_count") != F.col("_max_origin_count"))
        | (F.col("_min_origin_count") != F.col("_max_origin_count"))
    ).count()

    order = Window.partitionBy(*MODEL_GROUP_COLUMNS).orderBy("origin_fold")
    ordered = (
        rolling_metrics.withColumn(
            "_previous_cutoff",
            F.lag("origin_cutoff_utc").over(order),
        )
        .withColumn(
            "_previous_training_count",
            F.lag("training_observation_count").over(order),
        )
    )
    ordering_failures = ordered.where(
        (F.col("origin_fold") > 1)
        & (
            (F.col("origin_cutoff_utc") <= F.col("_previous_cutoff"))
            | (
                F.col("training_observation_count")
                < F.col("_previous_training_count")
            )
        )
    ).count()

    reused_evaluations = (
        rolling_predictions.groupBy(
            *MODEL_GROUP_COLUMNS,
            "feature_timestamp_utc",
        )
        .agg(F.countDistinct("origin_fold").alias("_origin_uses"))
        .where(F.col("_origin_uses") > 1)
        .count()
    )
    return int(incomplete_groups + ordering_failures + reused_evaluations)


def run_checks(spark_session):
    run_id = _latest_run_id(spark_session)
    predictions = spark_session.table(PREDICTIONS_TABLE).where(
        F.col("run_id") == run_id
    )
    metrics = spark_session.table(METRICS_TABLE).where(F.col("run_id") == run_id)

    holdout_prediction_failures = predictions.where(
        (
            F.col("evaluation_contract_version")
            == HOLDOUT_EVALUATION_CONTRACT_VERSION
        )
        & (
            F.col("origin_fold").isNotNull()
            | F.col("origin_count").isNotNull()
            | F.col("origin_cutoff_utc").isNotNull()
            | F.col("training_observation_count").isNotNull()
            | (~F.col("split").isin("validation", "test"))
        )
    ).count()
    rolling_prediction_failures = predictions.where(
        (
            F.col("evaluation_contract_version")
            == ROLLING_ORIGIN_EVALUATION_CONTRACT_VERSION
        )
        & (
            F.col("origin_fold").isNull()
            | F.col("origin_count").isNull()
            | F.col("origin_cutoff_utc").isNull()
            | F.col("training_observation_count").isNull()
            | (F.col("origin_count") < 2)
            | (F.col("origin_fold") < 1)
            | (F.col("origin_fold") > F.col("origin_count"))
            | (F.col("training_observation_count") <= 0)
            | (F.col("trained_through_utc") >= F.col("origin_cutoff_utc"))
            | (F.col("origin_cutoff_utc") > F.col("feature_timestamp_utc"))
            | (
                (F.col("origin_fold") < F.col("origin_count"))
                & (F.col("split") != "validation")
            )
            | (
                (F.col("origin_fold") == F.col("origin_count"))
                & (F.col("split") != "test")
            )
        )
    ).count()
    unsupported_contract_failures = predictions.where(
        ~F.col("evaluation_contract_version").isin(
            *SUPPORTED_EVALUATION_CONTRACT_VERSIONS
        )
    ).count()

    checks = [
        {
            "check_name": "forecast_predictions_not_empty",
            "failed_rows": int(predictions.limit(1).count() == 0),
        },
        {
            "check_name": "forecast_metrics_not_empty",
            "failed_rows": int(metrics.limit(1).count() == 0),
        },
        {
            "check_name": "forecast_prediction_required_fields",
            "failed_rows": predictions.where(
                F.col("source_area").isNull()
                | F.col("resource_id").isNull()
                | F.col("city").isNull()
                | F.col("feature_timestamp_utc").isNull()
                | F.col("event_timestamp_utc").isNull()
                | F.col("requested_horizon_minutes").isNull()
                | F.col("target_tolerance_minutes").isNull()
                | F.col("horizon_steps").isNull()
                | F.col("horizon_minutes").isNull()
                | F.col("target_delay_minutes").isNull()
                | F.col("split").isNull()
                | F.col("model_name").isNull()
                | F.col("current_demand_mw").isNull()
                | F.col("actual_demand_mw").isNull()
                | F.col("predicted_demand_mw").isNull()
                | F.col("trained_through_utc").isNull()
                | F.col("feature_contract_version").isNull()
                | F.col("evaluation_contract_version").isNull()
            ).count(),
        },
        {
            "check_name": "forecast_prediction_training_boundary",
            "failed_rows": predictions.where(
                F.col("feature_timestamp_utc") <= F.col("trained_through_utc")
            ).count(),
        },
        {
            "check_name": "forecast_prediction_time_horizon_valid",
            "failed_rows": predictions.where(
                (F.col("event_timestamp_utc") <= F.col("feature_timestamp_utc"))
                | (
                    ~F.col("requested_horizon_minutes").isin(
                        *SUPPORTED_HORIZON_MINUTES
                    )
                )
                | (F.col("target_tolerance_minutes") < 0)
                | (F.col("horizon_steps") < 1)
                | (
                    F.col("horizon_minutes")
                    < F.col("requested_horizon_minutes")
                )
                | (F.col("target_delay_minutes") < 0)
                | (
                    F.col("target_delay_minutes")
                    > F.col("target_tolerance_minutes")
                )
                | (
                    F.abs(
                        F.col("horizon_minutes")
                        - F.col("requested_horizon_minutes")
                        - F.col("target_delay_minutes")
                    )
                    > F.lit(1e-6)
                )
                | (F.col("feature_contract_version") != FEATURE_CONTRACT_VERSION)
            ).count(),
        },
        {
            "check_name": "forecast_target_coverage_valid",
            "failed_rows": metrics.where(
                (F.col("eligible_target_count") <= 0)
                | (F.col("matched_target_count") <= 0)
                | (
                    F.col("matched_target_count")
                    > F.col("eligible_target_count")
                )
                | (F.col("target_coverage_pct") < 0)
                | (F.col("target_coverage_pct") > 100)
                | (
                    F.col("target_coverage_pct")
                    < F.col("minimum_target_coverage_pct")
                )
                | (
                    F.col("target_delay_minutes_max")
                    > F.col("target_tolerance_minutes")
                )
            ).count(),
        },
        {
            "check_name": "forecast_evaluation_contract_valid",
            "failed_rows": int(
                holdout_prediction_failures
                + rolling_prediction_failures
                + unsupported_contract_failures
            ),
        },
        {
            "check_name": "forecast_rolling_origin_sequence_valid",
            "failed_rows": _rolling_origin_sequence_failures(
                predictions,
                metrics,
            ),
        },
        {
            "check_name": "forecast_metrics_valid",
            "failed_rows": metrics.where(
                (F.col("observation_count") <= 0)
                | (
                    ~F.col("requested_horizon_minutes").isin(
                        *SUPPORTED_HORIZON_MINUTES
                    )
                )
                | (F.col("target_tolerance_minutes") < 0)
                | (F.col("horizon_steps_avg") < 1)
                | (
                    F.col("horizon_minutes_avg")
                    < F.col("requested_horizon_minutes")
                )
                | (F.col("target_delay_minutes_avg") < 0)
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
                | (F.col("feature_contract_version") != FEATURE_CONTRACT_VERSION)
                | (
                    ~F.col("evaluation_contract_version").isin(
                        *SUPPORTED_EVALUATION_CONTRACT_VERSIONS
                    )
                )
            ).count(),
        },
    ]

    now_utc = datetime.now(timezone.utc)
    results = [
        {
            "run_timestamp_utc": now_utc,
            "check_name": check["check_name"],
            "severity": "error",
            "failed_rows": int(check["failed_rows"]),
            "status": "passed" if int(check["failed_rows"]) == 0 else "failed",
        }
        for check in checks
    ]
    results_df = spark_session.createDataFrame(results)
    (
        results_df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(RESULTS_TABLE)
    )
    results_df.orderBy("check_name").show(truncate=False)

    failures = [result for result in results if result["failed_rows"] > 0]
    if failures:
        summary = ", ".join(
            f"{result['check_name']}={result['failed_rows']}" for result in failures
        )
        raise ValueError(f"Forecast quality checks failed: {summary}")
    return results


if __name__ == "__main__":
    run_checks(spark)

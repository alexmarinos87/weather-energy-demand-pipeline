# Fabric notebook source: 06_forecast_quality_checks
# Validates the newest 30/60-minute baseline run and records blocking evidence.

from datetime import datetime, timezone

from pyspark.sql import functions as F


PREDICTIONS_TABLE = "forecast_baseline_predictions"
METRICS_TABLE = "forecast_baseline_metrics"
RESULTS_TABLE = "dq_run_results"
SUPPORTED_HORIZON_MINUTES = (30, 60)
FEATURE_CONTRACT_VERSION = "time-horizon-v1"


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


def run_checks(spark_session):
    run_id = _latest_run_id(spark_session)
    predictions = spark_session.table(PREDICTIONS_TABLE).where(F.col("run_id") == run_id)
    metrics = spark_session.table(METRICS_TABLE).where(F.col("run_id") == run_id)
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
                | (~F.col("requested_horizon_minutes").isin(*SUPPORTED_HORIZON_MINUTES))
                | (F.col("target_tolerance_minutes") < 0)
                | (F.col("horizon_steps") < 1)
                | (F.col("horizon_minutes") < F.col("requested_horizon_minutes"))
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
                | (F.col("matched_target_count") > F.col("eligible_target_count"))
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
            "check_name": "forecast_metrics_valid",
            "failed_rows": metrics.where(
                (F.col("observation_count") <= 0)
                | (~F.col("requested_horizon_minutes").isin(*SUPPORTED_HORIZON_MINUTES))
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
                | (F.col("evaluation_end_utc") < F.col("evaluation_start_utc"))
                | (F.col("feature_contract_version") != FEATURE_CONTRACT_VERSION)
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

# Fabric notebook source: 06e_prediction_interval_quality_checks
# Optional/manual blocking checks for 05e_prediction_intervals.

from datetime import datetime, timezone

from pyspark.sql import functions as F


INTERVALS_TABLE = "forecast_prediction_intervals"
METRICS_TABLE = "forecast_prediction_interval_metrics"
RESULTS_TABLE = "dq_run_results"

GROUP_COLUMNS = ["source_area", "resource_id", "city"]
FEATURE_TIMESTAMP_COLUMN = "feature_timestamp_utc"
TARGET_TIMESTAMP_COLUMN = "event_timestamp_utc"
HORIZON_COLUMN = "requested_horizon_minutes"
EXPECTED_MODELS = (
    "persistence_current_value",
    "ridge_weather_lag",
    "seasonal_previous_day",
    "seasonal_previous_week",
)
CALIBRATION_METHOD = "absolute_residual_quantile"
INTERVAL_CONTRACT_VERSION = "split-conformal-absolute-residual-v1"
SUPPORTED_HORIZONS = (30, 60)


def _latest_interval_run_id(spark_session) -> str:
    row = (
        spark_session.table(INTERVALS_TABLE)
        .orderBy(
            F.col("interval_run_timestamp_utc").desc(),
            F.col("interval_run_id").desc(),
        )
        .select("interval_run_id")
        .first()
    )
    if row is None:
        raise ValueError("No Fabric prediction interval run is available.")
    return str(row["interval_run_id"])


def _required_null_failures(frame, columns: list[str]) -> int:
    condition = F.lit(False)
    for column in columns:
        condition = condition | F.col(column).isNull()
    return frame.where(condition).count()


def _pair_failures(intervals) -> int:
    keys = [
        *GROUP_COLUMNS,
        HORIZON_COLUMN,
        FEATURE_TIMESTAMP_COLUMN,
        TARGET_TIMESTAMP_COLUMN,
        "target_coverage_level",
    ]
    return (
        intervals.groupBy(*keys)
        .agg(
            F.count(F.lit(1)).alias("row_count"),
            F.countDistinct("model_name").alias("model_count"),
            F.countDistinct("actual_demand_mw").alias("actual_count"),
            F.countDistinct("point_model_trained_through_utc").alias(
                "boundary_count"
            ),
        )
        .where(
            (F.col("row_count") != len(EXPECTED_MODELS))
            | (F.col("model_count") != len(EXPECTED_MODELS))
            | (F.col("actual_count") != 1)
            | (F.col("boundary_count") != 1)
        )
        .count()
    )


def _fixed_radius_failures(intervals) -> int:
    identity = [
        *GROUP_COLUMNS,
        HORIZON_COLUMN,
        "model_name",
        "feature_contract_version",
        "target_coverage_level",
    ]
    return (
        intervals.groupBy(*identity)
        .agg(
            F.countDistinct("calibration_radius_mw").alias("radius_count"),
            F.countDistinct("calibration_quantile_rank").alias("rank_count"),
            F.countDistinct("calibration_observation_count").alias(
                "calibration_count_count"
            ),
            F.countDistinct(
                "calibration_label_available_through_utc"
            ).alias("label_boundary_count"),
        )
        .where(
            (F.col("radius_count") != 1)
            | (F.col("rank_count") != 1)
            | (F.col("calibration_count_count") != 1)
            | (F.col("label_boundary_count") != 1)
        )
        .count()
    )


def _coverage_set_failures(intervals) -> int:
    expected_levels = (
        intervals.select("target_coverage_level")
        .distinct()
        .count()
    )
    if expected_levels <= 0:
        return 1
    identity = [
        *GROUP_COLUMNS,
        HORIZON_COLUMN,
        "model_name",
        "feature_contract_version",
    ]
    return (
        intervals.groupBy(*identity)
        .agg(
            F.countDistinct("target_coverage_level").alias(
                "coverage_level_count"
            )
        )
        .where(F.col("coverage_level_count") != F.lit(expected_levels))
        .count()
    )


def _metric_consistency_failures(intervals, metrics) -> int:
    identity = [
        "interval_run_id",
        "point_prediction_run_id",
        "seasonal_comparison_run_id",
        *GROUP_COLUMNS,
        HORIZON_COLUMN,
        "model_name",
        "feature_contract_version",
        "target_coverage_level",
    ]
    calculated = intervals.groupBy(*identity).agg(
        F.count(F.lit(1)).alias("expected_evaluation_observation_count"),
        (
            F.avg(F.col("interval_covered").cast("double")) * F.lit(100.0)
        ).alias("expected_empirical_coverage_pct"),
        F.avg("interval_width_mw").alias(
            "expected_average_interval_width_mw"
        ),
        F.min("interval_width_mw").alias(
            "expected_minimum_interval_width_mw"
        ),
        F.max("interval_width_mw").alias(
            "expected_maximum_interval_width_mw"
        ),
    )
    joined = calculated.join(metrics, on=identity, how="full")
    return joined.where(
        F.col("evaluation_observation_count").isNull()
        | F.col("expected_evaluation_observation_count").isNull()
        | (
            F.col("evaluation_observation_count")
            != F.col("expected_evaluation_observation_count")
        )
        | (
            F.abs(
                F.col("empirical_coverage_pct")
                - F.col("expected_empirical_coverage_pct")
            )
            > F.lit(1e-9)
        )
        | (
            F.abs(
                F.col("average_interval_width_mw")
                - F.col("expected_average_interval_width_mw")
            )
            > F.lit(1e-9)
        )
        | (
            F.abs(
                F.col("minimum_interval_width_mw")
                - F.col("expected_minimum_interval_width_mw")
            )
            > F.lit(1e-9)
        )
        | (
            F.abs(
                F.col("maximum_interval_width_mw")
                - F.col("expected_maximum_interval_width_mw")
            )
            > F.lit(1e-9)
        )
    ).count()


def run_checks(spark_session):
    interval_run_id = _latest_interval_run_id(spark_session)
    intervals = spark_session.table(INTERVALS_TABLE).where(
        F.col("interval_run_id") == interval_run_id
    )
    metrics = spark_session.table(METRICS_TABLE).where(
        F.col("interval_run_id") == interval_run_id
    )

    required_interval_fields = [
        "interval_run_id",
        "interval_run_timestamp_utc",
        "point_prediction_run_id",
        "seasonal_comparison_run_id",
        *GROUP_COLUMNS,
        FEATURE_TIMESTAMP_COLUMN,
        TARGET_TIMESTAMP_COLUMN,
        HORIZON_COLUMN,
        "model_name",
        "feature_contract_version",
        "evaluation_feature_start_utc",
        "evaluation_feature_end_utc",
        "evaluation_start_utc",
        "evaluation_end_utc",
        "evaluation_observation_count",
        "actual_demand_mw",
        "point_prediction_mw",
        "target_coverage_level",
        "lower_prediction_mw",
        "upper_prediction_mw",
        "interval_width_mw",
        "interval_covered",
        "calibration_method",
        "calibration_observation_count",
        "calibration_quantile_rank",
        "calibration_radius_mw",
        "calibration_feature_start_utc",
        "calibration_feature_end_utc",
        "calibration_label_available_through_utc",
        "point_model_trained_through_utc",
        "interval_contract_version",
    ]
    required_metric_fields = [
        "interval_run_id",
        "point_prediction_run_id",
        "seasonal_comparison_run_id",
        *GROUP_COLUMNS,
        HORIZON_COLUMN,
        "model_name",
        "feature_contract_version",
        "target_coverage_level",
        "calibration_observation_count",
        "calibration_quantile_rank",
        "calibration_radius_mw",
        "calibration_label_available_through_utc",
        "evaluation_feature_start_utc",
        "evaluation_feature_end_utc",
        "evaluation_start_utc",
        "evaluation_end_utc",
        "evaluation_observation_count",
        "empirical_coverage_pct",
        "average_interval_width_mw",
        "median_interval_width_mw",
        "minimum_interval_width_mw",
        "maximum_interval_width_mw",
        "interval_contract_version",
    ]

    expected_rank = F.least(
        F.col("calibration_observation_count"),
        F.greatest(
            F.lit(1),
            F.ceil(
                (F.col("calibration_observation_count") + F.lit(1))
                * F.col("target_coverage_level")
            ).cast("int"),
        ),
    )

    checks = [
        {
            "check_name": "prediction_intervals_not_empty",
            "failed_rows": int(intervals.limit(1).count() == 0),
        },
        {
            "check_name": "prediction_interval_metrics_not_empty",
            "failed_rows": int(metrics.limit(1).count() == 0),
        },
        {
            "check_name": "prediction_interval_required_fields",
            "failed_rows": _required_null_failures(
                intervals, required_interval_fields
            ),
        },
        {
            "check_name": "prediction_interval_metric_required_fields",
            "failed_rows": _required_null_failures(
                metrics, required_metric_fields
            ),
        },
        {
            "check_name": "prediction_interval_single_point_run_binding",
            "failed_rows": int(
                intervals.select("point_prediction_run_id").distinct().count()
                != 1
                or intervals.select("seasonal_comparison_run_id")
                .distinct()
                .count()
                != 1
                or metrics.select("point_prediction_run_id")
                .distinct()
                .count()
                != 1
            ),
        },
        {
            "check_name": "prediction_interval_exact_model_pairs",
            "failed_rows": _pair_failures(intervals),
        },
        {
            "check_name": "prediction_interval_calibration_causality",
            "failed_rows": intervals.where(
                (
                    F.col("calibration_label_available_through_utc")
                    >= F.col(FEATURE_TIMESTAMP_COLUMN)
                )
                | (
                    F.col("point_model_trained_through_utc")
                    >= F.col(FEATURE_TIMESTAMP_COLUMN)
                )
                | (
                    F.col("calibration_feature_end_utc")
                    < F.col("calibration_feature_start_utc")
                )
                | (
                    F.col("calibration_label_available_through_utc")
                    >= F.col("evaluation_feature_start_utc")
                )
            ).count(),
        },
        {
            "check_name": "prediction_interval_finite_sample_rank",
            "failed_rows": intervals.where(
                (F.col("calibration_observation_count") <= 0)
                | (
                    F.col("calibration_quantile_rank")
                    != expected_rank
                )
                | (
                    F.col("calibration_quantile_rank")
                    > F.col("calibration_observation_count")
                )
                | (F.col("calibration_radius_mw") < 0)
            ).count(),
        },
        {
            "check_name": "prediction_interval_fixed_radius",
            "failed_rows": _fixed_radius_failures(intervals),
        },
        {
            "check_name": "prediction_interval_bounds",
            "failed_rows": intervals.where(
                (F.col("target_coverage_level") <= 0)
                | (F.col("target_coverage_level") >= 1)
                | (
                    F.col("lower_prediction_mw")
                    > F.col("point_prediction_mw")
                )
                | (
                    F.col("point_prediction_mw")
                    > F.col("upper_prediction_mw")
                )
                | (
                    F.abs(
                        F.col("interval_width_mw")
                        - (
                            F.col("upper_prediction_mw")
                            - F.col("lower_prediction_mw")
                        )
                    )
                    > F.lit(1e-9)
                )
                | (F.col("interval_width_mw") < 0)
                | (
                    F.col("interval_covered")
                    != F.col("actual_demand_mw").between(
                        F.col("lower_prediction_mw"),
                        F.col("upper_prediction_mw"),
                    )
                )
            ).count(),
        },
        {
            "check_name": "prediction_interval_contract",
            "failed_rows": intervals.where(
                (~F.col("model_name").isin(*EXPECTED_MODELS))
                | (~F.col(HORIZON_COLUMN).isin(*SUPPORTED_HORIZONS))
                | (F.col("calibration_method") != CALIBRATION_METHOD)
                | (
                    F.col("interval_contract_version")
                    != INTERVAL_CONTRACT_VERSION
                )
                | (
                    F.col(FEATURE_TIMESTAMP_COLUMN)
                    >= F.col(TARGET_TIMESTAMP_COLUMN)
                )
            ).count(),
        },
        {
            "check_name": "prediction_interval_coverage_levels_complete",
            "failed_rows": _coverage_set_failures(intervals),
        },
        {
            "check_name": "prediction_interval_metrics_valid",
            "failed_rows": metrics.where(
                (F.col("evaluation_observation_count") <= 0)
                | (F.col("empirical_coverage_pct") < 0)
                | (F.col("empirical_coverage_pct") > 100)
                | (F.col("average_interval_width_mw") < 0)
                | (F.col("median_interval_width_mw") < 0)
                | (F.col("minimum_interval_width_mw") < 0)
                | (
                    F.col("maximum_interval_width_mw")
                    < F.col("minimum_interval_width_mw")
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
                | (
                    F.col("calibration_label_available_through_utc")
                    >= F.col("evaluation_feature_start_utc")
                )
            ).count(),
        },
        {
            "check_name": "prediction_interval_metric_consistency",
            "failed_rows": _metric_consistency_failures(
                intervals, metrics
            ),
        },
    ]

    now = datetime.now(timezone.utc)
    results = [
        {
            "run_timestamp_utc": now,
            "interval_run_id": interval_run_id,
            "check_name": check["check_name"],
            "severity": "error",
            "failed_rows": int(check["failed_rows"]),
            "status": (
                "passed" if int(check["failed_rows"]) == 0 else "failed"
            ),
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

    failures = [result for result in results if result["failed_rows"] > 0]
    if failures:
        summary = ", ".join(
            f"{result['check_name']}={result['failed_rows']}"
            for result in failures
        )
        raise ValueError(f"Prediction interval checks failed: {summary}")
    return results


if __name__ == "__main__":
    run_checks(spark)

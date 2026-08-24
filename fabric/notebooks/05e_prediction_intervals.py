# Fabric notebook source: 05e_prediction_intervals
# Optional/manual calibration-only intervals over one retained seasonal comparison run.
# No point model is fitted or refitted.

from datetime import datetime, timezone
from math import ceil
from typing import Any
from uuid import uuid4

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StructField, StructType


POINT_PREDICTIONS_TABLE = "forecast_seasonal_comparison_predictions"
INTERVALS_TABLE = "forecast_prediction_intervals"
METRICS_TABLE = "forecast_prediction_interval_metrics"

POINT_PREDICTION_RUN_ID = ""
COVERAGE_LEVELS = "0.80,0.90,0.95"
MIN_CALIBRATION_ROWS = 24

GROUP_COLUMNS = ["source_area", "resource_id", "city"]
FEATURE_TIMESTAMP_COLUMN = "feature_timestamp_utc"
TARGET_TIMESTAMP_COLUMN = "event_timestamp_utc"
HORIZON_COLUMN = "requested_horizon_minutes"
ORIGIN_FOLD_COLUMN = "origin_fold"
EXPECTED_MODELS = (
    "persistence_current_value",
    "ridge_weather_lag",
    "seasonal_previous_day",
    "seasonal_previous_week",
)
CALIBRATION_METHOD = "absolute_residual_quantile"
INTERVAL_CONTRACT_VERSION = "split-conformal-absolute-residual-v1"


def _parameter(name: str, default: Any) -> Any:
    return globals().get(name, default)


def _configuration() -> dict[str, Any]:
    raw_levels = str(_parameter("COVERAGE_LEVELS", COVERAGE_LEVELS))
    levels: list[float] = []
    for token in raw_levels.split(","):
        stripped = token.strip()
        if not stripped:
            continue
        value = float(stripped)
        if not 0.0 < value < 1.0:
            raise ValueError("COVERAGE_LEVELS values must be in (0, 1).")
        levels.append(value)
    if not levels:
        raise ValueError("COVERAGE_LEVELS must contain at least one value.")
    if len(set(levels)) != len(levels):
        raise ValueError("COVERAGE_LEVELS must not contain duplicates.")
    minimum_rows = int(
        _parameter("MIN_CALIBRATION_ROWS", MIN_CALIBRATION_ROWS)
    )
    if minimum_rows < 1:
        raise ValueError("MIN_CALIBRATION_ROWS must be positive.")
    return {
        "coverage_levels": tuple(sorted(levels)),
        "min_calibration_rows": minimum_rows,
    }


def _selected_point_run_id(point: DataFrame) -> str:
    requested = str(
        _parameter("POINT_PREDICTION_RUN_ID", POINT_PREDICTION_RUN_ID)
    ).strip()
    if requested:
        exists = (
            point.where(F.col("seasonal_comparison_run_id") == requested)
            .limit(1)
            .count()
        )
        if not exists:
            raise ValueError(
                f"No seasonal comparison run found for {requested!r}."
            )
        return requested
    row = (
        point.orderBy(
            F.col("seasonal_comparison_run_timestamp_utc").desc(),
            F.col("seasonal_comparison_run_id").desc(),
        )
        .select("seasonal_comparison_run_id")
        .first()
    )
    if row is None:
        raise ValueError(
            "No retained seasonal comparison prediction run is available."
        )
    return str(row["seasonal_comparison_run_id"])


def _point_rows() -> tuple[DataFrame, str]:
    point = spark.table(POINT_PREDICTIONS_TABLE)
    required = {
        "seasonal_comparison_run_id",
        "seasonal_comparison_run_timestamp_utc",
        "point_prediction_run_id",
        *GROUP_COLUMNS,
        FEATURE_TIMESTAMP_COLUMN,
        TARGET_TIMESTAMP_COLUMN,
        HORIZON_COLUMN,
        "split",
        ORIGIN_FOLD_COLUMN,
        "origin_count",
        "origin_cutoff_utc",
        "model_name",
        "actual_demand_mw",
        "predicted_demand_mw",
        "trained_through_utc",
        "feature_contract_version",
        "evaluation_contract_version",
    }
    missing = sorted(required - set(point.columns))
    if missing:
        raise ValueError(
            f"{POINT_PREDICTIONS_TABLE} is missing required columns: "
            + ", ".join(missing)
            + "."
        )
    selected_run_id = _selected_point_run_id(point)
    point = point.where(
        (F.col("seasonal_comparison_run_id") == selected_run_id)
        & F.col("split").isin("validation", "test")
        & F.col("model_name").isin(*EXPECTED_MODELS)
    )
    if point.limit(1).count() == 0:
        raise ValueError("Selected seasonal comparison run has no usable rows.")

    target_keys = [
        *GROUP_COLUMNS,
        HORIZON_COLUMN,
        "split",
        ORIGIN_FOLD_COLUMN,
        FEATURE_TIMESTAMP_COLUMN,
        TARGET_TIMESTAMP_COLUMN,
    ]
    pairing_failure = (
        point.groupBy(*target_keys)
        .agg(
            F.count(F.lit(1)).alias("row_count"),
            F.countDistinct("model_name").alias("model_count"),
            F.countDistinct("actual_demand_mw").alias("actual_count"),
            F.countDistinct("trained_through_utc").alias("boundary_count"),
        )
        .where(
            (F.col("row_count") != len(EXPECTED_MODELS))
            | (F.col("model_count") != len(EXPECTED_MODELS))
            | (F.col("actual_count") != 1)
            | (F.col("boundary_count") != 1)
        )
        .limit(1)
        .count()
    )
    if pairing_failure:
        raise ValueError(
            "Selected seasonal run does not contain exact four-model pairs "
            "with one target and one training boundary."
        )
    models = {
        row["model_name"]
        for row in point.select("model_name").distinct().collect()
    }
    if models != set(EXPECTED_MODELS):
        raise ValueError(
            f"Expected models {sorted(EXPECTED_MODELS)}, found {sorted(models)}."
        )
    return point, selected_run_id


def _coverage_frame(levels: tuple[float, ...]) -> DataFrame:
    schema = StructType(
        [StructField("target_coverage_level", DoubleType(), nullable=False)]
    )
    return spark.createDataFrame([(level,) for level in levels], schema=schema)


def _test_summary(point: DataFrame) -> DataFrame:
    group = [
        *GROUP_COLUMNS,
        HORIZON_COLUMN,
        "model_name",
        "feature_contract_version",
    ]
    test = point.where(F.col("split") == "test")
    summary = (
        test.groupBy(*group)
        .agg(
            F.min(FEATURE_TIMESTAMP_COLUMN).alias(
                "evaluation_feature_start_utc"
            ),
            F.max(FEATURE_TIMESTAMP_COLUMN).alias(
                "evaluation_feature_end_utc"
            ),
            F.min(TARGET_TIMESTAMP_COLUMN).alias("evaluation_start_utc"),
            F.max(TARGET_TIMESTAMP_COLUMN).alias("evaluation_end_utc"),
            F.count(F.lit(1)).alias("evaluation_observation_count"),
            F.countDistinct(ORIGIN_FOLD_COLUMN).alias(
                "_non_null_test_origin_count"
            ),
            F.max(ORIGIN_FOLD_COLUMN).alias("evaluation_origin_fold"),
            F.max("origin_count").alias("evaluation_origin_count"),
            F.max("origin_cutoff_utc").alias(
                "evaluation_origin_cutoff_utc"
            ),
            F.first("evaluation_contract_version").alias(
                "evaluation_contract_version"
            ),
            F.countDistinct("trained_through_utc").alias(
                "_test_training_boundary_count"
            ),
            F.first("trained_through_utc").alias(
                "point_model_trained_through_utc"
            ),
        )
    )
    invalid = summary.where(
        (F.col("evaluation_observation_count") <= 0)
        | (F.col("_non_null_test_origin_count") > 1)
        | (F.col("_test_training_boundary_count") != 1)
        | (
            F.col("evaluation_feature_end_utc")
            < F.col("evaluation_feature_start_utc")
        )
        | (
            F.col("evaluation_start_utc")
            <= F.col("evaluation_feature_start_utc")
        )
        | (F.col("evaluation_end_utc") < F.col("evaluation_start_utc"))
    ).limit(1).count()
    if invalid:
        raise ValueError("Test evidence has invalid boundaries or origin identity.")
    return summary.drop(
        "_non_null_test_origin_count", "_test_training_boundary_count"
    )


def _calibration_rows(
    point: DataFrame,
    summary: DataFrame,
    config: dict[str, Any],
) -> DataFrame:
    group = [
        *GROUP_COLUMNS,
        HORIZON_COLUMN,
        "model_name",
        "feature_contract_version",
    ]
    validation = (
        point.where(F.col("split") == "validation")
        .join(summary, on=group, how="inner")
        .where(
            F.col(TARGET_TIMESTAMP_COLUMN)
            < F.col("evaluation_feature_start_utc")
        )
        .withColumn(
            "absolute_calibration_error_mw",
            F.abs(
                F.col("predicted_demand_mw")
                - F.col("actual_demand_mw")
            ),
        )
    )
    counts = validation.groupBy(*group).agg(
        F.count(F.lit(1)).alias("calibration_observation_count")
    )
    missing_or_short = (
        summary.select(*group)
        .join(counts, on=group, how="left")
        .fillna({"calibration_observation_count": 0})
        .where(
            F.col("calibration_observation_count")
            < F.lit(config["min_calibration_rows"])
        )
        .collect()
    )
    if missing_or_short:
        examples = "; ".join(
            f"{row['source_area']}/{row['resource_id']}/{row['city']} "
            f"horizon={row[HORIZON_COLUMN]}m model={row['model_name']} "
            f"rows={row['calibration_observation_count']}"
            for row in missing_or_short[:5]
        )
        raise ValueError(
            "Causally available calibration history is insufficient: "
            + examples
        )
    invalid = validation.where(
        F.col(TARGET_TIMESTAMP_COLUMN)
        >= F.col("evaluation_feature_start_utc")
    ).limit(1).count()
    if invalid:
        raise ValueError(
            "Calibration includes labels unavailable before test feature time."
        )
    return validation


def _calibration_radii(
    calibration: DataFrame,
    coverage_levels: DataFrame,
) -> DataFrame:
    group = [
        *GROUP_COLUMNS,
        HORIZON_COLUMN,
        "model_name",
        "feature_contract_version",
    ]
    order = Window.partitionBy(*group).orderBy(
        F.col("absolute_calibration_error_mw").asc(),
        F.col(TARGET_TIMESTAMP_COLUMN).asc(),
        F.col(FEATURE_TIMESTAMP_COLUMN).asc(),
    )
    partition = Window.partitionBy(*group)
    ranked = (
        calibration.withColumn(
            "_calibration_error_rank", F.row_number().over(order)
        )
        .withColumn(
            "calibration_observation_count",
            F.count(F.lit(1)).over(partition),
        )
        .withColumn(
            "calibration_feature_start_utc",
            F.min(FEATURE_TIMESTAMP_COLUMN).over(partition),
        )
        .withColumn(
            "calibration_feature_end_utc",
            F.max(FEATURE_TIMESTAMP_COLUMN).over(partition),
        )
        .withColumn(
            "calibration_label_available_through_utc",
            F.max(TARGET_TIMESTAMP_COLUMN).over(partition),
        )
    )
    expanded = ranked.crossJoin(coverage_levels).withColumn(
        "calibration_quantile_rank",
        F.least(
            F.col("calibration_observation_count"),
            F.greatest(
                F.lit(1),
                F.ceil(
                    (F.col("calibration_observation_count") + F.lit(1))
                    * F.col("target_coverage_level")
                ).cast("int"),
            ),
        ),
    )
    radii = (
        expanded.where(
            F.col("_calibration_error_rank")
            == F.col("calibration_quantile_rank")
        )
        .select(
            *group,
            "target_coverage_level",
            "calibration_observation_count",
            "calibration_quantile_rank",
            F.col("absolute_calibration_error_mw").alias(
                "calibration_radius_mw"
            ),
            "calibration_feature_start_utc",
            "calibration_feature_end_utc",
            "calibration_label_available_through_utc",
        )
    )
    if radii.limit(1).count() == 0:
        raise ValueError("No calibration radii were produced.")
    return radii


def _interval_rows(
    point: DataFrame,
    summary: DataFrame,
    radii: DataFrame,
    *,
    interval_run_id: str,
    interval_run_timestamp: datetime,
    point_prediction_run_id: str,
) -> DataFrame:
    group = [
        *GROUP_COLUMNS,
        HORIZON_COLUMN,
        "model_name",
        "feature_contract_version",
    ]
    test = point.where(F.col("split") == "test").join(
        summary, on=group, how="inner"
    )
    intervals = (
        test.join(radii, on=group, how="inner")
        .withColumn(
            "lower_prediction_mw",
            F.col("predicted_demand_mw") - F.col("calibration_radius_mw"),
        )
        .withColumn(
            "upper_prediction_mw",
            F.col("predicted_demand_mw") + F.col("calibration_radius_mw"),
        )
        .withColumn(
            "interval_width_mw",
            F.col("upper_prediction_mw") - F.col("lower_prediction_mw"),
        )
        .withColumn(
            "interval_covered",
            F.col("actual_demand_mw").between(
                F.col("lower_prediction_mw"),
                F.col("upper_prediction_mw"),
            ),
        )
        .withColumn("interval_run_id", F.lit(interval_run_id))
        .withColumn(
            "interval_run_timestamp_utc",
            F.lit(interval_run_timestamp).cast("timestamp"),
        )
        .withColumn(
            "point_prediction_run_id", F.lit(point_prediction_run_id)
        )
        .withColumn("point_prediction_mw", F.col("predicted_demand_mw"))
        .withColumn("calibration_method", F.lit(CALIBRATION_METHOD))
        .withColumn(
            "interval_contract_version",
            F.lit(INTERVAL_CONTRACT_VERSION),
        )
    )
    selected = intervals.select(
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
        "evaluation_contract_version",
        "evaluation_origin_fold",
        "evaluation_origin_count",
        "evaluation_origin_cutoff_utc",
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
    )
    invalid = selected.where(
        (
            F.col("calibration_label_available_through_utc")
            >= F.col(FEATURE_TIMESTAMP_COLUMN)
        )
        | (
            F.col("point_model_trained_through_utc")
            >= F.col(FEATURE_TIMESTAMP_COLUMN)
        )
        | (
            F.col(FEATURE_TIMESTAMP_COLUMN)
            >= F.col(TARGET_TIMESTAMP_COLUMN)
        )
        | (
            F.col("lower_prediction_mw")
            > F.col("point_prediction_mw")
        )
        | (
            F.col("point_prediction_mw")
            > F.col("upper_prediction_mw")
        )
        | (F.col("interval_width_mw") < 0)
        | (
            F.col("calibration_quantile_rank")
            > F.col("calibration_observation_count")
        )
    ).limit(1).count()
    if invalid:
        raise ValueError(
            "Prediction interval evidence violates calibration or bound rules."
        )
    return selected


def _metrics(intervals: DataFrame) -> DataFrame:
    group = [
        "interval_run_id",
        "interval_run_timestamp_utc",
        "point_prediction_run_id",
        "seasonal_comparison_run_id",
        *GROUP_COLUMNS,
        HORIZON_COLUMN,
        "model_name",
        "feature_contract_version",
        "evaluation_contract_version",
        "evaluation_origin_fold",
        "evaluation_origin_count",
        "evaluation_origin_cutoff_utc",
        "target_coverage_level",
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
    return intervals.groupBy(*group).agg(
        F.min(FEATURE_TIMESTAMP_COLUMN).alias(
            "evaluation_feature_start_utc"
        ),
        F.max(FEATURE_TIMESTAMP_COLUMN).alias(
            "evaluation_feature_end_utc"
        ),
        F.min(TARGET_TIMESTAMP_COLUMN).alias("evaluation_start_utc"),
        F.max(TARGET_TIMESTAMP_COLUMN).alias("evaluation_end_utc"),
        F.count(F.lit(1)).alias("evaluation_observation_count"),
        (
            F.avg(F.col("interval_covered").cast("double")) * F.lit(100.0)
        ).alias("empirical_coverage_pct"),
        F.avg("interval_width_mw").alias("average_interval_width_mw"),
        F.expr("percentile_approx(interval_width_mw, 0.5)").alias(
            "median_interval_width_mw"
        ),
        F.min("interval_width_mw").alias("minimum_interval_width_mw"),
        F.max("interval_width_mw").alias("maximum_interval_width_mw"),
    )


def _validate_pairing(intervals: DataFrame) -> None:
    keys = [
        *GROUP_COLUMNS,
        HORIZON_COLUMN,
        FEATURE_TIMESTAMP_COLUMN,
        TARGET_TIMESTAMP_COLUMN,
        "target_coverage_level",
    ]
    failures = (
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
        .limit(1)
        .count()
    )
    if failures:
        raise ValueError(
            "Interval evidence does not retain exact four-model target pairs."
        )


def run_intervals() -> tuple[DataFrame, DataFrame]:
    config = _configuration()
    point, selected_run_id = _point_rows()
    summary = _test_summary(point)
    calibration = _calibration_rows(point, summary, config)
    radii = _calibration_radii(
        calibration, _coverage_frame(config["coverage_levels"])
    )
    interval_run_id = str(uuid4())
    interval_run_timestamp = datetime.now(timezone.utc)
    intervals = _interval_rows(
        point,
        summary,
        radii,
        interval_run_id=interval_run_id,
        interval_run_timestamp=interval_run_timestamp,
        point_prediction_run_id=selected_run_id,
    )
    _validate_pairing(intervals)
    metrics = _metrics(intervals)
    if metrics.limit(1).count() == 0:
        raise ValueError("No prediction interval metrics were produced.")

    intervals.write.format("delta").mode("append").option(
        "mergeSchema", "true"
    ).saveAsTable(INTERVALS_TABLE)
    metrics.write.format("delta").mode("append").option(
        "mergeSchema", "true"
    ).saveAsTable(METRICS_TABLE)

    intervals.orderBy(
        *GROUP_COLUMNS,
        HORIZON_COLUMN,
        "target_coverage_level",
        "model_name",
        FEATURE_TIMESTAMP_COLUMN,
    ).show(20, truncate=False)
    metrics.orderBy(
        *GROUP_COLUMNS,
        HORIZON_COLUMN,
        "target_coverage_level",
        "model_name",
    ).show(truncate=False)
    return intervals, metrics


if __name__ == "__main__":
    run_intervals()

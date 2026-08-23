# Fabric notebook source: 05d_seasonal_baseline_comparison
# Optional/manual elapsed-time seasonal comparison. Does not retrain ridge.

from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

spark.conf.set("spark.sql.session.timeZone", "UTC")

SOURCE_TABLE = "gold_feature_engineering"
POINT_PREDICTIONS_TABLE = "forecast_baseline_predictions"
PREDICTIONS_TABLE = "forecast_seasonal_comparison_predictions"
METRICS_TABLE = "forecast_seasonal_comparison_metrics"
POINT_PREDICTION_RUN_ID = ""
REFERENCE_TOLERANCE_MINUTES = 15
MIN_REFERENCE_COVERAGE = 0.90

GROUP_COLUMNS = ["source_area", "resource_id", "city"]
FEATURE_TIMESTAMP_COLUMN = "feature_timestamp_utc"
TARGET_TIMESTAMP_COLUMN = "event_timestamp_utc"
HORIZON_COLUMN = "requested_horizon_minutes"
ORIGIN_FOLD_COLUMN = "origin_fold"
BASELINE_MODELS = ("persistence_current_value", "ridge_weather_lag")
SEASONAL_MODELS = {
    "seasonal_previous_day": ("previous_day", 1440),
    "seasonal_previous_week": ("previous_week", 10080),
}
ALL_MODELS = (*BASELINE_MODELS, *SEASONAL_MODELS)
SEASONAL_CONTRACT_VERSION = "elapsed-seasonal-v1"


def _parameter(name: str, default: Any) -> Any:
    return globals().get(name, default)


def _configuration() -> dict[str, Any]:
    tolerance = int(_parameter("REFERENCE_TOLERANCE_MINUTES", REFERENCE_TOLERANCE_MINUTES))
    coverage = float(_parameter("MIN_REFERENCE_COVERAGE", MIN_REFERENCE_COVERAGE))
    if tolerance < 0:
        raise ValueError("REFERENCE_TOLERANCE_MINUTES must be non-negative.")
    if not 0 < coverage <= 1:
        raise ValueError("MIN_REFERENCE_COVERAGE must be in (0, 1].")
    return {
        "reference_tolerance_minutes": tolerance,
        "min_reference_coverage": coverage,
    }


def _selected_run_id(point: DataFrame) -> str:
    requested = str(_parameter("POINT_PREDICTION_RUN_ID", POINT_PREDICTION_RUN_ID)).strip()
    if requested:
        if point.where(F.col("run_id") == requested).limit(1).count() == 0:
            raise ValueError(f"No point-prediction run found for {requested!r}.")
        return requested
    row = (
        point.where(F.col("model_name").isin(*BASELINE_MODELS))
        .orderBy(F.col("run_timestamp_utc").desc(), F.col("run_id").desc())
        .select("run_id")
        .first()
    )
    if row is None:
        raise ValueError("No retained Fabric baseline prediction run is available.")
    return str(row["run_id"])


def _point_rows() -> tuple[DataFrame, str]:
    point = spark.table(POINT_PREDICTIONS_TABLE)
    required = {
        "run_id", "run_timestamp_utc", *GROUP_COLUMNS,
        FEATURE_TIMESTAMP_COLUMN, TARGET_TIMESTAMP_COLUMN, HORIZON_COLUMN,
        "target_tolerance_minutes", "horizon_steps", "horizon_minutes",
        "target_delay_minutes", "split", "model_name", "current_demand_mw",
        "actual_demand_mw", "predicted_demand_mw", "trained_through_utc",
        "feature_contract_version",
    }
    missing = sorted(required - set(point.columns))
    if missing:
        raise ValueError(
            f"{POINT_PREDICTIONS_TABLE} is missing required columns: "
            + ", ".join(missing)
            + "."
        )
    run_id = _selected_run_id(point)
    point = point.where(
        (F.col("run_id") == run_id)
        & F.col("model_name").isin(*BASELINE_MODELS)
        & F.col("split").isin("validation", "test")
    )
    for column, data_type in (
        (ORIGIN_FOLD_COLUMN, "int"),
        ("origin_count", "int"),
        ("origin_cutoff_utc", "timestamp"),
        ("training_observation_count", "long"),
        ("evaluation_contract_version", "string"),
    ):
        if column not in point.columns:
            point = point.withColumn(column, F.lit(None).cast(data_type))
    keys = [
        *GROUP_COLUMNS, HORIZON_COLUMN, "split", ORIGIN_FOLD_COLUMN,
        FEATURE_TIMESTAMP_COLUMN, TARGET_TIMESTAMP_COLUMN,
    ]
    failures = (
        point.groupBy(*keys)
        .agg(
            F.count(F.lit(1)).alias("rows"),
            F.countDistinct("model_name").alias("models"),
            F.countDistinct("actual_demand_mw").alias("actuals"),
            F.countDistinct("trained_through_utc").alias("boundaries"),
        )
        .where(
            (F.col("rows") != 2)
            | (F.col("models") != 2)
            | (F.col("actuals") != 1)
            | (F.col("boundaries") != 1)
        )
        .limit(1)
        .count()
    )
    if failures:
        raise ValueError("Selected point run does not contain exact persistence/ridge pairs.")
    return point, run_id


def _common(point: DataFrame, run_id: str) -> DataFrame:
    keys = [
        *GROUP_COLUMNS, HORIZON_COLUMN, "split", ORIGIN_FOLD_COLUMN,
        FEATURE_TIMESTAMP_COLUMN, TARGET_TIMESTAMP_COLUMN,
    ]
    return (
        point.groupBy(*keys)
        .agg(
            F.first("run_timestamp_utc").alias("point_run_timestamp_utc"),
            F.first("target_tolerance_minutes").alias("target_tolerance_minutes"),
            F.first("horizon_steps").alias("horizon_steps"),
            F.first("horizon_minutes").alias("horizon_minutes"),
            F.first("target_delay_minutes").alias("target_delay_minutes"),
            F.first("current_demand_mw").alias("current_demand_mw"),
            F.first("actual_demand_mw").alias("actual_demand_mw"),
            F.first("trained_through_utc").alias("trained_through_utc"),
            F.first("feature_contract_version").alias("feature_contract_version"),
            F.first("origin_count").alias("origin_count"),
            F.first("origin_cutoff_utc").alias("origin_cutoff_utc"),
            F.first("training_observation_count").alias("training_observation_count"),
            F.first("evaluation_contract_version").alias("evaluation_contract_version"),
        )
        .withColumn("point_prediction_run_id", F.lit(run_id))
    )


def _source_history() -> DataFrame:
    source = spark.table(SOURCE_TABLE)
    required = {*GROUP_COLUMNS, "event_timestamp_utc", "demand_mw"}
    missing = sorted(required - set(source.columns))
    if missing:
        raise ValueError(
            f"{SOURCE_TABLE} is missing required columns: "
            + ", ".join(missing)
            + "."
        )
    source = source.select(
        *GROUP_COLUMNS,
        F.col("event_timestamp_utc").alias("source_event_timestamp_utc"),
        F.col("demand_mw").cast("double").alias("source_demand_mw"),
    ).dropna()
    duplicate = (
        source.groupBy(*GROUP_COLUMNS, "source_event_timestamp_utc")
        .count()
        .where(F.col("count") > 1)
        .limit(1)
        .count()
    )
    if duplicate:
        raise ValueError("Gold demand history contains duplicate group timestamps.")
    window = Window.partitionBy(*GROUP_COLUMNS)
    return (
        source.withColumn(
            "source_min_timestamp_utc",
            F.min("source_event_timestamp_utc").over(window),
        )
        .withColumn(
            "source_max_timestamp_utc",
            F.max("source_event_timestamp_utc").over(window),
        )
    )


def _attach_reference(
    cohort: DataFrame,
    source: DataFrame,
    *,
    name: str,
    minutes: int,
    config: dict[str, Any],
) -> DataFrame:
    bounds = source.select(
        *GROUP_COLUMNS, "source_min_timestamp_utc", "source_max_timestamp_utc"
    ).distinct()
    feature = (
        cohort.join(bounds, on=GROUP_COLUMNS, how="inner")
        .withColumn(
            f"{name}_ideal_timestamp_utc",
            F.to_timestamp(
                F.from_unixtime(
                    F.col(TARGET_TIMESTAMP_COLUMN).cast("long")
                    - F.lit(minutes * 60)
                )
            ),
        )
        .withColumn(
            f"{name}_eligible",
            F.col(f"{name}_ideal_timestamp_utc").between(
                F.col("source_min_timestamp_utc"),
                F.col("source_max_timestamp_utc"),
            ),
        )
    )
    same_group = F.lit(True)
    for column in GROUP_COLUMNS:
        same_group = same_group & (
            F.col(f"feature.{column}") == F.col(f"source.{column}")
        )
    difference_seconds = (
        F.col("source.source_event_timestamp_utc").cast("long")
        - F.col(f"feature.{name}_ideal_timestamp_utc").cast("long")
    )
    condition = (
        same_group
        & F.col(f"feature.{name}_eligible")
        & (F.abs(difference_seconds) <= F.lit(config["reference_tolerance_minutes"] * 60))
        & (F.col("source.source_event_timestamp_utc") <= F.col(f"feature.{FEATURE_TIMESTAMP_COLUMN}"))
    )
    candidates = (
        feature.alias("feature")
        .join(source.alias("source"), condition, "left")
        .select(
            *[F.col(f"feature.{column}").alias(column) for column in feature.columns],
            F.col("source.source_event_timestamp_utc").alias(f"{name}_source_timestamp_utc"),
            F.col("source.source_demand_mw").alias(f"{name}_demand_mw"),
            (difference_seconds / F.lit(60.0)).alias(f"{name}_offset_minutes"),
        )
    )
    keys = [
        *GROUP_COLUMNS, HORIZON_COLUMN, "split", ORIGIN_FOLD_COLUMN,
        FEATURE_TIMESTAMP_COLUMN, TARGET_TIMESTAMP_COLUMN,
    ]
    rank = Window.partitionBy(*keys).orderBy(
        F.abs(F.col(f"{name}_offset_minutes")).asc_nulls_last(),
        (F.col(f"{name}_offset_minutes") > F.lit(0.0)).asc_nulls_last(),
        F.col(f"{name}_source_timestamp_utc").desc_nulls_last(),
    )
    matched = (
        candidates.withColumn("reference_rank", F.row_number().over(rank))
        .where(F.col("reference_rank") == 1)
        .drop("reference_rank")
        .withColumn(f"{name}_absolute_offset_minutes", F.abs(F.col(f"{name}_offset_minutes")))
        .withColumn(
            f"{name}_source_age_minutes",
            (
                F.col(FEATURE_TIMESTAMP_COLUMN).cast("long")
                - F.col(f"{name}_source_timestamp_utc").cast("long")
            ) / F.lit(60.0),
        )
    )
    coverage_keys = [*GROUP_COLUMNS, HORIZON_COLUMN]
    coverage = (
        matched.groupBy(*coverage_keys)
        .agg(
            F.sum(F.when(F.col(f"{name}_eligible"), 1).otherwise(0)).alias(f"{name}_eligible_count"),
            F.sum(
                F.when(
                    F.col(f"{name}_eligible")
                    & F.col(f"{name}_source_timestamp_utc").isNotNull(),
                    1,
                ).otherwise(0)
            ).alias(f"{name}_matched_count"),
        )
        .withColumn(
            f"{name}_coverage_pct",
            F.when(
                F.col(f"{name}_eligible_count") > 0,
                F.col(f"{name}_matched_count") / F.col(f"{name}_eligible_count") * F.lit(100.0),
            ).otherwise(F.lit(0.0)),
        )
    )
    failures = coverage.where(
        (F.col(f"{name}_eligible_count") <= 0)
        | (F.col(f"{name}_coverage_pct") < F.lit(config["min_reference_coverage"] * 100.0))
    ).collect()
    if failures:
        examples = "; ".join(
            f"{row['source_area']}/{row['resource_id']}/{row['city']} "
            f"horizon={row[HORIZON_COLUMN]}m matched={row[f'{name}_matched_count']}/"
            f"{row[f'{name}_eligible_count']}"
            for row in failures[:5]
        )
        raise ValueError(f"{name.replace('_', '-')} coverage is inadequate: {examples}")
    return matched.join(coverage, on=coverage_keys, how="inner")


def _cohort(common: DataFrame, config: dict[str, Any]) -> DataFrame:
    source = _source_history()
    cohort = _attach_reference(common, source, name="previous_day", minutes=1440, config=config)
    cohort = _attach_reference(cohort, source, name="previous_week", minutes=10080, config=config)
    cohort = cohort.dropna(
        subset=[
            "previous_day_source_timestamp_utc", "previous_day_demand_mw",
            "previous_week_source_timestamp_utc", "previous_week_demand_mw",
        ]
    )
    if cohort.limit(1).count() == 0:
        raise ValueError("No rows have both previous-day and previous-week evidence.")
    invalid = cohort.where(
        (F.col("previous_day_source_timestamp_utc") > F.col(FEATURE_TIMESTAMP_COLUMN))
        | (F.col("previous_week_source_timestamp_utc") > F.col(FEATURE_TIMESTAMP_COLUMN))
        | (F.col("previous_day_absolute_offset_minutes") > F.lit(config["reference_tolerance_minutes"]))
        | (F.col("previous_week_absolute_offset_minutes") > F.lit(config["reference_tolerance_minutes"]))
    ).limit(1).count()
    if invalid:
        raise ValueError("Seasonal source evidence violates its causal contract.")
    return cohort


def _reference_columns() -> list[str]:
    return [
        "previous_day_eligible_count", "previous_day_matched_count", "previous_day_coverage_pct",
        "previous_week_eligible_count", "previous_week_matched_count", "previous_week_coverage_pct",
    ]


def _baseline_rows(point: DataFrame, cohort: DataFrame, comparison_id: str, timestamp: datetime) -> DataFrame:
    keys = [
        *GROUP_COLUMNS, HORIZON_COLUMN, "split", ORIGIN_FOLD_COLUMN,
        FEATURE_TIMESTAMP_COLUMN, TARGET_TIMESTAMP_COLUMN,
    ]
    evidence = cohort.select(*keys, "point_prediction_run_id", "point_run_timestamp_utc", *_reference_columns())
    return (
        point.join(evidence, on=keys, how="inner")
        .withColumn("seasonal_comparison_run_id", F.lit(comparison_id))
        .withColumn("seasonal_comparison_run_timestamp_utc", F.lit(timestamp).cast("timestamp"))
        .withColumn("model_fit_required", F.col("model_name") == F.lit("ridge_weather_lag"))
        .withColumn("seasonal_reference_period_minutes", F.lit(None).cast("int"))
        .withColumn("seasonal_reference_ideal_timestamp_utc", F.lit(None).cast("timestamp"))
        .withColumn("seasonal_reference_timestamp_utc", F.lit(None).cast("timestamp"))
        .withColumn("seasonal_reference_demand_mw", F.lit(None).cast("double"))
        .withColumn("seasonal_reference_offset_minutes", F.lit(None).cast("double"))
        .withColumn("seasonal_reference_absolute_offset_minutes", F.lit(None).cast("double"))
        .withColumn("seasonal_reference_source_age_minutes", F.lit(None).cast("double"))
    )


def _seasonal_rows(cohort: DataFrame, comparison_id: str, timestamp: datetime) -> DataFrame:
    frames = []
    for model_name, (name, period) in SEASONAL_MODELS.items():
        prediction = F.col(f"{name}_demand_mw").cast("double")
        frames.append(
            cohort.withColumn("model_name", F.lit(model_name))
            .withColumn("model_fit_required", F.lit(False))
            .withColumn("predicted_demand_mw", prediction)
            .withColumn("absolute_error_mw", F.abs(prediction - F.col("actual_demand_mw")))
            .withColumn("squared_error_mw2", F.pow(prediction - F.col("actual_demand_mw"), 2))
            .withColumn("seasonal_comparison_run_id", F.lit(comparison_id))
            .withColumn("seasonal_comparison_run_timestamp_utc", F.lit(timestamp).cast("timestamp"))
            .withColumn("seasonal_reference_period_minutes", F.lit(period).cast("int"))
            .withColumn("seasonal_reference_ideal_timestamp_utc", F.col(f"{name}_ideal_timestamp_utc"))
            .withColumn("seasonal_reference_timestamp_utc", F.col(f"{name}_source_timestamp_utc"))
            .withColumn("seasonal_reference_demand_mw", F.col(f"{name}_demand_mw").cast("double"))
            .withColumn("seasonal_reference_offset_minutes", F.col(f"{name}_offset_minutes").cast("double"))
            .withColumn("seasonal_reference_absolute_offset_minutes", F.col(f"{name}_absolute_offset_minutes").cast("double"))
            .withColumn("seasonal_reference_source_age_minutes", F.col(f"{name}_source_age_minutes").cast("double"))
        )
    return frames[0].unionByName(frames[1], allowMissingColumns=True)


def _validate(predictions: DataFrame, config: dict[str, Any]) -> None:
    models = {row["model_name"] for row in predictions.select("model_name").distinct().collect()}
    if models != set(ALL_MODELS):
        raise ValueError(f"Expected {sorted(ALL_MODELS)}, found {sorted(models)}.")
    keys = [
        *GROUP_COLUMNS, HORIZON_COLUMN, "split", ORIGIN_FOLD_COLUMN,
        FEATURE_TIMESTAMP_COLUMN, TARGET_TIMESTAMP_COLUMN,
    ]
    pairs = (
        predictions.groupBy(*keys)
        .agg(
            F.count(F.lit(1)).alias("rows"),
            F.countDistinct("model_name").alias("models"),
            F.countDistinct("actual_demand_mw").alias("actuals"),
            F.countDistinct("trained_through_utc").alias("boundaries"),
        )
        .where(
            (F.col("rows") != len(ALL_MODELS))
            | (F.col("models") != len(ALL_MODELS))
            | (F.col("actuals") != 1)
            | (F.col("boundaries") != 1)
        )
        .limit(1)
        .count()
    )
    if pairs:
        raise ValueError("Seasonal models do not form exact paired rows.")
    seasonal = predictions.where(F.col("model_name").isin(*SEASONAL_MODELS))
    invalid = seasonal.where(
        F.col("seasonal_reference_timestamp_utc").isNull()
        | (F.col("seasonal_reference_timestamp_utc") > F.col(FEATURE_TIMESTAMP_COLUMN))
        | (F.col("seasonal_reference_source_age_minutes") < 0)
        | (F.col("seasonal_reference_absolute_offset_minutes") > F.lit(config["reference_tolerance_minutes"]))
        | ((F.col("model_name") == "seasonal_previous_day") & (F.col("seasonal_reference_period_minutes") != 1440))
        | ((F.col("model_name") == "seasonal_previous_week") & (F.col("seasonal_reference_period_minutes") != 10080))
    ).limit(1).count()
    if invalid:
        raise ValueError("Seasonal prediction evidence violates its reference contract.")


def _metrics(predictions: DataFrame, config: dict[str, Any]) -> DataFrame:
    error = F.col("predicted_demand_mw") - F.col("actual_demand_mw")
    return (
        predictions.groupBy(
            "seasonal_comparison_run_id", "seasonal_comparison_run_timestamp_utc",
            "point_prediction_run_id", *GROUP_COLUMNS, HORIZON_COLUMN, "split",
            ORIGIN_FOLD_COLUMN, "origin_count", "origin_cutoff_utc", "model_name",
            "model_fit_required", "trained_through_utc", "training_observation_count",
            "feature_contract_version", "evaluation_contract_version",
        )
        .agg(
            F.count(F.lit(1)).alias("observation_count"),
            *[F.first(column).alias(column) for column in _reference_columns()],
            F.avg(F.abs(error)).alias("mae_mw"),
            F.sqrt(F.avg(F.pow(error, 2))).alias("rmse_mw"),
            F.avg(error).alias("bias_mw"),
            F.min(FEATURE_TIMESTAMP_COLUMN).alias("evaluation_feature_start_utc"),
            F.max(FEATURE_TIMESTAMP_COLUMN).alias("evaluation_feature_end_utc"),
            F.min(TARGET_TIMESTAMP_COLUMN).alias("evaluation_start_utc"),
            F.max(TARGET_TIMESTAMP_COLUMN).alias("evaluation_end_utc"),
        )
        .withColumn("seasonal_reference_tolerance_minutes", F.lit(config["reference_tolerance_minutes"]))
        .withColumn("seasonal_baseline_contract_version", F.lit(SEASONAL_CONTRACT_VERSION))
    )


def run_comparison() -> tuple[DataFrame, DataFrame]:
    config = _configuration()
    point, point_run_id = _point_rows()
    cohort = _cohort(_common(point, point_run_id), config)
    comparison_id = str(uuid4())
    timestamp = datetime.now(timezone.utc)
    predictions = (
        _baseline_rows(point, cohort, comparison_id, timestamp)
        .unionByName(_seasonal_rows(cohort, comparison_id, timestamp), allowMissingColumns=True)
        .withColumn("seasonal_reference_tolerance_minutes", F.lit(config["reference_tolerance_minutes"]))
        .withColumn("seasonal_baseline_contract_version", F.lit(SEASONAL_CONTRACT_VERSION))
    )
    _validate(predictions, config)
    metrics = _metrics(predictions, config)
    predictions.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(PREDICTIONS_TABLE)
    metrics.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(METRICS_TABLE)
    predictions.orderBy(*GROUP_COLUMNS, HORIZON_COLUMN, "split", ORIGIN_FOLD_COLUMN, "model_name", FEATURE_TIMESTAMP_COLUMN).show(20, truncate=False)
    metrics.orderBy(*GROUP_COLUMNS, HORIZON_COLUMN, "split", ORIGIN_FOLD_COLUMN, "model_name").show(truncate=False)
    return predictions, metrics


if __name__ == "__main__":
    run_comparison()

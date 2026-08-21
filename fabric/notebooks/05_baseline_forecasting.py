# Fabric notebook source: 05_baseline_forecasting
# Historical 30/60-minute demand backtesting with bounded target matching.

from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from pyspark import StorageLevel
from pyspark.ml import Pipeline
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

spark.conf.set("spark.sql.session.timeZone", "UTC")

SOURCE_TABLE = "gold_feature_engineering"
PREDICTIONS_TABLE = "forecast_baseline_predictions"
METRICS_TABLE = "forecast_baseline_metrics"
GROUP_COLUMNS = ["source_area", "resource_id", "city"]
REQUESTED_HORIZON_COLUMN = "requested_horizon_minutes"
TARGET_TOLERANCE_COLUMN = "target_tolerance_minutes"
MODEL_GROUP_COLUMNS = [*GROUP_COLUMNS, REQUESTED_HORIZON_COLUMN]
SOURCE_TIMESTAMP_COLUMN = "event_timestamp_utc"
FEATURE_TIMESTAMP_COLUMN = "feature_timestamp_utc"
TARGET_TIMESTAMP_COLUMN = "target_timestamp_utc"
TARGET_COLUMN = "demand_mw"
SUPERVISED_TARGET_COLUMN = "target_demand_mw"
FEATURE_COLUMNS = [
    "demand_mw",
    "demand_lag_1",
    "demand_rolling_mean_12",
    "temperature",
    "humidity",
    "hour_of_day_utc",
    "day_of_week_utc",
    "is_weekend_utc",
    "weather_age_minutes",
]
SUPPORTED_HORIZON_MINUTES = (30, 60)
FEATURE_CONTRACT_VERSION = "time-horizon-v1"

TRAIN_FRACTION = 0.60
VALIDATION_FRACTION = 0.20
MIN_TRAIN_ROWS = 24
MIN_VALIDATION_ROWS = 6
MIN_TEST_ROWS = 6
RIDGE_REG_PARAM = 1.0
HORIZON_MINUTES = "30,60"
TARGET_TOLERANCE_MINUTES = 5
MIN_TARGET_COVERAGE = 0.90


def _get_parameter(name: str, default: Any) -> Any:
    return globals().get(name, default)


def _integer(value: Any, name: str, *, minimum: int) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be an integer >= {minimum}.")
    parsed = int(value)
    if parsed < minimum:
        raise ValueError(f"{name} must be an integer >= {minimum}.")
    return parsed


def _fraction(value: Any, name: str, *, allow_one: bool = False) -> float:
    parsed = float(value)
    valid = 0 < parsed <= 1 if allow_one else 0 < parsed < 1
    if not valid:
        upper = "at most 1" if allow_one else "less than 1"
        raise ValueError(f"{name} must be greater than 0 and {upper}.")
    return parsed


def _horizons(value: Any) -> tuple[int, ...]:
    if isinstance(value, str):
        values = [part.strip() for part in value.split(",") if part.strip()]
    elif isinstance(value, (list, tuple, set)):
        values = list(value)
    else:
        values = [value]
    horizons = tuple(sorted(int(item) for item in values))
    if not horizons or len(set(horizons)) != len(horizons):
        raise ValueError("HORIZON_MINUTES must contain unique values.")
    unsupported = [item for item in horizons if item not in SUPPORTED_HORIZON_MINUTES]
    if unsupported:
        raise ValueError(
            f"Unsupported HORIZON_MINUTES={unsupported}; supported values are 30 and 60."
        )
    return horizons


def _configuration() -> dict[str, Any]:
    train_fraction = _fraction(
        _get_parameter("TRAIN_FRACTION", TRAIN_FRACTION), "TRAIN_FRACTION"
    )
    validation_fraction = _fraction(
        _get_parameter("VALIDATION_FRACTION", VALIDATION_FRACTION),
        "VALIDATION_FRACTION",
    )
    if train_fraction + validation_fraction >= 1:
        raise ValueError(
            "TRAIN_FRACTION + VALIDATION_FRACTION must leave a test split."
        )
    ridge_reg_param = float(_get_parameter("RIDGE_REG_PARAM", RIDGE_REG_PARAM))
    if ridge_reg_param <= 0:
        raise ValueError("RIDGE_REG_PARAM must be positive.")
    return {
        "train_fraction": train_fraction,
        "validation_fraction": validation_fraction,
        "min_train_rows": _integer(
            _get_parameter("MIN_TRAIN_ROWS", MIN_TRAIN_ROWS),
            "MIN_TRAIN_ROWS",
            minimum=1,
        ),
        "min_validation_rows": _integer(
            _get_parameter("MIN_VALIDATION_ROWS", MIN_VALIDATION_ROWS),
            "MIN_VALIDATION_ROWS",
            minimum=1,
        ),
        "min_test_rows": _integer(
            _get_parameter("MIN_TEST_ROWS", MIN_TEST_ROWS),
            "MIN_TEST_ROWS",
            minimum=1,
        ),
        "ridge_reg_param": ridge_reg_param,
        "horizon_minutes": _horizons(
            _get_parameter("HORIZON_MINUTES", HORIZON_MINUTES)
        ),
        "target_tolerance_minutes": _integer(
            _get_parameter("TARGET_TOLERANCE_MINUTES", TARGET_TOLERANCE_MINUTES),
            "TARGET_TOLERANCE_MINUTES",
            minimum=0,
        ),
        "min_target_coverage": _fraction(
            _get_parameter("MIN_TARGET_COVERAGE", MIN_TARGET_COVERAGE),
            "MIN_TARGET_COVERAGE",
            allow_one=True,
        ),
    }


def _target_matches(source: DataFrame, config: dict[str, Any]) -> DataFrame:
    order = Window.partitionBy(*GROUP_COLUMNS).orderBy(SOURCE_TIMESTAMP_COLUMN)
    group = Window.partitionBy(*GROUP_COLUMNS)
    source = (
        source.withColumn("_source_row_number", F.row_number().over(order))
        .withColumn(
            "_source_max_timestamp",
            F.max(SOURCE_TIMESTAMP_COLUMN).over(group),
        )
    )
    horizon_rows = [
        (int(value), int(config["target_tolerance_minutes"]))
        for value in config["horizon_minutes"]
    ]
    horizons = spark.createDataFrame(
        horizon_rows,
        f"{REQUESTED_HORIZON_COLUMN} int, {TARGET_TOLERANCE_COLUMN} int",
    )
    expected_combinations = (
        source.select(*GROUP_COLUMNS).distinct().crossJoin(F.broadcast(horizons))
    )
    features = (
        source.crossJoin(F.broadcast(horizons))
        .withColumn(FEATURE_TIMESTAMP_COLUMN, F.col(SOURCE_TIMESTAMP_COLUMN))
        .withColumn(
            "_ideal_target_epoch",
            F.col(FEATURE_TIMESTAMP_COLUMN).cast("long")
            + F.col(REQUESTED_HORIZON_COLUMN) * F.lit(60),
        )
        .withColumn(
            "_latest_target_epoch",
            F.col("_ideal_target_epoch")
            + F.col(TARGET_TOLERANCE_COLUMN) * F.lit(60),
        )
        .where(
            F.col("_ideal_target_epoch")
            <= F.col("_source_max_timestamp").cast("long")
        )
    )
    targets = source.select(
        *GROUP_COLUMNS,
        F.col(SOURCE_TIMESTAMP_COLUMN).alias(TARGET_TIMESTAMP_COLUMN),
        F.col(TARGET_COLUMN).alias(SUPERVISED_TARGET_COLUMN),
        F.col("_source_row_number").alias("_target_row_number"),
    )
    same_group = F.lit(True)
    for column in GROUP_COLUMNS:
        same_group = same_group & (
            F.col(f"feature.{column}") == F.col(f"target.{column}")
        )
    within_window = (
        F.col(f"target.{TARGET_TIMESTAMP_COLUMN}").cast("long")
        >= F.col("feature._ideal_target_epoch")
    ) & (
        F.col(f"target.{TARGET_TIMESTAMP_COLUMN}").cast("long")
        <= F.col("feature._latest_target_epoch")
    )
    candidates = (
        features.alias("feature")
        .join(targets.alias("target"), same_group & within_window, "left")
        .select(
            *[F.col(f"feature.{column}").alias(column) for column in features.columns],
            F.col(f"target.{TARGET_TIMESTAMP_COLUMN}").alias(TARGET_TIMESTAMP_COLUMN),
            F.col(f"target.{SUPERVISED_TARGET_COLUMN}").alias(
                SUPERVISED_TARGET_COLUMN
            ),
            F.col("target._target_row_number").alias("_target_row_number"),
        )
    )
    rank = Window.partitionBy(
        *GROUP_COLUMNS,
        FEATURE_TIMESTAMP_COLUMN,
        REQUESTED_HORIZON_COLUMN,
    ).orderBy(F.col(TARGET_TIMESTAMP_COLUMN).asc_nulls_last())
    matched = (
        candidates.withColumn("_match_rank", F.row_number().over(rank))
        .where(F.col("_match_rank") == 1)
        .drop("_match_rank")
    )
    coverage_counts = matched.groupBy(
        *MODEL_GROUP_COLUMNS, TARGET_TOLERANCE_COLUMN
    ).agg(
        F.count(F.lit(1)).alias("eligible_target_count"),
        F.sum(
            F.when(F.col(TARGET_TIMESTAMP_COLUMN).isNotNull(), 1).otherwise(0)
        ).alias("matched_target_count"),
    )
    coverage = (
        expected_combinations.join(
            coverage_counts,
            on=[*MODEL_GROUP_COLUMNS, TARGET_TOLERANCE_COLUMN],
            how="left",
        )
        .fillna({"eligible_target_count": 0, "matched_target_count": 0})
        .withColumn(
            "target_coverage_pct",
            F.when(
                F.col("eligible_target_count") > 0,
                F.col("matched_target_count")
                / F.col("eligible_target_count")
                * F.lit(100.0),
            ).otherwise(F.lit(0.0)),
        )
    )
    bad_coverage = coverage.where(
        (F.col("eligible_target_count") <= 0)
        | (
            F.col("target_coverage_pct")
            < F.lit(config["min_target_coverage"] * 100.0)
        )
    ).collect()
    if bad_coverage:
        examples = "; ".join(
            f"{row['source_area']}/{row['resource_id']}/{row['city']} "
            f"horizon={row[REQUESTED_HORIZON_COLUMN]}m "
            f"matched={row['matched_target_count']}/{row['eligible_target_count']}"
            for row in bad_coverage[:5]
        )
        raise ValueError(
            "Forecast target coverage is below MIN_TARGET_COVERAGE: " + examples
        )
    return (
        matched.where(F.col(TARGET_TIMESTAMP_COLUMN).isNotNull())
        .join(
            coverage,
            on=[*MODEL_GROUP_COLUMNS, TARGET_TOLERANCE_COLUMN],
            how="inner",
        )
        .withColumn(
            "horizon_steps",
            F.col("_target_row_number") - F.col("_source_row_number"),
        )
        .withColumn(
            "horizon_minutes",
            (
                F.col(TARGET_TIMESTAMP_COLUMN).cast("long")
                - F.col(FEATURE_TIMESTAMP_COLUMN).cast("long")
            )
            / F.lit(60.0),
        )
        .withColumn(
            "target_delay_minutes",
            F.col("horizon_minutes") - F.col(REQUESTED_HORIZON_COLUMN),
        )
    )


def _prepare_features(config: dict[str, Any]) -> DataFrame:
    required = list(
        dict.fromkeys(
            [*GROUP_COLUMNS, SOURCE_TIMESTAMP_COLUMN, TARGET_COLUMN, *FEATURE_COLUMNS]
        )
    )
    source = spark.table(SOURCE_TABLE)
    missing = sorted(set(required) - set(source.columns))
    if missing:
        raise ValueError(
            f"{SOURCE_TABLE} is missing required columns: {', '.join(missing)}."
        )
    source = source.select(*required).dropna(subset=required)
    if (
        source.groupBy(*GROUP_COLUMNS, SOURCE_TIMESTAMP_COLUMN)
        .count()
        .where(F.col("count") > 1)
        .limit(1)
        .count()
    ):
        raise ValueError("Forecast groups contain duplicate event timestamps.")
    prepared = _target_matches(source, config)
    invalid = prepared.where(
        (F.col(TARGET_TIMESTAMP_COLUMN) <= F.col(FEATURE_TIMESTAMP_COLUMN))
        | (F.col("horizon_steps") < 1)
        | (F.col("horizon_minutes") < F.col(REQUESTED_HORIZON_COLUMN))
        | (F.col("target_delay_minutes") < 0)
        | (F.col("target_delay_minutes") > F.col(TARGET_TOLERANCE_COLUMN))
    ).limit(1).count()
    if invalid:
        raise ValueError("Matched forecast targets violate the time-horizon contract.")

    order = Window.partitionBy(*MODEL_GROUP_COLUMNS).orderBy(FEATURE_TIMESTAMP_COLUMN)
    group = Window.partitionBy(*MODEL_GROUP_COLUMNS)
    prepared = (
        prepared.withColumn("_row_number", F.row_number().over(order))
        .withColumn("_group_count", F.count(F.lit(1)).over(group))
        .withColumn(
            "_train_end",
            F.floor(F.col("_group_count") * F.lit(config["train_fraction"])).cast(
                "int"
            ),
        )
        .withColumn(
            "_validation_end",
            F.floor(
                F.col("_group_count")
                * F.lit(config["train_fraction"] + config["validation_fraction"])
            ).cast("int"),
        )
        .withColumn(
            "split",
            F.when(F.col("_row_number") <= F.col("_train_end"), F.lit("train"))
            .when(
                F.col("_row_number") <= F.col("_validation_end"),
                F.lit("validation"),
            )
            .otherwise(F.lit("test")),
        )
    )
    bad_splits = (
        prepared.groupBy(*MODEL_GROUP_COLUMNS)
        .agg(
            F.sum(F.when(F.col("split") == "train", 1).otherwise(0)).alias(
                "train_rows"
            ),
            F.sum(F.when(F.col("split") == "validation", 1).otherwise(0)).alias(
                "validation_rows"
            ),
            F.sum(F.when(F.col("split") == "test", 1).otherwise(0)).alias(
                "test_rows"
            ),
        )
        .where(
            (F.col("train_rows") < F.lit(config["min_train_rows"]))
            | (F.col("validation_rows") < F.lit(config["min_validation_rows"]))
            | (F.col("test_rows") < F.lit(config["min_test_rows"]))
        )
        .collect()
    )
    if bad_splits:
        examples = "; ".join(
            f"{row['source_area']}/{row['resource_id']}/{row['city']} "
            f"horizon={row[REQUESTED_HORIZON_COLUMN]}m: "
            f"train={row['train_rows']}, validation={row['validation_rows']}, "
            f"test={row['test_rows']}"
            for row in bad_splits[:5]
        )
        raise ValueError(f"Forecast groups have insufficient history: {examples}.")
    return prepared.persist(StorageLevel.MEMORY_AND_DISK)


def _group_filter(row: Any):
    condition = F.lit(True)
    for column in MODEL_GROUP_COLUMNS:
        condition = condition & (F.col(column) == F.lit(row[column]))
    return condition


def _fit_ridge(training: DataFrame, reg_param: float):
    return Pipeline(
        stages=[
            VectorAssembler(
                inputCols=FEATURE_COLUMNS,
                outputCol="_unscaled_features",
                handleInvalid="error",
            ),
            StandardScaler(
                inputCol="_unscaled_features",
                outputCol="_features",
                withMean=True,
                withStd=True,
            ),
            LinearRegression(
                featuresCol="_features",
                labelCol=SUPERVISED_TARGET_COLUMN,
                predictionCol="predicted_demand_mw",
                regParam=reg_param,
                elasticNetParam=0.0,
                fitIntercept=True,
                standardization=False,
                maxIter=200,
                tol=1e-8,
            ),
        ]
    ).fit(training)


def _purge_training(
    training: DataFrame,
    evaluation: DataFrame,
    *,
    min_train_rows: int,
) -> tuple[DataFrame, datetime]:
    evaluation_start = evaluation.agg(F.min(FEATURE_TIMESTAMP_COLUMN)).first()[0]
    if evaluation_start is None:
        raise ValueError("Evaluation split has no rows.")
    purged = training.where(
        F.col(TARGET_TIMESTAMP_COLUMN) < F.lit(evaluation_start)
    ).persist(StorageLevel.MEMORY_AND_DISK)
    row_count = purged.count()
    if row_count < min_train_rows:
        purged.unpersist()
        raise ValueError(
            "Purging overlapping time-horizon labels left "
            f"{row_count} training rows; minimum is {min_train_rows}."
        )
    return purged, purged.agg(F.max(TARGET_TIMESTAMP_COLUMN)).first()[0]


def _decorate(
    frame: DataFrame,
    *,
    split: str,
    model_name: str,
    trained_through_utc: datetime,
    run_id: str,
    run_timestamp_utc: datetime,
    min_target_coverage: float,
) -> DataFrame:
    return frame.select(
        F.lit(run_id).alias("run_id"),
        F.lit(run_timestamp_utc).cast("timestamp").alias("run_timestamp_utc"),
        *GROUP_COLUMNS,
        F.col(FEATURE_TIMESTAMP_COLUMN),
        F.col(TARGET_TIMESTAMP_COLUMN).alias(SOURCE_TIMESTAMP_COLUMN),
        F.col(REQUESTED_HORIZON_COLUMN).cast("int"),
        F.col(TARGET_TOLERANCE_COLUMN).cast("int"),
        F.col("horizon_steps").cast("int"),
        F.col("horizon_minutes").cast("double"),
        F.col("target_delay_minutes").cast("double"),
        F.col("eligible_target_count").cast("long"),
        F.col("matched_target_count").cast("long"),
        F.col("target_coverage_pct").cast("double"),
        F.lit(min_target_coverage * 100.0).alias("minimum_target_coverage_pct"),
        F.lit(split).alias("split"),
        F.lit(model_name).alias("model_name"),
        F.col(TARGET_COLUMN).cast("double").alias("current_demand_mw"),
        F.col(SUPERVISED_TARGET_COLUMN).cast("double").alias("actual_demand_mw"),
        F.col("predicted_demand_mw").cast("double"),
        F.abs(F.col("predicted_demand_mw") - F.col(SUPERVISED_TARGET_COLUMN)).alias(
            "absolute_error_mw"
        ),
        F.pow(
            F.col("predicted_demand_mw") - F.col(SUPERVISED_TARGET_COLUMN), 2
        ).alias("squared_error_mw2"),
        F.lit(trained_through_utc).cast("timestamp").alias("trained_through_utc"),
        F.lit(FEATURE_CONTRACT_VERSION).alias("feature_contract_version"),
    )


def _evaluate_group(
    group_frame: DataFrame,
    config: dict[str, Any],
    *,
    run_id: str,
    run_timestamp_utc: datetime,
) -> list[DataFrame]:
    train = group_frame.where(F.col("split") == "train")
    validation = group_frame.where(F.col("split") == "validation")
    test = group_frame.where(F.col("split") == "test")
    validation_training, validation_boundary = _purge_training(
        train, validation, min_train_rows=config["min_train_rows"]
    )
    test_training, test_boundary = _purge_training(
        train.unionByName(validation),
        test,
        min_train_rows=config["min_train_rows"],
    )
    validation_ridge = _fit_ridge(
        validation_training, config["ridge_reg_param"]
    ).transform(validation)
    test_ridge = _fit_ridge(test_training, config["ridge_reg_param"]).transform(test)
    common = {
        "run_id": run_id,
        "run_timestamp_utc": run_timestamp_utc,
        "min_target_coverage": config["min_target_coverage"],
    }
    frames = [
        _decorate(
            validation.withColumn(
                "predicted_demand_mw", F.col(TARGET_COLUMN).cast("double")
            ),
            split="validation",
            model_name="persistence_current_value",
            trained_through_utc=validation_boundary,
            **common,
        ),
        _decorate(
            validation_ridge,
            split="validation",
            model_name="ridge_weather_lag",
            trained_through_utc=validation_boundary,
            **common,
        ),
        _decorate(
            test.withColumn(
                "predicted_demand_mw", F.col(TARGET_COLUMN).cast("double")
            ),
            split="test",
            model_name="persistence_current_value",
            trained_through_utc=test_boundary,
            **common,
        ),
        _decorate(
            test_ridge,
            split="test",
            model_name="ridge_weather_lag",
            trained_through_utc=test_boundary,
            **common,
        ),
    ]
    validation_training.unpersist()
    test_training.unpersist()
    return frames


def _union(frames: list[DataFrame]) -> DataFrame:
    if not frames:
        raise ValueError("No forecast groups produced predictions.")
    combined = frames[0]
    for frame in frames[1:]:
        combined = combined.unionByName(frame)
    return combined


def _build_metrics(predictions: DataFrame) -> DataFrame:
    error = F.col("predicted_demand_mw") - F.col("actual_demand_mw")
    return predictions.groupBy(
        "run_id",
        "run_timestamp_utc",
        *GROUP_COLUMNS,
        REQUESTED_HORIZON_COLUMN,
        TARGET_TOLERANCE_COLUMN,
        "split",
        "model_name",
        "trained_through_utc",
        "feature_contract_version",
    ).agg(
        F.count(F.lit(1)).alias("observation_count"),
        F.first("eligible_target_count").alias("eligible_target_count"),
        F.first("matched_target_count").alias("matched_target_count"),
        F.first("target_coverage_pct").alias("target_coverage_pct"),
        F.first("minimum_target_coverage_pct").alias(
            "minimum_target_coverage_pct"
        ),
        F.avg("horizon_steps").alias("horizon_steps_avg"),
        F.avg("horizon_minutes").alias("horizon_minutes_avg"),
        F.avg("target_delay_minutes").alias("target_delay_minutes_avg"),
        F.max("target_delay_minutes").alias("target_delay_minutes_max"),
        F.avg(F.abs(error)).alias("mae_mw"),
        F.sqrt(F.avg(F.pow(error, 2))).alias("rmse_mw"),
        F.avg(
            F.when(
                F.abs(F.col("actual_demand_mw")) > F.lit(1e-12),
                F.abs(error) / F.abs(F.col("actual_demand_mw")) * F.lit(100.0),
            )
        ).alias("mape_pct"),
        F.avg(error).alias("bias_mw"),
        F.min(FEATURE_TIMESTAMP_COLUMN).alias("evaluation_feature_start_utc"),
        F.max(FEATURE_TIMESTAMP_COLUMN).alias("evaluation_feature_end_utc"),
        F.min(SOURCE_TIMESTAMP_COLUMN).alias("evaluation_start_utc"),
        F.max(SOURCE_TIMESTAMP_COLUMN).alias("evaluation_end_utc"),
    )


def run_backtest() -> tuple[DataFrame, DataFrame]:
    config = _configuration()
    run_id = str(uuid4())
    run_timestamp_utc = datetime.now(timezone.utc)
    prepared = _prepare_features(config)
    frames: list[DataFrame] = []
    groups = (
        prepared.select(*MODEL_GROUP_COLUMNS)
        .distinct()
        .orderBy(*MODEL_GROUP_COLUMNS)
        .collect()
    )
    for group_row in groups:
        frames.extend(
            _evaluate_group(
                prepared.where(_group_filter(group_row)),
                config,
                run_id=run_id,
                run_timestamp_utc=run_timestamp_utc,
            )
        )
    predictions = _union(frames).persist(StorageLevel.MEMORY_AND_DISK)
    if predictions.where(
        F.col(FEATURE_TIMESTAMP_COLUMN) <= F.col("trained_through_utc")
    ).limit(1).count():
        raise ValueError("A prediction used labels that were not known at feature time.")
    if predictions.where(
        (F.col(SOURCE_TIMESTAMP_COLUMN) <= F.col(FEATURE_TIMESTAMP_COLUMN))
        | (~F.col(REQUESTED_HORIZON_COLUMN).isin(*SUPPORTED_HORIZON_MINUTES))
        | (F.col(TARGET_TOLERANCE_COLUMN) < 0)
        | (F.col("horizon_steps") < 1)
        | (F.col("horizon_minutes") < F.col(REQUESTED_HORIZON_COLUMN))
        | (F.col("target_delay_minutes") < 0)
        | (F.col("target_delay_minutes") > F.col(TARGET_TOLERANCE_COLUMN))
        | (
            F.abs(
                F.col("horizon_minutes")
                - F.col(REQUESTED_HORIZON_COLUMN)
                - F.col("target_delay_minutes")
            )
            > F.lit(1e-6)
        )
    ).limit(1).count():
        raise ValueError("Forecast prediction time horizons are invalid.")
    if predictions.where(
        F.col("predicted_demand_mw").isNull()
        | F.isnan("predicted_demand_mw")
        | F.col("predicted_demand_mw").isin(float("inf"), float("-inf"))
    ).limit(1).count():
        raise ValueError("Forecast predictions contain null or non-finite values.")
    metrics = _build_metrics(predictions)
    if metrics.limit(1).count() == 0:
        raise ValueError("No forecast metrics were produced.")
    predictions.write.format("delta").mode("append").option(
        "mergeSchema", "true"
    ).saveAsTable(PREDICTIONS_TABLE)
    metrics.write.format("delta").mode("append").option(
        "mergeSchema", "true"
    ).saveAsTable(METRICS_TABLE)
    predictions.orderBy(
        *GROUP_COLUMNS,
        REQUESTED_HORIZON_COLUMN,
        "split",
        "model_name",
        SOURCE_TIMESTAMP_COLUMN,
    ).show(20, truncate=False)
    metrics.orderBy(
        *GROUP_COLUMNS, REQUESTED_HORIZON_COLUMN, "split", "model_name"
    ).show(truncate=False)
    prepared.unpersist()
    predictions.unpersist()
    return predictions, metrics


if __name__ == "__main__":
    run_backtest()

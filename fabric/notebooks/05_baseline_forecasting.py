# Fabric notebook source: 05_baseline_forecasting
#
# Runs chronological persistence and ridge baselines over gold features.
# This is a historical one-step benchmark using causal observed weather. It is
# not a production future-weather forecast.

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
FEATURE_COLUMNS = [
    "demand_lag_1",
    "demand_rolling_mean_12",
    "temperature",
    "humidity",
    "hour_of_day_utc",
    "day_of_week_utc",
    "is_weekend_utc",
    "weather_age_minutes",
]
TARGET_COLUMN = "demand_mw"
TIMESTAMP_COLUMN = "event_timestamp_utc"
FEATURE_CONTRACT_VERSION = "baseline-v1"

TRAIN_FRACTION = 0.60
VALIDATION_FRACTION = 0.20
MIN_TRAIN_ROWS = 24
MIN_VALIDATION_ROWS = 6
MIN_TEST_ROWS = 6
RIDGE_REG_PARAM = 1.0


def _get_parameter(name: str, default: Any) -> Any:
    return globals().get(name, default)


def _positive_int(value: Any, name: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a positive integer.")
    parsed = int(value)
    if parsed < 1:
        raise ValueError(f"{name} must be a positive integer.")
    return parsed


def _fraction(value: Any, name: str) -> float:
    parsed = float(value)
    if not 0 < parsed < 1:
        raise ValueError(f"{name} must be between 0 and 1.")
    return parsed


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
        "min_train_rows": _positive_int(
            _get_parameter("MIN_TRAIN_ROWS", MIN_TRAIN_ROWS), "MIN_TRAIN_ROWS"
        ),
        "min_validation_rows": _positive_int(
            _get_parameter("MIN_VALIDATION_ROWS", MIN_VALIDATION_ROWS),
            "MIN_VALIDATION_ROWS",
        ),
        "min_test_rows": _positive_int(
            _get_parameter("MIN_TEST_ROWS", MIN_TEST_ROWS), "MIN_TEST_ROWS"
        ),
        "ridge_reg_param": ridge_reg_param,
    }


def _prepare_features(config: dict[str, Any]) -> DataFrame:
    required_columns = [
        *GROUP_COLUMNS,
        TIMESTAMP_COLUMN,
        TARGET_COLUMN,
        *FEATURE_COLUMNS,
    ]
    source = spark.table(SOURCE_TABLE)
    missing = sorted(set(required_columns) - set(source.columns))
    if missing:
        raise ValueError(
            f"{SOURCE_TABLE} is missing required columns: {', '.join(missing)}."
        )

    prepared = source.select(*required_columns).dropna(subset=required_columns)
    duplicate_count = (
        prepared.groupBy(*GROUP_COLUMNS, TIMESTAMP_COLUMN)
        .count()
        .where(F.col("count") > 1)
        .limit(1)
        .count()
    )
    if duplicate_count:
        raise ValueError("Forecast groups contain duplicate event timestamps.")

    order_window = Window.partitionBy(*GROUP_COLUMNS).orderBy(TIMESTAMP_COLUMN)
    group_window = Window.partitionBy(*GROUP_COLUMNS)
    train_fraction = F.lit(config["train_fraction"])
    validation_end_fraction = F.lit(
        config["train_fraction"] + config["validation_fraction"]
    )

    prepared = (
        prepared
        .withColumn("_row_number", F.row_number().over(order_window))
        .withColumn("_group_count", F.count(F.lit(1)).over(group_window))
        .withColumn(
            "_train_end",
            F.floor(F.col("_group_count") * train_fraction).cast("int"),
        )
        .withColumn(
            "_validation_end",
            F.floor(F.col("_group_count") * validation_end_fraction).cast("int"),
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

    split_counts = (
        prepared.groupBy(*GROUP_COLUMNS)
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
    if split_counts:
        examples = "; ".join(
            (
                f"{row['source_area']}/{row['resource_id']}/{row['city']}: "
                f"train={row['train_rows']}, validation={row['validation_rows']}, "
                f"test={row['test_rows']}"
            )
            for row in split_counts[:5]
        )
        raise ValueError(f"Forecast groups have insufficient history: {examples}.")

    if prepared.limit(1).count() == 0:
        raise ValueError("No complete gold feature rows are available for backtesting.")
    return prepared.persist(StorageLevel.MEMORY_AND_DISK)


def _group_filter(group_row: Any):
    condition = F.lit(True)
    for column in GROUP_COLUMNS:
        condition = condition & (F.col(column) == F.lit(group_row[column]))
    return condition


def _fit_ridge(training: DataFrame, reg_param: float):
    assembler = VectorAssembler(
        inputCols=FEATURE_COLUMNS,
        outputCol="_unscaled_features",
        handleInvalid="error",
    )
    scaler = StandardScaler(
        inputCol="_unscaled_features",
        outputCol="_features",
        withMean=True,
        withStd=True,
    )
    regression = LinearRegression(
        featuresCol="_features",
        labelCol=TARGET_COLUMN,
        predictionCol="predicted_demand_mw",
        regParam=reg_param,
        elasticNetParam=0.0,
        fitIntercept=True,
        standardization=False,
        maxIter=200,
        tol=1e-8,
    )
    return Pipeline(stages=[assembler, scaler, regression]).fit(training)


def _decorate_predictions(
    frame: DataFrame,
    *,
    split: str,
    model_name: str,
    trained_through_utc: datetime,
    run_id: str,
    run_timestamp_utc: datetime,
) -> DataFrame:
    return frame.select(
        F.lit(run_id).alias("run_id"),
        F.lit(run_timestamp_utc).cast("timestamp").alias("run_timestamp_utc"),
        *GROUP_COLUMNS,
        F.col(TIMESTAMP_COLUMN),
        F.lit(split).alias("split"),
        F.lit(model_name).alias("model_name"),
        F.col(TARGET_COLUMN).cast("double").alias("actual_demand_mw"),
        F.col("predicted_demand_mw").cast("double"),
        F.abs(F.col("predicted_demand_mw") - F.col(TARGET_COLUMN)).alias(
            "absolute_error_mw"
        ),
        F.pow(F.col("predicted_demand_mw") - F.col(TARGET_COLUMN), 2).alias(
            "squared_error_mw2"
        ),
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

    validation_training_end = train.agg(F.max(TIMESTAMP_COLUMN)).first()[0]
    validation_model = _fit_ridge(train, config["ridge_reg_param"])
    validation_ridge = validation_model.transform(validation)
    validation_persistence = validation.withColumn(
        "predicted_demand_mw", F.col("demand_lag_1").cast("double")
    )

    test_training = train.unionByName(validation)
    test_training_end = test_training.agg(F.max(TIMESTAMP_COLUMN)).first()[0]
    test_model = _fit_ridge(test_training, config["ridge_reg_param"])
    test_ridge = test_model.transform(test)
    test_persistence = test.withColumn(
        "predicted_demand_mw", F.col("demand_lag_1").cast("double")
    )

    return [
        _decorate_predictions(
            validation_persistence,
            split="validation",
            model_name="persistence_lag_1",
            trained_through_utc=validation_training_end,
            run_id=run_id,
            run_timestamp_utc=run_timestamp_utc,
        ),
        _decorate_predictions(
            validation_ridge,
            split="validation",
            model_name="ridge_weather_lag",
            trained_through_utc=validation_training_end,
            run_id=run_id,
            run_timestamp_utc=run_timestamp_utc,
        ),
        _decorate_predictions(
            test_persistence,
            split="test",
            model_name="persistence_lag_1",
            trained_through_utc=test_training_end,
            run_id=run_id,
            run_timestamp_utc=run_timestamp_utc,
        ),
        _decorate_predictions(
            test_ridge,
            split="test",
            model_name="ridge_weather_lag",
            trained_through_utc=test_training_end,
            run_id=run_id,
            run_timestamp_utc=run_timestamp_utc,
        ),
    ]


def _union_frames(frames: list[DataFrame]) -> DataFrame:
    if not frames:
        raise ValueError("No forecast groups produced predictions.")
    combined = frames[0]
    for frame in frames[1:]:
        combined = combined.unionByName(frame)
    return combined


def _build_metrics(predictions: DataFrame) -> DataFrame:
    error = F.col("predicted_demand_mw") - F.col("actual_demand_mw")
    return (
        predictions.groupBy(
            "run_id",
            "run_timestamp_utc",
            *GROUP_COLUMNS,
            "split",
            "model_name",
            "trained_through_utc",
            "feature_contract_version",
        )
        .agg(
            F.count(F.lit(1)).alias("observation_count"),
            F.avg(F.abs(error)).alias("mae_mw"),
            F.sqrt(F.avg(F.pow(error, 2))).alias("rmse_mw"),
            F.avg(
                F.when(
                    F.abs(F.col("actual_demand_mw")) > F.lit(1e-12),
                    F.abs(error) / F.abs(F.col("actual_demand_mw")) * F.lit(100.0),
                )
            ).alias("mape_pct"),
            F.avg(error).alias("bias_mw"),
            F.min(TIMESTAMP_COLUMN).alias("evaluation_start_utc"),
            F.max(TIMESTAMP_COLUMN).alias("evaluation_end_utc"),
        )
    )


def run_backtest() -> tuple[DataFrame, DataFrame]:
    config = _configuration()
    run_id = str(uuid4())
    run_timestamp_utc = datetime.now(timezone.utc)
    prepared = _prepare_features(config)

    prediction_frames: list[DataFrame] = []
    groups = prepared.select(*GROUP_COLUMNS).distinct().orderBy(*GROUP_COLUMNS).collect()
    for group_row in groups:
        prediction_frames.extend(
            _evaluate_group(
                prepared.where(_group_filter(group_row)),
                config,
                run_id=run_id,
                run_timestamp_utc=run_timestamp_utc,
            )
        )

    predictions = _union_frames(prediction_frames).persist(StorageLevel.MEMORY_AND_DISK)
    leakage_count = predictions.where(
        F.col(TIMESTAMP_COLUMN) <= F.col("trained_through_utc")
    ).limit(1).count()
    if leakage_count:
        raise ValueError("A prediction was evaluated at or before its training boundary.")

    invalid_prediction_count = predictions.where(
        F.col("predicted_demand_mw").isNull()
        | F.isnan("predicted_demand_mw")
        | F.col("predicted_demand_mw").isin(float("inf"), float("-inf"))
    ).limit(1).count()
    if invalid_prediction_count:
        raise ValueError("Forecast predictions contain null or non-finite values.")

    metrics = _build_metrics(predictions)
    if metrics.limit(1).count() == 0:
        raise ValueError("No forecast metrics were produced.")

    (
        predictions.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(PREDICTIONS_TABLE)
    )
    (
        metrics.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(METRICS_TABLE)
    )

    predictions.orderBy(*GROUP_COLUMNS, "split", "model_name", TIMESTAMP_COLUMN).show(
        20, truncate=False
    )
    metrics.orderBy(*GROUP_COLUMNS, "split", "model_name").show(truncate=False)
    prepared.unpersist()
    predictions.unpersist()
    return predictions, metrics


if __name__ == "__main__":
    run_backtest()

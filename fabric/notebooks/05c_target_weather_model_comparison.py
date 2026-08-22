# Fabric notebook source: 05c_target_weather_model_comparison
# Optional/manual paired comparison; does not replace 05_baseline_forecasting.

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
FORECAST_WEATHER_TABLE = "silver_forecast_weather"
PREDICTIONS_TABLE = "forecast_weather_comparison_predictions"
METRICS_TABLE = "forecast_weather_comparison_metrics"
GROUP_COLUMNS = ["source_area", "resource_id", "city"]
REQUESTED_HORIZON_COLUMN = "requested_horizon_minutes"
TARGET_TOLERANCE_COLUMN = "target_tolerance_minutes"
MODEL_GROUP_COLUMNS = [*GROUP_COLUMNS, REQUESTED_HORIZON_COLUMN]
SOURCE_TIMESTAMP_COLUMN = "event_timestamp_utc"
FEATURE_TIMESTAMP_COLUMN = "feature_timestamp_utc"
TARGET_TIMESTAMP_COLUMN = "target_timestamp_utc"
TARGET_COLUMN = "demand_mw"
SUPERVISED_TARGET_COLUMN = "target_demand_mw"
ORIGIN_FOLD_COLUMN = "origin_fold"
ORIGIN_COUNT_COLUMN = "origin_count"
ORIGIN_CUTOFF_COLUMN = "origin_cutoff_utc"
TRAINING_OBSERVATION_COUNT_COLUMN = "training_observation_count"
EVALUATION_CONTRACT_VERSION_COLUMN = "evaluation_contract_version"
WEATHER_COMPARISON_CONTRACT_VERSION = "weather-model-comparison-v1"
FORECAST_WEATHER_CONTRACT_VERSION = "target-weather-v1"
FEATURE_CONTRACT_VERSION = "time-horizon-v1"
HOLDOUT_EVALUATION_CONTRACT_VERSION = "fixed-holdout-v1"
ROLLING_ORIGIN_EVALUATION_CONTRACT_VERSION = "rolling-origin-v1"
BASELINE_MODEL = "ridge_weather_lag"
CANDIDATE_MODEL = "ridge_target_weather"

DEMAND_FEATURE_COLUMNS = [
    "demand_mw",
    "demand_lag_1",
    "demand_rolling_mean_12",
    "hour_of_day_utc",
    "day_of_week_utc",
    "is_weekend_utc",
]
OBSERVED_FEATURE_COLUMNS = [
    *DEMAND_FEATURE_COLUMNS,
    "temperature",
    "humidity",
    "weather_age_minutes",
]
TARGET_WEATHER_FEATURE_COLUMNS = [
    *DEMAND_FEATURE_COLUMNS,
    "target_weather_temperature_c",
    "target_weather_humidity_pct",
    "target_weather_availability_age_minutes",
]
SUPPORTED_HORIZON_MINUTES = (30, 60)
SUPPORTED_EVALUATION_MODES = ("holdout", "rolling-origin")

TRAIN_FRACTION = 0.60
VALIDATION_FRACTION = 0.20
MIN_TRAIN_ROWS = 24
MIN_VALIDATION_ROWS = 6
MIN_TEST_ROWS = 6
RIDGE_REG_PARAM = 1.0
HORIZON_MINUTES = "30,60"
TARGET_TOLERANCE_MINUTES = 5
MIN_TARGET_COVERAGE = 0.90
EVALUATION_MODE = "holdout"
ROLLING_ORIGIN_FOLDS = 3
FORECAST_VALID_TOLERANCE_MINUTES = 90
MAX_FORECAST_AVAILABILITY_AGE_MINUTES = 360
MIN_FORECAST_WEATHER_COVERAGE = 0.90


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
    parsed = tuple(sorted(int(item) for item in values))
    if not parsed or len(set(parsed)) != len(parsed):
        raise ValueError("HORIZON_MINUTES must contain unique values.")
    unsupported = sorted(set(parsed) - set(SUPPORTED_HORIZON_MINUTES))
    if unsupported:
        raise ValueError(
            f"Unsupported HORIZON_MINUTES={unsupported}; supported values are 30 and 60."
        )
    return parsed


def _evaluation_mode(value: Any) -> str:
    parsed = str(value).strip().lower()
    if parsed not in SUPPORTED_EVALUATION_MODES:
        raise ValueError(
            f"EVALUATION_MODE must be one of: {', '.join(SUPPORTED_EVALUATION_MODES)}."
        )
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
    evaluation_mode = _evaluation_mode(
        _get_parameter("EVALUATION_MODE", EVALUATION_MODE)
    )
    rolling_origin_folds = _integer(
        _get_parameter("ROLLING_ORIGIN_FOLDS", ROLLING_ORIGIN_FOLDS),
        "ROLLING_ORIGIN_FOLDS",
        minimum=1,
    )
    if evaluation_mode == "rolling-origin" and rolling_origin_folds < 2:
        raise ValueError(
            "ROLLING_ORIGIN_FOLDS must be at least 2 in rolling-origin mode."
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
        "evaluation_mode": evaluation_mode,
        "rolling_origin_folds": rolling_origin_folds,
        "forecast_valid_tolerance_minutes": _integer(
            _get_parameter(
                "FORECAST_VALID_TOLERANCE_MINUTES",
                FORECAST_VALID_TOLERANCE_MINUTES,
            ),
            "FORECAST_VALID_TOLERANCE_MINUTES",
            minimum=0,
        ),
        "max_forecast_availability_age_minutes": _integer(
            _get_parameter(
                "MAX_FORECAST_AVAILABILITY_AGE_MINUTES",
                MAX_FORECAST_AVAILABILITY_AGE_MINUTES,
            ),
            "MAX_FORECAST_AVAILABILITY_AGE_MINUTES",
            minimum=1,
        ),
        "min_forecast_weather_coverage": _fraction(
            _get_parameter(
                "MIN_FORECAST_WEATHER_COVERAGE",
                MIN_FORECAST_WEATHER_COVERAGE,
            ),
            "MIN_FORECAST_WEATHER_COVERAGE",
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
    horizons = spark.createDataFrame(
        [
            (int(value), int(config["target_tolerance_minutes"]))
            for value in config["horizon_minutes"]
        ],
        f"{REQUESTED_HORIZON_COLUMN} int, {TARGET_TOLERANCE_COLUMN} int",
    )
    expected = source.select(*GROUP_COLUMNS).distinct().crossJoin(F.broadcast(horizons))
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
        candidates.withColumn("_target_match_rank", F.row_number().over(rank))
        .where(F.col("_target_match_rank") == 1)
        .drop("_target_match_rank")
    )
    counts = matched.groupBy(
        *MODEL_GROUP_COLUMNS, TARGET_TOLERANCE_COLUMN
    ).agg(
        F.count(F.lit(1)).alias("eligible_target_count"),
        F.sum(
            F.when(F.col(TARGET_TIMESTAMP_COLUMN).isNotNull(), 1).otherwise(0)
        ).alias("matched_target_count"),
    )
    coverage = (
        expected.join(
            counts,
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
    failures = coverage.where(
        (F.col("eligible_target_count") <= 0)
        | (
            F.col("target_coverage_pct")
            < F.lit(config["min_target_coverage"] * 100.0)
        )
    ).collect()
    if failures:
        examples = "; ".join(
            f"{row['source_area']}/{row['resource_id']}/{row['city']} "
            f"horizon={row[REQUESTED_HORIZON_COLUMN]}m "
            f"matched={row['matched_target_count']}/{row['eligible_target_count']}"
            for row in failures[:5]
        )
        raise ValueError("Demand target coverage is inadequate: " + examples)
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


def _attach_target_weather(
    supervised: DataFrame,
    config: dict[str, Any],
) -> DataFrame:
    weather_required = [
        "source_area",
        "city",
        "forecast_issued_at_utc",
        "forecast_ingested_at_utc",
        "forecast_valid_at_utc",
        "forecast_temperature_c",
        "forecast_humidity_pct",
        "forecast_provider",
        "forecast_model",
        "forecast_issue_basis",
        "forecast_provider_record_id",
        "raw_snapshot_id",
    ]
    weather = spark.table(FORECAST_WEATHER_TABLE)
    missing = sorted(set(weather_required) - set(weather.columns))
    if missing:
        raise ValueError(
            f"{FORECAST_WEATHER_TABLE} is missing required columns: "
            + ", ".join(missing)
            + "."
        )
    weather = weather.select(*weather_required).dropna(subset=weather_required)
    same_identity = (
        F.col("feature.source_area") == F.col("weather.source_area")
    ) & (
        F.lower(F.col("feature.city")) == F.lower(F.col("weather.city"))
    )
    available = (
        F.col("weather.forecast_issued_at_utc")
        <= F.col(f"feature.{FEATURE_TIMESTAMP_COLUMN}")
    ) & (
        F.col("weather.forecast_ingested_at_utc")
        <= F.col(f"feature.{FEATURE_TIMESTAMP_COLUMN}")
    )
    future_valid = (
        F.col("weather.forecast_valid_at_utc")
        > F.col(f"feature.{FEATURE_TIMESTAMP_COLUMN}")
    )
    availability_age = (
        F.col(f"feature.{FEATURE_TIMESTAMP_COLUMN}").cast("long")
        - F.col("weather.forecast_ingested_at_utc").cast("long")
    ) <= F.lit(config["max_forecast_availability_age_minutes"] * 60)
    valid_difference = F.abs(
        F.col("weather.forecast_valid_at_utc").cast("long")
        - F.col(f"feature.{TARGET_TIMESTAMP_COLUMN}").cast("long")
    ) <= F.lit(config["forecast_valid_tolerance_minutes"] * 60)
    joined = (
        supervised.alias("feature")
        .join(
            weather.alias("weather"),
            same_identity & available & future_valid & availability_age & valid_difference,
            "left",
        )
        .select(
            *[
                F.col(f"feature.{column}").alias(column)
                for column in supervised.columns
            ],
            F.col("weather.forecast_issued_at_utc").alias(
                "target_weather_forecast_issued_at_utc"
            ),
            F.col("weather.forecast_ingested_at_utc").alias(
                "target_weather_forecast_ingested_at_utc"
            ),
            F.col("weather.forecast_valid_at_utc").alias(
                "target_weather_forecast_valid_at_utc"
            ),
            F.col("weather.forecast_temperature_c").cast("double").alias(
                "target_weather_temperature_c"
            ),
            F.col("weather.forecast_humidity_pct").cast("double").alias(
                "target_weather_humidity_pct"
            ),
            F.col("weather.forecast_provider").alias("target_weather_provider"),
            F.col("weather.forecast_model").alias("target_weather_model"),
            F.col("weather.forecast_issue_basis").alias(
                "target_weather_issue_basis"
            ),
            F.col("weather.forecast_provider_record_id").alias(
                "target_weather_provider_record_id"
            ),
            F.col("weather.raw_snapshot_id").alias("target_weather_raw_snapshot_id"),
        )
    )
    rank = Window.partitionBy(
        *GROUP_COLUMNS,
        FEATURE_TIMESTAMP_COLUMN,
        REQUESTED_HORIZON_COLUMN,
    ).orderBy(
        F.abs(
            F.col("target_weather_forecast_valid_at_utc").cast("long")
            - F.col(TARGET_TIMESTAMP_COLUMN).cast("long")
        ).asc_nulls_last(),
        F.col("target_weather_forecast_ingested_at_utc").desc_nulls_last(),
        F.col("target_weather_forecast_issued_at_utc").desc_nulls_last(),
        F.col("target_weather_provider").asc_nulls_last(),
        F.col("target_weather_model").asc_nulls_last(),
    )
    matched = (
        joined.withColumn("_weather_match_rank", F.row_number().over(rank))
        .where(F.col("_weather_match_rank") == 1)
        .drop("_weather_match_rank")
    )
    coverage = matched.groupBy(*MODEL_GROUP_COLUMNS).agg(
        F.count(F.lit(1)).alias("forecast_weather_eligible_count"),
        F.sum(
            F.when(
                F.col("target_weather_forecast_valid_at_utc").isNotNull(), 1
            ).otherwise(0)
        ).alias("forecast_weather_matched_count"),
    ).withColumn(
        "forecast_weather_coverage_pct",
        F.col("forecast_weather_matched_count")
        / F.col("forecast_weather_eligible_count")
        * F.lit(100.0),
    )
    failures = coverage.where(
        (F.col("forecast_weather_eligible_count") <= 0)
        | (
            F.col("forecast_weather_coverage_pct")
            < F.lit(config["min_forecast_weather_coverage"] * 100.0)
        )
    ).collect()
    if failures:
        examples = "; ".join(
            f"{row['source_area']}/{row['resource_id']}/{row['city']} "
            f"horizon={row[REQUESTED_HORIZON_COLUMN]}m "
            f"matched={row['forecast_weather_matched_count']}/"
            f"{row['forecast_weather_eligible_count']}"
            for row in failures[:5]
        )
        raise ValueError("Forecast-weather coverage is inadequate: " + examples)
    return (
        matched.where(F.col("target_weather_forecast_valid_at_utc").isNotNull())
        .join(coverage, on=MODEL_GROUP_COLUMNS, how="inner")
        .withColumn(
            "target_weather_valid_delta_minutes",
            F.abs(
                F.col("target_weather_forecast_valid_at_utc").cast("long")
                - F.col(TARGET_TIMESTAMP_COLUMN).cast("long")
            )
            / F.lit(60.0),
        )
        .withColumn(
            "target_weather_provider_lead_minutes",
            (
                F.col("target_weather_forecast_valid_at_utc").cast("long")
                - F.col("target_weather_forecast_issued_at_utc").cast("long")
            )
            / F.lit(60.0),
        )
        .withColumn(
            "target_weather_feature_lead_minutes",
            (
                F.col("target_weather_forecast_valid_at_utc").cast("long")
                - F.col(FEATURE_TIMESTAMP_COLUMN).cast("long")
            )
            / F.lit(60.0),
        )
        .withColumn(
            "target_weather_availability_age_minutes",
            (
                F.col(FEATURE_TIMESTAMP_COLUMN).cast("long")
                - F.col("target_weather_forecast_ingested_at_utc").cast("long")
            )
            / F.lit(60.0),
        )
        .withColumn(
            "forecast_lead_time_bucket",
            F.when(
                F.col("target_weather_feature_lead_minutes") < 360,
                F.lit("00-06h"),
            )
            .when(
                F.col("target_weather_feature_lead_minutes") < 720,
                F.lit("06-12h"),
            )
            .when(
                F.col("target_weather_feature_lead_minutes") < 1440,
                F.lit("12-24h"),
            )
            .when(
                F.col("target_weather_feature_lead_minutes") < 2880,
                F.lit("24-48h"),
            )
            .otherwise(F.lit("48h+")),
        )
    )


def _prepare_cohort(config: dict[str, Any]) -> DataFrame:
    required = list(
        dict.fromkeys(
            [
                *GROUP_COLUMNS,
                SOURCE_TIMESTAMP_COLUMN,
                TARGET_COLUMN,
                *OBSERVED_FEATURE_COLUMNS,
            ]
        )
    )
    source = spark.table(SOURCE_TABLE)
    missing = sorted(set(required) - set(source.columns))
    if missing:
        raise ValueError(
            f"{SOURCE_TABLE} is missing required columns: {', '.join(missing)}."
        )
    source = source.select(*required).dropna(subset=required)
    duplicates = (
        source.groupBy(*GROUP_COLUMNS, SOURCE_TIMESTAMP_COLUMN)
        .count()
        .where(F.col("count") > 1)
        .limit(1)
        .count()
    )
    if duplicates:
        raise ValueError("Gold feature groups contain duplicate event timestamps.")
    cohort = _attach_target_weather(_target_matches(source, config), config)
    invalid = cohort.where(
        (F.col(TARGET_TIMESTAMP_COLUMN) <= F.col(FEATURE_TIMESTAMP_COLUMN))
        | (F.col("horizon_steps") < 1)
        | (F.col("target_delay_minutes") < 0)
        | (F.col("target_delay_minutes") > F.col(TARGET_TOLERANCE_COLUMN))
        | (
            F.col("target_weather_forecast_issued_at_utc")
            > F.col(FEATURE_TIMESTAMP_COLUMN)
        )
        | (
            F.col("target_weather_forecast_ingested_at_utc")
            > F.col(FEATURE_TIMESTAMP_COLUMN)
        )
        | (
            F.col("target_weather_forecast_valid_at_utc")
            <= F.col(FEATURE_TIMESTAMP_COLUMN)
        )
        | (
            F.col("target_weather_valid_delta_minutes")
            > F.lit(config["forecast_valid_tolerance_minutes"])
        )
        | (
            F.col("target_weather_availability_age_minutes") < 0
        )
        | (
            F.col("target_weather_availability_age_minutes")
            > F.lit(config["max_forecast_availability_age_minutes"])
        )
    ).limit(1).count()
    if invalid:
        raise ValueError("Target-weather cohort violates a causal matching contract.")

    order = Window.partitionBy(*MODEL_GROUP_COLUMNS).orderBy(FEATURE_TIMESTAMP_COLUMN)
    group = Window.partitionBy(*MODEL_GROUP_COLUMNS)
    cohort = (
        cohort.withColumn("_row_number", F.row_number().over(order))
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
    minimum_validation = config["min_validation_rows"]
    if config["evaluation_mode"] == "rolling-origin":
        minimum_validation *= config["rolling_origin_folds"] - 1
    failures = (
        cohort.groupBy(*MODEL_GROUP_COLUMNS)
        .agg(
            F.sum(F.when(F.col("split") == "train", 1).otherwise(0)).alias(
                "train_rows"
            ),
            F.sum(
                F.when(F.col("split") == "validation", 1).otherwise(0)
            ).alias("validation_rows"),
            F.sum(F.when(F.col("split") == "test", 1).otherwise(0)).alias(
                "test_rows"
            ),
        )
        .where(
            (F.col("train_rows") < config["min_train_rows"])
            | (F.col("validation_rows") < minimum_validation)
            | (F.col("test_rows") < config["min_test_rows"])
        )
        .collect()
    )
    if failures:
        examples = "; ".join(
            f"{row['source_area']}/{row['resource_id']}/{row['city']} "
            f"horizon={row[REQUESTED_HORIZON_COLUMN]}m: "
            f"train={row['train_rows']}, validation={row['validation_rows']}, "
            f"test={row['test_rows']}"
            for row in failures[:5]
        )
        raise ValueError("Target-weather comparison has insufficient history: " + examples)
    return cohort.persist(StorageLevel.MEMORY_AND_DISK)


def _fit_ridge(
    training: DataFrame,
    feature_columns: list[str],
    reg_param: float,
):
    return Pipeline(
        stages=[
            VectorAssembler(
                inputCols=feature_columns,
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
) -> tuple[DataFrame, datetime, int]:
    cutoff = evaluation.agg(F.min(FEATURE_TIMESTAMP_COLUMN)).first()[0]
    if cutoff is None:
        raise ValueError("Evaluation window has no rows.")
    purged = training.where(
        F.col(TARGET_TIMESTAMP_COLUMN) < F.lit(cutoff)
    ).persist(StorageLevel.MEMORY_AND_DISK)
    count = purged.count()
    if count < min_train_rows:
        purged.unpersist()
        raise ValueError(
            f"Purging unavailable labels left {count} rows; minimum is "
            f"{min_train_rows}."
        )
    return purged, purged.agg(F.max(TARGET_TIMESTAMP_COLUMN)).first()[0], count


def _decorate(
    frame: DataFrame,
    *,
    split: str,
    model_name: str,
    weather_feature_mode: str,
    trained_through_utc: datetime,
    training_observation_count: int,
    run_id: str,
    run_timestamp_utc: datetime,
    evaluation_contract_version: str,
    config: dict[str, Any],
    origin_fold: int | None = None,
    origin_count: int | None = None,
    origin_cutoff_utc: datetime | None = None,
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
        F.lit(config["min_target_coverage"] * 100.0).alias(
            "minimum_target_coverage_pct"
        ),
        F.col("forecast_weather_eligible_count").cast("long"),
        F.col("forecast_weather_matched_count").cast("long"),
        F.col("forecast_weather_coverage_pct").cast("double"),
        F.lit(config["min_forecast_weather_coverage"] * 100.0).alias(
            "minimum_forecast_weather_coverage_pct"
        ),
        F.lit(split).alias("split"),
        F.lit(model_name).alias("model_name"),
        F.lit(weather_feature_mode).alias("weather_feature_mode"),
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
        F.lit(training_observation_count).cast("long").alias(
            TRAINING_OBSERVATION_COUNT_COLUMN
        ),
        F.col("target_weather_forecast_issued_at_utc"),
        F.col("target_weather_forecast_ingested_at_utc"),
        F.col("target_weather_forecast_valid_at_utc"),
        F.col("target_weather_temperature_c"),
        F.col("target_weather_humidity_pct"),
        F.col("target_weather_provider"),
        F.col("target_weather_model"),
        F.col("target_weather_issue_basis"),
        F.col("target_weather_provider_record_id"),
        F.col("target_weather_raw_snapshot_id"),
        F.col("target_weather_valid_delta_minutes"),
        F.col("target_weather_provider_lead_minutes"),
        F.col("target_weather_feature_lead_minutes"),
        F.col("target_weather_availability_age_minutes"),
        F.col("forecast_lead_time_bucket"),
        F.lit(FEATURE_CONTRACT_VERSION).alias("feature_contract_version"),
        F.lit(FORECAST_WEATHER_CONTRACT_VERSION).alias(
            "forecast_weather_contract_version"
        ),
        F.lit(WEATHER_COMPARISON_CONTRACT_VERSION).alias(
            "weather_comparison_contract_version"
        ),
        F.lit(origin_fold).cast("int").alias(ORIGIN_FOLD_COLUMN),
        F.lit(origin_count).cast("int").alias(ORIGIN_COUNT_COLUMN),
        F.lit(origin_cutoff_utc).cast("timestamp").alias(ORIGIN_CUTOFF_COLUMN),
        F.lit(evaluation_contract_version).alias(
            EVALUATION_CONTRACT_VERSION_COLUMN
        ),
    )


def _evaluate_pair(
    training_candidates: DataFrame,
    evaluation: DataFrame,
    *,
    split: str,
    config: dict[str, Any],
    run_id: str,
    run_timestamp_utc: datetime,
    evaluation_contract_version: str,
    origin_fold: int | None = None,
    origin_count: int | None = None,
    origin_cutoff_utc: datetime | None = None,
) -> list[DataFrame]:
    training, boundary, training_count = _purge_training(
        training_candidates,
        evaluation,
        min_train_rows=config["min_train_rows"],
    )
    observed = _fit_ridge(
        training,
        OBSERVED_FEATURE_COLUMNS,
        config["ridge_reg_param"],
    ).transform(evaluation)
    target_weather = _fit_ridge(
        training,
        TARGET_WEATHER_FEATURE_COLUMNS,
        config["ridge_reg_param"],
    ).transform(evaluation)
    common = {
        "split": split,
        "trained_through_utc": boundary,
        "training_observation_count": training_count,
        "run_id": run_id,
        "run_timestamp_utc": run_timestamp_utc,
        "evaluation_contract_version": evaluation_contract_version,
        "config": config,
        "origin_fold": origin_fold,
        "origin_count": origin_count,
        "origin_cutoff_utc": origin_cutoff_utc,
    }
    frames = [
        _decorate(
            observed,
            model_name=BASELINE_MODEL,
            weather_feature_mode="observed_at_feature",
            **common,
        ),
        _decorate(
            target_weather,
            model_name=CANDIDATE_MODEL,
            weather_feature_mode="target_forecast",
            **common,
        ),
    ]
    training.unpersist()
    return frames


def _balanced_partition_sizes(total: int, parts: int) -> list[int]:
    base, remainder = divmod(total, parts)
    return [base + (1 if index < remainder else 0) for index in range(parts)]


def _rolling_folds(group_frame: DataFrame, config: dict[str, Any]) -> list[dict[str, Any]]:
    metadata = group_frame.agg(
        F.max("_group_count").alias("group_count"),
        F.max("_train_end").alias("train_end"),
        F.max("_validation_end").alias("validation_end"),
    ).first()
    group_count = int(metadata["group_count"])
    train_end = int(metadata["train_end"])
    validation_end = int(metadata["validation_end"])
    origin_count = int(config["rolling_origin_folds"])
    validation_origins = origin_count - 1
    validation_count = validation_end - train_end
    test_count = group_count - validation_end
    required_validation = validation_origins * int(config["min_validation_rows"])
    if validation_count < required_validation or test_count < int(config["min_test_rows"]):
        raise ValueError("Comparison group has insufficient rolling-origin history.")
    folds: list[dict[str, Any]] = []
    cursor = train_end
    for origin_fold, size in enumerate(
        _balanced_partition_sizes(validation_count, validation_origins), start=1
    ):
        end = cursor + size
        evaluation = group_frame.where(
            (F.col("_row_number") > cursor) & (F.col("_row_number") <= end)
        )
        cutoff = evaluation.agg(F.min(FEATURE_TIMESTAMP_COLUMN)).first()[0]
        folds.append(
            {
                "origin_fold": origin_fold,
                "origin_count": origin_count,
                "split": "validation",
                "training": group_frame.where(F.col("_row_number") <= cursor),
                "evaluation": evaluation,
                "cutoff": cutoff,
            }
        )
        cursor = end
    evaluation = group_frame.where(F.col("_row_number") > validation_end)
    folds.append(
        {
            "origin_fold": origin_count,
            "origin_count": origin_count,
            "split": "test",
            "training": group_frame.where(F.col("_row_number") <= validation_end),
            "evaluation": evaluation,
            "cutoff": evaluation.agg(F.min(FEATURE_TIMESTAMP_COLUMN)).first()[0],
        }
    )
    cutoffs = [fold["cutoff"] for fold in folds]
    if any(cutoff is None for cutoff in cutoffs) or not all(
        earlier < later for earlier, later in zip(cutoffs, cutoffs[1:])
    ):
        raise ValueError("Rolling-origin comparison cutoffs must be strictly increasing.")
    return folds


def _evaluate_group(
    group_frame: DataFrame,
    config: dict[str, Any],
    *,
    run_id: str,
    run_timestamp_utc: datetime,
) -> list[DataFrame]:
    if config["evaluation_mode"] == "rolling-origin":
        frames: list[DataFrame] = []
        for fold in _rolling_folds(group_frame, config):
            frames.extend(
                _evaluate_pair(
                    fold["training"],
                    fold["evaluation"],
                    split=fold["split"],
                    config=config,
                    run_id=run_id,
                    run_timestamp_utc=run_timestamp_utc,
                    evaluation_contract_version=(
                        ROLLING_ORIGIN_EVALUATION_CONTRACT_VERSION
                    ),
                    origin_fold=fold["origin_fold"],
                    origin_count=fold["origin_count"],
                    origin_cutoff_utc=fold["cutoff"],
                )
            )
        return frames
    train = group_frame.where(F.col("split") == "train")
    validation = group_frame.where(F.col("split") == "validation")
    test = group_frame.where(F.col("split") == "test")
    return [
        *_evaluate_pair(
            train,
            validation,
            split="validation",
            config=config,
            run_id=run_id,
            run_timestamp_utc=run_timestamp_utc,
            evaluation_contract_version=HOLDOUT_EVALUATION_CONTRACT_VERSION,
        ),
        *_evaluate_pair(
            train.unionByName(validation),
            test,
            split="test",
            config=config,
            run_id=run_id,
            run_timestamp_utc=run_timestamp_utc,
            evaluation_contract_version=HOLDOUT_EVALUATION_CONTRACT_VERSION,
        ),
    ]


def _group_filter(row: Any):
    condition = F.lit(True)
    for column in MODEL_GROUP_COLUMNS:
        condition = condition & (F.col(column) == F.lit(row[column]))
    return condition


def _union(frames: list[DataFrame]) -> DataFrame:
    if not frames:
        raise ValueError("No target-weather comparison predictions were produced.")
    combined = frames[0]
    for frame in frames[1:]:
        combined = combined.unionByName(frame)
    return combined


def _pair_failures(predictions: DataFrame) -> int:
    keys = [
        *MODEL_GROUP_COLUMNS,
        "split",
        ORIGIN_FOLD_COLUMN,
        FEATURE_TIMESTAMP_COLUMN,
        SOURCE_TIMESTAMP_COLUMN,
        "target_weather_provider",
        "target_weather_model",
    ]
    pairs = predictions.groupBy(*keys).agg(
        F.count(F.lit(1)).alias("_row_count"),
        F.countDistinct("model_name").alias("_model_count"),
        F.countDistinct("actual_demand_mw").alias("_actual_count"),
        F.countDistinct("trained_through_utc").alias("_boundary_count"),
    )
    return pairs.where(
        (F.col("_row_count") != 2)
        | (F.col("_model_count") != 2)
        | (F.col("_actual_count") != 1)
        | (F.col("_boundary_count") != 1)
    ).count()


def _build_metrics(predictions: DataFrame) -> DataFrame:
    error = F.col("predicted_demand_mw") - F.col("actual_demand_mw")
    return predictions.groupBy(
        "run_id",
        "run_timestamp_utc",
        *MODEL_GROUP_COLUMNS,
        "split",
        "model_name",
        "weather_feature_mode",
        "target_weather_provider",
        "target_weather_model",
        "target_weather_issue_basis",
        "forecast_lead_time_bucket",
        "trained_through_utc",
        ORIGIN_FOLD_COLUMN,
        ORIGIN_COUNT_COLUMN,
        ORIGIN_CUTOFF_COLUMN,
        TRAINING_OBSERVATION_COUNT_COLUMN,
        EVALUATION_CONTRACT_VERSION_COLUMN,
        "feature_contract_version",
        "forecast_weather_contract_version",
        "weather_comparison_contract_version",
    ).agg(
        F.count(F.lit(1)).alias("observation_count"),
        F.first("eligible_target_count").alias("eligible_target_count"),
        F.first("matched_target_count").alias("matched_target_count"),
        F.first("target_coverage_pct").alias("target_coverage_pct"),
        F.first("minimum_target_coverage_pct").alias(
            "minimum_target_coverage_pct"
        ),
        F.first("forecast_weather_eligible_count").alias(
            "forecast_weather_eligible_count"
        ),
        F.first("forecast_weather_matched_count").alias(
            "forecast_weather_matched_count"
        ),
        F.first("forecast_weather_coverage_pct").alias(
            "forecast_weather_coverage_pct"
        ),
        F.first("minimum_forecast_weather_coverage_pct").alias(
            "minimum_forecast_weather_coverage_pct"
        ),
        F.avg(F.abs(error)).alias("mae_mw"),
        F.sqrt(F.avg(F.pow(error, 2))).alias("rmse_mw"),
        F.avg(error).alias("bias_mw"),
        F.min(FEATURE_TIMESTAMP_COLUMN).alias("evaluation_feature_start_utc"),
        F.max(FEATURE_TIMESTAMP_COLUMN).alias("evaluation_feature_end_utc"),
        F.min(SOURCE_TIMESTAMP_COLUMN).alias("evaluation_start_utc"),
        F.max(SOURCE_TIMESTAMP_COLUMN).alias("evaluation_end_utc"),
    )


def _validate_predictions(predictions: DataFrame, config: dict[str, Any]) -> None:
    expected_models = {BASELINE_MODEL, CANDIDATE_MODEL}
    models = {row["model_name"] for row in predictions.select("model_name").distinct().collect()}
    if models != expected_models:
        raise ValueError(f"Expected paired models {sorted(expected_models)}, found {sorted(models)}.")
    if _pair_failures(predictions):
        raise ValueError("Observed and target-weather models do not form exact paired rows.")
    invalid = predictions.where(
        (F.col("trained_through_utc") >= F.col(FEATURE_TIMESTAMP_COLUMN))
        | (F.col(FEATURE_TIMESTAMP_COLUMN) >= F.col(SOURCE_TIMESTAMP_COLUMN))
        | (
            F.col("target_weather_forecast_issued_at_utc")
            > F.col(FEATURE_TIMESTAMP_COLUMN)
        )
        | (
            F.col("target_weather_forecast_ingested_at_utc")
            > F.col(FEATURE_TIMESTAMP_COLUMN)
        )
        | (
            F.col("target_weather_forecast_valid_at_utc")
            <= F.col(FEATURE_TIMESTAMP_COLUMN)
        )
        | (
            F.col("target_weather_valid_delta_minutes")
            > F.lit(config["forecast_valid_tolerance_minutes"])
        )
        | (
            F.col("forecast_weather_coverage_pct")
            < F.col("minimum_forecast_weather_coverage_pct")
        )
        | (
            (F.col("model_name") == BASELINE_MODEL)
            & (F.col("weather_feature_mode") != "observed_at_feature")
        )
        | (
            (F.col("model_name") == CANDIDATE_MODEL)
            & (F.col("weather_feature_mode") != "target_forecast")
        )
        | (
            F.col("weather_comparison_contract_version")
            != WEATHER_COMPARISON_CONTRACT_VERSION
        )
    ).limit(1).count()
    if invalid:
        raise ValueError("Target-weather comparison prediction evidence is invalid.")


def run_comparison() -> tuple[DataFrame, DataFrame]:
    config = _configuration()
    run_id = str(uuid4())
    run_timestamp = datetime.now(timezone.utc)
    cohort = _prepare_cohort(config)
    frames: list[DataFrame] = []
    groups = (
        cohort.select(*MODEL_GROUP_COLUMNS)
        .distinct()
        .orderBy(*MODEL_GROUP_COLUMNS)
        .collect()
    )
    for group in groups:
        frames.extend(
            _evaluate_group(
                cohort.where(_group_filter(group)),
                config,
                run_id=run_id,
                run_timestamp_utc=run_timestamp,
            )
        )
    predictions = _union(frames).persist(StorageLevel.MEMORY_AND_DISK)
    _validate_predictions(predictions, config)
    metrics = _build_metrics(predictions)
    if metrics.limit(1).count() == 0:
        raise ValueError("No target-weather comparison metrics were produced.")
    predictions.write.format("delta").mode("append").option(
        "mergeSchema", "true"
    ).saveAsTable(PREDICTIONS_TABLE)
    metrics.write.format("delta").mode("append").option(
        "mergeSchema", "true"
    ).saveAsTable(METRICS_TABLE)
    predictions.orderBy(
        *MODEL_GROUP_COLUMNS,
        "split",
        ORIGIN_FOLD_COLUMN,
        "model_name",
        FEATURE_TIMESTAMP_COLUMN,
    ).show(20, truncate=False)
    metrics.orderBy(
        *MODEL_GROUP_COLUMNS,
        "split",
        ORIGIN_FOLD_COLUMN,
        "model_name",
    ).show(truncate=False)
    cohort.unpersist()
    predictions.unpersist()
    return predictions, metrics


if __name__ == "__main__":
    run_comparison()

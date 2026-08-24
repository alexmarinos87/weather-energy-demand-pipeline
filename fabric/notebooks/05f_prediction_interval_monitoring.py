# Fabric notebook source: 05f_prediction_interval_monitoring
# Optional/manual advisory monitoring over retained prediction interval metrics.
# No point model is fitted or refitted and no interval is recalibrated.

from datetime import datetime, timezone
from math import isfinite
from typing import Any
from uuid import uuid4

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


INTERVAL_METRICS_TABLE = "forecast_prediction_interval_metrics"
HEALTH_CHECKS_TABLE = "forecast_prediction_interval_health_checks"
HEALTH_SUMMARY_TABLE = "forecast_prediction_interval_health_summary"

AS_OF_UTC = ""
RECENT_INTERVAL_RUN_COUNT = 3
REFERENCE_INTERVAL_RUN_COUNT = 6
MIN_RECENT_INTERVAL_RUNS = 2
MIN_REFERENCE_INTERVAL_RUNS = 3
MAX_INTERVAL_RUN_AGE_MINUTES = 10080
MAX_EVALUATION_AGE_MINUTES = 20160
MIN_CALIBRATION_OBSERVATION_COUNT = 24
MAX_RECENT_COVERAGE_SHORTFALL_PCT_POINTS = 5.0
MAX_COVERAGE_DROP_PCT_POINTS = 5.0
MAX_AVERAGE_INTERVAL_WIDTH_INCREASE_PCT = 25.0
MAX_CALIBRATION_HISTORY_DROP_PCT = 25.0

POLICY_VERSION = "prediction-interval-monitoring-policy-v1"
MONITORING_CONTRACT_VERSION = "prediction-interval-monitoring-v1"
HEALTHY_STATUS = "healthy"
WARNING_STATUS = "warning"
FAILED_STATUS = "failed"

TEXT_IDENTITY_COLUMNS = [
    "source_area",
    "resource_id",
    "city",
    "model_name",
    "feature_contract_version",
    "interval_contract_version",
]
SLICE_KEY_COLUMNS = [
    "_source_area_key",
    "_resource_id_key",
    "_city_key",
    "requested_horizon_minutes",
    "_model_name_key",
    "_feature_contract_version_key",
    "target_coverage_level",
    "_interval_contract_version_key",
]
REQUIRED_METRIC_COLUMNS = {
    "interval_run_id",
    "interval_run_timestamp_utc",
    *TEXT_IDENTITY_COLUMNS,
    "requested_horizon_minutes",
    "target_coverage_level",
    "calibration_observation_count",
    "calibration_radius_mw",
    "evaluation_end_utc",
    "evaluation_observation_count",
    "empirical_coverage_pct",
    "average_interval_width_mw",
}


def _parameter(name: str, default: Any) -> Any:
    return globals().get(name, default)


def _positive_integer(name: str, value: Any) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a positive integer.")
    parsed = float(value)
    if not isfinite(parsed) or parsed < 1 or parsed % 1:
        raise ValueError(f"{name} must be a positive integer.")
    return int(parsed)


def _non_negative_number(name: str, value: Any) -> float:
    parsed = float(value)
    if not isfinite(parsed) or parsed < 0:
        raise ValueError(f"{name} must be finite and non-negative.")
    return parsed


def _configuration() -> dict[str, Any]:
    config = {
        "recent_interval_run_count": _positive_integer(
            "RECENT_INTERVAL_RUN_COUNT",
            _parameter("RECENT_INTERVAL_RUN_COUNT", RECENT_INTERVAL_RUN_COUNT),
        ),
        "reference_interval_run_count": _positive_integer(
            "REFERENCE_INTERVAL_RUN_COUNT",
            _parameter(
                "REFERENCE_INTERVAL_RUN_COUNT", REFERENCE_INTERVAL_RUN_COUNT
            ),
        ),
        "min_recent_interval_runs": _positive_integer(
            "MIN_RECENT_INTERVAL_RUNS",
            _parameter("MIN_RECENT_INTERVAL_RUNS", MIN_RECENT_INTERVAL_RUNS),
        ),
        "min_reference_interval_runs": _positive_integer(
            "MIN_REFERENCE_INTERVAL_RUNS",
            _parameter(
                "MIN_REFERENCE_INTERVAL_RUNS", MIN_REFERENCE_INTERVAL_RUNS
            ),
        ),
        "max_interval_run_age_minutes": _positive_integer(
            "MAX_INTERVAL_RUN_AGE_MINUTES",
            _parameter(
                "MAX_INTERVAL_RUN_AGE_MINUTES", MAX_INTERVAL_RUN_AGE_MINUTES
            ),
        ),
        "max_evaluation_age_minutes": _positive_integer(
            "MAX_EVALUATION_AGE_MINUTES",
            _parameter("MAX_EVALUATION_AGE_MINUTES", MAX_EVALUATION_AGE_MINUTES),
        ),
        "min_calibration_observation_count": _positive_integer(
            "MIN_CALIBRATION_OBSERVATION_COUNT",
            _parameter(
                "MIN_CALIBRATION_OBSERVATION_COUNT",
                MIN_CALIBRATION_OBSERVATION_COUNT,
            ),
        ),
        "max_recent_coverage_shortfall_pct_points": _non_negative_number(
            "MAX_RECENT_COVERAGE_SHORTFALL_PCT_POINTS",
            _parameter(
                "MAX_RECENT_COVERAGE_SHORTFALL_PCT_POINTS",
                MAX_RECENT_COVERAGE_SHORTFALL_PCT_POINTS,
            ),
        ),
        "max_coverage_drop_pct_points": _non_negative_number(
            "MAX_COVERAGE_DROP_PCT_POINTS",
            _parameter(
                "MAX_COVERAGE_DROP_PCT_POINTS", MAX_COVERAGE_DROP_PCT_POINTS
            ),
        ),
        "max_average_interval_width_increase_pct": _non_negative_number(
            "MAX_AVERAGE_INTERVAL_WIDTH_INCREASE_PCT",
            _parameter(
                "MAX_AVERAGE_INTERVAL_WIDTH_INCREASE_PCT",
                MAX_AVERAGE_INTERVAL_WIDTH_INCREASE_PCT,
            ),
        ),
        "max_calibration_history_drop_pct": _non_negative_number(
            "MAX_CALIBRATION_HISTORY_DROP_PCT",
            _parameter(
                "MAX_CALIBRATION_HISTORY_DROP_PCT",
                MAX_CALIBRATION_HISTORY_DROP_PCT,
            ),
        ),
    }
    if config["min_recent_interval_runs"] > config["recent_interval_run_count"]:
        raise ValueError(
            "MIN_RECENT_INTERVAL_RUNS cannot exceed RECENT_INTERVAL_RUN_COUNT."
        )
    if (
        config["min_reference_interval_runs"]
        > config["reference_interval_run_count"]
    ):
        raise ValueError(
            "MIN_REFERENCE_INTERVAL_RUNS cannot exceed "
            "REFERENCE_INTERVAL_RUN_COUNT."
        )
    return config


def _parse_utc(value: Any, name: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value).strip()
        if not text:
            raise ValueError(f"{name} must be non-empty.")
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(
                f"{name} must be an ISO-8601 timezone-aware timestamp."
            ) from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{name} must be timezone-aware.")
    return parsed.astimezone(timezone.utc)


def _as_of_utc() -> datetime:
    raw = str(_parameter("AS_OF_UTC", AS_OF_UTC)).strip()
    return _parse_utc(raw, "AS_OF_UTC") if raw else datetime.now(timezone.utc)


def _blank_identity_condition() -> Any:
    condition = F.lit(False)
    for column in ["interval_run_id", *TEXT_IDENTITY_COLUMNS]:
        condition = condition | F.col(column).isNull() | (
            F.trim(F.col(column).cast("string")) == ""
        )
    return condition


def _not_finite(column: str) -> Any:
    return (
        F.col(column).isNull()
        | F.isnan(column)
        | F.col(column).isin(float("inf"), float("-inf"))
    )


def _prepared_metric_history(
    spark_session,
    config: dict[str, Any],
) -> tuple[DataFrame, int]:
    metrics = spark_session.table(INTERVAL_METRICS_TABLE)
    missing = sorted(REQUIRED_METRIC_COLUMNS - set(metrics.columns))
    if missing:
        raise ValueError(
            f"{INTERVAL_METRICS_TABLE} is missing required columns: "
            + ", ".join(missing)
            + "."
        )
    if metrics.limit(1).count() == 0:
        raise ValueError("No retained prediction interval metrics are available.")
    if metrics.where(_blank_identity_condition()).limit(1).count():
        raise ValueError(
            "Prediction interval metrics contain null or blank identity fields."
        )

    prepared = metrics.select(
        "interval_run_id",
        F.col("interval_run_timestamp_utc").cast("timestamp").alias(
            "interval_run_timestamp_utc"
        ),
        *TEXT_IDENTITY_COLUMNS,
        F.col("requested_horizon_minutes").cast("int").alias(
            "requested_horizon_minutes"
        ),
        F.col("target_coverage_level").cast("double").alias(
            "target_coverage_level"
        ),
        F.col("calibration_observation_count").cast("long").alias(
            "calibration_observation_count"
        ),
        F.col("calibration_radius_mw").cast("double").alias(
            "calibration_radius_mw"
        ),
        F.col("evaluation_end_utc").cast("timestamp").alias(
            "evaluation_end_utc"
        ),
        F.col("evaluation_observation_count").cast("long").alias(
            "evaluation_observation_count"
        ),
        F.col("empirical_coverage_pct").cast("double").alias(
            "empirical_coverage_pct"
        ),
        F.col("average_interval_width_mw").cast("double").alias(
            "average_interval_width_mw"
        ),
    )
    for column in TEXT_IDENTITY_COLUMNS:
        prepared = prepared.withColumn(
            f"_{column}_key", F.lower(F.trim(F.col(column)))
        )

    invalid = prepared.where(
        F.col("interval_run_timestamp_utc").isNull()
        | F.col("evaluation_end_utc").isNull()
        | (F.col("evaluation_end_utc") > F.col("interval_run_timestamp_utc"))
        | (F.col("requested_horizon_minutes") <= 0)
        | (F.col("calibration_observation_count") <= 0)
        | (F.col("evaluation_observation_count") <= 0)
        | _not_finite("target_coverage_level")
        | (F.col("target_coverage_level") <= 0)
        | (F.col("target_coverage_level") >= 1)
        | _not_finite("calibration_radius_mw")
        | (F.col("calibration_radius_mw") < 0)
        | _not_finite("empirical_coverage_pct")
        | (F.col("empirical_coverage_pct") < 0)
        | (F.col("empirical_coverage_pct") > 100)
        | _not_finite("average_interval_width_mw")
        | (F.col("average_interval_width_mw") < 0)
    ).limit(1).count()
    if invalid:
        raise ValueError(
            "Prediction interval metrics contain invalid counts, values, or timestamps."
        )

    timestamp_conflict = (
        prepared.groupBy("interval_run_id")
        .agg(F.countDistinct("interval_run_timestamp_utc").alias("count"))
        .where(F.col("count") != 1)
        .limit(1)
        .count()
    )
    if timestamp_conflict:
        raise ValueError(
            "Each interval_run_id must have exactly one run timestamp."
        )

    run_slice_identity = ["interval_run_id", *SLICE_KEY_COLUMNS]
    if (
        prepared.groupBy(*run_slice_identity)
        .count()
        .where(F.col("count") != 1)
        .limit(1)
        .count()
    ):
        raise ValueError(
            "Prediction interval metrics contain duplicate run/slice identities."
        )

    total_run_count = prepared.select("interval_run_id").distinct().count()
    retained_limit = (
        config["recent_interval_run_count"]
        + config["reference_interval_run_count"]
    )
    order = Window.partitionBy(*SLICE_KEY_COLUMNS).orderBy(
        F.col("interval_run_timestamp_utc").desc(),
        F.col("interval_run_id").desc(),
    )
    bounded = prepared.withColumn("_run_rank", F.row_number().over(order)).where(
        F.col("_run_rank") <= F.lit(retained_limit)
    )
    return bounded, int(total_run_count)


def _weighted_aggregate(frame: DataFrame, prefix: str) -> DataFrame:
    weight = F.col("evaluation_observation_count").cast("double")
    return frame.groupBy(*SLICE_KEY_COLUMNS).agg(
        F.count(F.lit(1)).alias(f"{prefix}_interval_run_count"),
        F.sum(weight).alias(f"{prefix}_evaluation_observation_count"),
        (
            F.sum(F.col("empirical_coverage_pct") * weight) / F.sum(weight)
        ).alias(f"{prefix}_empirical_coverage_pct"),
        (
            F.sum(F.col("average_interval_width_mw") * weight) / F.sum(weight)
        ).alias(f"{prefix}_average_interval_width_mw"),
        F.avg("calibration_observation_count").alias(
            f"{prefix}_mean_calibration_observation_count"
        ),
        F.min("calibration_observation_count").alias(
            f"{prefix}_minimum_calibration_observation_count"
        ),
    )


def _monitoring_statistics(
    history: DataFrame,
    config: dict[str, Any],
) -> DataFrame:
    recent = history.where(
        F.col("_run_rank") <= F.lit(config["recent_interval_run_count"])
    )
    reference = history.where(
        (F.col("_run_rank") > F.lit(config["recent_interval_run_count"]))
        & (
            F.col("_run_rank")
            <= F.lit(
                config["recent_interval_run_count"]
                + config["reference_interval_run_count"]
            )
        )
    )
    recent_stats = _weighted_aggregate(recent, "recent")
    reference_stats = _weighted_aggregate(reference, "reference")
    latest = history.where(F.col("_run_rank") == 1).select(
        *SLICE_KEY_COLUMNS,
        *TEXT_IDENTITY_COLUMNS,
        "requested_horizon_minutes",
        "target_coverage_level",
        F.col("interval_run_id").alias("latest_interval_run_id"),
        F.col("interval_run_timestamp_utc").alias(
            "latest_interval_run_timestamp_utc"
        ),
        F.col("evaluation_end_utc").alias("latest_evaluation_end_utc"),
    )
    return (
        latest.join(recent_stats, on=SLICE_KEY_COLUMNS, how="inner")
        .join(reference_stats, on=SLICE_KEY_COLUMNS, how="left")
        .fillna(
            {
                "reference_interval_run_count": 0,
                "reference_evaluation_observation_count": 0.0,
                "reference_empirical_coverage_pct": 0.0,
                "reference_average_interval_width_mw": 0.0,
                "reference_mean_calibration_observation_count": 0.0,
                "reference_minimum_calibration_observation_count": 0.0,
            }
        )
    )


def _increase_pct(recent: float, reference: float) -> float:
    if reference <= 0:
        return 0.0 if recent <= 0 else 100.0
    return (recent - reference) / reference * 100.0


def _drop_pct(recent: float, reference: float) -> float:
    if reference <= 0:
        return 0.0
    return (reference - recent) / reference * 100.0


def _check(
    *,
    run_id: str,
    run_timestamp: datetime,
    as_of: datetime,
    scope: str,
    severity: str,
    name: str,
    observed: float,
    threshold: float,
    comparator: str,
    passed: bool,
    details: str,
    identity: dict[str, Any],
) -> dict[str, Any]:
    return {
        "monitor_run_id": run_id,
        "monitor_timestamp_utc": run_timestamp,
        "monitor_as_of_utc": as_of,
        "monitor_status": None,
        "check_scope": scope,
        "severity": severity,
        "check_name": name,
        "observed_value": float(observed),
        "threshold_value": float(threshold),
        "comparator": comparator,
        "passed": bool(passed),
        "details": details,
        "source_area": identity["source_area"],
        "resource_id": identity["resource_id"],
        "city": identity["city"],
        "requested_horizon_minutes": int(
            identity["requested_horizon_minutes"]
        ),
        "model_name": identity["model_name"],
        "feature_contract_version": identity["feature_contract_version"],
        "target_coverage_level": float(identity["target_coverage_level"]),
        "interval_contract_version": identity["interval_contract_version"],
        "latest_interval_run_id": identity["latest_interval_run_id"],
        "policy_version": POLICY_VERSION,
        "monitoring_contract_version": MONITORING_CONTRACT_VERSION,
    }


def _check_rows(
    statistics: DataFrame,
    *,
    config: dict[str, Any],
    as_of: datetime,
    run_id: str,
    run_timestamp: datetime,
) -> list[dict[str, Any]]:
    rows = statistics.collect()
    if not rows:
        raise ValueError("Prediction interval monitoring produced no checks.")
    checks: list[dict[str, Any]] = []
    for row in rows:
        latest_run = row["latest_interval_run_timestamp_utc"].replace(
            tzinfo=timezone.utc
        )
        latest_evaluation = row["latest_evaluation_end_utc"].replace(
            tzinfo=timezone.utc
        )
        run_age = (as_of - latest_run).total_seconds() / 60.0
        evaluation_age = (as_of - latest_evaluation).total_seconds() / 60.0
        if run_age < 0 or evaluation_age < 0:
            raise ValueError("AS_OF_UTC cannot precede retained interval evidence.")

        identity = {
            "source_area": row["source_area"],
            "resource_id": row["resource_id"],
            "city": row["city"],
            "requested_horizon_minutes": row["requested_horizon_minutes"],
            "model_name": row["model_name"],
            "feature_contract_version": row["feature_contract_version"],
            "target_coverage_level": row["target_coverage_level"],
            "interval_contract_version": row["interval_contract_version"],
            "latest_interval_run_id": row["latest_interval_run_id"],
        }
        recent_count = int(row["recent_interval_run_count"])
        reference_count = int(row["reference_interval_run_count"])
        recent_coverage = float(row["recent_empirical_coverage_pct"])
        recent_width = float(row["recent_average_interval_width_mw"])
        recent_calibration = float(
            row["recent_mean_calibration_observation_count"]
        )
        minimum_recent_calibration = float(
            row["recent_minimum_calibration_observation_count"]
        )
        nominal_coverage = float(row["target_coverage_level"]) * 100.0
        coverage_shortfall = max(0.0, nominal_coverage - recent_coverage)

        values = [
            (
                "history",
                "error",
                "minimum_recent_interval_runs",
                float(recent_count),
                config["min_recent_interval_runs"],
                ">=",
                recent_count >= config["min_recent_interval_runs"],
                "Number of retained interval runs in the recent window.",
            ),
            (
                "freshness",
                "error",
                "latest_interval_run_age_minutes",
                run_age,
                config["max_interval_run_age_minutes"],
                "<=",
                run_age <= config["max_interval_run_age_minutes"],
                f"Latest interval run was {latest_run.isoformat()}.",
            ),
            (
                "freshness",
                "error",
                "latest_interval_evaluation_age_minutes",
                evaluation_age,
                config["max_evaluation_age_minutes"],
                "<=",
                evaluation_age <= config["max_evaluation_age_minutes"],
                f"Latest retained evaluation ended at {latest_evaluation.isoformat()}.",
            ),
            (
                "calibration",
                "error",
                "minimum_recent_calibration_observation_count",
                minimum_recent_calibration,
                config["min_calibration_observation_count"],
                ">=",
                minimum_recent_calibration
                >= config["min_calibration_observation_count"],
                "Minimum causal calibration history across recent interval runs.",
            ),
            (
                "coverage",
                "error",
                "maximum_recent_coverage_shortfall_pct_points",
                coverage_shortfall,
                config["max_recent_coverage_shortfall_pct_points"],
                "<=",
                coverage_shortfall
                <= config["max_recent_coverage_shortfall_pct_points"],
                "Nominal coverage minus weighted recent empirical coverage.",
            ),
            (
                "history",
                "warning",
                "minimum_reference_interval_runs",
                float(reference_count),
                config["min_reference_interval_runs"],
                ">=",
                reference_count >= config["min_reference_interval_runs"],
                "Reference history required before calculating interval drift.",
            ),
        ]
        if reference_count >= config["min_reference_interval_runs"]:
            reference_coverage = float(
                row["reference_empirical_coverage_pct"]
            )
            reference_width = float(
                row["reference_average_interval_width_mw"]
            )
            reference_calibration = float(
                row["reference_mean_calibration_observation_count"]
            )
            values.extend(
                [
                    (
                        "coverage",
                        "warning",
                        "maximum_interval_coverage_drop_pct_points",
                        reference_coverage - recent_coverage,
                        config["max_coverage_drop_pct_points"],
                        "<=",
                        reference_coverage - recent_coverage
                        <= config["max_coverage_drop_pct_points"],
                        "Reference weighted coverage minus recent weighted coverage.",
                    ),
                    (
                        "width",
                        "warning",
                        "maximum_average_interval_width_increase_pct",
                        _increase_pct(recent_width, reference_width),
                        config["max_average_interval_width_increase_pct"],
                        "<=",
                        _increase_pct(recent_width, reference_width)
                        <= config["max_average_interval_width_increase_pct"],
                        "Weighted average-width increase from reference to recent runs.",
                    ),
                    (
                        "calibration",
                        "warning",
                        "maximum_calibration_history_drop_pct",
                        _drop_pct(recent_calibration, reference_calibration),
                        config["max_calibration_history_drop_pct"],
                        "<=",
                        _drop_pct(recent_calibration, reference_calibration)
                        <= config["max_calibration_history_drop_pct"],
                        "Mean causal calibration-history decrease from reference to recent runs.",
                    ),
                ]
            )

        for scope, severity, name, observed, threshold, comparator, passed, details in values:
            checks.append(
                _check(
                    run_id=run_id,
                    run_timestamp=run_timestamp,
                    as_of=as_of,
                    scope=scope,
                    severity=severity,
                    name=name,
                    observed=observed,
                    threshold=threshold,
                    comparator=comparator,
                    passed=passed,
                    details=details,
                    identity=identity,
                )
            )
    return checks


def run_interval_monitoring(spark_session) -> tuple[DataFrame, DataFrame]:
    spark_session.conf.set("spark.sql.session.timeZone", "UTC")
    config = _configuration()
    as_of = _as_of_utc()
    run_timestamp = datetime.now(timezone.utc)
    monitor_run_id = str(uuid4())

    history, retained_interval_run_count = _prepared_metric_history(
        spark_session, config
    )
    statistics = _monitoring_statistics(history, config)
    checks = _check_rows(
        statistics,
        config=config,
        as_of=as_of,
        run_id=monitor_run_id,
        run_timestamp=run_timestamp,
    )
    if not checks:
        raise ValueError("Prediction interval monitoring produced no checks.")

    failed_errors = sum(
        1
        for check in checks
        if check["severity"] == "error" and not check["passed"]
    )
    failed_warnings = sum(
        1
        for check in checks
        if check["severity"] == "warning" and not check["passed"]
    )
    status = (
        FAILED_STATUS
        if failed_errors
        else WARNING_STATUS
        if failed_warnings
        else HEALTHY_STATUS
    )
    for check in checks:
        check["monitor_status"] = status

    summary = [
        {
            "monitor_run_id": monitor_run_id,
            "monitor_timestamp_utc": run_timestamp,
            "monitor_as_of_utc": as_of,
            "monitor_status": status,
            "automatic_remediation_allowed": False,
            "automatic_recalibration_allowed": False,
            "automatic_model_change_allowed": False,
            "automatic_schedule_change_allowed": False,
            "automatic_promotion_allowed": False,
            "check_count": len(checks),
            "passed_check_count": sum(1 for check in checks if check["passed"]),
            "failed_error_check_count": failed_errors,
            "failed_warning_check_count": failed_warnings,
            "monitored_interval_slice_count": statistics.count(),
            "retained_interval_run_count": retained_interval_run_count,
            "source_interval_metrics_table": INTERVAL_METRICS_TABLE,
            "policy_version": POLICY_VERSION,
            "monitoring_contract_version": MONITORING_CONTRACT_VERSION,
        }
    ]

    checks_frame = spark_session.createDataFrame(checks)
    summary_frame = spark_session.createDataFrame(summary)
    checks_frame.write.format("delta").mode("append").option(
        "mergeSchema", "true"
    ).saveAsTable(HEALTH_CHECKS_TABLE)
    summary_frame.write.format("delta").mode("append").option(
        "mergeSchema", "true"
    ).saveAsTable(HEALTH_SUMMARY_TABLE)

    checks_frame.orderBy(
        "source_area",
        "requested_horizon_minutes",
        "target_coverage_level",
        "model_name",
        "check_name",
    ).show(100, truncate=False)
    summary_frame.show(truncate=False)
    return checks_frame, summary_frame


if __name__ == "__main__":
    run_interval_monitoring(spark)

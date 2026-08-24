# Fabric notebook source: 06f_prediction_interval_monitoring_quality_checks
# Optional/manual blocking validation for 05f_prediction_interval_monitoring.

from datetime import datetime, timezone
from typing import Any

from pyspark.sql import functions as F


INTERVAL_METRICS_TABLE = "forecast_prediction_interval_metrics"
HEALTH_CHECKS_TABLE = "forecast_prediction_interval_health_checks"
HEALTH_SUMMARY_TABLE = "forecast_prediction_interval_health_summary"
RESULTS_TABLE = "dq_run_results"

MONITOR_RUN_ID = ""

POLICY_VERSION = "prediction-interval-monitoring-policy-v1"
MONITORING_CONTRACT_VERSION = "prediction-interval-monitoring-v1"
SUPPORTED_STATUSES = ("healthy", "warning", "failed")
SUPPORTED_SCOPES = ("history", "freshness", "calibration", "coverage", "width")
SUPPORTED_SEVERITIES = ("error", "warning")
SUPPORTED_COMPARATORS = (">=", "<=", "==")

BASE_CHECK_NAMES = (
    "minimum_recent_interval_runs",
    "latest_interval_run_age_minutes",
    "latest_interval_evaluation_age_minutes",
    "minimum_recent_calibration_observation_count",
    "maximum_recent_coverage_shortfall_pct_points",
    "minimum_reference_interval_runs",
)
DRIFT_CHECK_NAMES = (
    "maximum_interval_coverage_drop_pct_points",
    "maximum_average_interval_width_increase_pct",
    "maximum_calibration_history_drop_pct",
)
EXPECTED_CHECK_NAMES = (*BASE_CHECK_NAMES, *DRIFT_CHECK_NAMES)

SLICE_COLUMNS = [
    "source_area",
    "resource_id",
    "city",
    "requested_horizon_minutes",
    "model_name",
    "feature_contract_version",
    "target_coverage_level",
    "interval_contract_version",
    "latest_interval_run_id",
]


def _parameter(name: str, default: Any) -> Any:
    return globals().get(name, default)


def _selected_monitor_run_id(spark_session) -> str:
    requested = str(_parameter("MONITOR_RUN_ID", MONITOR_RUN_ID)).strip()
    summary = spark_session.table(HEALTH_SUMMARY_TABLE)
    if requested:
        exists = (
            summary.where(F.col("monitor_run_id") == requested)
            .limit(1)
            .count()
        )
        if not exists:
            raise ValueError(
                f"No interval monitoring run found for {requested!r}."
            )
        return requested
    row = (
        summary.orderBy(
            F.col("monitor_timestamp_utc").desc(),
            F.col("monitor_run_id").desc(),
        )
        .select("monitor_run_id")
        .first()
    )
    if row is None:
        raise ValueError("No Fabric interval monitoring run is available.")
    return str(row["monitor_run_id"])


def _required_null_failures(frame, columns: list[str]) -> int:
    condition = F.lit(False)
    for column in columns:
        condition = condition | F.col(column).isNull()
    return frame.where(condition).count()


def _blank_text_failures(frame, columns: list[str]) -> int:
    condition = F.lit(False)
    for column in columns:
        condition = condition | (
            F.trim(F.col(column).cast("string")) == ""
        )
    return frame.where(condition).count()


def _duplicate_check_failures(checks) -> int:
    identity = ["monitor_run_id", *SLICE_COLUMNS, "check_name"]
    return (
        checks.groupBy(*identity)
        .agg(F.count(F.lit(1)).alias("row_count"))
        .where(F.col("row_count") != 1)
        .count()
    )


def _comparator_failures(checks) -> int:
    expected = (
        F.when(
            F.col("comparator") == ">=",
            F.col("observed_value") >= F.col("threshold_value"),
        )
        .when(
            F.col("comparator") == "<=",
            F.col("observed_value") <= F.col("threshold_value"),
        )
        .when(
            F.col("comparator") == "==",
            F.abs(
                F.col("observed_value") - F.col("threshold_value")
            )
            <= F.lit(1e-9),
        )
    )
    return checks.where(
        expected.isNull() | (F.col("passed") != expected)
    ).count()


def _check_set_failures(checks) -> int:
    unsupported = checks.where(
        ~F.col("check_name").isin(*EXPECTED_CHECK_NAMES)
    ).count()
    base = (
        checks.where(F.col("check_name").isin(*BASE_CHECK_NAMES))
        .groupBy(*SLICE_COLUMNS)
        .agg(F.countDistinct("check_name").alias("base_check_count"))
    )
    missing_base = base.where(
        F.col("base_check_count") != len(BASE_CHECK_NAMES)
    ).count()
    slice_count = checks.select(*SLICE_COLUMNS).distinct().count()
    missing_slice = max(0, slice_count - base.count())
    return unsupported + missing_base + missing_slice


def _reference_drift_contract_failures(checks) -> int:
    reference = checks.where(
        F.col("check_name") == "minimum_reference_interval_runs"
    ).select(
        *SLICE_COLUMNS,
        F.col("passed").alias("_reference_history_ready"),
    )
    drift = (
        checks.where(F.col("check_name").isin(*DRIFT_CHECK_NAMES))
        .groupBy(*SLICE_COLUMNS)
        .agg(F.countDistinct("check_name").alias("_drift_check_count"))
    )
    joined = reference.join(drift, on=SLICE_COLUMNS, how="left").fillna(
        {"_drift_check_count": 0}
    )
    return joined.where(
        (
            F.col("_reference_history_ready")
            & (F.col("_drift_check_count") != len(DRIFT_CHECK_NAMES))
        )
        | (
            (~F.col("_reference_history_ready"))
            & (F.col("_drift_check_count") != 0)
        )
    ).count()


def _source_metric_binding_failures(checks, metrics) -> int:
    source = metrics.select(
        F.col("interval_run_id").alias("latest_interval_run_id"),
        F.lower(F.trim(F.col("source_area"))).alias("_source_area_key"),
        F.lower(F.trim(F.col("resource_id"))).alias("_resource_id_key"),
        F.lower(F.trim(F.col("city"))).alias("_city_key"),
        "requested_horizon_minutes",
        F.lower(F.trim(F.col("model_name"))).alias("_model_name_key"),
        F.lower(F.trim(F.col("feature_contract_version"))).alias(
            "_feature_contract_version_key"
        ),
        "target_coverage_level",
        F.lower(F.trim(F.col("interval_contract_version"))).alias(
            "_interval_contract_version_key"
        ),
    ).dropDuplicates()
    check_slices = checks.select(
        "latest_interval_run_id",
        F.lower(F.trim(F.col("source_area"))).alias("_source_area_key"),
        F.lower(F.trim(F.col("resource_id"))).alias("_resource_id_key"),
        F.lower(F.trim(F.col("city"))).alias("_city_key"),
        "requested_horizon_minutes",
        F.lower(F.trim(F.col("model_name"))).alias("_model_name_key"),
        F.lower(F.trim(F.col("feature_contract_version"))).alias(
            "_feature_contract_version_key"
        ),
        "target_coverage_level",
        F.lower(F.trim(F.col("interval_contract_version"))).alias(
            "_interval_contract_version_key"
        ),
    ).dropDuplicates()
    keys = [
        "latest_interval_run_id",
        "_source_area_key",
        "_resource_id_key",
        "_city_key",
        "requested_horizon_minutes",
        "_model_name_key",
        "_feature_contract_version_key",
        "target_coverage_level",
        "_interval_contract_version_key",
    ]
    return check_slices.join(source, on=keys, how="left_anti").count()


def _summary_consistency_failures(checks, summary) -> int:
    if summary.count() != 1:
        return 1
    row = summary.first()
    if row is None:
        return 1
    check_count = checks.count()
    passed_count = checks.where(F.col("passed")).count()
    failed_error_count = checks.where(
        (F.col("severity") == "error") & (~F.col("passed"))
    ).count()
    failed_warning_count = checks.where(
        (F.col("severity") == "warning") & (~F.col("passed"))
    ).count()
    slice_count = checks.select(*SLICE_COLUMNS).distinct().count()
    expected_status = (
        "failed"
        if failed_error_count
        else "warning"
        if failed_warning_count
        else "healthy"
    )
    failures = [
        int(row["check_count"]) != check_count,
        int(row["passed_check_count"]) != passed_count,
        int(row["failed_error_check_count"]) != failed_error_count,
        int(row["failed_warning_check_count"]) != failed_warning_count,
        int(row["monitored_interval_slice_count"]) != slice_count,
        int(row["retained_interval_run_count"]) < 1,
        str(row["monitor_status"]) != expected_status,
    ]
    return int(any(failures))


def _authority_failures(summary) -> int:
    authority_columns = [
        "automatic_remediation_allowed",
        "automatic_recalibration_allowed",
        "automatic_model_change_allowed",
        "automatic_schedule_change_allowed",
        "automatic_promotion_allowed",
    ]
    condition = F.lit(False)
    for column in authority_columns:
        condition = condition | (F.col(column) != F.lit(False))
    return summary.where(condition).count()


def run_checks(spark_session):
    spark_session.conf.set("spark.sql.session.timeZone", "UTC")
    monitor_run_id = _selected_monitor_run_id(spark_session)
    checks = spark_session.table(HEALTH_CHECKS_TABLE).where(
        F.col("monitor_run_id") == monitor_run_id
    )
    summary = spark_session.table(HEALTH_SUMMARY_TABLE).where(
        F.col("monitor_run_id") == monitor_run_id
    )
    metrics = spark_session.table(INTERVAL_METRICS_TABLE)

    required_check_fields = [
        "monitor_run_id",
        "monitor_timestamp_utc",
        "monitor_as_of_utc",
        "check_scope",
        "severity",
        "check_name",
        "observed_value",
        "threshold_value",
        "comparator",
        "passed",
        "details",
        *SLICE_COLUMNS,
        "policy_version",
        "monitoring_contract_version",
    ]
    required_summary_fields = [
        "monitor_run_id",
        "monitor_timestamp_utc",
        "monitor_as_of_utc",
        "monitor_status",
        "automatic_remediation_allowed",
        "automatic_recalibration_allowed",
        "automatic_model_change_allowed",
        "automatic_schedule_change_allowed",
        "automatic_promotion_allowed",
        "check_count",
        "passed_check_count",
        "failed_error_check_count",
        "failed_warning_check_count",
        "monitored_interval_slice_count",
        "retained_interval_run_count",
        "policy_version",
        "monitoring_contract_version",
    ]

    missing_check_columns = sorted(
        set(required_check_fields) - set(checks.columns)
    )
    missing_summary_columns = sorted(
        set(required_summary_fields) - set(summary.columns)
    )
    if missing_check_columns or missing_summary_columns:
        raise ValueError(
            "Interval monitoring evidence is missing required columns: "
            + ", ".join(missing_check_columns + missing_summary_columns)
            + "."
        )

    contract_failures = checks.where(
        (~F.col("check_scope").isin(*SUPPORTED_SCOPES))
        | (~F.col("severity").isin(*SUPPORTED_SEVERITIES))
        | (~F.col("comparator").isin(*SUPPORTED_COMPARATORS))
        | (F.col("policy_version") != POLICY_VERSION)
        | (
            F.col("monitoring_contract_version")
            != MONITORING_CONTRACT_VERSION
        )
        | F.col("observed_value").isNull()
        | F.isnan("observed_value")
        | F.col("threshold_value").isNull()
        | F.isnan("threshold_value")
        | (F.col("requested_horizon_minutes") <= 0)
        | (F.col("target_coverage_level") <= 0)
        | (F.col("target_coverage_level") >= 1)
    ).count()
    summary_contract_failures = summary.where(
        (~F.col("monitor_status").isin(*SUPPORTED_STATUSES))
        | (F.col("policy_version") != POLICY_VERSION)
        | (
            F.col("monitoring_contract_version")
            != MONITORING_CONTRACT_VERSION
        )
        | (F.col("check_count") <= 0)
        | (F.col("monitored_interval_slice_count") <= 0)
        | (F.col("retained_interval_run_count") <= 0)
    ).count()

    validations = [
        {
            "check_name": "prediction_interval_monitor_checks_not_empty",
            "failed_rows": int(checks.limit(1).count() == 0),
        },
        {
            "check_name": "prediction_interval_monitor_summary_not_empty",
            "failed_rows": int(summary.limit(1).count() == 0),
        },
        {
            "check_name": "prediction_interval_monitor_required_fields",
            "failed_rows": (
                _required_null_failures(checks, required_check_fields)
                + _required_null_failures(summary, required_summary_fields)
                + _blank_text_failures(
                    checks,
                    [
                        "monitor_run_id",
                        "check_scope",
                        "severity",
                        "check_name",
                        "details",
                        "source_area",
                        "resource_id",
                        "city",
                        "model_name",
                        "feature_contract_version",
                        "interval_contract_version",
                        "latest_interval_run_id",
                        "policy_version",
                        "monitoring_contract_version",
                    ],
                )
            ),
        },
        {
            "check_name": "prediction_interval_monitor_contract",
            "failed_rows": contract_failures + summary_contract_failures,
        },
        {
            "check_name": "prediction_interval_monitor_check_identity_unique",
            "failed_rows": _duplicate_check_failures(checks),
        },
        {
            "check_name": "prediction_interval_monitor_check_set_complete",
            "failed_rows": _check_set_failures(checks),
        },
        {
            "check_name": "prediction_interval_monitor_comparator_consistency",
            "failed_rows": _comparator_failures(checks),
        },
        {
            "check_name": "prediction_interval_monitor_source_metric_binding",
            "failed_rows": _source_metric_binding_failures(checks, metrics),
        },
        {
            "check_name": "prediction_interval_monitor_reference_drift_contract",
            "failed_rows": _reference_drift_contract_failures(checks),
        },
        {
            "check_name": "prediction_interval_monitor_summary_consistency",
            "failed_rows": _summary_consistency_failures(checks, summary),
        },
        {
            "check_name": "prediction_interval_monitor_authority_boundary",
            "failed_rows": _authority_failures(summary),
        },
    ]

    now = datetime.now(timezone.utc)
    results = [
        {
            "run_timestamp_utc": now,
            "monitor_run_id": monitor_run_id,
            "check_name": validation["check_name"],
            "severity": "error",
            "failed_rows": int(validation["failed_rows"]),
            "status": (
                "passed"
                if int(validation["failed_rows"]) == 0
                else "failed"
            ),
        }
        for validation in validations
    ]
    result_frame = spark_session.createDataFrame(results)
    result_frame.write.format("delta").mode("append").option(
        "mergeSchema", "true"
    ).saveAsTable(RESULTS_TABLE)
    result_frame.orderBy("check_name").show(truncate=False)

    failures = [
        result for result in results if result["failed_rows"] > 0
    ]
    if failures:
        detail = ", ".join(
            f"{result['check_name']}={result['failed_rows']}"
            for result in failures
        )
        raise ValueError(
            f"Prediction interval monitoring checks failed: {detail}"
        )
    return results


if __name__ == "__main__":
    run_checks(spark)

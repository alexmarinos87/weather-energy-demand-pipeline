from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def text(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def test_fabric_interval_monitor_reads_retained_metrics_and_writes_separate_evidence():
    notebook = text("fabric/notebooks/05f_prediction_interval_monitoring.py")
    assert (
        'INTERVAL_METRICS_TABLE = "forecast_prediction_interval_metrics"'
        in notebook
    )
    assert (
        'HEALTH_CHECKS_TABLE = '
        '"forecast_prediction_interval_health_checks"'
        in notebook
    )
    assert (
        'HEALTH_SUMMARY_TABLE = '
        '"forecast_prediction_interval_health_summary"'
        in notebook
    )
    assert '.mode("append")' in notebook
    assert '"mergeSchema", "true"' in notebook
    assert "No point model is fitted or refitted" in notebook
    assert "LinearRegression" not in notebook
    assert ".fit(" not in notebook
    assert '.withColumn("calibration_radius_mw"' not in notebook


def test_fabric_interval_monitor_matches_the_local_default_policy():
    notebook = text("fabric/notebooks/05f_prediction_interval_monitoring.py")
    local = text("forecasting/interval_monitoring.py")
    expected = {
        "RECENT_INTERVAL_RUN_COUNT": 3,
        "REFERENCE_INTERVAL_RUN_COUNT": 6,
        "MIN_RECENT_INTERVAL_RUNS": 2,
        "MIN_REFERENCE_INTERVAL_RUNS": 3,
        "MAX_INTERVAL_RUN_AGE_MINUTES": 10080,
        "MAX_EVALUATION_AGE_MINUTES": 20160,
        "MIN_CALIBRATION_OBSERVATION_COUNT": 24,
    }
    for name, value in expected.items():
        assert f"{name} = {value}" in notebook
    for value in ("5.0", "25.0"):
        assert value in notebook
    assert (
        'POLICY_VERSION = "prediction-interval-monitoring-policy-v1"'
        in notebook
    )
    assert (
        'MONITORING_CONTRACT_VERSION = '
        '"prediction-interval-monitoring-v1"'
        in notebook
    )
    assert (
        'POLICY_VERSION = "prediction-interval-monitoring-policy-v1"'
        in local
    )
    assert (
        'MONITORING_VERSION = "prediction-interval-monitoring-v1"'
        in local
    )


def test_fabric_interval_monitor_preserves_exact_slices_and_bounded_history():
    notebook = text("fabric/notebooks/05f_prediction_interval_monitoring.py")
    for column in (
        "source_area",
        "resource_id",
        "city",
        "requested_horizon_minutes",
        "model_name",
        "feature_contract_version",
        "target_coverage_level",
        "interval_contract_version",
    ):
        assert f'"{column}"' in notebook
    assert "Window.partitionBy(*SLICE_KEY_COLUMNS)" in notebook
    assert 'F.col("interval_run_timestamp_utc").desc()' in notebook
    assert 'config["recent_interval_run_count"]' in notebook
    assert 'config["reference_interval_run_count"]' in notebook
    assert "retained_limit" in notebook
    assert "duplicate run/slice identities" in notebook


def test_fabric_interval_monitor_weights_coverage_and_width_by_evaluation_rows():
    notebook = text("fabric/notebooks/05f_prediction_interval_monitoring.py")
    assert 'weight = F.col("evaluation_observation_count").cast("double")' in notebook
    assert 'F.sum(F.col("empirical_coverage_pct") * weight)' in notebook
    assert 'F.sum(F.col("average_interval_width_mw") * weight)' in notebook
    assert '"recent_minimum_calibration_observation_count"' in notebook


def test_fabric_interval_monitor_emits_the_complete_health_contract():
    notebook = text("fabric/notebooks/05f_prediction_interval_monitoring.py")
    for check_name in (
        "minimum_recent_interval_runs",
        "latest_interval_run_age_minutes",
        "latest_interval_evaluation_age_minutes",
        "minimum_recent_calibration_observation_count",
        "maximum_recent_coverage_shortfall_pct_points",
        "minimum_reference_interval_runs",
        "maximum_interval_coverage_drop_pct_points",
        "maximum_average_interval_width_increase_pct",
        "maximum_calibration_history_drop_pct",
    ):
        assert f'"{check_name}"' in notebook
    assert (
        'reference_count >= config["min_reference_interval_runs"]'
        in notebook
    )
    assert "No drift value is fabricated" not in notebook
    assert "Prediction interval monitoring produced no checks." in notebook


def test_fabric_interval_monitor_summary_prohibits_automatic_action():
    notebook = text("fabric/notebooks/05f_prediction_interval_monitoring.py")
    for field in (
        "automatic_remediation_allowed",
        "automatic_recalibration_allowed",
        "automatic_model_change_allowed",
        "automatic_schedule_change_allowed",
        "automatic_promotion_allowed",
    ):
        assert f'"{field}": False' in notebook
    assert "healthy" in notebook
    assert "warning" in notebook
    assert "failed" in notebook


def test_fabric_interval_monitor_quality_gate_is_independent_and_blocking():
    checks = text(
        "fabric/notebooks/"
        "06f_prediction_interval_monitoring_quality_checks.py"
    )
    assert (
        'HEALTH_CHECKS_TABLE = '
        '"forecast_prediction_interval_health_checks"'
        in checks
    )
    assert (
        'HEALTH_SUMMARY_TABLE = '
        '"forecast_prediction_interval_health_summary"'
        in checks
    )
    assert 'RESULTS_TABLE = "dq_run_results"' in checks
    for check_name in (
        "prediction_interval_monitor_checks_not_empty",
        "prediction_interval_monitor_summary_not_empty",
        "prediction_interval_monitor_required_fields",
        "prediction_interval_monitor_contract",
        "prediction_interval_monitor_check_identity_unique",
        "prediction_interval_monitor_check_set_complete",
        "prediction_interval_monitor_comparator_consistency",
        "prediction_interval_monitor_source_metric_binding",
        "prediction_interval_monitor_reference_drift_contract",
        "prediction_interval_monitor_summary_consistency",
        "prediction_interval_monitor_authority_boundary",
    ):
        assert f'"{check_name}"' in checks
    assert "left_anti" in checks
    assert "raise ValueError" in checks


def test_fabric_interval_monitor_quality_gate_validates_conditional_drift_checks():
    checks = text(
        "fabric/notebooks/"
        "06f_prediction_interval_monitoring_quality_checks.py"
    )
    assert '"minimum_reference_interval_runs"' in checks
    assert "DRIFT_CHECK_NAMES" in checks
    assert "_reference_history_ready" in checks
    assert "_drift_check_count" in checks
    assert "len(DRIFT_CHECK_NAMES)" in checks


def test_fabric_interval_monitor_pipeline_is_manual_and_non_mutating():
    pipeline = text(
        "fabric/pipelines/prediction_interval_monitoring_pipeline.md"
    )
    assert "manual, advisory, and evidence-only" in pipeline
    assert "does not replace `05e_prediction_intervals`" in pipeline
    assert "does not create a new interval" in pipeline
    assert "interval_recalibration_performed=false" in pipeline
    assert "automatic_recalibration_allowed=false" in pipeline
    assert "Do not enable a trigger" in pipeline


def test_fabric_interval_monitor_views_are_pass_through_only():
    views = text(
        "fabric/sql/"
        "forecast_prediction_interval_monitoring_views_tsql.sql"
    )
    assert "FROM dbo.forecast_prediction_interval_health_checks" in views
    assert "FROM dbo.forecast_prediction_interval_health_summary" in views
    assert "JOIN" not in views
    assert "ROW_NUMBER" not in views
    assert "GROUP BY" not in views


def test_fabric_docs_record_interval_monitoring_parity_and_manual_operation():
    fabric = text("fabric/README.md")
    monitoring = text("fabric/monitoring/monitoring_runbook.md")
    interval = text("INTERVAL_MONITORING.md")
    assert "05f_prediction_interval_monitoring" in fabric
    assert "06f_prediction_interval_monitoring_quality_checks" in fabric
    assert "prediction_interval_monitoring_pipeline" in fabric
    assert "forecast_prediction_interval_health_checks" in fabric
    assert "forecast_prediction_interval_health_summary" in fabric
    assert "Manual interval-health checks" in monitoring
    assert "prediction-interval-monitoring-v1" in monitoring
    assert "Optional/manual Fabric parity" in interval
    assert "No alert delivery" in interval


def test_roadmap_records_fabric_interval_monitoring_and_advances_to_portfolio_history():
    roadmap = text("ROADMAP.md")
    assert (
        "G22 | Interval coverage, width, calibration-history, and freshness "
        "monitoring without automatic recalibration | Implemented as advisory "
        "local evidence"
        in roadmap
    )
    assert (
        "G23 | Fabric parity for advisory interval monitoring over retained "
        "interval metrics without automatic recalibration | Implemented as an "
        "optional manual subflow"
        in roadmap
    )
    assert (
        "G24 | Multi-area portfolio-demo integration for repeated "
        "interval-health evidence and advisory operator reporting | Next"
        in roadmap
    )

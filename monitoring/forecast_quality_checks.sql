-- SQL endpoint checks for the latest bounded 30/60-minute baseline run.

WITH latest_run AS (
    SELECT TOP (1) run_id
    FROM dbo.forecast_baseline_predictions
    ORDER BY run_timestamp_utc DESC, run_id DESC
)
SELECT 'forecast_prediction_training_boundary' AS check_name, COUNT_BIG(*) AS failed_rows
FROM dbo.forecast_baseline_predictions
WHERE run_id = (SELECT run_id FROM latest_run)
  AND feature_timestamp_utc <= trained_through_utc

UNION ALL
SELECT 'forecast_prediction_time_horizon_valid', COUNT_BIG(*)
FROM dbo.forecast_baseline_predictions
WHERE run_id = (SELECT run_id FROM latest_run)
  AND (
       event_timestamp_utc <= feature_timestamp_utc
    OR requested_horizon_minutes NOT IN (30, 60)
    OR target_tolerance_minutes < 0
    OR horizon_steps < 1
    OR horizon_minutes < requested_horizon_minutes
    OR target_delay_minutes < 0
    OR target_delay_minutes > target_tolerance_minutes
    OR ABS(horizon_minutes - requested_horizon_minutes - target_delay_minutes) > 0.000001
    OR feature_contract_version <> 'time-horizon-v1'
  )

UNION ALL
SELECT 'forecast_prediction_required_fields', COUNT_BIG(*)
FROM dbo.forecast_baseline_predictions
WHERE run_id = (SELECT run_id FROM latest_run)
  AND (
       source_area IS NULL
    OR resource_id IS NULL
    OR city IS NULL
    OR feature_timestamp_utc IS NULL
    OR event_timestamp_utc IS NULL
    OR requested_horizon_minutes IS NULL
    OR target_tolerance_minutes IS NULL
    OR horizon_steps IS NULL
    OR horizon_minutes IS NULL
    OR target_delay_minutes IS NULL
    OR split IS NULL
    OR model_name IS NULL
    OR current_demand_mw IS NULL
    OR actual_demand_mw IS NULL
    OR predicted_demand_mw IS NULL
    OR trained_through_utc IS NULL
    OR feature_contract_version IS NULL
  )

UNION ALL
SELECT 'forecast_target_coverage_valid', COUNT_BIG(*)
FROM dbo.forecast_baseline_metrics
WHERE run_id = (SELECT run_id FROM latest_run)
  AND (
       eligible_target_count <= 0
    OR matched_target_count <= 0
    OR matched_target_count > eligible_target_count
    OR target_coverage_pct < minimum_target_coverage_pct
    OR target_coverage_pct < 0
    OR target_coverage_pct > 100
    OR target_delay_minutes_max > target_tolerance_minutes
  )

UNION ALL
SELECT 'forecast_metrics_valid', COUNT_BIG(*)
FROM dbo.forecast_baseline_metrics
WHERE run_id = (SELECT run_id FROM latest_run)
  AND (
       observation_count <= 0
    OR requested_horizon_minutes NOT IN (30, 60)
    OR target_tolerance_minutes < 0
    OR horizon_steps_avg < 1
    OR horizon_minutes_avg < requested_horizon_minutes
    OR target_delay_minutes_avg < 0
    OR mae_mw < 0
    OR rmse_mw < 0
    OR evaluation_feature_start_utc <= trained_through_utc
    OR evaluation_feature_end_utc < evaluation_feature_start_utc
    OR evaluation_start_utc <= evaluation_feature_start_utc
    OR evaluation_end_utc < evaluation_start_utc
    OR feature_contract_version <> 'time-horizon-v1'
  );

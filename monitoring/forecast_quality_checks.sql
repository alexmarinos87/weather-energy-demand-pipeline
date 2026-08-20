-- SQL endpoint checks for the latest future-horizon baseline run.

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
SELECT 'forecast_prediction_horizon_valid', COUNT_BIG(*)
FROM dbo.forecast_baseline_predictions
WHERE run_id = (SELECT run_id FROM latest_run)
  AND (
       event_timestamp_utc <= feature_timestamp_utc
    OR horizon_steps < 1
    OR horizon_minutes <= 0
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
    OR horizon_steps IS NULL
    OR horizon_minutes IS NULL
    OR split IS NULL
    OR model_name IS NULL
    OR current_demand_mw IS NULL
    OR actual_demand_mw IS NULL
    OR predicted_demand_mw IS NULL
    OR trained_through_utc IS NULL
  )

UNION ALL
SELECT 'forecast_metrics_valid', COUNT_BIG(*)
FROM dbo.forecast_baseline_metrics
WHERE run_id = (SELECT run_id FROM latest_run)
  AND (
       observation_count <= 0
    OR horizon_steps < 1
    OR mae_mw < 0
    OR rmse_mw < 0
    OR evaluation_feature_start_utc <= trained_through_utc
    OR evaluation_feature_end_utc < evaluation_feature_start_utc
    OR evaluation_start_utc <= evaluation_feature_start_utc
    OR evaluation_end_utc < evaluation_start_utc
  );

-- SQL endpoint checks for the latest baseline forecasting run.

WITH latest_run AS (
    SELECT TOP (1) run_id
    FROM dbo.forecast_baseline_predictions
    ORDER BY run_timestamp_utc DESC, run_id DESC
)
SELECT 'forecast_prediction_training_boundary' AS check_name, COUNT_BIG(*) AS failed_rows
FROM dbo.forecast_baseline_predictions
WHERE run_id = (SELECT run_id FROM latest_run)
  AND event_timestamp_utc <= trained_through_utc

UNION ALL
SELECT 'forecast_prediction_required_fields', COUNT_BIG(*)
FROM dbo.forecast_baseline_predictions
WHERE run_id = (SELECT run_id FROM latest_run)
  AND (
       source_area IS NULL
    OR resource_id IS NULL
    OR city IS NULL
    OR event_timestamp_utc IS NULL
    OR split IS NULL
    OR model_name IS NULL
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
    OR mae_mw < 0
    OR rmse_mw < 0
    OR evaluation_start_utc <= trained_through_utc
    OR evaluation_end_utc < evaluation_start_utc
  );

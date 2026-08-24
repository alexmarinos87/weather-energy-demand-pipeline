-- Analyst-facing pass-through views for calibration-only interval evidence.

CREATE OR ALTER VIEW dbo.forecast_prediction_intervals_v AS
SELECT *
FROM dbo.forecast_prediction_intervals;
GO

CREATE OR ALTER VIEW dbo.forecast_prediction_interval_metrics_v AS
SELECT *
FROM dbo.forecast_prediction_interval_metrics;
GO

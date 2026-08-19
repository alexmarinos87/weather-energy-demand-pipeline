-- Analyst-facing pass-through views for baseline evaluation evidence.

CREATE OR ALTER VIEW dbo.forecast_baseline_predictions_v AS
SELECT *
FROM dbo.forecast_baseline_predictions;
GO

CREATE OR ALTER VIEW dbo.forecast_baseline_metrics_v AS
SELECT *
FROM dbo.forecast_baseline_metrics;
GO

-- Analyst-facing pass-through views for advisory interval-health evidence.

CREATE OR ALTER VIEW dbo.forecast_prediction_interval_health_checks_v AS
SELECT *
FROM dbo.forecast_prediction_interval_health_checks;
GO

CREATE OR ALTER VIEW dbo.forecast_prediction_interval_health_summary_v AS
SELECT *
FROM dbo.forecast_prediction_interval_health_summary;
GO

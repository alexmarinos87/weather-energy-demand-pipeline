-- Pass-through analyst views for the optional elapsed-time seasonal comparison.

CREATE OR ALTER VIEW dbo.forecast_seasonal_comparison_predictions_v AS
SELECT *
FROM dbo.forecast_seasonal_comparison_predictions;
GO

CREATE OR ALTER VIEW dbo.forecast_seasonal_comparison_metrics_v AS
SELECT *
FROM dbo.forecast_seasonal_comparison_metrics;
GO

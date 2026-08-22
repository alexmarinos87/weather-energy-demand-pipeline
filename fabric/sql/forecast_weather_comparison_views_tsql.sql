-- Pass-through analyst views for the optional Fabric target-weather comparison.

CREATE OR ALTER VIEW dbo.forecast_weather_comparison_predictions_v AS
SELECT *
FROM dbo.forecast_weather_comparison_predictions;
GO

CREATE OR ALTER VIEW dbo.forecast_weather_comparison_metrics_v AS
SELECT *
FROM dbo.forecast_weather_comparison_metrics;
GO

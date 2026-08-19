-- Analyst-facing Microsoft Fabric SQL endpoint views.
-- Spark Delta tables are the canonical gold implementation; these views do not
-- repeat joins or window logic in a second SQL dialect.

CREATE OR ALTER VIEW dbo.gold_weather_demand_join_v AS
SELECT *
FROM dbo.gold_weather_demand_join;
GO

CREATE OR ALTER VIEW dbo.gold_feature_engineering_v AS
SELECT *
FROM dbo.gold_feature_engineering;
GO

CREATE OR ALTER VIEW dbo.gold_demand_aggregation_v AS
SELECT *
FROM dbo.gold_demand_aggregation;
GO

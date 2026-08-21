"""Leakage-safe demand forecasting, target weather, and evaluation contracts."""

from forecasting.baseline import (
    BacktestConfig,
    ForecastWeatherConfig,
    ForecastWeatherContractError,
    ForecastingContractError,
    RollingOriginFold,
    attach_target_forecast_weather,
    build_demo_feature_frame,
    build_demo_forecast_weather_frame,
    build_rolling_origin_folds,
    build_supervised_frame,
    prepare_feature_frame,
    prepare_forecast_weather_frame,
    run_chronological_backtest,
    run_rolling_origin_backtest,
)

__all__ = [
    "BacktestConfig",
    "ForecastWeatherConfig",
    "ForecastWeatherContractError",
    "ForecastingContractError",
    "RollingOriginFold",
    "attach_target_forecast_weather",
    "build_demo_feature_frame",
    "build_demo_forecast_weather_frame",
    "build_rolling_origin_folds",
    "build_supervised_frame",
    "prepare_feature_frame",
    "prepare_forecast_weather_frame",
    "run_chronological_backtest",
    "run_rolling_origin_backtest",
]

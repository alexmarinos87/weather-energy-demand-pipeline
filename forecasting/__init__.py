"""Leakage-safe demand forecasting baselines and rolling-origin evaluation."""

from forecasting.baseline import (
    BacktestConfig,
    ForecastingContractError,
    RollingOriginFold,
    build_demo_feature_frame,
    build_rolling_origin_folds,
    build_supervised_frame,
    run_chronological_backtest,
    run_rolling_origin_backtest,
)

__all__ = [
    "BacktestConfig",
    "ForecastingContractError",
    "RollingOriginFold",
    "build_demo_feature_frame",
    "build_rolling_origin_folds",
    "build_supervised_frame",
    "run_chronological_backtest",
    "run_rolling_origin_backtest",
]

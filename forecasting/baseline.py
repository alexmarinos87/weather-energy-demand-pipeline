"""Public compatibility surface for leakage-safe baseline forecasting."""

from forecasting.contracts import (
    BacktestConfig,
    ForecastingContractError,
    build_supervised_frame,
    prepare_feature_frame,
)
from forecasting.demo import build_demo_feature_frame
from forecasting.evaluation import run_chronological_backtest
from forecasting.rolling_origin import (
    RollingOriginFold,
    build_rolling_origin_folds,
    run_rolling_origin_backtest,
)

__all__ = [
    "BacktestConfig",
    "ForecastingContractError",
    "RollingOriginFold",
    "build_demo_feature_frame",
    "build_rolling_origin_folds",
    "build_supervised_frame",
    "prepare_feature_frame",
    "run_chronological_backtest",
    "run_rolling_origin_backtest",
]

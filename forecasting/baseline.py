"""Public compatibility surface for leakage-safe baseline forecasting."""

from forecasting.contracts import (
    BacktestConfig,
    ForecastingContractError,
    prepare_feature_frame,
)
from forecasting.demo import build_demo_feature_frame
from forecasting.evaluation import run_chronological_backtest

__all__ = [
    "BacktestConfig",
    "ForecastingContractError",
    "build_demo_feature_frame",
    "prepare_feature_frame",
    "run_chronological_backtest",
]

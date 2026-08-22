"""Public compatibility surface for leakage-safe baseline forecasting."""

from forecasting.contracts import (
    BacktestConfig,
    ForecastingContractError,
    build_supervised_frame,
    prepare_feature_frame,
)
from forecasting.demo import build_demo_feature_frame
from forecasting.evaluation import run_chronological_backtest
from forecasting.forecast_weather import (
    ForecastWeatherConfig,
    ForecastWeatherContractError,
    attach_target_forecast_weather,
    build_demo_forecast_weather_frame,
    prepare_forecast_weather_frame,
)
from forecasting.promotion_assessment import (
    TargetWeatherPromotionError,
    TargetWeatherPromotionPolicy,
    assess_target_weather_promotion,
    prepare_comparison_predictions,
    prepare_reconciliation_metrics,
)
from forecasting.rolling_origin import (
    RollingOriginFold,
    build_rolling_origin_folds,
    run_rolling_origin_backtest,
)
from forecasting.weather_comparison import (
    prepare_weather_model_comparison,
    run_weather_model_comparison,
)
from forecasting.weather_reconciliation import (
    ForecastWeatherReconciliationConfig,
    ForecastWeatherReconciliationError,
    prepare_forecast_reconciliation_input,
    prepare_observed_weather_input,
    reconcile_forecast_weather,
)

__all__ = [
    "BacktestConfig",
    "ForecastWeatherConfig",
    "ForecastWeatherContractError",
    "ForecastWeatherReconciliationConfig",
    "ForecastWeatherReconciliationError",
    "ForecastingContractError",
    "RollingOriginFold",
    "TargetWeatherPromotionError",
    "TargetWeatherPromotionPolicy",
    "assess_target_weather_promotion",
    "attach_target_forecast_weather",
    "build_demo_feature_frame",
    "build_demo_forecast_weather_frame",
    "build_rolling_origin_folds",
    "build_supervised_frame",
    "prepare_comparison_predictions",
    "prepare_feature_frame",
    "prepare_forecast_reconciliation_input",
    "prepare_forecast_weather_frame",
    "prepare_observed_weather_input",
    "prepare_reconciliation_metrics",
    "prepare_weather_model_comparison",
    "reconcile_forecast_weather",
    "run_chronological_backtest",
    "run_rolling_origin_backtest",
    "run_weather_model_comparison",
]

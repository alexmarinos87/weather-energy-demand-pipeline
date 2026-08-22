"""Public compatibility surface for leakage-safe baseline forecasting."""

from forecasting.contracts import (
    BacktestConfig,
    ForecastingContractError,
    build_supervised_frame,
    prepare_feature_frame,
)
from forecasting.demo import build_demo_feature_frame
from forecasting.evaluation import run_chronological_backtest
from forecasting.evidence_lifecycle import (
    EvidenceLifecycleError,
    inventory_evidence,
    load_protected_candidate_references,
    load_retention_policy,
    plan_evidence_lifecycle,
)
from forecasting.forecast_weather import (
    ForecastWeatherConfig,
    ForecastWeatherContractError,
    attach_target_forecast_weather,
    build_demo_forecast_weather_frame,
    prepare_forecast_weather_frame,
)
from forecasting.model_registry import (
    ModelCandidateRegistryError,
    load_candidate_history,
    register_candidate,
    transition_candidate,
    verify_candidate_history,
    verify_manifest,
    write_candidate_revision,
)
from forecasting.promotion_assessment import (
    TargetWeatherPromotionError,
    TargetWeatherPromotionPolicy,
    assess_target_weather_promotion,
    prepare_comparison_predictions,
    prepare_reconciliation_metrics,
)
from forecasting.provider_monitoring import (
    ForecastProviderMonitoringConfig,
    ForecastProviderMonitoringError,
    monitor_forecast_provider_health,
    prepare_forecast_snapshot_evidence,
    prepare_reconciliation_history,
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
    "EvidenceLifecycleError",
    "ForecastProviderMonitoringConfig",
    "ForecastProviderMonitoringError",
    "ForecastWeatherConfig",
    "ForecastWeatherContractError",
    "ForecastWeatherReconciliationConfig",
    "ForecastWeatherReconciliationError",
    "ForecastingContractError",
    "ModelCandidateRegistryError",
    "RollingOriginFold",
    "TargetWeatherPromotionError",
    "TargetWeatherPromotionPolicy",
    "assess_target_weather_promotion",
    "attach_target_forecast_weather",
    "build_demo_feature_frame",
    "build_demo_forecast_weather_frame",
    "build_rolling_origin_folds",
    "build_supervised_frame",
    "inventory_evidence",
    "load_candidate_history",
    "load_protected_candidate_references",
    "load_retention_policy",
    "monitor_forecast_provider_health",
    "plan_evidence_lifecycle",
    "prepare_comparison_predictions",
    "prepare_feature_frame",
    "prepare_forecast_reconciliation_input",
    "prepare_forecast_snapshot_evidence",
    "prepare_forecast_weather_frame",
    "prepare_observed_weather_input",
    "prepare_reconciliation_history",
    "prepare_reconciliation_metrics",
    "prepare_weather_model_comparison",
    "reconcile_forecast_weather",
    "register_candidate",
    "run_chronological_backtest",
    "run_rolling_origin_backtest",
    "run_weather_model_comparison",
    "transition_candidate",
    "verify_candidate_history",
    "verify_manifest",
    "write_candidate_revision",
]

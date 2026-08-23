"""Public compatibility surface for leakage-safe baseline forecasting."""

from forecasting.contracts import (
    BacktestConfig,
    ForecastingContractError,
    build_supervised_frame,
    prepare_feature_frame,
)
from forecasting.demo import build_demo_feature_frame
from forecasting.evaluation import run_chronological_backtest
from forecasting.evidence_bundle import (
    EvidenceBundleError,
    create_evidence_bundle,
    recover_evidence_bundle,
    verify_evidence_bundle,
    verify_recovered_bundle,
)
from forecasting.evidence_compaction import (
    EvidenceCompactionError,
    load_compaction_manifest,
    prepare_compaction_groups,
    stage_compactions,
    verify_staged_compaction,
)
from forecasting.evidence_lifecycle import (
    EvidenceLifecycleError,
    inventory_evidence,
    load_protected_candidate_references,
    load_retention_policy,
    plan_evidence_lifecycle,
)
from forecasting.evidence_quarantine import (
    EvidenceQuarantineError,
    load_quarantine_manifest,
    prepare_quarantine_candidates,
    quarantine_evidence,
    restore_evidence,
    verify_quarantine_state,
)
from forecasting.fabric_pilot import (
    FabricPilotError,
    create_fabric_pilot_plan,
    load_fabric_pilot_plan,
    verify_fabric_pilot_plan,
    write_fabric_pilot_plan,
)
from forecasting.fabric_pilot_authorization import (
    FabricPilotAuthorizationError,
    assess_fabric_pilot_preflight,
    create_fabric_pilot_authorization,
    load_fabric_pilot_authorization,
    load_fabric_pilot_preflight,
    verify_fabric_pilot_authorization,
    verify_fabric_pilot_preflight,
    write_fabric_pilot_authorization,
    write_fabric_pilot_preflight,
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
    "EvidenceBundleError",
    "EvidenceCompactionError",
    "EvidenceLifecycleError",
    "EvidenceQuarantineError",
    "FabricPilotAuthorizationError",
    "FabricPilotError",
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
    "assess_fabric_pilot_preflight",
    "assess_target_weather_promotion",
    "attach_target_forecast_weather",
    "build_demo_feature_frame",
    "build_demo_forecast_weather_frame",
    "build_rolling_origin_folds",
    "build_supervised_frame",
    "create_evidence_bundle",
    "create_fabric_pilot_authorization",
    "create_fabric_pilot_plan",
    "inventory_evidence",
    "load_candidate_history",
    "load_compaction_manifest",
    "load_fabric_pilot_authorization",
    "load_fabric_pilot_plan",
    "load_fabric_pilot_preflight",
    "load_protected_candidate_references",
    "load_quarantine_manifest",
    "load_retention_policy",
    "monitor_forecast_provider_health",
    "plan_evidence_lifecycle",
    "prepare_compaction_groups",
    "prepare_comparison_predictions",
    "prepare_feature_frame",
    "prepare_forecast_reconciliation_input",
    "prepare_forecast_snapshot_evidence",
    "prepare_forecast_weather_frame",
    "prepare_observed_weather_input",
    "prepare_quarantine_candidates",
    "prepare_reconciliation_history",
    "prepare_reconciliation_metrics",
    "prepare_weather_model_comparison",
    "quarantine_evidence",
    "reconcile_forecast_weather",
    "recover_evidence_bundle",
    "register_candidate",
    "restore_evidence",
    "run_chronological_backtest",
    "run_rolling_origin_backtest",
    "run_weather_model_comparison",
    "stage_compactions",
    "transition_candidate",
    "verify_candidate_history",
    "verify_evidence_bundle",
    "verify_fabric_pilot_authorization",
    "verify_fabric_pilot_plan",
    "verify_fabric_pilot_preflight",
    "verify_manifest",
    "verify_quarantine_state",
    "verify_recovered_bundle",
    "verify_staged_compaction",
    "write_candidate_revision",
    "write_fabric_pilot_authorization",
    "write_fabric_pilot_plan",
    "write_fabric_pilot_preflight",
]

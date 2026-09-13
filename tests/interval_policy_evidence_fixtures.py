"""Shared, canonical G38 source evidence for compatibility and downstream reviews."""
from __future__ import annotations

import pandas as pd

from forecasting.interval_health_trends import build_interval_health_trends
from forecasting.interval_monitoring import (
    PredictionIntervalMonitoringConfig,
    monitor_prediction_interval_health,
)

AS_OF = pd.Timestamp("2026-01-20T00:00:00Z")


def three_scenario_evidence():
    histories, check_sets, summaries = [], [], []
    for scenario, shortfall in (("stable", 1.0), ("tightened", 4.0), ("failed", 10.0)):
        rows = []
        for index in range(9):
            timestamp = AS_OF - pd.Timedelta(hours=9 - index)
            rows.append({
                "scenario": scenario, "history_sequence": index + 1,
                "interval_run_id": f"{scenario}-interval-{index}",
                "interval_run_timestamp_utc": timestamp,
                "source_area": "east_midlands", "resource_id": "resource-1", "city": "Nottingham,GB",
                "requested_horizon_minutes": 30, "model_name": "ridge_weather_lag",
                "feature_contract_version": "time-horizon-v1", "target_coverage_level": 0.90,
                "calibration_observation_count": 48, "calibration_radius_mw": 10.0,
                "evaluation_end_utc": timestamp - pd.Timedelta(minutes=30),
                "evaluation_observation_count": 100, "empirical_coverage_pct": 90.0 - shortfall,
                "average_interval_width_mw": 20.0,
                "interval_contract_version": "split-conformal-absolute-residual-v1",
            })
        history = pd.DataFrame(rows)
        checks, summary = monitor_prediction_interval_health(
            history,
            config=PredictionIntervalMonitoringConfig(max_recent_coverage_shortfall_pct_points=5.0),
            as_of_utc=AS_OF, run_timestamp=AS_OF + pd.Timedelta(minutes=5),
            run_id=f"monitor-{scenario}",
        )
        summary["scenario"] = scenario
        histories.append(history)
        check_sets.append(checks)
        summaries.append(summary)
    history = pd.concat(histories, ignore_index=True)
    _, trends = build_interval_health_trends(
        history, pd.concat(summaries, ignore_index=True),
        trend_run_id="iht-" + "3" * 24,
        trend_run_timestamp=AS_OF + pd.Timedelta(minutes=10),
    )
    return history, pd.concat(check_sets, ignore_index=True), trends

from datetime import datetime, timezone

import pandas as pd
import pytest

from forecasting.portfolio_intervals import (
    INTERVAL_COVERAGE_LEVELS,
    MIN_INTERVAL_CALIBRATION_ROWS,
    PortfolioIntervalError,
    build_portfolio_interval_evidence,
    verify_portfolio_interval_evidence,
)


MODELS = (
    "persistence_current_value",
    "seasonal_previous_day",
    "seasonal_previous_week",
    "ridge_weather_lag",
)
HORIZONS = (30, 60)


def point_predictions() -> pd.DataFrame:
    start = pd.Timestamp("2026-01-01T00:00:00Z")
    rows = []
    for horizon in HORIZONS:
        for split, count, offset in (
            ("validation", 30, 0),
            ("test", 6, 36),
        ):
            for index in range(count):
                feature = start + pd.Timedelta(minutes=5 * (offset + index))
                target = feature + pd.Timedelta(minutes=horizon)
                actual = 100.0 + index + horizon / 100.0
                errors = {
                    "persistence_current_value": 4.0,
                    "seasonal_previous_day": 3.0,
                    "seasonal_previous_week": 2.0,
                    "ridge_weather_lag": 1.0,
                }
                for model_name, error in errors.items():
                    rows.append(
                        {
                            "run_id": "seasonal-run",
                            "source_area": "east_midlands",
                            "resource_id": "resource-1",
                            "city": "Nottingham,GB",
                            "feature_timestamp_utc": feature,
                            "event_timestamp_utc": target,
                            "requested_horizon_minutes": horizon,
                            "split": split,
                            "model_name": model_name,
                            "actual_demand_mw": actual,
                            "predicted_demand_mw": actual + error,
                            "trained_through_utc": start
                            - pd.Timedelta(hours=1),
                            "feature_contract_version": "time-horizon-v1",
                        }
                    )
    return pd.DataFrame(rows)


def build():
    return build_portfolio_interval_evidence(
        point_predictions(),
        run_id="pdm-" + "a" * 24,
        run_timestamp=datetime(2026, 1, 2, tzinfo=timezone.utc),
    )


def test_portfolio_interval_builder_retains_four_models_three_levels_and_two_horizons():
    evidence = build()
    intervals = evidence["frames"]["prediction_intervals"]
    metrics = evidence["frames"]["prediction_interval_metrics"]
    summary = evidence["frames"]["interval_coverage_summary"]
    assert set(intervals["model_name"]) == set(MODELS)
    assert set(metrics["model_name"]) == set(MODELS)
    assert set(intervals["requested_horizon_minutes"].astype(int)) == set(HORIZONS)
    assert set(intervals["target_coverage_level"]) == set(
        INTERVAL_COVERAGE_LEVELS
    )
    assert len(summary) == len(HORIZONS) * len(INTERVAL_COVERAGE_LEVELS)
    assert set(summary["model_count"]) == {4}
    assert intervals["calibration_observation_count"].min() >= (
        MIN_INTERVAL_CALIBRATION_ROWS
    )


def test_portfolio_interval_builder_preserves_causality_and_finite_sample_rank():
    intervals = build()["frames"]["prediction_intervals"]
    assert (
        intervals["calibration_label_available_through_utc"]
        < intervals["feature_timestamp_utc"]
    ).all()
    expected = [
        min(
            int(count),
            max(1, __import__("math").ceil((int(count) + 1) * float(level))),
        )
        for count, level in zip(
            intervals["calibration_observation_count"],
            intervals["target_coverage_level"],
        )
    ]
    assert intervals["calibration_quantile_rank"].astype(int).tolist() == expected
    assert (
        intervals["lower_prediction_mw"]
        <= intervals["point_prediction_mw"]
    ).all()
    assert (
        intervals["point_prediction_mw"]
        <= intervals["upper_prediction_mw"]
    ).all()


def test_portfolio_interval_verifier_accepts_complete_reopened_evidence():
    evidence = build()
    manifest = evidence["manifest"]
    verify_portfolio_interval_evidence(
        manifest=manifest,
        frames_by_role=evidence["frames"],
        expected_source_areas={"east_midlands"},
    )
    assert "unconditional future guarantee" in evidence["markdown"]


def _replace_first_interval(
    frames: dict[str, pd.DataFrame],
    *,
    actual: float,
    covered: bool,
) -> None:
    intervals = frames["prediction_intervals"]
    intervals.loc[0, "lower_prediction_mw"] = 0.0
    intervals.loc[0, "point_prediction_mw"] = 0.5
    intervals.loc[0, "upper_prediction_mw"] = 1.0
    intervals.loc[0, "actual_demand_mw"] = actual
    intervals.loc[0, "interval_width_mw"] = 1.0
    intervals.loc[0, "interval_covered"] = covered


def test_portfolio_interval_verifier_preserves_edge_flags_across_round_trip_drift():
    evidence = build()
    frames = {
        role: frame.copy() for role, frame in evidence["frames"].items()
    }
    _replace_first_interval(
        frames,
        actual=1.0 - 5e-10,
        covered=False,
    )

    verify_portfolio_interval_evidence(
        manifest=evidence["manifest"],
        frames_by_role=frames,
        expected_source_areas={"east_midlands"},
    )


def test_portfolio_interval_verifier_rejects_material_coverage_contradiction():
    evidence = build()
    frames = {
        role: frame.copy() for role, frame in evidence["frames"].items()
    }
    _replace_first_interval(
        frames,
        actual=0.5,
        covered=False,
    )

    with pytest.raises(PortfolioIntervalError, match="coverage flags"):
        verify_portfolio_interval_evidence(
            manifest=evidence["manifest"],
            frames_by_role=frames,
            expected_source_areas={"east_midlands"},
        )


def test_portfolio_interval_verifier_rejects_causal_tampering():
    evidence = build()
    frames = {
        role: frame.copy() for role, frame in evidence["frames"].items()
    }
    intervals = frames["prediction_intervals"]
    intervals.loc[0, "calibration_label_available_through_utc"] = intervals.loc[
        0, "feature_timestamp_utc"
    ]
    with pytest.raises(PortfolioIntervalError, match="causality"):
        verify_portfolio_interval_evidence(
            manifest=evidence["manifest"],
            frames_by_role=frames,
            expected_source_areas={"east_midlands"},
        )


def test_portfolio_interval_builder_rejects_incomplete_model_cohort():
    frame = point_predictions().loc[
        lambda value: value["model_name"] != "seasonal_previous_week"
    ]
    with pytest.raises(PortfolioIntervalError, match="complete four-model"):
        build_portfolio_interval_evidence(
            frame,
            run_id="pdm-" + "b" * 24,
            run_timestamp=datetime(2026, 1, 2, tzinfo=timezone.utc),
        )

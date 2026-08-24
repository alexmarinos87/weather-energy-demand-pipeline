from __future__ import annotations

from datetime import datetime
from math import ceil
from typing import Any

import pandas as pd

from forecasting.prediction_intervals import (
    INTERVAL_CONTRACT_VERSION,
    PredictionIntervalConfig,
    calibrate_prediction_intervals,
)
from forecasting.seasonal_baselines import ALL_MODELS


INTERVAL_COVERAGE_LEVELS = (0.80, 0.90, 0.95)
MIN_INTERVAL_CALIBRATION_ROWS = 24
INTERVAL_EDGE_TOLERANCE_MW = 1e-9
INTERVAL_ARTIFACT_ROLES = {
    "prediction_intervals",
    "prediction_interval_metrics",
    "interval_coverage_summary",
    "prediction_interval_summary_markdown",
}


class PortfolioIntervalError(ValueError):
    """Raised when portfolio interval evidence is incomplete or inconsistent."""


def _coverage_summary(metrics: pd.DataFrame) -> pd.DataFrame:
    grouping = [
        "source_area",
        "resource_id",
        "city",
        "requested_horizon_minutes",
        "target_coverage_level",
    ]
    rows: list[dict[str, Any]] = []
    for identity, group in metrics.groupby(grouping, sort=True, dropna=False):
        source_area, resource_id, city, horizon, coverage_level = identity
        if set(group["model_name"].astype(str)) != set(ALL_MODELS):
            raise PortfolioIntervalError(
                "Interval metrics do not contain the complete four-model cohort."
            )
        observation_counts = set(
            pd.to_numeric(
                group["evaluation_observation_count"], errors="coerce"
            ).astype(int)
        )
        if len(observation_counts) != 1:
            raise PortfolioIntervalError(
                "Interval models do not share one evaluation row count."
            )
        rows.append(
            {
                "source_area": source_area,
                "resource_id": resource_id,
                "city": city,
                "requested_horizon_minutes": int(horizon),
                "target_coverage_level": float(coverage_level),
                "nominal_coverage_pct": float(coverage_level) * 100.0,
                "model_count": int(group["model_name"].nunique()),
                "evaluation_observation_count_per_model": int(
                    next(iter(observation_counts))
                ),
                "empirical_coverage_pct_mean": float(
                    group["empirical_coverage_pct"].mean()
                ),
                "empirical_coverage_pct_min": float(
                    group["empirical_coverage_pct"].min()
                ),
                "empirical_coverage_pct_max": float(
                    group["empirical_coverage_pct"].max()
                ),
                "average_interval_width_mw_mean": float(
                    group["average_interval_width_mw"].mean()
                ),
                "median_interval_width_mw_mean": float(
                    group["median_interval_width_mw"].mean()
                ),
                "minimum_calibration_observation_count": int(
                    group["calibration_observation_count"].min()
                ),
                "maximum_calibration_radius_mw": float(
                    group["calibration_radius_mw"].max()
                ),
                "interval_contract_version": INTERVAL_CONTRACT_VERSION,
            }
        )
    summary = pd.DataFrame(rows)
    if summary.empty:
        raise PortfolioIntervalError("Interval coverage summary is empty.")
    return summary


def _markdown(summary: pd.DataFrame) -> str:
    lines = [
        "# Portfolio prediction-interval evidence",
        "",
        "Intervals are calibrated from validation target labels available before "
        "the first test feature timestamp. Test labels do not choose interval "
        "width. Empirical retained-test coverage is evidence rather than an "
        "unconditional future guarantee.",
        "",
        "| Source area | Horizon | Nominal coverage | Models | Rows/model | Mean empirical coverage | Minimum empirical coverage | Mean width MW | Minimum calibration rows |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in summary.sort_values(
        ["source_area", "requested_horizon_minutes", "target_coverage_level"]
    ).itertuples(index=False):
        lines.append(
            f"| {row.source_area} | {int(row.requested_horizon_minutes)} | "
            f"{float(row.nominal_coverage_pct):.1f}% | "
            f"{int(row.model_count)} | "
            f"{int(row.evaluation_observation_count_per_model)} | "
            f"{float(row.empirical_coverage_pct_mean):.2f}% | "
            f"{float(row.empirical_coverage_pct_min):.2f}% | "
            f"{float(row.average_interval_width_mw_mean):.4f} | "
            f"{int(row.minimum_calibration_observation_count)} |"
        )
    lines.extend(
        [
            "",
            "No radius, model, schedule, registry, or promotion state is changed "
            "from this retrospective evidence.",
            "",
        ]
    )
    return "\n".join(lines)


def build_portfolio_interval_evidence(
    point_predictions: pd.DataFrame,
    *,
    run_id: str,
    run_timestamp: datetime,
) -> dict[str, Any]:
    """Calibrate four-model portfolio intervals from UTC seasonal evidence."""
    config = PredictionIntervalConfig(
        coverage_levels=INTERVAL_COVERAGE_LEVELS,
        min_calibration_rows=MIN_INTERVAL_CALIBRATION_ROWS,
    )
    intervals, metrics = calibrate_prediction_intervals(
        point_predictions,
        config=config,
        interval_run_id=f"{run_id}-prediction-intervals",
        interval_run_timestamp=run_timestamp,
    )
    summary = _coverage_summary(metrics)
    return {
        "frames": {
            "prediction_intervals": intervals,
            "prediction_interval_metrics": metrics,
            "interval_coverage_summary": summary,
        },
        "markdown": _markdown(summary),
        "manifest": {
            "interval_models": sorted(ALL_MODELS),
            "interval_coverage_levels": list(INTERVAL_COVERAGE_LEVELS),
            "minimum_interval_calibration_rows": (
                MIN_INTERVAL_CALIBRATION_ROWS
            ),
            "interval_contract_version": INTERVAL_CONTRACT_VERSION,
            "interval_source_feature_contract_version": "time-horizon-v1",
        },
    }


def _areas(frame: pd.DataFrame) -> set[str]:
    if "source_area" not in frame.columns:
        raise PortfolioIntervalError("Interval artifact lacks source_area.")
    return set(frame["source_area"].astype(str))


def _expected_rank(count: int, coverage: float) -> int:
    return min(count, max(1, ceil((count + 1) * coverage)))


def verify_portfolio_interval_evidence(
    *,
    manifest: dict[str, Any],
    frames_by_role: dict[str, pd.DataFrame],
    expected_source_areas: set[str],
) -> None:
    """Verify reopened interval rows, metrics, and area/horizon summaries."""
    if set(manifest.get("interval_models", ())) != set(ALL_MODELS):
        raise PortfolioIntervalError("Portfolio interval models are invalid.")
    levels = [float(value) for value in manifest.get("interval_coverage_levels", ())]
    if levels != list(INTERVAL_COVERAGE_LEVELS):
        raise PortfolioIntervalError("Portfolio interval coverage levels are invalid.")
    if (
        manifest.get("minimum_interval_calibration_rows")
        != MIN_INTERVAL_CALIBRATION_ROWS
    ):
        raise PortfolioIntervalError(
            "Portfolio minimum calibration history is invalid."
        )
    if manifest.get("interval_contract_version") != INTERVAL_CONTRACT_VERSION:
        raise PortfolioIntervalError("Portfolio interval contract is invalid.")
    if (
        manifest.get("interval_source_feature_contract_version")
        != "time-horizon-v1"
    ):
        raise PortfolioIntervalError(
            "Portfolio interval source feature contract is invalid."
        )
    for role in INTERVAL_ARTIFACT_ROLES - {
        "prediction_interval_summary_markdown"
    }:
        frame = frames_by_role.get(role)
        if frame is None or frame.empty:
            raise PortfolioIntervalError(
                f"Portfolio interval artifact {role} is missing or empty."
            )
        if _areas(frame) != expected_source_areas:
            raise PortfolioIntervalError(
                f"Portfolio interval artifact {role} does not retain all areas."
            )

    intervals = frames_by_role["prediction_intervals"].copy()
    if set(intervals["model_name"].astype(str)) != set(ALL_MODELS):
        raise PortfolioIntervalError("Interval prediction models are incomplete.")
    if set(intervals["requested_horizon_minutes"].astype(int)) != {30, 60}:
        raise PortfolioIntervalError("Interval demand horizons are incomplete.")
    if set(intervals["target_coverage_level"].astype(float)) != set(
        INTERVAL_COVERAGE_LEVELS
    ):
        raise PortfolioIntervalError("Interval coverage levels are incomplete.")
    if set(intervals["feature_contract_version"].astype(str)) != {
        "time-horizon-v1"
    }:
        raise PortfolioIntervalError(
            "Portfolio intervals must use the UTC seasonal point run."
        )
    calibration_through = pd.to_datetime(
        intervals["calibration_label_available_through_utc"], utc=True
    )
    feature_time = pd.to_datetime(
        intervals["feature_timestamp_utc"], utc=True
    )
    target_time = pd.to_datetime(intervals["event_timestamp_utc"], utc=True)
    trained_through = pd.to_datetime(
        intervals["point_model_trained_through_utc"], utc=True
    )
    if not (
        (calibration_through < feature_time)
        & (trained_through < feature_time)
        & (feature_time < target_time)
    ).all():
        raise PortfolioIntervalError(
            "Portfolio intervals violate calibration or point-model causality."
        )
    counts = pd.to_numeric(
        intervals["calibration_observation_count"], errors="coerce"
    ).astype(int)
    ranks = pd.to_numeric(
        intervals["calibration_quantile_rank"], errors="coerce"
    ).astype(int)
    coverage = pd.to_numeric(
        intervals["target_coverage_level"], errors="coerce"
    ).astype(float)
    expected = [
        _expected_rank(count, level)
        for count, level in zip(counts, coverage)
    ]
    if ranks.tolist() != expected:
        raise PortfolioIntervalError(
            "Portfolio intervals have invalid finite-sample ranks."
        )
    lower = pd.to_numeric(intervals["lower_prediction_mw"], errors="coerce")
    point = pd.to_numeric(intervals["point_prediction_mw"], errors="coerce")
    upper = pd.to_numeric(intervals["upper_prediction_mw"], errors="coerce")
    width = pd.to_numeric(intervals["interval_width_mw"], errors="coerce")
    actual = pd.to_numeric(intervals["actual_demand_mw"], errors="coerce")
    covered = (
        (lower - INTERVAL_EDGE_TOLERANCE_MW <= actual)
        & (actual <= upper + INTERVAL_EDGE_TOLERANCE_MW)
    )
    if not (
        (lower <= point + INTERVAL_EDGE_TOLERANCE_MW)
        & (point <= upper + INTERVAL_EDGE_TOLERANCE_MW)
        & (width >= -INTERVAL_EDGE_TOLERANCE_MW)
        & (
            (upper - lower - width).abs()
            <= INTERVAL_EDGE_TOLERANCE_MW
        )
    ).all():
        raise PortfolioIntervalError("Portfolio interval bounds are invalid.")
    retained_covered = intervals["interval_covered"].astype(str).str.lower().map(
        {"true": True, "false": False}
    )
    if retained_covered.isna().any() or not (
        retained_covered.to_numpy() == covered.to_numpy()
    ).all():
        raise PortfolioIntervalError(
            "Portfolio interval coverage flags are inconsistent."
        )

    metrics = frames_by_role["prediction_interval_metrics"]
    if set(metrics["model_name"].astype(str)) != set(ALL_MODELS):
        raise PortfolioIntervalError("Interval metric models are incomplete.")
    if set(metrics["target_coverage_level"].astype(float)) != set(
        INTERVAL_COVERAGE_LEVELS
    ):
        raise PortfolioIntervalError("Interval metric levels are incomplete.")
    if (
        pd.to_numeric(
            metrics["calibration_observation_count"], errors="coerce"
        ).min()
        < MIN_INTERVAL_CALIBRATION_ROWS
    ):
        raise PortfolioIntervalError(
            "Interval metrics contain insufficient calibration history."
        )

    summary = frames_by_role["interval_coverage_summary"]
    if set(summary["requested_horizon_minutes"].astype(int)) != {30, 60}:
        raise PortfolioIntervalError("Interval summary horizons are incomplete.")
    if set(summary["target_coverage_level"].astype(float)) != set(
        INTERVAL_COVERAGE_LEVELS
    ):
        raise PortfolioIntervalError("Interval summary levels are incomplete.")
    if not (pd.to_numeric(summary["model_count"], errors="coerce") == 4).all():
        raise PortfolioIntervalError("Interval summary model counts are invalid.")
    for column in (
        "empirical_coverage_pct_mean",
        "empirical_coverage_pct_min",
        "empirical_coverage_pct_max",
    ):
        values = pd.to_numeric(summary[column], errors="coerce")
        if not values.between(0, 100, inclusive="both").all():
            raise PortfolioIntervalError(
                f"Interval summary {column} values are invalid."
            )

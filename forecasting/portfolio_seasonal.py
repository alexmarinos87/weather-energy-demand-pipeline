from __future__ import annotations

from datetime import datetime
from typing import Any

import pandas as pd

from forecasting.contracts import (
    GROUP_COLUMNS,
    TIMESTAMP_COLUMN,
    UK_LOCAL_FEATURE_COLUMNS,
    UK_LOCAL_FEATURE_CONTRACT_VERSION,
    UTC_FEATURE_CONTRACT_VERSION,
    BacktestConfig,
)
from forecasting.model_family_scorecard import build_model_family_scorecard
from forecasting.seasonal_baselines import (
    ALL_MODELS,
    SEASONAL_REFERENCES,
    SeasonalBaselineConfig,
    build_seasonal_demo_feature_frame,
    run_seasonal_backtest,
)


SEASONAL_SOURCE_CADENCE_MINUTES = 30
SEASONAL_DEMO_DAYS = 12
SEASONAL_ARTIFACT_ROLES = {
    "seasonal_demo_features",
    "seasonal_utc_predictions",
    "seasonal_utc_metrics",
    "seasonal_uk_local_predictions",
    "seasonal_uk_local_metrics",
    "model_family_scorecard",
    "model_family_pairwise_metrics",
    "model_family_summary_markdown",
}
MODEL_FAMILY_MODELS = {
    "persistence_current_value",
    "seasonal_previous_day",
    "seasonal_previous_week",
    "ridge_weather_lag_utc",
    "ridge_weather_lag_uk_local",
}


class PortfolioSeasonalError(ValueError):
    """Raised when seasonal portfolio evidence is incomplete or inconsistent."""


def _seasonal_features(source_areas: tuple[str, ...]) -> pd.DataFrame:
    frame = build_seasonal_demo_feature_frame(
        periods=SEASONAL_DEMO_DAYS * 288,
        source_areas=source_areas,
    ).sort_values([*GROUP_COLUMNS, TIMESTAMP_COLUMN])
    sampled_groups = [
        group.iloc[::6].copy()
        for _, group in frame.groupby(
            GROUP_COLUMNS, sort=True, dropna=False
        )
    ]
    sampled = pd.concat(sampled_groups, ignore_index=True)
    if sampled.empty:
        raise PortfolioSeasonalError("Seasonal portfolio features are empty.")
    for _, group in sampled.groupby(GROUP_COLUMNS, sort=True, dropna=False):
        ordered = pd.DatetimeIndex(group[TIMESTAMP_COLUMN].sort_values())
        deltas = ordered.to_series().diff().dropna().dt.total_seconds() / 60.0
        if deltas.empty or set(deltas.astype(int)) != {
            SEASONAL_SOURCE_CADENCE_MINUTES
        }:
            raise PortfolioSeasonalError(
                "Seasonal portfolio features must retain one 30-minute cadence."
            )
    return sampled


def _scorecard_markdown(
    scorecard: pd.DataFrame,
    pairwise: pd.DataFrame,
) -> str:
    test = scorecard.loc[scorecard["split"] == "test"].sort_values(
        ["source_area", "requested_horizon_minutes", "mae_mw", "model_name"]
    )
    paired_test = pairwise.loc[pairwise["split"] == "test"].sort_values(
        [
            "source_area",
            "requested_horizon_minutes",
            "mae_improvement_mw",
            "candidate_model_name",
        ],
        ascending=[True, True, False, True],
    )
    lines = [
        "# Portfolio seasonal and calendar scorecard",
        "",
        "Every retained result remains partitioned by source area and demand "
        "horizon. Lower error is comparison evidence only and does not approve "
        "or promote a model.",
        "",
        "## Test metrics",
        "",
        "| Source area | Horizon | Model | Rows | MAE MW | RMSE MW | Feature contract |",
        "| --- | ---: | --- | ---: | ---: | ---: | --- |",
    ]
    for row in test.itertuples(index=False):
        lines.append(
            f"| {row.source_area} | {int(row.requested_horizon_minutes)} | "
            f"{row.model_name} | {int(row.paired_observation_count)} | "
            f"{float(row.mae_mw):.4f} | {float(row.rmse_mw):.4f} | "
            f"{row.source_feature_contract_version} |"
        )
    lines.extend(
        [
            "",
            "## Test comparison with persistence",
            "",
            "| Source area | Horizon | Candidate | MAE improvement MW | Wins | Ties | Losses |",
            "| --- | ---: | --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in paired_test.itertuples(index=False):
        lines.append(
            f"| {row.source_area} | {int(row.requested_horizon_minutes)} | "
            f"{row.candidate_model_name} | "
            f"{float(row.mae_improvement_mw):.4f} | "
            f"{int(row.win_count)} | {int(row.tie_count)} | "
            f"{int(row.loss_count)} |"
        )
    lines.extend(
        [
            "",
            "Previous-day and previous-week references are matched by elapsed "
            "UTC time. UTC remains the target identity; Europe/London fields are "
            "derived model features only.",
            "",
        ]
    )
    return "\n".join(lines)


def build_portfolio_seasonal_evidence(
    *,
    source_areas: tuple[str, ...],
    run_id: str,
    run_timestamp: datetime,
) -> dict[str, Any]:
    """Build paired UTC/UK-local seasonal and scorecard evidence for four areas."""
    features = _seasonal_features(source_areas)
    seasonal_config = SeasonalBaselineConfig(
        reference_tolerance_minutes=0,
        min_reference_coverage=1.0,
    )
    common = {
        "horizon_minutes": (30, 60),
        "target_tolerance_minutes": 0,
        "min_target_coverage": 1.0,
    }
    utc_config = BacktestConfig(**common)
    uk_config = BacktestConfig(
        **common,
        feature_columns=tuple(UK_LOCAL_FEATURE_COLUMNS),
        feature_contract_version=UK_LOCAL_FEATURE_CONTRACT_VERSION,
    )
    utc_predictions, utc_metrics = run_seasonal_backtest(
        features,
        backtest_config=utc_config,
        seasonal_config=seasonal_config,
        evaluation_mode="holdout",
        run_id=f"{run_id}-seasonal-utc",
        run_timestamp=run_timestamp,
    )
    uk_predictions, uk_metrics = run_seasonal_backtest(
        features,
        backtest_config=uk_config,
        seasonal_config=seasonal_config,
        evaluation_mode="holdout",
        run_id=f"{run_id}-seasonal-uk-local",
        run_timestamp=run_timestamp,
    )
    scorecard, pairwise = build_model_family_scorecard(
        utc_predictions,
        uk_predictions,
        scorecard_run_id=f"{run_id}-model-family-scorecard",
        scorecard_run_timestamp=run_timestamp,
    )
    return {
        "frames": {
            "seasonal_demo_features": features,
            "seasonal_utc_predictions": utc_predictions,
            "seasonal_utc_metrics": utc_metrics,
            "seasonal_uk_local_predictions": uk_predictions,
            "seasonal_uk_local_metrics": uk_metrics,
            "model_family_scorecard": scorecard,
            "model_family_pairwise_metrics": pairwise,
        },
        "markdown": _scorecard_markdown(scorecard, pairwise),
        "manifest": {
            "seasonal_models": sorted(ALL_MODELS),
            "model_family_models": sorted(MODEL_FAMILY_MODELS),
            "calendar_feature_contract_versions": [
                UTC_FEATURE_CONTRACT_VERSION,
                UK_LOCAL_FEATURE_CONTRACT_VERSION,
            ],
            "seasonal_reference_periods_minutes": sorted(
                SEASONAL_REFERENCES.values()
            ),
            "seasonal_source_cadence_minutes": (
                SEASONAL_SOURCE_CADENCE_MINUTES
            ),
            "seasonal_demo_days": SEASONAL_DEMO_DAYS,
        },
    }


def _areas(frame: pd.DataFrame) -> set[str]:
    if "source_area" not in frame.columns:
        raise PortfolioSeasonalError("Seasonal artifact lacks source_area.")
    return set(frame["source_area"].astype(str))


def verify_portfolio_seasonal_evidence(
    *,
    manifest: dict[str, Any],
    frames_by_role: dict[str, pd.DataFrame],
    expected_source_areas: set[str],
) -> None:
    """Verify seasonal artifacts after reopening them from the immutable bundle."""
    if set(manifest.get("seasonal_models", ())) != set(ALL_MODELS):
        raise PortfolioSeasonalError("Portfolio seasonal models are invalid.")
    if set(manifest.get("model_family_models", ())) != MODEL_FAMILY_MODELS:
        raise PortfolioSeasonalError("Portfolio scorecard models are invalid.")
    if manifest.get("calendar_feature_contract_versions") != [
        UTC_FEATURE_CONTRACT_VERSION,
        UK_LOCAL_FEATURE_CONTRACT_VERSION,
    ]:
        raise PortfolioSeasonalError(
            "Portfolio calendar feature-contract identities are invalid."
        )
    if manifest.get("seasonal_reference_periods_minutes") != sorted(
        SEASONAL_REFERENCES.values()
    ):
        raise PortfolioSeasonalError(
            "Portfolio seasonal reference periods are invalid."
        )
    if (
        manifest.get("seasonal_source_cadence_minutes")
        != SEASONAL_SOURCE_CADENCE_MINUTES
    ):
        raise PortfolioSeasonalError(
            "Portfolio seasonal source cadence is invalid."
        )
    for role in SEASONAL_ARTIFACT_ROLES - {"model_family_summary_markdown"}:
        frame = frames_by_role.get(role)
        if frame is None or frame.empty:
            raise PortfolioSeasonalError(
                f"Portfolio seasonal artifact {role} is missing or empty."
            )
        if _areas(frame) != expected_source_areas:
            raise PortfolioSeasonalError(
                f"Portfolio seasonal artifact {role} does not retain all areas."
            )

    features = frames_by_role["seasonal_demo_features"].copy()
    for _, group in features.groupby(GROUP_COLUMNS, sort=True, dropna=False):
        timestamps = pd.to_datetime(group[TIMESTAMP_COLUMN], utc=True).sort_values()
        deltas = timestamps.diff().dropna().dt.total_seconds() / 60.0
        if deltas.empty or set(deltas.astype(int)) != {
            SEASONAL_SOURCE_CADENCE_MINUTES
        }:
            raise PortfolioSeasonalError(
                "Reopened seasonal features do not retain 30-minute cadence."
            )

    for role, expected_contract in (
        ("seasonal_utc_predictions", UTC_FEATURE_CONTRACT_VERSION),
        ("seasonal_uk_local_predictions", UK_LOCAL_FEATURE_CONTRACT_VERSION),
    ):
        frame = frames_by_role[role]
        if set(frame["model_name"].astype(str)) != set(ALL_MODELS):
            raise PortfolioSeasonalError(f"{role} has incomplete model identity.")
        if set(frame["requested_horizon_minutes"].astype(int)) != {30, 60}:
            raise PortfolioSeasonalError(f"{role} has incomplete demand horizons.")
        if set(frame["feature_contract_version"].astype(str)) != {
            expected_contract
        }:
            raise PortfolioSeasonalError(f"{role} has wrong feature contract.")
        seasonal = frame.loc[
            frame["model_name"].isin(
                ["seasonal_previous_day", "seasonal_previous_week"]
            )
        ]
        source_time = pd.to_datetime(
            seasonal["seasonal_reference_timestamp_utc"], utc=True
        )
        feature_time = pd.to_datetime(
            seasonal["feature_timestamp_utc"], utc=True
        )
        if not (source_time <= feature_time).all():
            raise PortfolioSeasonalError(
                f"{role} uses a seasonal reference unavailable at feature time."
            )
        if not (
            pd.to_numeric(
                seasonal["seasonal_reference_absolute_offset_minutes"],
                errors="coerce",
            )
            == 0
        ).all():
            raise PortfolioSeasonalError(
                f"{role} does not retain exact elapsed-time references."
            )

    scorecard = frames_by_role["model_family_scorecard"]
    if set(scorecard["model_name"].astype(str)) != MODEL_FAMILY_MODELS:
        raise PortfolioSeasonalError("Portfolio model-family scorecard is incomplete.")
    if set(scorecard["requested_horizon_minutes"].astype(int)) != {30, 60}:
        raise PortfolioSeasonalError("Portfolio model-family horizons are incomplete.")
    identity = [
        *GROUP_COLUMNS,
        "requested_horizon_minutes",
        "split",
        "origin_fold",
    ]
    paired = scorecard.groupby(identity, dropna=False).agg(
        model_count=("model_name", "nunique"),
        digest_count=("paired_target_identity_sha256", "nunique"),
        observation_count=("paired_observation_count", "nunique"),
    )
    if (
        (paired["model_count"] != len(MODEL_FAMILY_MODELS))
        | (paired["digest_count"] != 1)
        | (paired["observation_count"] != 1)
    ).any():
        raise PortfolioSeasonalError(
            "Portfolio scorecard models do not share one paired target cohort."
        )

    pairwise = frames_by_role["model_family_pairwise_metrics"]
    if set(pairwise["reference_model_name"].astype(str)) != {
        "persistence_current_value"
    }:
        raise PortfolioSeasonalError(
            "Portfolio pairwise evidence has an invalid reference model."
        )
    counts = pairwise[["win_count", "tie_count", "loss_count"]].sum(axis=1)
    if not (
        counts.astype(int)
        == pairwise["paired_observation_count"].astype(int)
    ).all():
        raise PortfolioSeasonalError(
            "Portfolio scorecard win/tie/loss evidence is inconsistent."
        )

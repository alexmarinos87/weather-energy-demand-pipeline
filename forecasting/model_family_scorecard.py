from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
from math import isfinite, sqrt
from uuid import uuid4

import pandas as pd

from forecasting.contracts import (
    UK_LOCAL_FEATURE_CONTRACT_VERSION,
    UTC_FEATURE_CONTRACT_VERSION,
)


SCORECARD_CONTRACT_VERSION = "paired-model-family-scorecard-v1"
GROUP_COLUMNS = ["source_area", "resource_id", "city"]
INPUT_MODELS = (
    "persistence_current_value",
    "seasonal_previous_day",
    "seasonal_previous_week",
    "ridge_weather_lag",
)
OUTPUT_MODELS = (
    "persistence_current_value",
    "seasonal_previous_day",
    "seasonal_previous_week",
    "ridge_weather_lag_utc",
    "ridge_weather_lag_uk_local",
)
CONTROL_MODELS = INPUT_MODELS[:3]
PAIRING_COLUMNS = [
    *GROUP_COLUMNS,
    "requested_horizon_minutes",
    "split",
    "_origin_fold_key",
    "feature_timestamp_utc",
    "event_timestamp_utc",
]
REQUIRED_COLUMNS = {
    "run_id",
    *GROUP_COLUMNS,
    "feature_timestamp_utc",
    "event_timestamp_utc",
    "requested_horizon_minutes",
    "split",
    "model_name",
    "actual_demand_mw",
    "predicted_demand_mw",
    "trained_through_utc",
    "training_observation_count",
    "feature_contract_version",
    "evaluation_contract_version",
}


class ModelFamilyScorecardError(ValueError):
    """Raised when UTC and UK-local model evidence cannot be paired fairly."""


def _text(frame: pd.DataFrame, column: str) -> pd.Series:
    values = frame[column].fillna("").astype(str).str.strip()
    if values.eq("").any():
        raise ModelFamilyScorecardError(f"{column} must contain non-empty values.")
    return values


def _utc(frame: pd.DataFrame, column: str) -> pd.Series:
    values: list[pd.Timestamp] = []
    for raw in frame[column]:
        try:
            timestamp = pd.Timestamp(raw)
        except (TypeError, ValueError) as exc:
            raise ModelFamilyScorecardError(
                f"{column} must contain valid timezone-aware timestamps."
            ) from exc
        if pd.isna(timestamp) or timestamp.tzinfo is None:
            raise ModelFamilyScorecardError(
                f"{column} must contain timezone-aware timestamps."
            )
        values.append(timestamp.tz_convert("UTC"))
    return pd.Series(values, index=frame.index, dtype="datetime64[ns, UTC]")


def _number(frame: pd.DataFrame, column: str) -> pd.Series:
    values = pd.to_numeric(frame[column], errors="coerce")
    if values.isna().any() or not values.map(
        lambda value: isfinite(float(value))
    ).all():
        raise ModelFamilyScorecardError(f"{column} must contain finite numbers.")
    return values.astype(float)


def prepare_seasonal_run(
    frame: pd.DataFrame,
    *,
    expected_feature_contract: str,
) -> tuple[pd.DataFrame, str]:
    """Normalize exactly one seasonal run with one complete four-model cohort."""
    missing = sorted(REQUIRED_COLUMNS - set(frame.columns))
    if missing:
        raise ModelFamilyScorecardError(
            "Seasonal predictions are missing required columns: "
            + ", ".join(missing)
            + "."
        )
    prepared = frame.copy()
    for column in (
        "run_id",
        *GROUP_COLUMNS,
        "split",
        "model_name",
        "feature_contract_version",
        "evaluation_contract_version",
    ):
        prepared[column] = _text(prepared, column)
    run_ids = sorted(set(prepared["run_id"]))
    if len(run_ids) != 1:
        raise ModelFamilyScorecardError(
            f"Seasonal predictions must contain exactly one run_id; found {len(run_ids)}."
        )
    if set(prepared["feature_contract_version"]) != {expected_feature_contract}:
        raise ModelFamilyScorecardError(
            f"Expected feature_contract_version={expected_feature_contract!r}."
        )
    if set(prepared["model_name"]) != set(INPUT_MODELS):
        raise ModelFamilyScorecardError(
            f"Expected models {sorted(INPUT_MODELS)}, found "
            f"{sorted(set(prepared['model_name']))}."
        )
    if not set(prepared["split"]).issubset({"validation", "test"}):
        raise ModelFamilyScorecardError(
            "Seasonal prediction splits must be validation or test."
        )
    for column in (
        "feature_timestamp_utc",
        "event_timestamp_utc",
        "trained_through_utc",
    ):
        prepared[column] = _utc(prepared, column)
    for column in ("actual_demand_mw", "predicted_demand_mw"):
        prepared[column] = _number(prepared, column)
    horizons = pd.to_numeric(
        prepared["requested_horizon_minutes"], errors="coerce"
    )
    training_counts = pd.to_numeric(
        prepared["training_observation_count"], errors="coerce"
    )
    if horizons.isna().any() or (horizons <= 0).any():
        raise ModelFamilyScorecardError(
            "requested_horizon_minutes must contain positive values."
        )
    if training_counts.isna().any() or (training_counts <= 0).any():
        raise ModelFamilyScorecardError(
            "training_observation_count must contain positive values."
        )
    prepared["requested_horizon_minutes"] = horizons.astype(int)
    prepared["training_observation_count"] = training_counts.astype(int)
    if not (
        (prepared["trained_through_utc"] < prepared["feature_timestamp_utc"])
        & (
            prepared["feature_timestamp_utc"]
            < prepared["event_timestamp_utc"]
        )
    ).all():
        raise ModelFamilyScorecardError(
            "Seasonal predictions violate training, feature, or target ordering."
        )
    if "origin_fold" not in prepared.columns:
        prepared["origin_fold"] = pd.NA
    if "origin_count" not in prepared.columns:
        prepared["origin_count"] = pd.NA
    if "origin_cutoff_utc" not in prepared.columns:
        prepared["origin_cutoff_utc"] = pd.NaT
    origin_fold = pd.to_numeric(prepared["origin_fold"], errors="coerce")
    prepared["_origin_fold_key"] = origin_fold.fillna(-1).astype(int)
    prepared["origin_fold"] = origin_fold.astype("Int64")
    origin_count = pd.to_numeric(prepared["origin_count"], errors="coerce")
    prepared["origin_count"] = origin_count.astype("Int64")
    cutoff_values = []
    for raw in prepared["origin_cutoff_utc"]:
        if pd.isna(raw):
            cutoff_values.append(pd.NaT)
        else:
            timestamp = pd.Timestamp(raw)
            if timestamp.tzinfo is None:
                raise ModelFamilyScorecardError(
                    "origin_cutoff_utc must be timezone-aware when present."
                )
            cutoff_values.append(timestamp.tz_convert("UTC"))
    prepared["origin_cutoff_utc"] = pd.Series(
        cutoff_values, index=prepared.index, dtype="datetime64[ns, UTC]"
    )

    identity = [*PAIRING_COLUMNS, "model_name"]
    if prepared.duplicated(subset=identity, keep=False).any():
        raise ModelFamilyScorecardError(
            "Seasonal predictions contain duplicate evaluation identities."
        )
    paired = (
        prepared.groupby(PAIRING_COLUMNS, dropna=False)
        .agg(
            row_count=("model_name", "size"),
            model_count=("model_name", "nunique"),
            actual_count=("actual_demand_mw", "nunique"),
            boundary_count=("trained_through_utc", "nunique"),
            training_count=("training_observation_count", "nunique"),
            evaluation_contract_count=("evaluation_contract_version", "nunique"),
        )
        .reset_index()
    )
    if (
        (paired["row_count"] != len(INPUT_MODELS))
        | (paired["model_count"] != len(INPUT_MODELS))
        | (paired["actual_count"] != 1)
        | (paired["boundary_count"] != 1)
        | (paired["training_count"] != 1)
        | (paired["evaluation_contract_count"] != 1)
    ).any():
        raise ModelFamilyScorecardError(
            "Seasonal run does not contain exact four-model target pairs with "
            "one target, training boundary, and evaluation contract."
        )
    return (
        prepared.sort_values(identity).reset_index(drop=True),
        run_ids[0],
    )


def _assert_same_target_cohort(
    utc_run: pd.DataFrame,
    uk_run: pd.DataFrame,
) -> None:
    utc_keys = set(
        map(tuple, utc_run[PAIRING_COLUMNS].itertuples(index=False, name=None))
    )
    uk_keys = set(
        map(tuple, uk_run[PAIRING_COLUMNS].itertuples(index=False, name=None))
    )
    if utc_keys != uk_keys:
        raise ModelFamilyScorecardError(
            "UTC and UK-local runs do not contain the same paired target cohort."
        )
    for model in CONTROL_MODELS:
        utc_model = utc_run.loc[utc_run["model_name"] == model].set_index(
            PAIRING_COLUMNS
        ).sort_index()
        uk_model = uk_run.loc[uk_run["model_name"] == model].set_index(
            PAIRING_COLUMNS
        ).sort_index()
        if not utc_model.index.equals(uk_model.index):
            raise ModelFamilyScorecardError(
                f"Control model {model} does not contain the same target rows."
            )
        for column in ("actual_demand_mw", "predicted_demand_mw"):
            if not (
                (utc_model[column].astype(float) - uk_model[column].astype(float))
                .abs()
                .le(1e-9)
                .all()
            ):
                raise ModelFamilyScorecardError(
                    f"Control model {model} differs between calendar runs."
                )
        for column in (
            "trained_through_utc",
            "training_observation_count",
            "evaluation_contract_version",
            "origin_count",
            "origin_cutoff_utc",
        ):
            left = utc_model[column].astype(str).tolist()
            right = uk_model[column].astype(str).tolist()
            if left != right:
                raise ModelFamilyScorecardError(
                    f"Control model {model} has different {column} evidence."
                )

    utc_ridge = utc_run.loc[
        utc_run["model_name"] == "ridge_weather_lag"
    ].set_index(PAIRING_COLUMNS).sort_index()
    uk_ridge = uk_run.loc[
        uk_run["model_name"] == "ridge_weather_lag"
    ].set_index(PAIRING_COLUMNS).sort_index()
    if not utc_ridge.index.equals(uk_ridge.index):
        raise ModelFamilyScorecardError(
            "UTC and UK-local ridge models do not contain the same target rows."
        )
    if not (
        (
            utc_ridge["actual_demand_mw"].astype(float)
            - uk_ridge["actual_demand_mw"].astype(float)
        )
        .abs()
        .le(1e-9)
        .all()
    ):
        raise ModelFamilyScorecardError(
            "UTC and UK-local ridge models have different actual targets."
        )
    for column in (
        "trained_through_utc",
        "training_observation_count",
        "evaluation_contract_version",
        "origin_count",
        "origin_cutoff_utc",
    ):
        if utc_ridge[column].astype(str).tolist() != uk_ridge[column].astype(
            str
        ).tolist():
            raise ModelFamilyScorecardError(
                f"UTC and UK-local ridge models have different {column} evidence."
            )


def _target_digest(group: pd.DataFrame) -> str:
    records = []
    for row in group.sort_values(
        ["feature_timestamp_utc", "event_timestamp_utc"]
    ).itertuples(index=False):
        records.append(
            "|".join(
                (
                    pd.Timestamp(row.feature_timestamp_utc).isoformat(),
                    pd.Timestamp(row.event_timestamp_utc).isoformat(),
                    f"{float(row.actual_demand_mw):.12g}",
                )
            )
        )
    return sha256("\n".join(records).encode("utf-8")).hexdigest()


def _metric_row(group: pd.DataFrame) -> dict[str, object]:
    first = group.iloc[0]
    errors = group["predicted_demand_mw"] - group["actual_demand_mw"]
    absolute = errors.abs()
    nonzero = group["actual_demand_mw"].abs() > 1e-12
    mape = None
    if nonzero.any():
        mape = float(
            (
                absolute[nonzero]
                / group.loc[nonzero, "actual_demand_mw"].abs()
            ).mean()
            * 100.0
        )
    return {
        "scorecard_run_id": first["scorecard_run_id"],
        "scorecard_run_timestamp_utc": first[
            "scorecard_run_timestamp_utc"
        ],
        "utc_source_run_id": first["utc_source_run_id"],
        "uk_local_source_run_id": first["uk_local_source_run_id"],
        "source_area": first["source_area"],
        "resource_id": first["resource_id"],
        "city": first["city"],
        "requested_horizon_minutes": int(
            first["requested_horizon_minutes"]
        ),
        "split": first["split"],
        "origin_fold": first["origin_fold"],
        "origin_count": first["origin_count"],
        "origin_cutoff_utc": first["origin_cutoff_utc"],
        "model_name": first["scorecard_model_name"],
        "model_family": first["model_family"],
        "source_feature_contract_version": first[
            "feature_contract_version"
        ],
        "evaluation_contract_version": first[
            "evaluation_contract_version"
        ],
        "trained_through_utc": first["trained_through_utc"],
        "training_observation_count": int(
            first["training_observation_count"]
        ),
        "paired_observation_count": int(len(group)),
        "paired_target_identity_sha256": _target_digest(group),
        "mae_mw": float(absolute.mean()),
        "rmse_mw": float(sqrt((errors * errors).mean())),
        "mape_pct": mape,
        "bias_mw": float(errors.mean()),
        "median_absolute_error_mw": float(absolute.median()),
        "p95_absolute_error_mw": float(absolute.quantile(0.95)),
        "scorecard_contract_version": SCORECARD_CONTRACT_VERSION,
    }


def _pairwise_rows(predictions: pd.DataFrame) -> list[dict[str, object]]:
    group_columns = [
        *GROUP_COLUMNS,
        "requested_horizon_minutes",
        "split",
        "_origin_fold_key",
    ]
    rows: list[dict[str, object]] = []
    for _, group in predictions.groupby(group_columns, sort=True, dropna=False):
        pivot = group.pivot_table(
            index=["feature_timestamp_utc", "event_timestamp_utc"],
            columns="scorecard_model_name",
            values="predicted_demand_mw",
            aggfunc="first",
        )
        actual = (
            group.drop_duplicates(
                ["feature_timestamp_utc", "event_timestamp_utc"]
            )
            .set_index(["feature_timestamp_utc", "event_timestamp_utc"])[
                "actual_demand_mw"
            ]
            .sort_index()
        )
        pivot = pivot.sort_index()
        if set(pivot.columns) != set(OUTPUT_MODELS) or not pivot.index.equals(
            actual.index
        ):
            raise ModelFamilyScorecardError(
                "Paired model predictions are incomplete for scorecard comparison."
            )
        persistence_errors = (
            pivot["persistence_current_value"] - actual
        ).abs()
        first_group = group.iloc[0]
        digest_source = group.loc[
            group["scorecard_model_name"] == "persistence_current_value"
        ]
        target_digest = _target_digest(digest_source)
        for candidate in OUTPUT_MODELS[1:]:
            candidate_errors = (pivot[candidate] - actual).abs()
            difference = candidate_errors - persistence_errors
            candidate_metadata = group.loc[
                group["scorecard_model_name"] == candidate
            ].iloc[0]
            rows.append(
                {
                    "scorecard_run_id": first_group["scorecard_run_id"],
                    "scorecard_run_timestamp_utc": first_group[
                        "scorecard_run_timestamp_utc"
                    ],
                    "utc_source_run_id": first_group[
                        "utc_source_run_id"
                    ],
                    "uk_local_source_run_id": first_group[
                        "uk_local_source_run_id"
                    ],
                    "source_area": first_group["source_area"],
                    "resource_id": first_group["resource_id"],
                    "city": first_group["city"],
                    "requested_horizon_minutes": int(
                        first_group["requested_horizon_minutes"]
                    ),
                    "split": first_group["split"],
                    "origin_fold": first_group["origin_fold"],
                    "reference_model_name": "persistence_current_value",
                    "candidate_model_name": candidate,
                    "candidate_model_family": candidate_metadata[
                        "model_family"
                    ],
                    "candidate_feature_contract_version": (
                        candidate_metadata["feature_contract_version"]
                    ),
                    "paired_observation_count": int(len(actual)),
                    "paired_target_identity_sha256": target_digest,
                    "reference_mae_mw": float(persistence_errors.mean()),
                    "candidate_mae_mw": float(candidate_errors.mean()),
                    "mae_delta_mw": float(
                        candidate_errors.mean() - persistence_errors.mean()
                    ),
                    "mae_improvement_mw": float(
                        persistence_errors.mean() - candidate_errors.mean()
                    ),
                    "reference_rmse_mw": float(
                        sqrt(
                            (
                                (
                                    pivot["persistence_current_value"]
                                    - actual
                                )
                                ** 2
                            ).mean()
                        )
                    ),
                    "candidate_rmse_mw": float(
                        sqrt(((pivot[candidate] - actual) ** 2).mean())
                    ),
                    "win_count": int((difference < -1e-9).sum()),
                    "tie_count": int((difference.abs() <= 1e-9).sum()),
                    "loss_count": int((difference > 1e-9).sum()),
                    "scorecard_contract_version": (
                        SCORECARD_CONTRACT_VERSION
                    ),
                }
            )
    return rows


def build_model_family_scorecard(
    utc_predictions: pd.DataFrame,
    uk_local_predictions: pd.DataFrame,
    *,
    scorecard_run_id: str | None = None,
    scorecard_run_timestamp: datetime | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Compare UTC and UK-local seasonal model families on one paired cohort."""
    utc_run, utc_run_id = prepare_seasonal_run(
        utc_predictions,
        expected_feature_contract=UTC_FEATURE_CONTRACT_VERSION,
    )
    uk_run, uk_run_id = prepare_seasonal_run(
        uk_local_predictions,
        expected_feature_contract=UK_LOCAL_FEATURE_CONTRACT_VERSION,
    )
    _assert_same_target_cohort(utc_run, uk_run)
    scorecard_run_id = scorecard_run_id or str(uuid4())
    scorecard_run_timestamp = scorecard_run_timestamp or datetime.now(
        timezone.utc
    )
    if scorecard_run_timestamp.tzinfo is None:
        raise ModelFamilyScorecardError(
            "scorecard_run_timestamp must be timezone-aware."
        )
    scorecard_run_timestamp = scorecard_run_timestamp.astimezone(timezone.utc)

    controls = utc_run.loc[
        utc_run["model_name"].isin(CONTROL_MODELS)
    ].copy()
    controls["scorecard_model_name"] = controls["model_name"]
    controls["model_family"] = controls["model_name"].map(
        {
            "persistence_current_value": "persistence",
            "seasonal_previous_day": "seasonal",
            "seasonal_previous_week": "seasonal",
        }
    )
    utc_ridge = utc_run.loc[
        utc_run["model_name"] == "ridge_weather_lag"
    ].copy()
    utc_ridge["scorecard_model_name"] = "ridge_weather_lag_utc"
    utc_ridge["model_family"] = "ridge_utc_calendar"
    uk_ridge = uk_run.loc[
        uk_run["model_name"] == "ridge_weather_lag"
    ].copy()
    uk_ridge["scorecard_model_name"] = "ridge_weather_lag_uk_local"
    uk_ridge["model_family"] = "ridge_uk_local_calendar"
    predictions = pd.concat(
        [controls, utc_ridge, uk_ridge], ignore_index=True
    )
    predictions["scorecard_run_id"] = scorecard_run_id
    predictions["scorecard_run_timestamp_utc"] = scorecard_run_timestamp
    predictions["utc_source_run_id"] = utc_run_id
    predictions["uk_local_source_run_id"] = uk_run_id

    paired = (
        predictions.groupby(PAIRING_COLUMNS, dropna=False)
        .agg(
            row_count=("scorecard_model_name", "size"),
            model_count=("scorecard_model_name", "nunique"),
            actual_count=("actual_demand_mw", "nunique"),
            boundary_count=("trained_through_utc", "nunique"),
            training_count=("training_observation_count", "nunique"),
        )
        .reset_index()
    )
    if (
        (paired["row_count"] != len(OUTPUT_MODELS))
        | (paired["model_count"] != len(OUTPUT_MODELS))
        | (paired["actual_count"] != 1)
        | (paired["boundary_count"] != 1)
        | (paired["training_count"] != 1)
    ).any():
        raise ModelFamilyScorecardError(
            "Model families do not form exact five-model target pairs."
        )

    metric_grouping = [
        *GROUP_COLUMNS,
        "requested_horizon_minutes",
        "split",
        "_origin_fold_key",
        "scorecard_model_name",
    ]
    scorecard = pd.DataFrame(
        [
            _metric_row(group)
            for _, group in predictions.groupby(
                metric_grouping, sort=True, dropna=False
            )
        ]
    )
    pairwise = pd.DataFrame(_pairwise_rows(predictions))
    if scorecard.empty or pairwise.empty:
        raise ModelFamilyScorecardError(
            "Model-family scorecard produced no evidence."
        )
    counts = pairwise[
        ["win_count", "tie_count", "loss_count"]
    ].sum(axis=1)
    if not (counts == pairwise["paired_observation_count"]).all():
        raise ModelFamilyScorecardError(
            "Pairwise win, tie, and loss counts are inconsistent."
        )
    return scorecard.reset_index(drop=True), pairwise.reset_index(drop=True)

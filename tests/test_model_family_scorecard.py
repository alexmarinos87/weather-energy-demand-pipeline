from datetime import datetime, timezone
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from jsonschema import Draft202012Validator, FormatChecker

from forecasting.contracts import (
    UK_LOCAL_FEATURE_CONTRACT_VERSION,
    UTC_FEATURE_CONTRACT_VERSION,
)
from forecasting.model_family_scorecard import (
    ModelFamilyScorecardError,
    build_model_family_scorecard,
    prepare_seasonal_run,
)
from forecasting.run_model_family_scorecard import main


ROOT = Path(__file__).resolve().parents[1]
MODELS = (
    "persistence_current_value",
    "seasonal_previous_day",
    "seasonal_previous_week",
    "ridge_weather_lag",
)


def seasonal_predictions(
    *,
    run_id: str,
    feature_contract_version: str,
    ridge_error: float,
    rolling: bool = False,
) -> pd.DataFrame:
    rows = []
    start = pd.Timestamp("2026-01-01T00:00:00Z")
    for split, count, offset in (
        ("validation", 8, 0),
        ("test", 5, 8),
    ):
        for index in range(count):
            feature = start + pd.Timedelta(minutes=5 * (offset + index))
            target = feature + pd.Timedelta(minutes=30)
            actual = 100.0 + index
            predictions = {
                "persistence_current_value": actual + 4.0,
                "seasonal_previous_day": actual + 3.0,
                "seasonal_previous_week": actual + 2.0,
                "ridge_weather_lag": actual + ridge_error,
            }
            for model_name, prediction in predictions.items():
                rows.append(
                    {
                        "run_id": run_id,
                        "source_area": "east_midlands",
                        "resource_id": "resource-1",
                        "city": "Nottingham",
                        "feature_timestamp_utc": feature,
                        "event_timestamp_utc": target,
                        "requested_horizon_minutes": 30,
                        "split": split,
                        "model_name": model_name,
                        "actual_demand_mw": actual,
                        "predicted_demand_mw": prediction,
                        "trained_through_utc": start
                        - pd.Timedelta(hours=1),
                        "training_observation_count": 48,
                        "feature_contract_version": feature_contract_version,
                        "evaluation_contract_version": (
                            "rolling-origin-v1"
                            if rolling
                            else "fixed-holdout-v1"
                        ),
                        "origin_fold": (
                            1
                            if rolling and split == "validation"
                            else 3
                            if rolling
                            else None
                        ),
                        "origin_count": 3 if rolling else None,
                        "origin_cutoff_utc": feature if rolling else None,
                    }
                )
    return pd.DataFrame(rows)


def paired_inputs(*, rolling: bool = False):
    return (
        seasonal_predictions(
            run_id="utc-run",
            feature_contract_version=UTC_FEATURE_CONTRACT_VERSION,
            ridge_error=1.0,
            rolling=rolling,
        ),
        seasonal_predictions(
            run_id="uk-run",
            feature_contract_version=UK_LOCAL_FEATURE_CONTRACT_VERSION,
            ridge_error=0.5,
            rolling=rolling,
        ),
    )


def build(utc=None, uk=None):
    default_utc, default_uk = paired_inputs()
    return build_model_family_scorecard(
        default_utc if utc is None else utc,
        default_uk if uk is None else uk,
        scorecard_run_id="scorecard-run",
        scorecard_run_timestamp=datetime(
            2026, 1, 2, tzinfo=timezone.utc
        ),
    )


def test_scorecard_compares_exact_five_model_families_on_one_cohort():
    scorecard, pairwise = build()
    assert set(scorecard["model_name"]) == {
        "persistence_current_value",
        "seasonal_previous_day",
        "seasonal_previous_week",
        "ridge_weather_lag_utc",
        "ridge_weather_lag_uk_local",
    }
    test = scorecard.loc[scorecard["split"] == "test"]
    assert set(test["paired_observation_count"]) == {5}
    assert test["paired_target_identity_sha256"].nunique() == 1
    assert set(
        pairwise.loc[
            pairwise["split"] == "test", "paired_observation_count"
        ]
    ) == {5}


def test_target_cohort_mismatch_is_rejected():
    utc, uk = paired_inputs()
    first_test = uk.loc[
        uk["split"] == "test", "feature_timestamp_utc"
    ].min()
    uk = uk.loc[
        ~(
            (uk["split"] == "test")
            & (uk["feature_timestamp_utc"] == first_test)
        )
    ].copy()
    with pytest.raises(
        ModelFamilyScorecardError, match="same paired target cohort"
    ):
        build(utc, uk)


def test_control_prediction_drift_between_calendar_runs_is_rejected():
    utc, uk = paired_inputs()
    mask = uk["model_name"] == "seasonal_previous_day"
    uk.loc[mask, "predicted_demand_mw"] += 0.1
    with pytest.raises(
        ModelFamilyScorecardError, match="differs between calendar runs"
    ):
        build(utc, uk)


def test_training_boundary_drift_between_calendar_runs_is_rejected():
    utc, uk = paired_inputs()
    uk.loc[
        uk["model_name"] == "ridge_weather_lag",
        "trained_through_utc",
    ] = pd.Timestamp("2025-12-31T22:00:00Z")
    with pytest.raises(ModelFamilyScorecardError, match="training boundary"):
        build(utc, uk)


def test_only_ridge_calendar_predictions_may_differ():
    scorecard, pairwise = build()
    test = scorecard.loc[scorecard["split"] == "test"].set_index("model_name")
    assert test.loc["ridge_weather_lag_uk_local", "mae_mw"] == 0.5
    assert test.loc["ridge_weather_lag_utc", "mae_mw"] == 1.0
    assert (
        test.loc[
            "ridge_weather_lag_uk_local",
            "source_feature_contract_version",
        ]
        == UK_LOCAL_FEATURE_CONTRACT_VERSION
    )
    assert (
        test.loc[
            "ridge_weather_lag_utc",
            "source_feature_contract_version",
        ]
        == UTC_FEATURE_CONTRACT_VERSION
    )
    uk_pair = pairwise.loc[
        pairwise["candidate_model_name"]
        == "ridge_weather_lag_uk_local"
    ]
    assert (uk_pair["mae_improvement_mw"] > 0).all()


def test_pairwise_win_tie_loss_counts_cover_the_paired_cohort():
    _, pairwise = build()
    counts = pairwise[["win_count", "tie_count", "loss_count"]].sum(axis=1)
    assert (counts == pairwise["paired_observation_count"]).all()
    assert set(pairwise["reference_model_name"]) == {
        "persistence_current_value"
    }


def test_rolling_origin_identity_is_retained():
    utc, uk = paired_inputs(rolling=True)
    scorecard, pairwise = build_model_family_scorecard(
        utc,
        uk,
        scorecard_run_id="rolling-scorecard",
        scorecard_run_timestamp=datetime(
            2026, 1, 2, tzinfo=timezone.utc
        ),
    )
    assert set(scorecard["origin_fold"].astype(int)) == {1, 3}
    assert set(pairwise["origin_fold"].astype(int)) == {1, 3}
    assert set(scorecard["evaluation_contract_version"]) == {
        "rolling-origin-v1"
    }


def _json_row(row: pd.Series) -> dict:
    payload = row.to_dict()
    for key, value in list(payload.items()):
        if isinstance(value, pd.Timestamp):
            payload[key] = value.isoformat()
        elif isinstance(value, np.generic):
            payload[key] = value.item()
        elif pd.isna(value):
            payload[key] = None
    return payload


def test_scorecard_and_pairwise_rows_satisfy_versioned_schemas():
    scorecard, pairwise = build()
    scorecard_schema = json.loads(
        (
            ROOT
            / "data-contracts"
            / "model_family_scorecard_schema.json"
        ).read_text(encoding="utf-8")
    )
    pairwise_schema = json.loads(
        (
            ROOT
            / "data-contracts"
            / "model_family_pairwise_metrics_schema.json"
        ).read_text(encoding="utf-8")
    )
    assert list(
        Draft202012Validator(
            scorecard_schema, format_checker=FormatChecker()
        ).iter_errors(_json_row(scorecard.iloc[0]))
    ) == []
    assert list(
        Draft202012Validator(
            pairwise_schema, format_checker=FormatChecker()
        ).iter_errors(_json_row(pairwise.iloc[0]))
    ) == []


def test_cli_writes_immutable_scorecard_pairwise_and_summary(tmp_path):
    utc, uk = paired_inputs()
    utc_path = tmp_path / "utc.csv"
    uk_path = tmp_path / "uk.csv"
    output = tmp_path / "output"
    utc.to_csv(utc_path, index=False)
    uk.to_csv(uk_path, index=False)
    assert (
        main(
            [
                "--utc-predictions",
                str(utc_path),
                "--uk-local-predictions",
                str(uk_path),
                "--output-dir",
                str(output),
                "--output-format",
                "csv",
            ]
        )
        == 0
    )
    assert len(list(output.glob("model_family_scorecard_*.csv"))) == 1
    assert len(
        list(output.glob("model_family_pairwise_metrics_*.csv"))
    ) == 1
    summaries = list(output.glob("model_family_summary_*.md"))
    assert len(summaries) == 1
    assert "Lower retained error" in summaries[0].read_text(encoding="utf-8")


def test_prepare_run_requires_explicit_single_run_and_expected_contract():
    utc, _ = paired_inputs()
    combined = pd.concat(
        [utc, utc.assign(run_id="another-run")], ignore_index=True
    )
    with pytest.raises(ModelFamilyScorecardError, match="exactly one run_id"):
        prepare_seasonal_run(
            combined,
            expected_feature_contract=UTC_FEATURE_CONTRACT_VERSION,
        )
    with pytest.raises(
        ModelFamilyScorecardError, match="Expected feature_contract"
    ):
        prepare_seasonal_run(
            utc,
            expected_feature_contract=UK_LOCAL_FEATURE_CONTRACT_VERSION,
        )

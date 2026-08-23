from datetime import datetime, timezone
import json
from pathlib import Path

import pandas as pd
import pytest
from jsonschema import Draft202012Validator, FormatChecker

from forecasting.contracts import (
    FEATURE_TIMESTAMP_COLUMN,
    GROUP_COLUMNS,
    REQUESTED_HORIZON_COLUMN,
    TARGET_COLUMN,
    TARGET_TIMESTAMP_COLUMN,
    TIMESTAMP_COLUMN,
    BacktestConfig,
    ForecastingContractError,
)
from forecasting.run_seasonal_baselines import main
from forecasting.seasonal_baselines import (
    ALL_MODELS,
    SeasonalBaselineConfig,
    attach_seasonal_references,
    build_seasonal_demo_feature_frame,
    run_seasonal_backtest,
)


ROOT = Path(__file__).resolve().parents[1]
IDENTITY = {
    "source_area": "east_midlands",
    "resource_id": "resource-1",
    "city": "Nottingham",
}


def _prepared(timestamps: list[str]) -> pd.DataFrame:
    values = pd.to_datetime(timestamps, utc=True)
    return pd.DataFrame(
        {
            **{key: value for key, value in IDENTITY.items()},
            TIMESTAMP_COLUMN: values,
            TARGET_COLUMN: [100.0 + index for index in range(len(values))],
        }
    )


def _supervised(rows: list[tuple[str, str]]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                **IDENTITY,
                FEATURE_TIMESTAMP_COLUMN: pd.Timestamp(feature),
                TARGET_TIMESTAMP_COLUMN: pd.Timestamp(target),
                REQUESTED_HORIZON_COLUMN: 30,
            }
            for feature, target in rows
        ]
    )


def test_day_and_week_references_match_by_elapsed_time_not_row_position():
    target = pd.Timestamp("2026-01-08T00:30:00Z")
    feature = pd.Timestamp("2026-01-08T00:00:00Z")
    source = _prepared(
        [
            "2026-01-01T00:30:00Z",
            "2026-01-07T00:30:00Z",
            "2026-01-07T23:57:00Z",
            "2026-01-08T00:00:00Z",
        ]
    )
    cohort = attach_seasonal_references(
        source,
        _supervised([(feature.isoformat(), target.isoformat())]),
        config=SeasonalBaselineConfig(
            reference_tolerance_minutes=5,
            min_reference_coverage=1.0,
        ),
    )
    assert cohort.loc[0, "previous_day_source_timestamp_utc"] == (
        target - pd.Timedelta(days=1)
    )
    assert cohort.loc[0, "previous_week_source_timestamp_utc"] == (
        target - pd.Timedelta(days=7)
    )
    assert cohort.loc[0, "previous_day_offset_minutes"] == 0.0
    assert cohort.loc[0, "previous_week_offset_minutes"] == 0.0


def test_equal_distance_reference_prefers_observation_at_or_before_ideal_time():
    source = _prepared(
        [
            "2026-01-01T00:30:00Z",
            "2026-01-07T00:25:00Z",
            "2026-01-07T00:35:00Z",
            "2026-01-08T00:00:00Z",
        ]
    )
    cohort = attach_seasonal_references(
        source,
        _supervised(
            [("2026-01-08T00:00:00Z", "2026-01-08T00:30:00Z")]
        ),
        config=SeasonalBaselineConfig(
            reference_tolerance_minutes=5,
            min_reference_coverage=1.0,
        ),
    )
    assert cohort.loc[0, "previous_day_source_timestamp_utc"] == pd.Timestamp(
        "2026-01-07T00:25:00Z"
    )
    assert cohort.loc[0, "previous_day_offset_minutes"] == -5.0


def test_internal_reference_gap_lowers_coverage_instead_of_disappearing():
    source = _prepared(
        [
            "2026-01-01T00:30:00Z",
            "2026-01-01T00:35:00Z",
            "2026-01-07T00:30:00Z",
            "2026-01-08T00:30:00Z",
            "2026-01-08T00:35:00Z",
        ]
    )
    supervised = _supervised(
        [
            ("2026-01-08T00:00:00Z", "2026-01-08T00:30:00Z"),
            ("2026-01-08T00:05:00Z", "2026-01-08T00:35:00Z"),
        ]
    )
    with pytest.raises(ForecastingContractError, match="matched 1/2"):
        attach_seasonal_references(
            source,
            supervised,
            config=SeasonalBaselineConfig(
                reference_tolerance_minutes=1,
                min_reference_coverage=0.75,
            ),
        )


def test_pre_history_rows_are_excluded_from_reference_coverage_denominator():
    source = _prepared(
        [
            "2026-01-01T00:30:00Z",
            "2026-01-06T00:30:00Z",
            "2026-01-07T00:30:00Z",
            "2026-01-08T00:00:00Z",
        ]
    )
    cohort = attach_seasonal_references(
        source,
        _supervised(
            [
                ("2026-01-07T00:00:00Z", "2026-01-07T00:30:00Z"),
                ("2026-01-08T00:00:00Z", "2026-01-08T00:30:00Z"),
            ]
        ),
        config=SeasonalBaselineConfig(
            reference_tolerance_minutes=0,
            min_reference_coverage=1.0,
        ),
    )
    assert len(cohort) == 1
    assert cohort.loc[0, "previous_day_eligible_count"] == 2
    assert cohort.loc[0, "previous_week_eligible_count"] == 1
    assert cohort.loc[0, FEATURE_TIMESTAMP_COLUMN] == pd.Timestamp(
        "2026-01-08T00:00:00Z"
    )


def test_elapsed_utc_reference_is_stable_across_autumn_dst_transition():
    target = pd.Timestamp("2026-10-25T01:30:00Z")
    source = _prepared(
        [
            "2026-10-18T01:30:00Z",
            "2026-10-24T01:30:00Z",
            "2026-10-25T00:30:00Z",
        ]
    )
    cohort = attach_seasonal_references(
        source,
        _supervised(
            [("2026-10-25T00:30:00Z", target.isoformat())]
        ),
        config=SeasonalBaselineConfig(
            reference_tolerance_minutes=0,
            min_reference_coverage=1.0,
        ),
    )
    assert (
        target - cohort.loc[0, "previous_day_source_timestamp_utc"]
    ) == pd.Timedelta(minutes=1440)
    assert (
        target - cohort.loc[0, "previous_week_source_timestamp_utc"]
    ) == pd.Timedelta(minutes=10080)


def test_holdout_comparison_uses_identical_rows_and_boundaries_for_all_models():
    frame = build_seasonal_demo_feature_frame(periods=9 * 288)
    predictions, metrics = run_seasonal_backtest(
        frame,
        backtest_config=BacktestConfig(horizon_minutes=(30,)),
        seasonal_config=SeasonalBaselineConfig(
            reference_tolerance_minutes=0,
            min_reference_coverage=1.0,
        ),
        run_id="seasonal-holdout",
        run_timestamp=datetime(2026, 8, 23, tzinfo=timezone.utc),
    )
    assert set(predictions["model_name"]) == set(ALL_MODELS)
    identity = [
        *GROUP_COLUMNS,
        REQUESTED_HORIZON_COLUMN,
        "split",
        FEATURE_TIMESTAMP_COLUMN,
        "event_timestamp_utc",
    ]
    paired = predictions.groupby(identity).agg(
        rows=("model_name", "size"),
        models=("model_name", "nunique"),
        boundaries=("trained_through_utc", "nunique"),
        actuals=("actual_demand_mw", "nunique"),
    )
    assert (paired["rows"] == 4).all()
    assert (paired["models"] == 4).all()
    assert (paired["boundaries"] == 1).all()
    assert (paired["actuals"] == 1).all()
    assert set(metrics["model_name"]) == set(ALL_MODELS)
    seasonal = predictions["model_name"].str.startswith("seasonal_")
    assert (
        predictions.loc[seasonal, "seasonal_reference_timestamp_utc"]
        <= predictions.loc[seasonal, FEATURE_TIMESTAMP_COLUMN]
    ).all()


def test_rolling_origin_comparison_preserves_complete_origin_sequence():
    frame = build_seasonal_demo_feature_frame(periods=9 * 288)
    predictions, metrics = run_seasonal_backtest(
        frame,
        backtest_config=BacktestConfig(horizon_minutes=(30,)),
        seasonal_config=SeasonalBaselineConfig(
            reference_tolerance_minutes=0,
            min_reference_coverage=1.0,
        ),
        evaluation_mode="rolling-origin",
        origin_count=3,
    )
    assert set(predictions["origin_fold"].astype(int)) == {1, 2, 3}
    assert set(predictions.loc[predictions["origin_fold"] < 3, "split"]) == {
        "validation"
    }
    assert set(predictions.loc[predictions["origin_fold"] == 3, "split"]) == {
        "test"
    }
    assert set(metrics["evaluation_contract_version"]) == {
        "rolling-origin-v1"
    }


def _json_row(row: pd.Series) -> dict:
    payload = row.to_dict()
    for key, value in list(payload.items()):
        if isinstance(value, pd.Timestamp):
            payload[key] = value.isoformat()
        elif pd.isna(value):
            payload[key] = None
    return payload


def test_prediction_and_metric_evidence_satisfy_versioned_schemas():
    frame = build_seasonal_demo_feature_frame(periods=9 * 288)
    predictions, metrics = run_seasonal_backtest(
        frame,
        backtest_config=BacktestConfig(horizon_minutes=(30,)),
        seasonal_config=SeasonalBaselineConfig(
            reference_tolerance_minutes=0,
            min_reference_coverage=1.0,
        ),
    )
    prediction_schema = json.loads(
        (
            ROOT
            / "data-contracts"
            / "seasonal_baseline_prediction_schema.json"
        ).read_text(encoding="utf-8")
    )
    metric_schema = json.loads(
        (
            ROOT
            / "data-contracts"
            / "seasonal_baseline_metrics_schema.json"
        ).read_text(encoding="utf-8")
    )
    prediction = predictions.loc[
        predictions["model_name"] == "seasonal_previous_week"
    ].iloc[0]
    assert list(
        Draft202012Validator(
            prediction_schema, format_checker=FormatChecker()
        ).iter_errors(_json_row(prediction))
    ) == []
    assert list(
        Draft202012Validator(
            metric_schema, format_checker=FormatChecker()
        ).iter_errors(_json_row(metrics.iloc[0]))
    ) == []


def test_cli_writes_distinct_credential_free_seasonal_outputs(tmp_path):
    assert (
        main(
            [
                "--demo",
                "--demo-days",
                "9",
                "--horizon-minutes",
                "30",
                "--seasonal-reference-tolerance-minutes",
                "0",
                "--min-seasonal-reference-coverage",
                "1.0",
                "--output-dir",
                str(tmp_path),
                "--output-format",
                "csv",
            ]
        )
        == 0
    )
    predictions = tmp_path / "seasonal_comparison_predictions.csv"
    metrics = tmp_path / "seasonal_comparison_metrics.csv"
    assert predictions.is_file()
    assert metrics.is_file()
    assert set(pd.read_csv(predictions)["model_name"]) == set(ALL_MODELS)
    assert not (tmp_path / "baseline_predictions.csv").exists()


def test_implementation_does_not_use_fixed_daily_or_weekly_row_offsets():
    source = (ROOT / "forecasting" / "seasonal_baselines.py").read_text(
        encoding="utf-8"
    )
    assert "shift(288" not in source
    assert "shift(2016" not in source
    assert "24 * 60" in source
    assert "7 * 24 * 60" in source
    assert "searchsorted" in source

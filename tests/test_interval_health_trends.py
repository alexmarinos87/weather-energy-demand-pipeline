from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
from jsonschema import Draft202012Validator, FormatChecker

from forecasting.interval_health_trends import (
    AUTHORITY_FIELDS,
    IntervalHealthTrendError,
    build_interval_health_trends,
)
from forecasting.run_interval_health_trends import main


ROOT = Path(__file__).resolve().parents[1]
TREND_RUN_ID = "iht-" + "1" * 24
TREND_TIMESTAMP = "2026-01-20T00:00:00Z"
AREAS = (
    ("east_midlands", "resource-east", "Nottingham"),
    ("south_wales", "resource-wales", "Cardiff"),
)
MODELS = ("persistence_current_value", "ridge_weather_lag")
HORIZONS = (30, 60)
COVERAGE_LEVELS = (0.80, 0.90)


def _history() -> pd.DataFrame:
    anchor = pd.Timestamp(TREND_TIMESTAMP)
    rows: list[dict[str, object]] = []
    for scenario in ("healthy", "warning", "failed"):
        for sequence in range(1, 10):
            run_timestamp = anchor - pd.Timedelta(hours=10 - sequence)
            recent = sequence > 6
            for source_area, resource_id, city in AREAS:
                for horizon in HORIZONS:
                    for model in MODELS:
                        for coverage in COVERAGE_LEVELS:
                            nominal = coverage * 100.0
                            width = 10.0 + horizon / 30.0
                            empirical = nominal - 2.0
                            calibration = 30
                            if scenario == "warning":
                                width *= 1.35 if recent else 1.0
                                empirical = nominal - (3.0 if recent else 1.0)
                            elif scenario == "failed" and recent:
                                empirical = nominal - 10.0
                                calibration = 20
                            rows.append(
                                {
                                    "scenario": scenario,
                                    "history_sequence": sequence,
                                    "interval_run_id": (
                                        f"{scenario}-interval-{sequence:02d}"
                                    ),
                                    "interval_run_timestamp_utc": run_timestamp,
                                    "source_area": source_area,
                                    "resource_id": resource_id,
                                    "city": city,
                                    "requested_horizon_minutes": horizon,
                                    "model_name": model,
                                    "feature_contract_version": "time-horizon-v1",
                                    "target_coverage_level": coverage,
                                    "calibration_observation_count": calibration,
                                    "calibration_radius_mw": width / 2.0,
                                    "evaluation_end_utc": (
                                        run_timestamp - pd.Timedelta(minutes=30)
                                    ),
                                    "evaluation_observation_count": 100,
                                    "empirical_coverage_pct": empirical,
                                    "average_interval_width_mw": width,
                                    "interval_contract_version": (
                                        "split-conformal-absolute-residual-v1"
                                    ),
                                }
                            )
    return pd.DataFrame(rows)


def _summaries() -> pd.DataFrame:
    rows = []
    for scenario in ("healthy", "warning", "failed"):
        rows.append(
            {
                "scenario": scenario,
                "monitor_run_id": f"monitor-{scenario}",
                "monitor_status": scenario,
                "failed_error_check_count": 2 if scenario == "failed" else 0,
                "failed_warning_check_count": (
                    1 if scenario in {"warning", "failed"} else 0
                ),
                "policy_version": "prediction-interval-monitoring-policy-v1",
                "monitoring_contract_version": (
                    "prediction-interval-monitoring-v1"
                ),
                **{field: False for field in AUTHORITY_FIELDS},
            }
        )
    return pd.DataFrame(rows)


def _json_row(row: pd.Series) -> dict[str, object]:
    output: dict[str, object] = {}
    for key, value in row.items():
        if isinstance(value, pd.Timestamp):
            output[key] = value.isoformat()
        elif pd.isna(value):
            output[key] = None
        elif hasattr(value, "item"):
            output[key] = value.item()
        else:
            output[key] = value
    return output


def test_trends_preserve_exact_slices_and_recent_reference_windows():
    run_trends, slice_trends = build_interval_health_trends(
        _history(),
        _summaries(),
        trend_run_id=TREND_RUN_ID,
        trend_run_timestamp=TREND_TIMESTAMP,
    )

    expected_slices = len(AREAS) * len(HORIZONS) * len(MODELS) * len(
        COVERAGE_LEVELS
    )
    assert len(run_trends) == 3 * 9 * expected_slices
    assert len(slice_trends) == 3 * expected_slices
    assert set(run_trends["monitoring_window"]) == {"reference", "recent"}
    assert set(slice_trends["recent_interval_run_count"]) == {3}
    assert set(slice_trends["reference_interval_run_count"]) == {6}
    assert set(slice_trends["interval_run_count"]) == {9}
    assert set(run_trends["source_area"]) == {
        "east_midlands",
        "south_wales",
    }
    assert set(run_trends["requested_horizon_minutes"]) == {30, 60}


def test_trends_expose_warning_width_growth_and_failed_calibration_decline():
    _, slice_trends = build_interval_health_trends(
        _history(),
        _summaries(),
        trend_run_id=TREND_RUN_ID,
        trend_run_timestamp=TREND_TIMESTAMP,
    )

    healthy = slice_trends.loc[slice_trends["scenario"] == "healthy"]
    warning = slice_trends.loc[slice_trends["scenario"] == "warning"]
    failed = slice_trends.loc[slice_trends["scenario"] == "failed"]
    assert healthy["average_interval_width_increase_pct"].abs().max() < 1e-9
    assert set(warning["average_interval_width_increase_pct"].round(6)) == {
        35.0
    }
    assert set(failed["latest_calibration_observation_count"]) == {20}
    assert set(failed["calibration_history_drop_pct"].round(6)) == {
        33.333333
    }
    assert set(healthy["attention_required"]) == {False}
    assert set(warning["attention_required"]) == {True}
    assert set(failed["attention_required"]) == {True}


def test_run_trends_retain_previous_run_deltas_without_inventing_first_delta():
    run_trends, _ = build_interval_health_trends(
        _history(),
        _summaries(),
        trend_run_id=TREND_RUN_ID,
        trend_run_timestamp=TREND_TIMESTAMP,
    )
    one_slice = run_trends.loc[
        (run_trends["scenario"] == "warning")
        & (run_trends["source_area"] == "east_midlands")
        & (run_trends["requested_horizon_minutes"] == 30)
        & (run_trends["model_name"] == "ridge_weather_lag")
        & (run_trends["target_coverage_level"] == 0.9)
    ].sort_values("history_sequence")

    assert pd.isna(
        one_slice.iloc[0]["coverage_change_from_previous_pct_points"]
    )
    assert pd.isna(
        one_slice.iloc[0]["interval_width_change_from_previous_pct"]
    )
    assert one_slice.iloc[-1]["is_latest_interval_run"]
    assert one_slice.iloc[-1]["previous_interval_run_id"] == (
        "warning-interval-08"
    )


def test_incomplete_run_slice_set_is_rejected():
    history = _history()
    history = history.drop(history.index[0]).reset_index(drop=True)

    with pytest.raises(IntervalHealthTrendError, match="incomplete slices"):
        build_interval_health_trends(
            history,
            _summaries(),
            trend_run_id=TREND_RUN_ID,
            trend_run_timestamp=TREND_TIMESTAMP,
        )


def test_summary_authority_cannot_enable_automatic_action():
    summaries = _summaries()
    summaries.loc[0, "automatic_recalibration_allowed"] = True

    with pytest.raises(IntervalHealthTrendError, match="false authority"):
        build_interval_health_trends(
            _history(),
            summaries,
            trend_run_id=TREND_RUN_ID,
            trend_run_timestamp=TREND_TIMESTAMP,
        )


def test_summary_scenarios_must_match_history():
    summaries = _summaries().loc[lambda frame: frame["scenario"] != "failed"]

    with pytest.raises(IntervalHealthTrendError, match="do not match"):
        build_interval_health_trends(
            _history(),
            summaries,
            trend_run_id=TREND_RUN_ID,
            trend_run_timestamp=TREND_TIMESTAMP,
        )


def test_trend_rows_satisfy_versioned_json_schemas():
    run_trends, slice_trends = build_interval_health_trends(
        _history(),
        _summaries(),
        trend_run_id=TREND_RUN_ID,
        trend_run_timestamp=TREND_TIMESTAMP,
    )
    contracts = (
        (
            "interval_health_run_trend_schema.json",
            _json_row(run_trends.iloc[0]),
        ),
        (
            "interval_health_run_trend_schema.json",
            _json_row(run_trends.iloc[-1]),
        ),
        (
            "interval_health_slice_trend_schema.json",
            _json_row(slice_trends.iloc[0]),
        ),
    )
    for filename, row in contracts:
        schema = json.loads(
            (ROOT / "data-contracts" / filename).read_text(encoding="utf-8")
        )
        errors = list(
            Draft202012Validator(
                schema, format_checker=FormatChecker()
            ).iter_errors(row)
        )
        assert errors == []


def test_cli_writes_immutable_csv_outputs(tmp_path):
    history_path = tmp_path / "history.csv"
    summary_path = tmp_path / "summary.csv"
    output_dir = tmp_path / "output"
    _history().to_csv(history_path, index=False)
    _summaries().to_csv(summary_path, index=False)

    arguments = [
        "--history",
        str(history_path),
        "--health-summary",
        str(summary_path),
        "--output-dir",
        str(output_dir),
        "--output-format",
        "csv",
        "--trend-run-id",
        TREND_RUN_ID,
        "--trend-run-timestamp",
        TREND_TIMESTAMP,
    ]
    assert main(arguments) == 0
    assert (
        output_dir / f"interval_health_run_trends_{TREND_RUN_ID}.csv"
    ).is_file()
    assert (
        output_dir / f"interval_health_slice_trends_{TREND_RUN_ID}.csv"
    ).is_file()
    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        main(arguments)


def test_documentation_keeps_trends_reproducible_and_non_mutating():
    document = (ROOT / "INTERVAL_HEALTH_TRENDS.md").read_text(encoding="utf-8")
    assert "interval_health_run_trends" in document
    assert "interval_health_slice_trends" in document
    assert "evaluation-row weighted" in document
    assert "does not recalculate an interval radius" in document
    assert "automatic recalibration" in document

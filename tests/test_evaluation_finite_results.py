"""Finite scores must not become non-finite or silently incomplete evaluations."""
from datetime import datetime, timezone
from math import sqrt

import pandas as pd
import pytest

from forecasting import evaluation
from forecasting.contracts import BacktestConfig, ForecastingContractError
from forecasting.run_baseline import main

STAMP = pd.Timestamp("2026-01-01T00:00:00Z")


def source_frame(actual=(10.0, 20.0)):
    return pd.DataFrame({
        "source_area": ["east_midlands"] * 2,
        "resource_id": ["resource-1"] * 2,
        "city": ["Nottingham"] * 2,
        "feature_timestamp_utc": [STAMP, STAMP + pd.Timedelta(minutes=5)],
        "target_timestamp_utc": [STAMP + pd.Timedelta(minutes=30), STAMP + pd.Timedelta(minutes=35)],
        "requested_horizon_minutes": [30, 30],
        "horizon_steps": [6, 6], "horizon_minutes": [30.0, 30.0],
        "target_delay_minutes": [0.0, 0.0], "demand_mw": [8.0, 18.0],
        "target_demand_mw": pd.Series(actual, dtype=object),
        "eligible_target_count": [2, 2], "matched_target_count": [2, 2],
        "target_coverage_pct": [100.0, 100.0],
    })


def rows(source, predicted=(12.0, 18.0)):
    return evaluation._prediction_rows(
        source, predicted=predicted, split="test", model_name="regression-model",
        trained_through=STAMP - pd.Timedelta(minutes=1), run_id="finite-test",
        run_timestamp=datetime(2026, 1, 2, tzinfo=timezone.utc), config=BacktestConfig(),
    )


@pytest.mark.parametrize("invalid", [float("nan"), float("inf"), None, pd.NA, True, 10 ** 400],
                         ids=["nan", "inf", "none", "pd-na", "bool", "huge-int"])
def test_invalid_prediction_is_rejected_contextually(invalid):
    with pytest.raises(ForecastingContractError, match="predicted_demand_mw"):
        rows(source_frame(), [invalid, 18.0])


@pytest.mark.parametrize("column", ["target_demand_mw", "demand_mw"])
@pytest.mark.parametrize("invalid", [float("nan"), float("inf"), True])
def test_invalid_source_measurement_is_rejected_without_mutation(column, invalid):
    source = source_frame()
    source[column] = source[column].astype(object)
    source.at[0, column] = invalid
    original = source.copy(deep=True)
    with pytest.raises(ForecastingContractError):
        rows(source)
    pd.testing.assert_frame_equal(source, original)


def test_finite_values_cannot_overflow_the_residual():
    with pytest.raises(ForecastingContractError, match="residual"):
        rows(source_frame((1e308, 20.0)), [-1e308, 18.0])


def test_finite_residual_cannot_overflow_squared_error():
    with pytest.raises(ForecastingContractError, match="squared_error"):
        rows(source_frame(), [1e200, 18.0])


@pytest.mark.parametrize("column", ["actual_demand_mw", "predicted_demand_mw"])
@pytest.mark.parametrize("invalid", [float("nan"), float("inf"), True])
def test_metric_input_cannot_skip_invalid_observations(column, invalid):
    source = source_frame()
    predictions = pd.DataFrame(rows(source))
    predictions[column] = predictions[column].astype(object)
    predictions.at[0, column] = invalid
    original = predictions.copy(deep=True)
    with pytest.raises(ForecastingContractError, match=column):
        evaluation._metric_row(predictions, source)
    pd.testing.assert_frame_equal(predictions, original)


def test_aggregate_squared_error_overflow_is_rejected():
    source = source_frame((0.0, 0.0))
    predictions = pd.DataFrame(rows(source, [1e154, 1e154]))
    # Each squared error is finite, but the existing mean's reduction overflows.
    with pytest.raises(ForecastingContractError, match="rmse_mw"):
        evaluation._metric_row(predictions, source)


def test_empty_metric_group_raises_domain_error():
    source = source_frame()
    predictions = pd.DataFrame(rows(source)).iloc[:0]
    with pytest.raises(ForecastingContractError, match="empty"):
        evaluation._metric_row(predictions, source)


def test_regular_metrics_keep_formulas_and_inputs():
    source = source_frame()
    predictions = pd.DataFrame(rows(source, [12.0, 17.0]))
    original = predictions.copy(deep=True)
    result = evaluation._metric_row(predictions, source)
    assert result["observation_count"] == 2
    assert result["mae_mw"] == 2.5
    assert result["rmse_mw"] == sqrt(6.5)
    assert result["bias_mw"] == -0.5
    assert result["mape_pct"] == pytest.approx(17.5)
    pd.testing.assert_frame_equal(predictions, original)


@pytest.mark.parametrize("actual,expected_mape", [((0.0, 0.0), None), ((0.0, 10.0), 20.0)])
def test_zero_target_mape_semantics_are_preserved(actual, expected_mape):
    source = source_frame(actual)
    predictions = pd.DataFrame(rows(source, [value + 2.0 for value in actual]))
    result = evaluation._metric_row(predictions, source)
    assert result["mape_pct"] == expected_mape
    assert result["mae_mw"] == 2.0
    assert result["observation_count"] == 2


def test_real_cli_stops_before_outputs_when_finite_prediction_squares_overflow(tmp_path, monkeypatch, capsys):
    class ExtremeModel:
        def predict(self, values):
            return [1e200 for _ in values]
    monkeypatch.setattr(evaluation, "fit_ridge_model", lambda *args, **kwargs: ExtremeModel())
    output = tmp_path / "must-not-exist"
    with pytest.raises(ForecastingContractError, match="squared_error"):
        main(["--demo", "--horizon-minutes", "30", "--output-dir", str(output)])
    assert not output.exists()
    assert "Wrote " not in capsys.readouterr().out

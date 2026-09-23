"""Exercise numeric preflight through configuration and real backtest entry points."""
from dataclasses import replace

import numpy as np
import pandas as pd
import pytest

from forecasting import contracts, evaluation
from forecasting.contracts import BacktestConfig, ForecastingContractError
from forecasting.demo import build_demo_feature_frame
from forecasting.run_baseline import main


@pytest.mark.parametrize("field", [
    "train_fraction", "validation_fraction", "min_target_coverage", "ridge_alpha",
])
@pytest.mark.parametrize("invalid", [True, "0.5", None])
def test_real_settings_reject_booleans_strings_and_missing_values(field, invalid):
    with pytest.raises(ForecastingContractError, match=field):
        replace(BacktestConfig(), **{field: invalid}).validate()


@pytest.mark.parametrize("invalid", [float("nan"), float("inf"), float("-inf")])
def test_ridge_penalty_must_be_finite(invalid):
    with pytest.raises(ForecastingContractError, match="ridge_alpha"):
        replace(BacktestConfig(), ridge_alpha=invalid).validate()


@pytest.mark.parametrize("field", ["min_train_rows", "min_validation_rows", "min_test_rows"])
@pytest.mark.parametrize("invalid", [True, 2.5, float("nan"), float("inf"), "2", None])
def test_row_floors_are_positive_integers_not_coercible_or_nonfinite_values(field, invalid):
    with pytest.raises(ForecastingContractError, match=field):
        replace(BacktestConfig(), **{field: invalid}).validate()


def test_valid_numeric_settings_preserve_values_and_defaults():
    defaults = BacktestConfig()
    defaults.validate()
    config = replace(defaults, ridge_alpha=2, min_train_rows=np.int64(24),
                     train_fraction=np.float64(0.60), min_target_coverage=1)
    config.validate()
    assert config.ridge_alpha == 2
    assert config.min_train_rows == 24
    assert config.ordered_horizons == (30, 60)
    assert defaults.ridge_alpha == 1.0
    assert defaults.train_fraction == 0.60
    assert defaults.validation_fraction == 0.20


def test_invalid_configuration_precedes_feature_preparation(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("Feature preparation must not start for invalid configuration")
    monkeypatch.setattr(contracts, "add_uk_local_calendar_features", forbidden)
    with pytest.raises(ForecastingContractError, match="ridge_alpha"):
        contracts.prepare_feature_frame(pd.DataFrame(), BacktestConfig(ridge_alpha=float("nan")))


def test_real_backtest_rejects_invalid_row_floor_before_fitting(monkeypatch):
    frame = build_demo_feature_frame()
    original = frame.copy(deep=True)
    def forbidden(*args, **kwargs):
        raise AssertionError("Fitting must not start for an invalid row floor")
    monkeypatch.setattr(evaluation, "fit_ridge_model", forbidden)
    with pytest.raises(ForecastingContractError, match="min_train_rows"):
        evaluation.run_chronological_backtest(frame, config=BacktestConfig(min_train_rows=float("nan")))
    pd.testing.assert_frame_equal(frame, original)


def test_real_cli_rejects_nan_penalty_without_creating_outputs(tmp_path, capsys):
    output = tmp_path / "must-not-exist"
    with pytest.raises(ForecastingContractError, match="ridge_alpha"):
        main(["--demo", "--ridge-alpha", "nan", "--horizon-minutes", "30",
              "--output-dir", str(output)])
    assert not output.exists()
    assert "Wrote " not in capsys.readouterr().out

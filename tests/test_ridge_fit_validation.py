"""Standalone fitting must not return invalid numeric state."""
import numpy as np
import pandas as pd
import pytest

from forecasting import ridge
from forecasting.contracts import ForecastingContractError


@pytest.fixture
def training():
    return pd.DataFrame({"x": [0.0, 1.0, 2.0], "demand_mw": [2.0, 4.0, 6.0]})


@pytest.mark.parametrize("invalid", [float("nan"), float("inf"), float("-inf"), 0, -1, True, "1", None, 10 ** 400],
                         ids=["nan", "inf", "negative-inf", "zero", "negative", "bool", "string", "none", "huge-int"])
def test_alpha_is_a_positive_finite_real_before_fitting(training, invalid):
    with pytest.raises(ForecastingContractError, match="alpha"):
        ridge.fit_ridge_model(training, feature_columns=("x",), alpha=invalid)


@pytest.mark.parametrize("column", ["x", "demand_mw"])
@pytest.mark.parametrize("invalid", [float("nan"), float("inf"), None, True, 10 ** 400],
                         ids=["nan", "inf", "none", "bool", "huge-int"])
def test_invalid_training_values_are_rejected_before_solver(training, column, invalid, monkeypatch):
    training[column] = training[column].astype(object)
    training.at[1, column] = invalid
    original = training.copy(deep=True)
    def forbidden(*args, **kwargs):
        raise AssertionError("Invalid input reached the solver")
    monkeypatch.setattr(ridge, "_solve", forbidden)
    with pytest.raises(ForecastingContractError, match=column):
        ridge.fit_ridge_model(training, feature_columns=("x",))
    pd.testing.assert_frame_equal(training, original)


@pytest.mark.parametrize("features", ["x", ("x", "x"), ("missing",), (None,), {"x"}])
def test_feature_selection_must_be_unambiguous(training, features):
    with pytest.raises(ForecastingContractError, match="feature"):
        ridge.fit_ridge_model(training, feature_columns=features)


def test_duplicate_dataframe_columns_are_rejected(training):
    ambiguous = pd.concat([training, training[["x"]]], axis=1)
    with pytest.raises(ForecastingContractError, match="duplicate"):
        ridge.fit_ridge_model(ambiguous, feature_columns=("x",))


def test_missing_target_raises_contextual_error(training):
    with pytest.raises(ForecastingContractError, match="target"):
        ridge.fit_ridge_model(training, feature_columns=("x",), target_column="missing")


@pytest.mark.parametrize("values,targets", [
    ([1e308, 1e308], [1.0, 2.0]),
    ([1e200, -1e200], [1.0, 2.0]),
    ([0.0, 1.0, 2.0], [1e308, 1e308, 1e308]),
], ids=["mean-overflow", "variance-overflow", "rhs-overflow"])
def test_finite_inputs_cannot_return_nonfinite_computed_state(values, targets):
    training = pd.DataFrame({"x": values, "demand_mw": targets})
    with pytest.raises(ForecastingContractError, match="Ridge fitting"):
        ridge.fit_ridge_model(training, feature_columns=("x",))


def test_nonfinite_solver_result_is_rejected_before_model_return(training, monkeypatch):
    monkeypatch.setattr(ridge, "_solve", lambda matrix, vector: [float("nan"), 1.0])
    with pytest.raises(ForecastingContractError, match="coefficients"):
        ridge.fit_ridge_model(training, feature_columns=("x",))


def test_regular_fit_preserves_closed_form_result_and_inputs(training):
    original = training.copy(deep=True)
    model = ridge.fit_ridge_model(training, feature_columns=("x",), alpha=np.float64(1.0))
    assert model.means == (1.0,)
    assert model.scales == (1.0,)
    assert model.coefficients == pytest.approx((4.0, 4 / 3))
    assert model.predict([[0.0], [1.0], [2.0]]) == pytest.approx([8 / 3, 4.0, 16 / 3])
    pd.testing.assert_frame_equal(training, original)


def test_finite_numeric_strings_and_constant_features_keep_existing_behavior(training):
    training["x"] = ["1", "1", "1"]
    training["demand_mw"] = ["2", "4", "6"]
    model = ridge.fit_ridge_model(training, feature_columns=("x",), alpha=1.0)
    assert model.scales == (1.0,)
    assert model.predict([[1.0]]) == pytest.approx([4.0])

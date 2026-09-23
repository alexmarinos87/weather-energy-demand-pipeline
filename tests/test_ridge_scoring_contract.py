"""Ridge scoring must respect the complete positional feature contract."""
from dataclasses import replace

import pandas as pd
import pytest

from forecasting import evaluation
from forecasting.contracts import ForecastingContractError
from forecasting.ridge import RidgeModel, fit_ridge_model
from forecasting.run_baseline import main


@pytest.fixture
def model():
    return RidgeModel(feature_columns=("x", "y"), means=(1.0, 2.0),
                      scales=(2.0, 4.0), coefficients=(10.0, 3.0, -2.0), alpha=1.0)


@pytest.mark.parametrize("row", [[], [1.0], [1.0, 2.0, 3.0], "12", b"12", {"x": 1, "y": 2}, None])
def test_row_must_be_a_sequence_with_exact_feature_width(model, row):
    with pytest.raises(ForecastingContractError, match="row 0"):
        model.predict([row])


@pytest.mark.parametrize("invalid", [float("nan"), float("inf"), float("-inf"), None, pd.NA, True, 10 ** 400])
def test_nonfinite_or_malformed_values_fail_contextually(model, invalid):
    with pytest.raises(ForecastingContractError, match="row 0.*x"):
        model.predict([[invalid, 2.0]])


@pytest.mark.parametrize("change", [
    {"means": (1.0,)}, {"scales": (2.0,)},
    {"coefficients": (10.0, 3.0)}, {"coefficients": (10.0, 3.0, -2.0, 8.0)},
    {"scales": (0.0, 4.0)}, {"scales": (-2.0, 4.0)},
    {"means": (float("nan"), 2.0)}, {"scales": (float("inf"), 4.0)},
    {"coefficients": (float("inf"), 3.0, -2.0)},
])
def test_inconsistent_model_state_is_rejected_before_scoring(model, change):
    broken = replace(model, **change)
    with pytest.raises(ForecastingContractError, match="Ridge model"):
        broken.predict([[3.0, 6.0]])


def test_model_validation_occurs_even_for_an_empty_prediction_batch(model):
    with pytest.raises(ForecastingContractError, match="Ridge model"):
        replace(model, scales=(0.0, 4.0)).predict([])


@pytest.mark.parametrize("kind", ["standardization", "prediction"])
def test_finite_inputs_that_overflow_during_scoring_are_rejected(model, kind):
    if kind == "standardization":
        model = replace(model, means=(-1e308, 2.0), scales=(1.0, 4.0))
    else:
        model = replace(model, means=(0.0, 2.0), scales=(1.0, 4.0), coefficients=(0.0, 2.0, 0.0))
    with pytest.raises(ForecastingContractError, match="row 0.*finite"):
        model.predict([[1e308, 2.0]])


def test_valid_sequences_generators_and_numeric_strings_keep_exact_predictions(model):
    rows = [[3.0, 6.0], [5.0, 10.0]]
    assert model.predict(rows) == [11.0, 12.0]
    assert rows == [[3.0, 6.0], [5.0, 10.0]]
    assert model.predict(iter([iter(["3.0", "6.0"])])) == [11.0]
    assert model.predict([]) == []


def test_valid_intercept_only_model_keeps_empty_width_behavior():
    intercept = RidgeModel((), (), (), (7.0,), 1.0)
    assert intercept.predict([[], ()]) == [7.0, 7.0]


def test_real_fit_keeps_hand_computable_regularized_predictions():
    frame = pd.DataFrame({"x": [0.0, 1.0, 2.0], "demand_mw": [2.0, 4.0, 6.0]})
    original = frame.copy(deep=True)
    fitted = fit_ridge_model(frame, feature_columns=("x",), alpha=1.0)
    # x has mean 1 and sample scale 1: intercept=4, slope=4/(2+1).
    assert fitted.predict([[0.0], [1.0], [2.0]]) == pytest.approx([8 / 3, 4.0, 16 / 3])
    pd.testing.assert_frame_equal(frame, original)


def test_real_cli_does_not_publish_invalid_fitted_state(tmp_path, monkeypatch, capsys):
    original_fit = evaluation.fit_ridge_model
    def invalid_fit(*args, **kwargs):
        fitted = original_fit(*args, **kwargs)
        return replace(fitted, coefficients=(float("nan"), *fitted.coefficients[1:]))
    monkeypatch.setattr(evaluation, "fit_ridge_model", invalid_fit)
    output = tmp_path / "must-not-exist"
    with pytest.raises(ForecastingContractError, match="Ridge model"):
        main(["--demo", "--horizon-minutes", "30", "--output-dir", str(output)])
    assert not output.exists()
    assert "Wrote " not in capsys.readouterr().out

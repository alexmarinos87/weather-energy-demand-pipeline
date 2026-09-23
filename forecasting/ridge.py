from __future__ import annotations

from collections.abc import Mapping, Set
from dataclasses import dataclass
from math import isfinite, sqrt
from numbers import Real
from typing import Iterable, Sequence

import pandas as pd

from forecasting.contracts import ForecastingContractError, TARGET_COLUMN


@dataclass(frozen=True)
class RidgeModel:
    feature_columns: tuple[str, ...]
    means: tuple[float, ...]
    scales: tuple[float, ...]
    coefficients: tuple[float, ...]
    alpha: float

    def predict(self, rows: Iterable[Sequence[float]]) -> list[float]:
        predictions: list[float] = []
        for row in rows:
            standardized = [
                (float(value) - mean) / scale
                for value, mean, scale in zip(row, self.means, self.scales)
            ]
            prediction = self.coefficients[0] + sum(
                coefficient * value
                for coefficient, value in zip(
                    self.coefficients[1:], standardized
                )
            )
            predictions.append(float(prediction))
        return predictions


def _mean(values: Sequence[float]) -> float:
    if not values:
        raise ForecastingContractError("Cannot calculate a mean for no values.")
    return float(sum(values) / len(values))


def _sample_scale(values: Sequence[float], mean: float) -> float:
    if len(values) < 2:
        return 1.0
    variance = sum((value - mean) ** 2 for value in values) / (len(values) - 1)
    scale = sqrt(variance)
    return scale if scale > 1e-12 else 1.0


def _solve(matrix: list[list[float]], vector: list[float]) -> list[float]:
    size = len(vector)
    augmented = [row[:] + [rhs] for row, rhs in zip(matrix, vector)]
    for pivot_index in range(size):
        pivot_row = max(
            range(pivot_index, size),
            key=lambda row_index: abs(augmented[row_index][pivot_index]),
        )
        if abs(augmented[pivot_row][pivot_index]) < 1e-12:
            raise ForecastingContractError("Ridge system could not be solved.")
        augmented[pivot_index], augmented[pivot_row] = (
            augmented[pivot_row],
            augmented[pivot_index],
        )
        pivot = augmented[pivot_index][pivot_index]
        augmented[pivot_index] = [value / pivot for value in augmented[pivot_index]]
        for row_index in range(size):
            if row_index == pivot_index:
                continue
            factor = augmented[row_index][pivot_index]
            if abs(factor) < 1e-18:
                continue
            augmented[row_index] = [
                current - factor * pivot_value
                for current, pivot_value in zip(
                    augmented[row_index], augmented[pivot_index]
                )
            ]
    return [row[-1] for row in augmented]


def _finite_fit_value(value: object, context: str) -> float:
    if (
        not pd.api.types.is_scalar(value)
        or pd.api.types.is_bool(value)
        or isinstance(value, complex)
    ):
        raise ForecastingContractError(f"Ridge fitting {context} must be a finite real scalar.")
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ForecastingContractError(
            f"Ridge fitting {context} must be a finite float-representable number."
        ) from exc
    if not isfinite(number):
        raise ForecastingContractError(f"Ridge fitting {context} must be finite.")
    return number


def fit_ridge_model(
    frame: pd.DataFrame,
    *,
    feature_columns: Sequence[str],
    target_column: str = TARGET_COLUMN,
    alpha: float = 1.0,
) -> RidgeModel:
    if frame.empty:
        raise ForecastingContractError("Cannot fit a ridge model with no rows.")
    if isinstance(alpha, bool) or not isinstance(alpha, Real):
        raise ForecastingContractError("Ridge fitting alpha must be a positive finite real number.")
    alpha = _finite_fit_value(alpha, "alpha")
    if alpha <= 0:
        raise ForecastingContractError("Ridge fitting alpha must be positive.")
    if isinstance(feature_columns, (str, bytes, bytearray, Mapping, Set)):
        raise ForecastingContractError("Ridge fitting feature columns must be an ordered sequence of names.")
    try:
        feature_columns = tuple(feature_columns)
    except TypeError as exc:
        raise ForecastingContractError("Ridge fitting feature columns must be an ordered sequence of names.") from exc
    if not feature_columns or any(
        not isinstance(column, str) or not column.strip() for column in feature_columns
    ):
        raise ForecastingContractError("Ridge fitting feature columns must contain nonempty names.")
    if len(set(feature_columns)) != len(feature_columns):
        raise ForecastingContractError("Ridge fitting feature columns must not contain duplicates.")
    if frame.columns.duplicated().any():
        raise ForecastingContractError("Ridge fitting frame has duplicate column names.")
    missing = sorted(set(feature_columns) - set(frame.columns))
    if missing:
        raise ForecastingContractError(f"Ridge fitting feature columns are missing: {', '.join(missing)}.")
    if not isinstance(target_column, str) or not target_column.strip() or target_column not in frame:
        raise ForecastingContractError("Ridge fitting target column must name an existing column.")

    # Validate all selected source values before any training arithmetic or solver call.
    numeric = {
        column: [
            _finite_fit_value(value, f"column {column} row {position}")
            for position, value in enumerate(frame[column])
        ]
        for column in dict.fromkeys([*feature_columns, target_column])
    }
    x_columns = [numeric[column] for column in feature_columns]
    target = numeric[target_column]
    stage = "means"
    try:
        means = tuple(
            _finite_fit_value(_mean(values), f"mean {column}")
            for column, values in zip(feature_columns, x_columns)
        )
        stage = "scales"
        scales = tuple(
            _finite_fit_value(_sample_scale(values, mean), f"scale {column}")
            for column, values, mean in zip(feature_columns, x_columns, means)
        )
        stage = "standardized design"
        design = [
            [1.0, *[
                _finite_fit_value((value - mean) / scale, f"design row {position} feature {column}")
                for column, value, mean, scale in zip(feature_columns, row, means, scales)
            ]]
            for position, row in enumerate(zip(*x_columns))
        ]
        width = len(feature_columns) + 1
        gram = [[0.0 for _ in range(width)] for _ in range(width)]
        rhs = [0.0 for _ in range(width)]
        stage = "normal equations"
        for row, actual in zip(design, target):
            for left in range(width):
                rhs[left] += row[left] * actual
                for right in range(width):
                    gram[left][right] += row[left] * row[right]
        for index in range(1, width):
            gram[index][index] += float(alpha)
        for index, value in enumerate(rhs):
            _finite_fit_value(value, f"right-hand side {index}")
        for left, row in enumerate(gram):
            for right, value in enumerate(row):
                _finite_fit_value(value, f"Gram matrix [{left},{right}]")
        stage = "coefficients"
        coefficients = tuple(
            _finite_fit_value(value, f"coefficients[{index}]")
            for index, value in enumerate(_solve(gram, rhs))
        )
        if len(coefficients) != width:
            raise ForecastingContractError("Ridge fitting coefficients have an invalid dimension.")
    except (OverflowError, ZeroDivisionError, FloatingPointError) as exc:
        raise ForecastingContractError(
            f"Ridge fitting {stage} exceeded finite arithmetic limits."
        ) from exc
    return RidgeModel(
        feature_columns=feature_columns,
        means=means,
        scales=scales,
        coefficients=coefficients,
        alpha=float(alpha),
    )

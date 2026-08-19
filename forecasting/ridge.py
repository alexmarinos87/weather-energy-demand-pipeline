from __future__ import annotations

from dataclasses import dataclass
from math import sqrt
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


def fit_ridge_model(
    frame: pd.DataFrame,
    *,
    feature_columns: Sequence[str],
    target_column: str = TARGET_COLUMN,
    alpha: float = 1.0,
) -> RidgeModel:
    if frame.empty:
        raise ForecastingContractError("Cannot fit a ridge model with no rows.")
    feature_columns = tuple(feature_columns)
    x_columns = [frame[column].astype(float).tolist() for column in feature_columns]
    means = tuple(_mean(values) for values in x_columns)
    scales = tuple(
        _sample_scale(values, mean) for values, mean in zip(x_columns, means)
    )
    design = [
        [
            1.0,
            *[
                (float(value) - mean) / scale
                for value, mean, scale in zip(row, means, scales)
            ],
        ]
        for row in frame.loc[:, feature_columns].itertuples(index=False, name=None)
    ]
    target = frame[target_column].astype(float).tolist()
    width = len(feature_columns) + 1
    gram = [[0.0 for _ in range(width)] for _ in range(width)]
    rhs = [0.0 for _ in range(width)]
    for row, actual in zip(design, target):
        for left in range(width):
            rhs[left] += row[left] * actual
            for right in range(width):
                gram[left][right] += row[left] * row[right]
    for index in range(1, width):
        gram[index][index] += float(alpha)
    return RidgeModel(
        feature_columns=feature_columns,
        means=means,
        scales=scales,
        coefficients=tuple(_solve(gram, rhs)),
        alpha=float(alpha),
    )

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from forecasting.prediction_intervals import (
    PredictionIntervalConfig,
    calibrate_prediction_intervals,
)


def _read_file(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix in {".parquet", ".pq"}:
        return pd.read_parquet(path)
    raise ValueError(f"Unsupported point-prediction file type: {path}.")


def read_frame(path: Path) -> pd.DataFrame:
    if path.is_file():
        return _read_file(path)
    if not path.is_dir():
        raise FileNotFoundError(f"Point-prediction path does not exist: {path}.")
    files = sorted(
        [*path.rglob("*.parquet"), *path.rglob("*.pq"), *path.rglob("*.csv")]
    )
    if not files:
        raise FileNotFoundError(
            f"Point-prediction directory contains no CSV or Parquet files: {path}."
        )
    return pd.concat([_read_file(file_path) for file_path in files], ignore_index=True)


def _select_run(frame: pd.DataFrame, selected: str | None) -> pd.DataFrame:
    if "run_id" not in frame.columns:
        raise ValueError("Point predictions are missing run_id.")
    run_ids = frame["run_id"].fillna("").astype(str).str.strip()
    if selected is not None:
        result = frame.loc[run_ids == selected].copy()
        if result.empty:
            raise ValueError(f"No point-prediction rows found for run_id={selected!r}.")
        return result
    unique = sorted(set(run_ids) - {""})
    if len(unique) != 1:
        raise ValueError(
            f"Point-prediction input contains {len(unique)} run IDs; select one with --run-id."
        )
    return frame.loc[run_ids == unique[0]].copy()


def _write_frame(frame: pd.DataFrame, path: Path, output_format: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    output = path.with_suffix(".csv" if output_format == "csv" else ".parquet")
    if output.exists():
        raise FileExistsError(f"Refusing to overwrite {output}.")
    temporary = output.with_suffix(f".tmp{output.suffix}")
    if temporary.exists():
        raise FileExistsError(f"Temporary output already exists: {temporary}.")
    if output_format == "csv":
        frame.to_csv(temporary, index=False)
    else:
        frame.to_parquet(temporary, index=False)
    temporary.replace(output)
    return output


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Calibrate symmetric test prediction intervals from validation "
            "absolute residuals whose labels were available before test feature time."
        )
    )
    parser.add_argument("--predictions-input", type=Path, required=True)
    parser.add_argument("--run-id")
    parser.add_argument(
        "--coverage-levels",
        type=float,
        nargs="+",
        default=[0.80, 0.90, 0.95],
    )
    parser.add_argument("--min-calibration-rows", type=int, default=24)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/forecasting/prediction_intervals"),
    )
    parser.add_argument(
        "--output-format", choices=("csv", "parquet"), default="parquet"
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    intervals, metrics = calibrate_prediction_intervals(
        _select_run(read_frame(args.predictions_input), args.run_id),
        config=PredictionIntervalConfig(
            coverage_levels=tuple(args.coverage_levels),
            min_calibration_rows=args.min_calibration_rows,
        ),
    )
    interval_run_id = str(intervals.iloc[0]["interval_run_id"])
    intervals_path = _write_frame(
        intervals,
        args.output_dir / f"prediction_intervals_{interval_run_id}",
        args.output_format,
    )
    metrics_path = _write_frame(
        metrics,
        args.output_dir / f"prediction_interval_metrics_{interval_run_id}",
        args.output_format,
    )
    print(f"Wrote prediction intervals: {intervals_path}")
    print(f"Wrote interval metrics: {metrics_path}")
    print(
        metrics[
            [
                "source_area",
                "requested_horizon_minutes",
                "model_name",
                "target_coverage_level",
                "calibration_observation_count",
                "evaluation_observation_count",
                "empirical_coverage_pct",
                "average_interval_width_mw",
            ]
        ].to_string(index=False)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

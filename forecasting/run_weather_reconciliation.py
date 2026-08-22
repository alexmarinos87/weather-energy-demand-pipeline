from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from forecasting.weather_reconciliation import (
    ForecastWeatherReconciliationConfig,
    reconcile_forecast_weather,
)


def _read_file(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix in {".parquet", ".pq"}:
        return pd.read_parquet(path)
    raise ValueError(f"Unsupported input file type: {path}.")


def read_frame(path: Path) -> pd.DataFrame:
    if path.is_file():
        return _read_file(path)
    if not path.is_dir():
        raise FileNotFoundError(f"Input path does not exist: {path}.")
    files = sorted(
        [
            *path.rglob("*.parquet"),
            *path.rglob("*.pq"),
            *path.rglob("*.csv"),
        ]
    )
    if not files:
        raise FileNotFoundError(
            f"Input directory contains no CSV or Parquet files: {path}."
        )
    return pd.concat([_read_file(file_path) for file_path in files], ignore_index=True)


def _write_frame(frame: pd.DataFrame, path: Path, output_format: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    output = path.with_suffix(".csv" if output_format == "csv" else ".parquet")
    if output.exists():
        raise FileExistsError(f"Refusing to overwrite {output}.")
    temp = output.with_suffix(f".tmp{output.suffix}")
    if temp.exists():
        raise FileExistsError(f"Temporary output already exists: {temp}.")
    if output_format == "csv":
        frame.to_csv(temp, index=False)
    else:
        frame.to_parquet(temp, index=False)
    temp.replace(output)
    return output


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Reconcile normalized forecast weather with mature same-area silver "
            "weather observations and emit row-level plus aggregate evidence."
        )
    )
    parser.add_argument(
        "--forecast-input",
        type=Path,
        required=True,
        help="Normalized forecast-weather CSV/Parquet file or directory.",
    )
    parser.add_argument(
        "--observed-input",
        type=Path,
        required=True,
        help="Silver observed-weather CSV/Parquet file or directory.",
    )
    parser.add_argument(
        "--observation-tolerance-minutes",
        type=int,
        default=90,
        help="Maximum absolute difference between forecast valid and observed event time.",
    )
    parser.add_argument(
        "--min-coverage",
        type=float,
        default=0.80,
        help="Minimum matched/eligible coverage for every provider/model/lead bucket.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/reconciliation/forecast_weather"),
    )
    parser.add_argument(
        "--output-format", choices=("csv", "parquet"), default="parquet"
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    rows, metrics = reconcile_forecast_weather(
        read_frame(args.forecast_input),
        read_frame(args.observed_input),
        config=ForecastWeatherReconciliationConfig(
            observation_tolerance_minutes=args.observation_tolerance_minutes,
            min_coverage=args.min_coverage,
        ),
    )
    run_id = str(rows.iloc[0]["reconciliation_run_id"])
    rows_path = _write_frame(
        rows,
        args.output_dir / f"forecast_weather_reconciliation_{run_id}",
        args.output_format,
    )
    metrics_path = _write_frame(
        metrics,
        args.output_dir / f"forecast_weather_quality_metrics_{run_id}",
        args.output_format,
    )
    print(f"Wrote reconciliation rows: {rows_path}")
    print(f"Wrote reconciliation metrics: {metrics_path}")
    print(
        metrics[
            [
                "source_area",
                "city",
                "forecast_provider",
                "forecast_model",
                "forecast_lead_time_bucket",
                "matched_forecast_count",
                "eligible_forecast_count",
                "forecast_observation_coverage_pct",
                "temperature_mae_c",
                "humidity_mae_pct",
            ]
        ].to_string(index=False)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

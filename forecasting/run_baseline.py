from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from forecasting.baseline import (
    BacktestConfig,
    ForecastWeatherConfig,
    build_demo_feature_frame,
    build_demo_forecast_weather_frame,
    run_chronological_backtest,
    run_rolling_origin_backtest,
    run_weather_model_comparison,
)


def _read_frame(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix in {".parquet", ".pq"}:
        return pd.read_parquet(path)
    raise ValueError("Input must be CSV or Parquet.")


def _write_frame(frame: pd.DataFrame, path: Path, output_format: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    if output_format == "csv":
        output = path.with_suffix(".csv")
        frame.to_csv(output, index=False)
        return output
    output = path.with_suffix(".parquet")
    frame.to_parquet(output, index=False)
    return output


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run leakage-safe 30/60-minute demand baselines or a paired "
            "observed-versus-target-weather model comparison."
        )
    )
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--input", type=Path, help="Gold feature CSV or Parquet.")
    source.add_argument(
        "--demo",
        action="store_true",
        help="Use deterministic credential-free five-minute feature data.",
    )
    parser.add_argument("--output-dir", type=Path, default=Path("data/forecasting"))
    parser.add_argument(
        "--output-format", choices=("csv", "parquet"), default="csv"
    )
    parser.add_argument(
        "--model-set",
        choices=("baseline", "weather-comparison"),
        default="baseline",
        help=(
            "Keep the persistence/observed-weather baselines by default, or "
            "compare the observed-weather ridge with a target-valid-weather ridge."
        ),
    )
    parser.add_argument(
        "--forecast-weather-input",
        type=Path,
        help=(
            "Normalized forecast-weather CSV or Parquet. Required for a "
            "non-demo weather comparison; demo mode can generate it."
        ),
    )
    parser.add_argument(
        "--evaluation-mode",
        choices=("holdout", "rolling-origin"),
        default="holdout",
        help=(
            "Use a fixed validation/test holdout or repeated expanding-window "
            "historical cutoffs."
        ),
    )
    parser.add_argument("--ridge-alpha", type=float, default=1.0)
    parser.add_argument(
        "--horizon-minutes",
        type=int,
        nargs="+",
        choices=(30, 60),
        default=[30, 60],
        help="Approved elapsed-time target horizons. Defaults to both 30 and 60.",
    )
    parser.add_argument(
        "--target-tolerance-minutes",
        type=int,
        default=5,
        help="Maximum allowed delay for the first demand observation after target time.",
    )
    parser.add_argument(
        "--min-target-coverage",
        type=float,
        default=0.90,
        help="Minimum matched/eligible demand-target coverage per group and horizon.",
    )
    parser.add_argument(
        "--rolling-origin-folds",
        type=int,
        default=3,
        help=(
            "Total origins when --evaluation-mode rolling-origin is selected. "
            "The final origin evaluates the untouched test window."
        ),
    )
    parser.add_argument(
        "--forecast-valid-time-tolerance-minutes",
        type=int,
        default=15,
        help="Maximum absolute difference between weather valid time and demand target.",
    )
    parser.add_argument(
        "--forecast-max-availability-age-minutes",
        type=int,
        default=180,
        help="Maximum age of forecast evidence at demand feature time.",
    )
    parser.add_argument(
        "--min-forecast-weather-coverage",
        type=float,
        default=0.90,
        help="Minimum target-valid weather coverage per paired comparison group.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    frame = build_demo_feature_frame() if args.demo else _read_frame(args.input)
    backtest_config = BacktestConfig(
        ridge_alpha=args.ridge_alpha,
        horizon_minutes=tuple(args.horizon_minutes),
        target_tolerance_minutes=args.target_tolerance_minutes,
        min_target_coverage=args.min_target_coverage,
    )

    if args.model_set == "weather-comparison":
        if args.forecast_weather_input is not None:
            forecast_weather = _read_frame(args.forecast_weather_input)
        elif args.demo:
            forecast_weather = build_demo_forecast_weather_frame(
                frame,
                horizon_minutes=tuple(args.horizon_minutes),
            )
        else:
            parser.error(
                "--forecast-weather-input is required when "
                "--model-set weather-comparison is used with --input."
            )
        predictions, metrics = run_weather_model_comparison(
            frame,
            forecast_weather,
            backtest_config=backtest_config,
            forecast_config=ForecastWeatherConfig(
                valid_time_tolerance_minutes=(
                    args.forecast_valid_time_tolerance_minutes
                ),
                max_availability_age_minutes=(
                    args.forecast_max_availability_age_minutes
                ),
                min_coverage=args.min_forecast_weather_coverage,
            ),
            evaluation_mode=args.evaluation_mode,
            origin_count=args.rolling_origin_folds,
        )
        output_prefix = (
            "rolling_origin_weather_comparison"
            if args.evaluation_mode == "rolling-origin"
            else "weather_comparison"
        )
    else:
        if args.forecast_weather_input is not None:
            parser.error(
                "--forecast-weather-input requires "
                "--model-set weather-comparison."
            )
        if args.evaluation_mode == "rolling-origin":
            predictions, metrics = run_rolling_origin_backtest(
                frame,
                config=backtest_config,
                origin_count=args.rolling_origin_folds,
            )
            output_prefix = "rolling_origin"
        else:
            predictions, metrics = run_chronological_backtest(
                frame,
                config=backtest_config,
            )
            output_prefix = "baseline"

    predictions_path = _write_frame(
        predictions,
        args.output_dir / f"{output_prefix}_predictions",
        args.output_format,
    )
    metrics_path = _write_frame(
        metrics,
        args.output_dir / f"{output_prefix}_metrics",
        args.output_format,
    )
    print(f"Wrote predictions: {predictions_path}")
    print(f"Wrote metrics: {metrics_path}")

    summary_columns = ["source_area", "requested_horizon_minutes"]
    for column in (
        "origin_fold",
        "origin_count",
        "training_observation_count",
        "split",
        "model_name",
        "weather_feature_mode",
        "observation_count",
        "target_coverage_pct",
        "forecast_weather_coverage_pct",
        "mae_mw",
        "rmse_mw",
    ):
        if column in metrics.columns:
            summary_columns.append(column)
    print(metrics[summary_columns].to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

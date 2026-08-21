from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from forecasting.baseline import (
    BacktestConfig,
    build_demo_feature_frame,
    run_rolling_origin_backtest,
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
            "Run purged rolling-origin 30/60-minute persistence and ridge "
            "demand baselines."
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
        help="Maximum allowed delay for the first observation at or after target time.",
    )
    parser.add_argument(
        "--min-target-coverage",
        type=float,
        default=0.90,
        help="Minimum matched/eligible target coverage per group and horizon.",
    )
    parser.add_argument(
        "--rolling-origin-folds",
        type=int,
        default=3,
        help=(
            "Total expanding-window origins. All but the final origin evaluate "
            "validation history; the final origin evaluates the untouched test window."
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    frame = build_demo_feature_frame() if args.demo else _read_frame(args.input)
    predictions, metrics = run_rolling_origin_backtest(
        frame,
        config=BacktestConfig(
            ridge_alpha=args.ridge_alpha,
            horizon_minutes=tuple(args.horizon_minutes),
            target_tolerance_minutes=args.target_tolerance_minutes,
            min_target_coverage=args.min_target_coverage,
        ),
        origin_count=args.rolling_origin_folds,
    )
    predictions_path = _write_frame(
        predictions, args.output_dir / "baseline_predictions", args.output_format
    )
    metrics_path = _write_frame(
        metrics, args.output_dir / "baseline_metrics", args.output_format
    )
    print(f"Wrote predictions: {predictions_path}")
    print(f"Wrote metrics: {metrics_path}")
    print(
        metrics[
            [
                "source_area",
                "requested_horizon_minutes",
                "origin_fold",
                "origin_count",
                "split",
                "model_name",
                "training_observation_count",
                "observation_count",
                "target_coverage_pct",
                "mae_mw",
                "rmse_mw",
            ]
        ].to_string(index=False)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

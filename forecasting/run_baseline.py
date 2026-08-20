from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from forecasting.baseline import (
    BacktestConfig,
    build_demo_feature_frame,
    run_chronological_backtest,
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
        description="Run purged future-horizon persistence and ridge demand baselines."
    )
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--input", type=Path, help="Gold feature CSV or Parquet.")
    source.add_argument(
        "--demo",
        action="store_true",
        help="Use deterministic credential-free feature data.",
    )
    parser.add_argument("--output-dir", type=Path, default=Path("data/forecasting"))
    parser.add_argument(
        "--output-format", choices=("csv", "parquet"), default="csv"
    )
    parser.add_argument("--ridge-alpha", type=float, default=1.0)
    parser.add_argument(
        "--horizon-steps",
        type=int,
        default=1,
        help="Number of ordered observations between feature time and target time.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    frame = build_demo_feature_frame() if args.demo else _read_frame(args.input)
    predictions, metrics = run_chronological_backtest(
        frame,
        config=BacktestConfig(
            ridge_alpha=args.ridge_alpha,
            horizon_steps=args.horizon_steps,
        ),
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
                "horizon_steps",
                "split",
                "model_name",
                "observation_count",
                "mae_mw",
                "rmse_mw",
            ]
        ].to_string(index=False)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

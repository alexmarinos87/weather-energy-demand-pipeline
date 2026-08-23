from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from forecasting.contracts import (
    DEFAULT_FEATURE_COLUMNS,
    UK_LOCAL_FEATURE_COLUMNS,
    UK_LOCAL_FEATURE_CONTRACT_VERSION,
    UTC_FEATURE_CONTRACT_VERSION,
    BacktestConfig,
)
from forecasting.seasonal_baselines import (
    SeasonalBaselineConfig,
    build_seasonal_demo_feature_frame,
    run_seasonal_backtest,
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
            "Compare current-value, elapsed previous-day/week, and ridge demand "
            "baselines on one paired holdout or rolling-origin cohort."
        )
    )
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--input", type=Path, help="Gold feature CSV or Parquet.")
    source.add_argument(
        "--demo",
        action="store_true",
        help="Use deterministic credential-free daily/weekly feature history.",
    )
    parser.add_argument(
        "--demo-days",
        type=int,
        default=12,
        help="Deterministic demo history length; must be at least 9 days.",
    )
    parser.add_argument("--output-dir", type=Path, default=Path("data/forecasting"))
    parser.add_argument(
        "--output-format", choices=("csv", "parquet"), default="csv"
    )
    parser.add_argument(
        "--calendar-mode", choices=("utc", "uk-local"), default="utc"
    )
    parser.add_argument(
        "--evaluation-mode",
        choices=("holdout", "rolling-origin"),
        default="holdout",
    )
    parser.add_argument("--rolling-origin-folds", type=int, default=3)
    parser.add_argument("--ridge-alpha", type=float, default=1.0)
    parser.add_argument(
        "--horizon-minutes",
        type=int,
        nargs="+",
        choices=(30, 60),
        default=[30, 60],
    )
    parser.add_argument("--target-tolerance-minutes", type=int, default=5)
    parser.add_argument("--min-target-coverage", type=float, default=0.90)
    parser.add_argument(
        "--seasonal-reference-tolerance-minutes",
        type=int,
        default=15,
        help="Maximum absolute elapsed-time offset for day/week source matches.",
    )
    parser.add_argument(
        "--min-seasonal-reference-coverage",
        type=float,
        default=0.90,
        help="Minimum matched/eligible coverage for each group, horizon, and period.",
    )
    return parser


def _calendar_contract(calendar_mode: str) -> tuple[tuple[str, ...], str, str]:
    if calendar_mode == "uk-local":
        return (
            tuple(UK_LOCAL_FEATURE_COLUMNS),
            UK_LOCAL_FEATURE_CONTRACT_VERSION,
            "_uk_local_calendar",
        )
    return tuple(DEFAULT_FEATURE_COLUMNS), UTC_FEATURE_CONTRACT_VERSION, ""


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    frame = (
        build_seasonal_demo_feature_frame(periods=args.demo_days * 288)
        if args.demo
        else _read_frame(args.input)
    )
    feature_columns, feature_contract_version, calendar_suffix = _calendar_contract(
        args.calendar_mode
    )
    predictions, metrics = run_seasonal_backtest(
        frame,
        backtest_config=BacktestConfig(
            ridge_alpha=args.ridge_alpha,
            horizon_minutes=tuple(args.horizon_minutes),
            target_tolerance_minutes=args.target_tolerance_minutes,
            min_target_coverage=args.min_target_coverage,
            feature_columns=feature_columns,
            feature_contract_version=feature_contract_version,
        ),
        seasonal_config=SeasonalBaselineConfig(
            reference_tolerance_minutes=(
                args.seasonal_reference_tolerance_minutes
            ),
            min_reference_coverage=args.min_seasonal_reference_coverage,
        ),
        evaluation_mode=args.evaluation_mode,
        origin_count=args.rolling_origin_folds,
    )
    prefix = (
        "rolling_origin_seasonal_comparison"
        if args.evaluation_mode == "rolling-origin"
        else "seasonal_comparison"
    ) + calendar_suffix
    predictions_path = _write_frame(
        predictions,
        args.output_dir / f"{prefix}_predictions",
        args.output_format,
    )
    metrics_path = _write_frame(
        metrics,
        args.output_dir / f"{prefix}_metrics",
        args.output_format,
    )
    print(f"Wrote predictions: {predictions_path}")
    print(f"Wrote metrics: {metrics_path}")
    print(
        metrics[
            [
                "source_area",
                "requested_horizon_minutes",
                "split",
                "origin_fold",
                "model_name",
                "observation_count",
                "previous_day_coverage_pct",
                "previous_week_coverage_pct",
                "mae_mw",
                "rmse_mw",
                "feature_contract_version",
            ]
        ].to_string(index=False)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

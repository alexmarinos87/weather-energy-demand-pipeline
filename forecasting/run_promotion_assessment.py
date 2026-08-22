from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from forecasting.promotion_assessment import (
    BLOCKED_STATUS,
    TargetWeatherPromotionPolicy,
    assess_target_weather_promotion,
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
        [*path.rglob("*.parquet"), *path.rglob("*.pq"), *path.rglob("*.csv")]
    )
    if not files:
        raise FileNotFoundError(
            f"Input directory contains no CSV or Parquet files: {path}."
        )
    return pd.concat([_read_file(file_path) for file_path in files], ignore_index=True)


def _select_run(frame: pd.DataFrame, column: str, selected: str | None) -> pd.DataFrame:
    if column not in frame.columns:
        raise ValueError(f"Input is missing run identity column {column}.")
    values = frame[column].dropna().astype(str).str.strip()
    if selected is not None:
        result = frame.loc[values == selected].copy()
        if result.empty:
            raise ValueError(f"No rows found for {column}={selected!r}.")
        return result
    unique = sorted(set(values))
    if len(unique) != 1:
        raise ValueError(
            f"Input contains {len(unique)} {column} values; select one explicitly."
        )
    return frame.loc[values == unique[0]].copy()


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
            "Assess whether target-weather evidence is eligible for human promotion "
            "review. This command never promotes or deploys a model."
        )
    )
    parser.add_argument("--comparison-predictions", type=Path, required=True)
    parser.add_argument("--reconciliation-metrics", type=Path, required=True)
    parser.add_argument("--comparison-run-id")
    parser.add_argument("--reconciliation-run-id")
    parser.add_argument("--min-model-observations", type=int, default=24)
    parser.add_argument("--min-reconciliation-observations", type=int, default=24)
    parser.add_argument("--min-model-forecast-coverage-pct", type=float, default=90.0)
    parser.add_argument("--min-reconciliation-coverage-pct", type=float, default=90.0)
    parser.add_argument("--max-temperature-mae-c", type=float, default=2.5)
    parser.add_argument("--max-humidity-mae-pct", type=float, default=15.0)
    parser.add_argument("--min-mae-improvement-pct", type=float, default=0.0)
    parser.add_argument("--min-rmse-improvement-pct", type=float, default=0.0)
    parser.add_argument("--max-absolute-bias-regression-mw", type=float, default=0.0)
    parser.add_argument(
        "--required-horizons", type=int, nargs="+", default=[30, 60]
    )
    parser.add_argument(
        "--required-lead-buckets",
        nargs="*",
        default=[],
        help="Optional explicit lead buckets; otherwise infer buckets used by candidate rows.",
    )
    parser.add_argument(
        "--allow-no-test-split",
        action="store_true",
        help="Do not require an untouched test split. Not recommended for promotion review.",
    )
    parser.add_argument(
        "--require-eligible",
        action="store_true",
        help="Return exit code 2 when the assessment is blocked, after writing evidence.",
    )
    parser.add_argument(
        "--output-dir", type=Path, default=Path("data/promotion/target_weather")
    )
    parser.add_argument(
        "--output-format", choices=("csv", "parquet"), default="parquet"
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    comparison = _select_run(
        read_frame(args.comparison_predictions),
        "run_id",
        args.comparison_run_id,
    )
    reconciliation = _select_run(
        read_frame(args.reconciliation_metrics),
        "reconciliation_run_id",
        args.reconciliation_run_id,
    )
    checks, summary = assess_target_weather_promotion(
        comparison,
        reconciliation,
        policy=TargetWeatherPromotionPolicy(
            min_model_observations=args.min_model_observations,
            min_reconciliation_observations=args.min_reconciliation_observations,
            min_model_forecast_coverage_pct=args.min_model_forecast_coverage_pct,
            min_reconciliation_coverage_pct=args.min_reconciliation_coverage_pct,
            max_temperature_mae_c=args.max_temperature_mae_c,
            max_humidity_mae_pct=args.max_humidity_mae_pct,
            min_mae_improvement_pct=args.min_mae_improvement_pct,
            min_rmse_improvement_pct=args.min_rmse_improvement_pct,
            max_absolute_bias_regression_mw=args.max_absolute_bias_regression_mw,
            required_horizons=tuple(args.required_horizons),
            required_lead_buckets=tuple(args.required_lead_buckets),
            require_test_split=not args.allow_no_test_split,
        ),
    )
    assessment_id = str(summary.loc[0, "assessment_id"])
    checks_path = _write_frame(
        checks,
        args.output_dir / f"target_weather_promotion_checks_{assessment_id}",
        args.output_format,
    )
    summary_path = _write_frame(
        summary,
        args.output_dir / f"target_weather_promotion_summary_{assessment_id}",
        args.output_format,
    )
    print(f"Wrote promotion checks: {checks_path}")
    print(f"Wrote promotion summary: {summary_path}")
    print(summary.to_string(index=False))
    blocked = summary.loc[0, "assessment_status"] == BLOCKED_STATUS
    return 2 if blocked and args.require_eligible else 0


if __name__ == "__main__":
    raise SystemExit(main())

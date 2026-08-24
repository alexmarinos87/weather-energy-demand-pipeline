from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from forecasting.interval_monitoring import (
    FAILED_STATUS,
    WARNING_STATUS,
    PredictionIntervalMonitoringConfig,
    monitor_prediction_interval_health,
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
    return pd.concat(
        [_read_file(file_path) for file_path in files], ignore_index=True
    )


def _write_frame(frame: pd.DataFrame, path: Path, output_format: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    output = path.with_suffix(".csv" if output_format == "csv" else ".parquet")
    if output.exists():
        raise FileExistsError(f"Refusing to overwrite {output}.")
    temporary = output.with_suffix(f".tmp{output.suffix}")
    if temporary.exists():
        raise FileExistsError(f"Temporary output already exists: {temporary}.")
    try:
        if output_format == "csv":
            frame.to_csv(temporary, index=False)
        else:
            frame.to_parquet(temporary, index=False)
        temporary.replace(output)
    finally:
        temporary.unlink(missing_ok=True)
    return output


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Monitor retained prediction-interval freshness, calibration history, "
            "empirical coverage, width, and longitudinal drift."
        )
    )
    parser.add_argument("--interval-metrics", type=Path, required=True)
    parser.add_argument(
        "--as-of-utc",
        help="Timezone-aware monitoring boundary. Defaults to current UTC time.",
    )
    parser.add_argument("--recent-interval-run-count", type=int, default=3)
    parser.add_argument("--reference-interval-run-count", type=int, default=6)
    parser.add_argument("--min-recent-interval-runs", type=int, default=2)
    parser.add_argument("--min-reference-interval-runs", type=int, default=3)
    parser.add_argument("--max-interval-run-age-minutes", type=int, default=10080)
    parser.add_argument("--max-evaluation-age-minutes", type=int, default=20160)
    parser.add_argument(
        "--min-calibration-observation-count", type=int, default=24
    )
    parser.add_argument(
        "--max-recent-coverage-shortfall-pct-points",
        type=float,
        default=5.0,
    )
    parser.add_argument(
        "--max-coverage-drop-pct-points", type=float, default=5.0
    )
    parser.add_argument(
        "--max-average-interval-width-increase-pct",
        type=float,
        default=25.0,
    )
    parser.add_argument(
        "--max-calibration-history-drop-pct", type=float, default=25.0
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/monitoring/prediction_intervals"),
    )
    parser.add_argument(
        "--output-format", choices=("csv", "parquet"), default="parquet"
    )
    parser.add_argument(
        "--fail-on-error",
        action="store_true",
        help="Return exit code 2 for failed status after writing evidence.",
    )
    parser.add_argument(
        "--fail-on-warning",
        action="store_true",
        help="Return exit code 3 for warning status after writing evidence.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    checks, summary = monitor_prediction_interval_health(
        read_frame(args.interval_metrics),
        config=PredictionIntervalMonitoringConfig(
            recent_interval_run_count=args.recent_interval_run_count,
            reference_interval_run_count=args.reference_interval_run_count,
            min_recent_interval_runs=args.min_recent_interval_runs,
            min_reference_interval_runs=args.min_reference_interval_runs,
            max_interval_run_age_minutes=args.max_interval_run_age_minutes,
            max_evaluation_age_minutes=args.max_evaluation_age_minutes,
            min_calibration_observation_count=(
                args.min_calibration_observation_count
            ),
            max_recent_coverage_shortfall_pct_points=(
                args.max_recent_coverage_shortfall_pct_points
            ),
            max_coverage_drop_pct_points=args.max_coverage_drop_pct_points,
            max_average_interval_width_increase_pct=(
                args.max_average_interval_width_increase_pct
            ),
            max_calibration_history_drop_pct=(
                args.max_calibration_history_drop_pct
            ),
        ),
        as_of_utc=args.as_of_utc,
    )
    monitor_run_id = str(summary.loc[0, "monitor_run_id"])
    checks_path = _write_frame(
        checks,
        args.output_dir / f"prediction_interval_health_checks_{monitor_run_id}",
        args.output_format,
    )
    summary_path = _write_frame(
        summary,
        args.output_dir / f"prediction_interval_health_summary_{monitor_run_id}",
        args.output_format,
    )
    print(f"Wrote prediction-interval health checks: {checks_path}")
    print(f"Wrote prediction-interval health summary: {summary_path}")
    print(summary.to_string(index=False))
    status = str(summary.loc[0, "monitor_status"])
    if status == FAILED_STATUS and args.fail_on_error:
        return 2
    if status == WARNING_STATUS and args.fail_on_warning:
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from forecasting.provider_monitoring import (
    FAILED_STATUS,
    WARNING_STATUS,
    ForecastProviderMonitoringConfig,
    monitor_forecast_provider_health,
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
            "Monitor forecast-provider freshness, snapshot cadence/completeness, "
            "reconciliation quality, and longitudinal drift."
        )
    )
    parser.add_argument("--forecast-input", type=Path, required=True)
    parser.add_argument("--reconciliation-metrics", type=Path, required=True)
    parser.add_argument(
        "--as-of-utc",
        help="Timezone-aware monitoring boundary. Defaults to current UTC time.",
    )
    parser.add_argument("--max-forecast-ingestion-age-minutes", type=int, default=240)
    parser.add_argument("--max-snapshot-gap-minutes", type=int, default=360)
    parser.add_argument("--min-slots-per-latest-snapshot", type=int, default=8)
    parser.add_argument("--min-latest-snapshot-horizon-minutes", type=int, default=1440)
    parser.add_argument("--recent-reconciliation-run-count", type=int, default=3)
    parser.add_argument("--reference-reconciliation-run-count", type=int, default=6)
    parser.add_argument("--min-recent-reconciliation-runs", type=int, default=2)
    parser.add_argument("--min-reference-reconciliation-runs", type=int, default=3)
    parser.add_argument("--max-reconciliation-age-minutes", type=int, default=1440)
    parser.add_argument("--min-reconciliation-coverage-pct", type=float, default=90.0)
    parser.add_argument("--max-temperature-mae-c", type=float, default=2.5)
    parser.add_argument("--max-humidity-mae-pct", type=float, default=15.0)
    parser.add_argument("--max-coverage-drop-pct-points", type=float, default=5.0)
    parser.add_argument("--max-temperature-mae-increase-c", type=float, default=0.5)
    parser.add_argument("--max-humidity-mae-increase-pct", type=float, default=3.0)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/monitoring/forecast_provider"),
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
    checks, summary = monitor_forecast_provider_health(
        read_frame(args.forecast_input),
        read_frame(args.reconciliation_metrics),
        config=ForecastProviderMonitoringConfig(
            max_forecast_ingestion_age_minutes=args.max_forecast_ingestion_age_minutes,
            max_snapshot_gap_minutes=args.max_snapshot_gap_minutes,
            min_slots_per_latest_snapshot=args.min_slots_per_latest_snapshot,
            min_latest_snapshot_horizon_minutes=(
                args.min_latest_snapshot_horizon_minutes
            ),
            recent_reconciliation_run_count=args.recent_reconciliation_run_count,
            reference_reconciliation_run_count=(
                args.reference_reconciliation_run_count
            ),
            min_recent_reconciliation_runs=args.min_recent_reconciliation_runs,
            min_reference_reconciliation_runs=(
                args.min_reference_reconciliation_runs
            ),
            max_reconciliation_age_minutes=args.max_reconciliation_age_minutes,
            min_reconciliation_coverage_pct=args.min_reconciliation_coverage_pct,
            max_temperature_mae_c=args.max_temperature_mae_c,
            max_humidity_mae_pct=args.max_humidity_mae_pct,
            max_coverage_drop_pct_points=args.max_coverage_drop_pct_points,
            max_temperature_mae_increase_c=(
                args.max_temperature_mae_increase_c
            ),
            max_humidity_mae_increase_pct=args.max_humidity_mae_increase_pct,
        ),
        as_of_utc=args.as_of_utc,
    )
    monitor_run_id = str(summary.loc[0, "monitor_run_id"])
    checks_path = _write_frame(
        checks,
        args.output_dir / f"forecast_provider_health_checks_{monitor_run_id}",
        args.output_format,
    )
    summary_path = _write_frame(
        summary,
        args.output_dir / f"forecast_provider_health_summary_{monitor_run_id}",
        args.output_format,
    )
    print(f"Wrote provider health checks: {checks_path}")
    print(f"Wrote provider health summary: {summary_path}")
    print(summary.to_string(index=False))
    status = str(summary.loc[0, "monitor_status"])
    if status == FAILED_STATUS and args.fail_on_error:
        return 2
    if status == WARNING_STATUS and args.fail_on_warning:
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from forecasting.interval_health_trends import (
    IntervalHealthTrendConfig,
    build_interval_health_trends,
    read_frame,
    write_frame_atomic,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Build reproducible run-level and exact-slice trend datasets from "
            "retained prediction-interval health history."
        )
    )
    parser.add_argument("--history", type=Path, required=True)
    parser.add_argument("--health-summary", type=Path, required=True)
    parser.add_argument(
        "--output-dir", type=Path, default=Path("data/interval-health-trends")
    )
    parser.add_argument(
        "--output-format", choices=("csv", "parquet"), default="parquet"
    )
    parser.add_argument("--trend-run-id")
    parser.add_argument("--trend-run-timestamp")
    parser.add_argument("--recent-runs", type=int, default=3)
    parser.add_argument("--reference-runs", type=int, default=6)
    parser.add_argument("--min-recent-runs", type=int, default=2)
    parser.add_argument("--min-reference-runs", type=int, default=3)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    timestamp = args.trend_run_timestamp or datetime.now(timezone.utc).isoformat()
    config = IntervalHealthTrendConfig(
        recent_interval_run_count=args.recent_runs,
        reference_interval_run_count=args.reference_runs,
        min_recent_interval_runs=args.min_recent_runs,
        min_reference_interval_runs=args.min_reference_runs,
    )
    run_trends, slice_trends = build_interval_health_trends(
        read_frame(args.history),
        read_frame(args.health_summary),
        config=config,
        trend_run_id=args.trend_run_id,
        trend_run_timestamp=timestamp,
    )
    run_id = str(run_trends.loc[0, "trend_run_id"])
    suffix = ".csv" if args.output_format == "csv" else ".parquet"
    args.output_dir.mkdir(parents=True, exist_ok=True)
    run_path = args.output_dir / f"interval_health_run_trends_{run_id}{suffix}"
    slice_path = args.output_dir / f"interval_health_slice_trends_{run_id}{suffix}"
    write_frame_atomic(run_trends, run_path, args.output_format)
    write_frame_atomic(slice_trends, slice_path, args.output_format)
    print(
        json.dumps(
            {
                "trend_run_id": run_id,
                "trend_run_timestamp_utc": str(
                    run_trends.loc[0, "trend_run_timestamp_utc"]
                ),
                "run_trend_path": str(run_path),
                "run_trend_rows": int(len(run_trends)),
                "slice_trend_path": str(slice_path),
                "slice_trend_rows": int(len(slice_trends)),
                "scenarios": sorted(
                    run_trends["scenario"].astype(str).unique().tolist()
                ),
                "source_areas": sorted(
                    run_trends["source_area"].astype(str).unique().tolist()
                ),
                "automatic_recalibration_performed": False,
                "automatic_model_change_performed": False,
                "automatic_schedule_change_performed": False,
                "automatic_promotion_performed": False,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

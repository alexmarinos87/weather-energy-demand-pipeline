from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from forecasting.interval_health_reporting import (
    build_interval_health_report,
    read_frame,
    write_frame_atomic,
    write_text_atomic,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Build a reproducible Markdown/HTML thin-client report from retained "
            "interval-health trend datasets."
        )
    )
    parser.add_argument("--run-trends", type=Path, required=True)
    parser.add_argument("--slice-trends", type=Path, required=True)
    parser.add_argument(
        "--output-dir", type=Path, default=Path("data/interval-health-report")
    )
    parser.add_argument(
        "--output-format", choices=("csv", "parquet"), default="parquet"
    )
    parser.add_argument("--report-run-id")
    parser.add_argument("--report-run-timestamp")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    timestamp = args.report_run_timestamp or datetime.now(timezone.utc).isoformat()
    report = build_interval_health_report(
        read_frame(args.run_trends),
        read_frame(args.slice_trends),
        report_run_id=args.report_run_id,
        report_run_timestamp=timestamp,
    )
    run_id = str(report["metadata"]["report_run_id"])
    suffix = ".csv" if args.output_format == "csv" else ".parquet"
    args.output_dir.mkdir(parents=True, exist_ok=True)
    outputs: dict[str, Path] = {}
    for role, frame in report["frames"].items():
        path = args.output_dir / f"{role}_{run_id}{suffix}"
        outputs[role] = write_frame_atomic(frame, path, args.output_format)
    outputs["markdown"] = write_text_atomic(
        report["markdown"],
        args.output_dir / f"interval_health_report_{run_id}.md",
    )
    outputs["html"] = write_text_atomic(
        report["html"],
        args.output_dir / f"interval_health_report_{run_id}.html",
    )
    print(
        json.dumps(
            {
                **{
                    key: (
                        value.isoformat()
                        if hasattr(value, "isoformat")
                        else value
                    )
                    for key, value in report["metadata"].items()
                },
                "outputs": {key: str(value) for key, value in outputs.items()},
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

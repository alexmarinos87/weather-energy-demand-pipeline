from __future__ import annotations

import argparse
from pathlib import Path

from forecasting.interval_policy_retained_compatibility import evaluate_retained_policy_compatibility
from forecasting.interval_policy_compatibility_publication import write_retained_compatibility_bundle
from forecasting.interval_policy_compatibility_io import read_compatibility_frame as read_frame


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Produce a scenario report using the canonical retained-health-check "
            "comparison engine. Original health checks are required; trend-only "
            "historical reconstruction is not supported."
        )
    )
    parser.add_argument("--slice-trends", type=Path, required=True)
    parser.add_argument("--health-checks", type=Path, required=True,
                        help="Matching original CSV/Parquet checks, including monitor_as_of_utc.")
    parser.add_argument("--output-dir", type=Path,
                        default=Path("data/interval-policy-retained-compatibility"))
    parser.add_argument("--output-format", choices=("csv", "parquet"), default="csv")
    parser.add_argument("--compatibility-run-id")
    parser.add_argument("--compatibility-run-timestamp")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    slices, summary, report = evaluate_retained_policy_compatibility(
        read_frame(args.slice_trends),
        retained_health_checks=read_frame(args.health_checks),
        compatibility_run_id=args.compatibility_run_id,
        compatibility_run_timestamp=args.compatibility_run_timestamp,
    )
    outputs = write_retained_compatibility_bundle(
        args.output_dir, slices, summary, report, output_format=args.output_format,
    )
    for name, path in outputs.items():
        print(f"Wrote {name}: {path}")
    print(summary[["scenario", "retained_monitor_status", "previous_policy_status",
                   "current_policy_status", "compatibility_classification",
                   "changed_slice_count", "newly_failed_slice_count"]].to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

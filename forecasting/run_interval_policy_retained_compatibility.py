from __future__ import annotations

import argparse
from pathlib import Path

from forecasting.interval_policy_retained_compatibility import (
    evaluate_retained_policy_compatibility,
)
from forecasting.interval_policy_retained_compatibility_manifest import (
    build_compatibility_manifest,
    write_json_atomic,
)
from forecasting.interval_policy_sensitivity import (
    read_frame,
    write_frame_atomic,
    write_text_atomic,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Compare retained interval-health trends under the previous five-point "
            "and reviewed three-point coverage-shortfall policies without rewriting "
            "historical monitor status."
        )
    )
    parser.add_argument("--slice-trends", type=Path, required=True)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/interval-policy-retained-compatibility"),
    )
    parser.add_argument(
        "--output-format",
        choices=("csv", "parquet"),
        default="csv",
    )
    parser.add_argument("--compatibility-run-id")
    parser.add_argument("--compatibility-run-timestamp")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    slices, summary, report = evaluate_retained_policy_compatibility(
        read_frame(args.slice_trends),
        compatibility_run_id=args.compatibility_run_id,
        compatibility_run_timestamp=args.compatibility_run_timestamp,
    )
    run_id = str(summary.iloc[0]["compatibility_run_id"])
    args.output_dir.mkdir(parents=True, exist_ok=True)
    suffix = ".csv" if args.output_format == "csv" else ".parquet"
    outputs = {
        "slices": write_frame_atomic(
            slices,
            args.output_dir
            / f"interval_policy_retained_compatibility_slices_{run_id}{suffix}",
            args.output_format,
        ),
        "summary": write_frame_atomic(
            summary,
            args.output_dir
            / f"interval_policy_retained_compatibility_summary_{run_id}{suffix}",
            args.output_format,
        ),
        "report": write_text_atomic(
            report,
            args.output_dir
            / f"interval_policy_retained_compatibility_report_{run_id}.md",
        ),
    }
    manifest = build_compatibility_manifest(summary, artifacts=outputs)
    outputs["manifest"] = write_json_atomic(
        manifest,
        args.output_dir
        / f"interval_policy_retained_compatibility_manifest_{run_id}.json",
    )
    for name, path in outputs.items():
        print(f"Wrote {name}: {path}")
    print(
        summary[
            [
                "scenario",
                "retained_monitor_status",
                "previous_policy_status",
                "current_policy_status",
                "compatibility_classification",
                "changed_slice_count",
                "newly_failed_slice_count",
            ]
        ].to_string(index=False)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

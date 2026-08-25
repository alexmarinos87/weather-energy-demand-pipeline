from __future__ import annotations

import argparse
from pathlib import Path

from forecasting.interval_policy_sensitivity import (
    default_policy_candidates,
    evaluate_policy_sensitivity,
    load_policy_candidates,
    read_frame,
    write_frame_atomic,
    write_text_atomic,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Compare retained interval-health trends under reviewed counterfactual policies."
    )
    parser.add_argument("--slice-trends", type=Path, required=True)
    parser.add_argument("--candidate-policy-file", type=Path)
    parser.add_argument(
        "--output-dir", type=Path, default=Path("data/interval-policy-sensitivity")
    )
    parser.add_argument("--output-format", choices=("csv", "parquet"), default="csv")
    parser.add_argument("--sensitivity-run-id")
    parser.add_argument("--sensitivity-run-timestamp")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    candidates = (
        load_policy_candidates(args.candidate_policy_file)
        if args.candidate_policy_file
        else default_policy_candidates()
    )
    slices, summary, report = evaluate_policy_sensitivity(
        read_frame(args.slice_trends),
        candidates=candidates,
        sensitivity_run_id=args.sensitivity_run_id,
        sensitivity_run_timestamp=args.sensitivity_run_timestamp,
    )
    run_id = str(summary.iloc[0]["sensitivity_run_id"])
    args.output_dir.mkdir(parents=True, exist_ok=True)
    suffix = ".csv" if args.output_format == "csv" else ".parquet"
    outputs = {
        "slices": write_frame_atomic(
            slices,
            args.output_dir / f"interval_policy_sensitivity_slices_{run_id}{suffix}",
            args.output_format,
        ),
        "summary": write_frame_atomic(
            summary,
            args.output_dir / f"interval_policy_sensitivity_summary_{run_id}{suffix}",
            args.output_format,
        ),
        "report": write_text_atomic(
            report,
            args.output_dir / f"interval_policy_sensitivity_report_{run_id}.md",
        ),
    }
    for name, path in outputs.items():
        print(f"Wrote {name}: {path}")
    print(
        summary[
            [
                "scenario",
                "candidate_id",
                "retained_monitor_status",
                "candidate_status",
                "sensitivity_classification",
                "changed_slice_count",
            ]
        ].to_string(index=False)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import argparse
from pathlib import Path

from forecasting.interval_policy_candidate_revision import read_json
from forecasting.interval_policy_candidate_revision_review import (
    read_json as read_review_json,
)
from forecasting.interval_policy_revision_sensitivity import (
    read_frame,
    run_revision_sensitivity,
    write_revision_sensitivity,
)


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description="Run an explicit reviewed-candidate sensitivity comparison."
    )
    result.add_argument("--slice-trends", type=Path, required=True)
    result.add_argument("--revision-review", type=Path, required=True)
    result.add_argument("--revision-package", type=Path, required=True)
    result.add_argument("--source-decision", type=Path, required=True)
    result.add_argument("--source-sensitivity-summary", type=Path, required=True)
    result.add_argument("--sensitivity-run-id")
    result.add_argument("--sensitivity-run-timestamp")
    result.add_argument(
        "--output-format", choices=("csv", "parquet"), default="parquet"
    )
    result.add_argument("--output-dir", type=Path, required=True)
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    trends = read_frame(args.slice_trends)
    review = read_review_json(args.revision_review, "revision review")
    package = read_json(args.revision_package, "revision package")
    decision = read_json(args.source_decision, "source decision")
    source_summary = read_frame(args.source_sensitivity_summary)
    slices, summary, report = run_revision_sensitivity(
        trends,
        review,
        package,
        decision,
        source_summary,
        sensitivity_run_id=args.sensitivity_run_id,
        sensitivity_run_timestamp=args.sensitivity_run_timestamp,
    )
    for path in write_revision_sensitivity(
        args.output_dir,
        slices,
        summary,
        report,
        review,
        package,
        decision,
        source_summary,
        output_format=args.output_format,
    ):
        print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

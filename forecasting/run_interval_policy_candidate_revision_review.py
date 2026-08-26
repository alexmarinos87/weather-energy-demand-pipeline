from __future__ import annotations

import argparse
from pathlib import Path

from forecasting.interval_policy_candidate_revision_review import (
    create_candidate_revision_review,
    read_frame,
    read_json,
    write_candidate_revision_review,
)


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description="Record a named non-activating review of one candidate revision package."
    )
    result.add_argument("--revision-package", type=Path, required=True)
    result.add_argument("--source-decision", type=Path, required=True)
    result.add_argument("--sensitivity-summary", type=Path, required=True)
    result.add_argument(
        "--review-decision",
        choices=(
            "accept_for_sensitivity_review",
            "reject_revision_package",
            "request_package_revision",
        ),
        required=True,
    )
    result.add_argument("--reviewer-name", required=True)
    result.add_argument("--reviewer-role", required=True)
    result.add_argument("--review-ticket", required=True)
    result.add_argument("--rationale", required=True)
    result.add_argument("--requested-change", action="append", default=[])
    result.add_argument("--reviewed-at-utc")
    result.add_argument("--output-dir", type=Path, required=True)
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    package = read_json(args.revision_package, "revision package")
    decision = read_json(args.source_decision, "source decision")
    summary = read_frame(args.sensitivity_summary)
    review = create_candidate_revision_review(
        package,
        decision,
        summary,
        review_decision=args.review_decision,
        reviewer_name=args.reviewer_name,
        reviewer_role=args.reviewer_role,
        review_ticket=args.review_ticket,
        rationale=args.rationale,
        requested_changes=args.requested_change,
        reviewed_at_utc=args.reviewed_at_utc,
    )
    for path in write_candidate_revision_review(
        args.output_dir, review, package, decision, summary
    ):
        print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

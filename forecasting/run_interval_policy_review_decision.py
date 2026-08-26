from __future__ import annotations

import argparse
from pathlib import Path

from forecasting.interval_policy_review_decision import (
    ALLOWED_DECISIONS,
    create_policy_review_decision,
    read_frame,
    write_policy_review_decision,
)


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description="Record one immutable, named, non-activating interval-policy review decision."
    )
    result.add_argument("--sensitivity-summary", type=Path, required=True)
    result.add_argument("--decision", choices=sorted(ALLOWED_DECISIONS), required=True)
    result.add_argument("--target-candidate-id", required=True)
    result.add_argument("--reviewer-name", required=True)
    result.add_argument("--reviewer-role", required=True)
    result.add_argument("--review-ticket", required=True)
    result.add_argument("--rationale", required=True)
    result.add_argument("--requested-change", action="append", default=[])
    result.add_argument("--decision-timestamp-utc")
    result.add_argument("--output-dir", type=Path, required=True)
    return result


def main(argv: list[str] | None = None) -> int:
    arguments = parser().parse_args(argv)
    summary = read_frame(arguments.sensitivity_summary)
    decision = create_policy_review_decision(
        summary,
        decision=arguments.decision,
        target_candidate_id=arguments.target_candidate_id,
        reviewer_name=arguments.reviewer_name,
        reviewer_role=arguments.reviewer_role,
        review_ticket=arguments.review_ticket,
        rationale=arguments.rationale,
        requested_changes=arguments.requested_change,
        decision_timestamp_utc=arguments.decision_timestamp_utc,
    )
    json_path, markdown_path = write_policy_review_decision(
        arguments.output_dir, decision, summary
    )
    print(json_path)
    print(markdown_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

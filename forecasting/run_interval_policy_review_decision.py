from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

from forecasting.interval_policy_review_decision import (
    ALLOWED_DECISIONS,
    create_interval_policy_review_decision,
    read_sensitivity_summary,
    write_interval_policy_review_decision,
)


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description="Record one immutable named decision over retained interval-policy sensitivity evidence."
    )
    result.add_argument("--sensitivity-summary", required=True, type=Path)
    result.add_argument("--decision", required=True, choices=sorted(ALLOWED_DECISIONS))
    result.add_argument("--target-policy-id", required=True)
    result.add_argument("--reviewer-name", required=True)
    result.add_argument("--reviewer-role", required=True)
    result.add_argument("--review-ticket", required=True)
    result.add_argument("--rationale", required=True)
    result.add_argument("--requested-revision")
    result.add_argument("--decided-at-utc")
    result.add_argument("--output-dir", required=True, type=Path)
    return result


def main(arguments: Sequence[str] | None = None) -> int:
    options = parser().parse_args(arguments)
    summary = read_sensitivity_summary(options.sensitivity_summary)
    decision = create_interval_policy_review_decision(
        summary,
        decision=options.decision,
        target_policy_id=options.target_policy_id,
        reviewer_name=options.reviewer_name,
        reviewer_role=options.reviewer_role,
        review_ticket=options.review_ticket,
        rationale=options.rationale,
        requested_revision=options.requested_revision,
        decided_at_utc=options.decided_at_utc,
    )
    json_path, markdown_path = write_interval_policy_review_decision(
        options.output_dir, decision
    )
    print(json_path)
    print(markdown_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

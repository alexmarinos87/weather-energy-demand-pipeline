from __future__ import annotations

import argparse
from pathlib import Path

from forecasting.interval_policy_candidate_revision import (
    create_candidate_revision_package,
    read_frame,
    read_json,
    write_candidate_revision_package,
)


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description="Create an immutable non-activating policy-candidate revision package."
    )
    result.add_argument("--decision", type=Path, required=True)
    result.add_argument("--sensitivity-summary", type=Path, required=True)
    result.add_argument("--revision-plan", type=Path, required=True)
    result.add_argument("--prepared-by", required=True)
    result.add_argument("--preparer-role", required=True)
    result.add_argument("--revision-ticket", required=True)
    result.add_argument("--rationale", required=True)
    result.add_argument("--prepared-at-utc")
    result.add_argument("--output-dir", type=Path, required=True)
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    decision = read_json(args.decision, "decision")
    summary = read_frame(args.sensitivity_summary)
    plan = read_json(args.revision_plan, "revision plan")
    package = create_candidate_revision_package(
        decision,
        summary,
        source_candidate=plan.get("source_candidate", {}),
        revised_candidate=plan.get("revised_candidate", {}),
        requested_change_responses=plan.get(
            "requested_change_responses", ()
        ),
        prepared_by=args.prepared_by,
        preparer_role=args.preparer_role,
        revision_ticket=args.revision_ticket,
        rationale=args.rationale,
        prepared_at_utc=args.prepared_at_utc,
    )
    paths = write_candidate_revision_package(
        args.output_dir, package, decision, summary
    )
    for path in paths:
        print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

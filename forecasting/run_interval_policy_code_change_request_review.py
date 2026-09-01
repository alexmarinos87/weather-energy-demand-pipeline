from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

from forecasting.interval_policy_code_change_request_review import (
    create_interval_policy_code_change_request_review,
    read_frame,
    read_json,
    write_interval_policy_code_change_request_review,
)


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(
        description="Create one named review of an interval-policy code-change request."
    )
    value.add_argument("--code-change-request", type=Path, required=True)
    value.add_argument("--implementation-dry-run-review", type=Path, required=True)
    value.add_argument("--implementation-dry-run", type=Path, required=True)
    value.add_argument("--disposition", type=Path, required=True)
    value.add_argument("--revision-sensitivity-summary", type=Path, required=True)
    value.add_argument("--revision-sensitivity-manifest", type=Path, required=True)
    value.add_argument("--revision-package", type=Path, required=True)
    value.add_argument("--source-decision", type=Path, required=True)
    value.add_argument("--source-sensitivity-summary", type=Path, required=True)
    value.add_argument("--policy-source", type=Path, required=True)
    value.add_argument("--current-repository-commit", required=True)
    value.add_argument("--current-repository-tree", required=True)
    value.add_argument(
        "--review-decision",
        choices=(
            "accept_for_separate_policy_defaults_pr",
            "reject_code_change_request",
            "request_code_change_request_revision",
        ),
        required=True,
    )
    value.add_argument("--reviewer-name", required=True)
    value.add_argument("--reviewer-role", required=True)
    value.add_argument("--review-ticket", required=True)
    value.add_argument("--rationale", required=True)
    value.add_argument("--requested-update", action="append", default=[])
    value.add_argument("--reviewed-at-utc")
    value.add_argument("--output-dir", type=Path, required=True)
    return value


def main(argv: Sequence[str] | None = None) -> int:
    args = parser().parse_args(argv)
    request = read_json(args.code_change_request, "code_change_request")
    dry_run_review = read_json(
        args.implementation_dry_run_review,
        "implementation_dry_run_review",
    )
    proposal = read_json(args.implementation_dry_run, "implementation_dry_run")
    disposition = read_json(args.disposition, "disposition")
    manifest = read_json(
        args.revision_sensitivity_manifest,
        "revision_sensitivity_manifest",
    )
    package = read_json(args.revision_package, "revision_package")
    decision = read_json(args.source_decision, "source_decision")
    revision_summary = read_frame(args.revision_sensitivity_summary)
    source_summary = read_frame(args.source_sensitivity_summary)
    source_text = args.policy_source.read_text(encoding="utf-8")
    review = create_interval_policy_code_change_request_review(
        request,
        dry_run_review,
        proposal,
        disposition,
        revision_summary,
        manifest,
        package,
        decision,
        source_summary,
        current_repository_commit=args.current_repository_commit,
        current_repository_tree=args.current_repository_tree,
        policy_source_text=source_text,
        review_decision=args.review_decision,
        reviewer_name=args.reviewer_name,
        reviewer_role=args.reviewer_role,
        review_ticket=args.review_ticket,
        rationale=args.rationale,
        requested_updates=args.requested_update,
        reviewed_at_utc=args.reviewed_at_utc,
    )
    write_interval_policy_code_change_request_review(
        args.output_dir,
        review,
        request,
        dry_run_review,
        proposal,
        disposition,
        revision_summary,
        manifest,
        package,
        decision,
        source_summary,
        current_repository_commit=args.current_repository_commit,
        current_repository_tree=args.current_repository_tree,
        policy_source_text=source_text,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

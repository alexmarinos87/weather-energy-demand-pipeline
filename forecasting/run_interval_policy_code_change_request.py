from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

from forecasting.interval_policy_code_change_request import (
    create_interval_policy_code_change_request,
    read_frame,
    read_json,
    write_interval_policy_code_change_request,
)


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(
        description="Create one repository-bound interval-policy code-change request."
    )
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
    value.add_argument("--requested-branch-name", required=True)
    value.add_argument("--requested-pr-title", required=True)
    value.add_argument("--requested-by", required=True)
    value.add_argument("--requester-role", required=True)
    value.add_argument("--request-ticket", required=True)
    value.add_argument("--rationale", required=True)
    value.add_argument("--requested-at-utc")
    value.add_argument("--output-dir", type=Path, required=True)
    return value


def main(argv: Sequence[str] | None = None) -> int:
    args = parser().parse_args(argv)
    review = read_json(
        args.implementation_dry_run_review,
        "implementation_dry_run_review",
    )
    proposal = read_json(args.implementation_dry_run, "implementation_dry_run")
    disposition = read_json(args.disposition, "disposition")
    revision_manifest = read_json(
        args.revision_sensitivity_manifest,
        "revision_sensitivity_manifest",
    )
    revision_package = read_json(args.revision_package, "revision_package")
    source_decision = read_json(args.source_decision, "source_decision")
    revision_summary = read_frame(args.revision_sensitivity_summary)
    source_summary = read_frame(args.source_sensitivity_summary)
    source_text = args.policy_source.read_text(encoding="utf-8")
    request = create_interval_policy_code_change_request(
        review,
        proposal,
        disposition,
        revision_summary,
        revision_manifest,
        revision_package,
        source_decision,
        source_summary,
        current_repository_commit=args.current_repository_commit,
        current_repository_tree=args.current_repository_tree,
        policy_source_text=source_text,
        requested_branch_name=args.requested_branch_name,
        requested_pr_title=args.requested_pr_title,
        requested_by=args.requested_by,
        requester_role=args.requester_role,
        request_ticket=args.request_ticket,
        rationale=args.rationale,
        requested_at_utc=args.requested_at_utc,
    )
    write_interval_policy_code_change_request(
        args.output_dir,
        request,
        review,
        proposal,
        disposition,
        revision_summary,
        revision_manifest,
        revision_package,
        source_decision,
        source_summary,
        current_repository_commit=args.current_repository_commit,
        current_repository_tree=args.current_repository_tree,
        policy_source_text=source_text,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

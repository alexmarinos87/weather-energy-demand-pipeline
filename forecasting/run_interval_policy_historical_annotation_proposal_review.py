from __future__ import annotations

import argparse
import json
from pathlib import Path

from forecasting.interval_policy_historical_annotation_proposal_review import (
    create_historical_annotation_proposal_review,
    write_historical_annotation_proposal_review,
)
from forecasting.interval_policy_sensitivity import read_frame


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Record one immutable named review over a G41 historical annotation "
            "proposal."
        )
    )
    parser.add_argument("--annotation-proposal", type=Path, required=True)
    parser.add_argument("--compatibility-review", type=Path, required=True)
    parser.add_argument("--compatibility-summary", type=Path, required=True)
    parser.add_argument("--compatibility-manifest", type=Path, required=True)
    parser.add_argument("--artifact-directory", type=Path, required=True)
    parser.add_argument(
        "--decision",
        choices=(
            "accept_for_separate_annotation_storage_change",
            "reject_historical_annotation_proposal",
            "request_historical_annotation_proposal_revision",
        ),
        required=True,
    )
    parser.add_argument("--reviewer-name", required=True)
    parser.add_argument("--reviewer-role", required=True)
    parser.add_argument("--review-ticket", required=True)
    parser.add_argument("--rationale", required=True)
    parser.add_argument("--requested-update", action="append", default=[])
    parser.add_argument("--reviewed-at-utc")
    parser.add_argument("--review-id")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(
            "data/interval-policy-historical-annotation-proposal-reviews"
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    proposal = json.loads(args.annotation_proposal.read_text(encoding="utf-8"))
    source_review = json.loads(
        args.compatibility_review.read_text(encoding="utf-8")
    )
    summary = read_frame(args.compatibility_summary)
    manifest = json.loads(
        args.compatibility_manifest.read_text(encoding="utf-8")
    )
    review = create_historical_annotation_proposal_review(
        proposal,
        source_review,
        summary,
        manifest,
        artifact_directory=args.artifact_directory,
        decision=args.decision,
        reviewer_name=args.reviewer_name,
        reviewer_role=args.reviewer_role,
        review_ticket=args.review_ticket,
        rationale=args.rationale,
        requested_updates=args.requested_update,
        reviewed_at_utc=args.reviewed_at_utc,
        review_id=args.review_id,
    )
    outputs = write_historical_annotation_proposal_review(
        args.output_dir,
        review,
        proposal,
        source_review,
        summary,
        manifest,
        artifact_directory=args.artifact_directory,
    )
    for name, path in outputs.items():
        print(f"Wrote {name}: {path}")
    print(
        f"Recorded {review['decision']} for {review['source_proposal_id']} "
        f"as {review['review_id']}."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

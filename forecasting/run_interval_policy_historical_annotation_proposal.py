from __future__ import annotations

import argparse
import json
from pathlib import Path

from forecasting.interval_policy_historical_annotation_proposal import (
    create_historical_annotation_proposal,
    write_historical_annotation_proposal,
)
from forecasting.interval_policy_sensitivity import read_frame


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Create one non-applying historical annotation proposal from a "
            "verified G39 compatibility review."
        )
    )
    parser.add_argument("--compatibility-review", type=Path, required=True)
    parser.add_argument("--compatibility-summary", type=Path, required=True)
    parser.add_argument("--compatibility-manifest", type=Path, required=True)
    parser.add_argument("--artifact-directory", type=Path, required=True)
    parser.add_argument(
        "--proposal-input",
        type=Path,
        required=True,
        help=(
            "JSON object containing annotations and requested_action_responses."
        ),
    )
    parser.add_argument("--proposed-by", required=True)
    parser.add_argument("--proposer-role", required=True)
    parser.add_argument("--proposal-ticket", required=True)
    parser.add_argument("--rationale", required=True)
    parser.add_argument("--proposed-at-utc")
    parser.add_argument("--proposal-id")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/interval-policy-historical-annotation-proposals"),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    review = json.loads(args.compatibility_review.read_text(encoding="utf-8"))
    summary = read_frame(args.compatibility_summary)
    manifest = json.loads(
        args.compatibility_manifest.read_text(encoding="utf-8")
    )
    proposal_input = json.loads(args.proposal_input.read_text(encoding="utf-8"))
    if not isinstance(proposal_input, dict):
        raise ValueError("proposal-input must be a JSON object.")
    proposal = create_historical_annotation_proposal(
        review,
        summary,
        manifest,
        artifact_directory=args.artifact_directory,
        annotations=proposal_input.get("annotations", ()),
        requested_action_responses=proposal_input.get(
            "requested_action_responses", ()
        ),
        proposed_by=args.proposed_by,
        proposer_role=args.proposer_role,
        proposal_ticket=args.proposal_ticket,
        rationale=args.rationale,
        proposed_at_utc=args.proposed_at_utc,
        proposal_id=args.proposal_id,
    )
    outputs = write_historical_annotation_proposal(
        args.output_dir,
        proposal,
        review,
        summary,
        manifest,
        artifact_directory=args.artifact_directory,
    )
    for name, path in outputs.items():
        print(f"Wrote {name}: {path}")
    print(
        f"Prepared {proposal['annotation_count']} non-applying annotations "
        f"as {proposal['proposal_id']}."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

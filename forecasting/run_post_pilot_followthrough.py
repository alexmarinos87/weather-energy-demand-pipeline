from __future__ import annotations

import argparse
import json
from pathlib import Path

from forecasting.post_pilot_decision import load_post_pilot_decision
from forecasting.post_pilot_followthrough import (
    create_post_pilot_followthrough_request,
    load_post_pilot_followthrough_request,
    write_post_pilot_followthrough_request,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Create or verify one bounded post-pilot follow-through request. "
            "This command never runs Fabric, mutates the model registry, or "
            "changes the active model."
        )
    )
    commands = parser.add_subparsers(dest="command", required=True)

    create = commands.add_parser("create")
    create.add_argument("--decision-record", type=Path, required=True)
    create.add_argument("--closure-bundle", type=Path, required=True)
    create.add_argument("--recovered-directory", type=Path, required=True)
    create.add_argument("--confirm-decision-id", required=True)
    create.add_argument("--confirm-closure-id", required=True)
    create.add_argument("--requested-by", required=True)
    create.add_argument("--owner", required=True)
    create.add_argument("--review-ticket", required=True)
    create.add_argument("--reason", required=True)
    create.add_argument(
        "--action-item",
        action="append",
        required=True,
        help="Repeat for each reviewed follow-through action.",
    )
    create.add_argument("--requested-at-utc")
    create.add_argument(
        "--output-root", type=Path, default=Path("data/pilot-followthrough")
    )

    verify = commands.add_parser("verify")
    verify.add_argument("--request-record", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "verify":
        request = load_post_pilot_followthrough_request(args.request_record)
        print(
            json.dumps(
                {
                    "pilot_id": request["pilot_id"],
                    "request_id": request["request_id"],
                    "followthrough_type": request["followthrough_type"],
                    "verification_status": "verified",
                    "automatic_execution_allowed": False,
                    "model_registry_mutation_allowed": False,
                    "pilot_execution_authorized": False,
                    "active_model_unchanged": True,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0

    decision = load_post_pilot_decision(args.decision_record)
    request = create_post_pilot_followthrough_request(
        decision,
        args.closure_bundle,
        args.recovered_directory,
        confirm_decision_id=args.confirm_decision_id,
        confirm_closure_id=args.confirm_closure_id,
        requested_by=args.requested_by,
        owner=args.owner,
        review_ticket=args.review_ticket,
        reason=args.reason,
        action_items=tuple(args.action_item),
        requested_at_utc=args.requested_at_utc,
    )
    path = write_post_pilot_followthrough_request(args.output_root, request)
    print(
        json.dumps(
            {
                "pilot_id": request["pilot_id"],
                "closure_id": request["closure_id"],
                "decision_id": request["decision_id"],
                "request_id": request["request_id"],
                "followthrough_type": request["followthrough_type"],
                "request_path": str(path),
                "automatic_execution_allowed": False,
                "model_registry_mutation_allowed": False,
                "pilot_execution_authorized": False,
                "active_model_unchanged": True,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import argparse
import json
from pathlib import Path

from forecasting.fabric_pilot import load_fabric_pilot_plan
from forecasting.fabric_pilot_authorization import load_fabric_pilot_authorization
from forecasting.fabric_pilot_receipt import (
    load_fabric_pilot_run_assessment,
    load_fabric_pilot_run_receipt,
)
from forecasting.post_pilot_decision import (
    ALLOWED_DECISIONS,
    create_post_pilot_decision,
    load_post_pilot_decision,
    write_post_pilot_decision,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Record or verify one named human post-pilot decision. "
            "This command never connects to Fabric or mutates the model registry."
        )
    )
    subcommands = parser.add_subparsers(dest="command", required=True)

    decide = subcommands.add_parser("decide")
    decide.add_argument("--plan", type=Path, required=True)
    decide.add_argument("--authorization", type=Path, required=True)
    decide.add_argument("--receipt", type=Path, required=True)
    decide.add_argument("--assessment", type=Path, required=True)
    decide.add_argument("--confirm-pilot-id", required=True)
    decide.add_argument("--confirm-receipt-id", required=True)
    decide.add_argument("--confirm-assessment-id", required=True)
    decide.add_argument(
        "--decision", choices=tuple(sorted(ALLOWED_DECISIONS)), required=True
    )
    decide.add_argument("--decision-maker", required=True)
    decide.add_argument("--review-ticket", required=True)
    decide.add_argument("--reason", required=True)
    decide.add_argument(
        "--action-item",
        action="append",
        required=True,
        help="Repeat for each reviewed follow-up action.",
    )
    decide.add_argument("--decided-at-utc")
    decide.add_argument(
        "--output-root", type=Path, default=Path("data/fabric-pilots")
    )

    verify = subcommands.add_parser("verify")
    verify.add_argument("--decision-record", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "verify":
        record = load_post_pilot_decision(args.decision_record)
        print(
            json.dumps(
                {
                    "pilot_id": record["pilot_id"],
                    "decision_id": record["decision_id"],
                    "decision": record["decision"],
                    "verification_status": "verified",
                    "automatic_decision_allowed": False,
                    "model_registry_mutation_allowed": False,
                    "active_model_unchanged": True,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0

    plan = load_fabric_pilot_plan(args.plan)
    authorization = load_fabric_pilot_authorization(args.authorization)
    receipt = load_fabric_pilot_run_receipt(args.receipt)
    assessment = load_fabric_pilot_run_assessment(args.assessment)
    record = create_post_pilot_decision(
        plan,
        authorization,
        receipt,
        assessment,
        confirm_pilot_id=args.confirm_pilot_id,
        confirm_receipt_id=args.confirm_receipt_id,
        confirm_assessment_id=args.confirm_assessment_id,
        decision=args.decision,
        decision_maker=args.decision_maker,
        review_ticket=args.review_ticket,
        reason=args.reason,
        action_items=tuple(args.action_item),
        decided_at_utc=args.decided_at_utc,
    )
    path = write_post_pilot_decision(args.output_root, record)
    print(
        json.dumps(
            {
                "pilot_id": record["pilot_id"],
                "decision_id": record["decision_id"],
                "decision": record["decision"],
                "decision_effect": record["decision_effect"],
                "decision_path": str(path),
                "automatic_decision_allowed": False,
                "model_registry_mutation_allowed": False,
                "active_model_unchanged": True,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

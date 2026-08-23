from __future__ import annotations

import argparse
import json
from pathlib import Path

from forecasting.model_registry import load_candidate_history
from forecasting.post_pilot_decision import (
    ALLOWED_DECISIONS,
    create_post_pilot_decision,
    verify_post_pilot_decision,
    write_post_pilot_decision,
)


def _load_json(path: Path) -> dict:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain one JSON object.")
    return payload


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Record or verify a named post-pilot human decision. This command "
            "does not mutate the model registry, authorize a new pilot, deploy, "
            "schedule, or activate a model."
        )
    )
    commands = parser.add_subparsers(dest="command", required=True)

    record = commands.add_parser("record")
    record.add_argument("--candidate-dir", type=Path, required=True)
    record.add_argument("--receipt", type=Path, required=True)
    record.add_argument("--assessment", type=Path, required=True)
    record.add_argument(
        "--decision", choices=tuple(sorted(ALLOWED_DECISIONS)), required=True
    )
    record.add_argument("--decided-by", required=True)
    record.add_argument("--decision-role", required=True)
    record.add_argument("--review-ticket", required=True)
    record.add_argument("--reason", required=True)
    record.add_argument(
        "--follow-up-action",
        action="append",
        required=True,
        help="Repeat for every separately reviewed follow-up action.",
    )
    record.add_argument(
        "--revision-requirement",
        action="append",
        default=[],
        help="Required for revise_candidate; repeat for each requirement.",
    )
    record.add_argument("--retirement-reason")
    record.add_argument("--decided-at-utc")
    record.add_argument("--confirm-assessment-id", required=True)
    record.add_argument(
        "--output-dir",
        type=Path,
        help="Defaults to the assessment file directory.",
    )

    verify = commands.add_parser("verify")
    verify.add_argument("--decision", type=Path, required=True)
    return parser


def _record(args: argparse.Namespace) -> int:
    _, _, candidate = load_candidate_history(args.candidate_dir)
    receipt = _load_json(args.receipt)
    assessment = _load_json(args.assessment)
    if args.confirm_assessment_id != assessment.get("assessment_id"):
        raise ValueError(
            "--confirm-assessment-id must exactly match the immutable assessment."
        )
    decision = create_post_pilot_decision(
        candidate,
        receipt,
        assessment,
        decision=args.decision,
        decided_by=args.decided_by,
        decision_role=args.decision_role,
        review_ticket=args.review_ticket,
        reason=args.reason,
        follow_up_actions=args.follow_up_action,
        revision_requirements=args.revision_requirement,
        retirement_reason=args.retirement_reason,
        decided_at_utc=args.decided_at_utc,
    )
    output_directory = args.output_dir or args.assessment.parent
    path = write_post_pilot_decision(output_directory, decision)
    print(
        json.dumps(
            {
                "post_pilot_decision_path": str(path),
                "decision_id": decision["decision_id"],
                "decision": decision["decision"],
                "decision_effect": decision["decision_effect"],
                "registry_mutation_performed": False,
                "new_pilot_authorized": False,
                "model_activation_authorized": False,
                "follow_up_human_action_required": True,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def _verify(args: argparse.Namespace) -> int:
    decision = _load_json(args.decision)
    verify_post_pilot_decision(decision)
    print(
        json.dumps(
            {
                "decision_id": decision["decision_id"],
                "decision": decision["decision"],
                "decision_hash": decision["decision_hash"],
                "verified": True,
                "registry_mutation_performed": False,
                "model_activation_authorized": False,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return _record(args) if args.command == "record" else _verify(args)


if __name__ == "__main__":
    raise SystemExit(main())

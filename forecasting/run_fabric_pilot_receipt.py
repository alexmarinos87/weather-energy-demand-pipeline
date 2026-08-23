from __future__ import annotations

import argparse
import json
from pathlib import Path

from forecasting.fabric_pilot import load_fabric_pilot_plan
from forecasting.fabric_pilot_authorization import (
    load_fabric_pilot_authorization,
    load_fabric_pilot_preflight,
)
from forecasting.fabric_pilot_receipt import (
    FAILED_OUTCOME,
    assess_fabric_pilot_run,
    create_fabric_pilot_run_receipt,
    load_fabric_pilot_run_assessment,
    load_fabric_pilot_run_receipt,
    write_fabric_pilot_run_assessment,
    write_fabric_pilot_run_receipt,
)


def _read_json_object(path: Path, label: str) -> dict:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{label} must contain one JSON object.")
    return payload


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Record and assess an externally performed controlled Fabric pilot. "
            "This command never connects to Fabric or runs a notebook."
        )
    )
    subcommands = parser.add_subparsers(dest="command", required=True)

    record = subcommands.add_parser("record")
    record.add_argument("--plan", type=Path, required=True)
    record.add_argument("--preflight", type=Path, required=True)
    record.add_argument("--authorization", type=Path, required=True)
    record.add_argument("--run-report", type=Path, required=True)
    record.add_argument("--evidence-root", type=Path, required=True)
    record.add_argument("--evidence-map", type=Path, required=True)
    record.add_argument("--confirm-authorization-id", required=True)
    record.add_argument("--recorded-at-utc")
    record.add_argument(
        "--output-root", type=Path, default=Path("data/fabric-pilots")
    )

    assess = subcommands.add_parser("assess")
    assess.add_argument("--plan", type=Path, required=True)
    assess.add_argument("--authorization", type=Path, required=True)
    assess.add_argument("--receipt", type=Path, required=True)
    assess.add_argument("--required-evidence-role", action="append", default=[])
    assess.add_argument("--assessed-at-utc")
    assess.add_argument(
        "--output-root", type=Path, default=Path("data/fabric-pilots")
    )
    assess.add_argument(
        "--require-eligible",
        action="store_true",
        help="Return exit code 2 after writing a failed assessment.",
    )

    verify_receipt = subcommands.add_parser("verify-receipt")
    verify_receipt.add_argument("--receipt", type=Path, required=True)

    verify_assessment = subcommands.add_parser("verify-assessment")
    verify_assessment.add_argument("--assessment", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "record":
        plan = load_fabric_pilot_plan(args.plan)
        preflight = load_fabric_pilot_preflight(args.preflight)
        authorization = load_fabric_pilot_authorization(args.authorization)
        receipt = create_fabric_pilot_run_receipt(
            plan,
            preflight,
            authorization,
            _read_json_object(args.run_report, "run_report"),
            args.evidence_root,
            _read_json_object(args.evidence_map, "evidence_map"),
            confirm_authorization_id=args.confirm_authorization_id,
            recorded_at_utc=args.recorded_at_utc,
        )
        path = write_fabric_pilot_run_receipt(args.output_root, receipt)
        print(
            json.dumps(
                {
                    "pilot_id": receipt["pilot_id"],
                    "receipt_id": receipt["receipt_id"],
                    "authorization_id": receipt["authorization_id"],
                    "receipt_path": str(path),
                    "run_status": receipt["run_status"],
                    "authorization_consumed": True,
                    "execution_performed": True,
                    "model_activation_performed": False,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0

    if args.command == "assess":
        plan = load_fabric_pilot_plan(args.plan)
        authorization = load_fabric_pilot_authorization(args.authorization)
        receipt = load_fabric_pilot_run_receipt(args.receipt)
        kwargs = {}
        if args.required_evidence_role:
            kwargs["required_evidence_roles"] = tuple(args.required_evidence_role)
        assessment = assess_fabric_pilot_run(
            plan,
            authorization,
            receipt,
            assessed_at_utc=args.assessed_at_utc,
            **kwargs,
        )
        path = write_fabric_pilot_run_assessment(args.output_root, assessment)
        print(
            json.dumps(
                {
                    "pilot_id": assessment["pilot_id"],
                    "receipt_id": assessment["receipt_id"],
                    "assessment_id": assessment["assessment_id"],
                    "assessment_outcome": assessment["assessment_outcome"],
                    "failed_check_count": assessment["failed_check_count"],
                    "rollback_required": assessment["rollback_required"],
                    "assessment_path": str(path),
                    "automatic_model_activation_allowed": False,
                },
                indent=2,
                sort_keys=True,
            )
        )
        if args.require_eligible and assessment["assessment_outcome"] == FAILED_OUTCOME:
            return 2
        return 0

    if args.command == "verify-receipt":
        receipt = load_fabric_pilot_run_receipt(args.receipt)
        print(
            json.dumps(
                {
                    "receipt_id": receipt["receipt_id"],
                    "verification_status": "verified",
                    "authorization_consumed": True,
                    "execution_performed": True,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0

    assessment = load_fabric_pilot_run_assessment(args.assessment)
    print(
        json.dumps(
            {
                "assessment_id": assessment["assessment_id"],
                "verification_status": "verified",
                "assessment_outcome": assessment["assessment_outcome"],
                "automatic_model_activation_allowed": False,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

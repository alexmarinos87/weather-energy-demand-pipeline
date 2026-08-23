from __future__ import annotations

import argparse
import json
from pathlib import Path

from forecasting.fabric_pilot import (
    create_fabric_pilot_plan,
    load_fabric_pilot_plan,
    write_fabric_pilot_plan,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Create or verify a non-executing controlled Fabric pilot plan. "
            "This command never connects to Fabric."
        )
    )
    subcommands = parser.add_subparsers(dest="command", required=True)

    plan = subcommands.add_parser("plan")
    plan.add_argument("--candidate-dir", type=Path, required=True)
    plan.add_argument("--evidence-bundle", type=Path, required=True)
    plan.add_argument("--recovered-bundle-dir", type=Path, required=True)
    plan.add_argument("--environment", default="non-production")
    plan.add_argument("--workspace-name", required=True)
    plan.add_argument("--lakehouse-name", required=True)
    plan.add_argument("--capacity-name", required=True)
    plan.add_argument(
        "--credential-reference",
        action="append",
        default=[],
        help="Credential reference name only; values and NAME=value assignments are rejected.",
    )
    plan.add_argument("--max-duration-minutes", type=int, default=60)
    plan.add_argument("--max-capacity-units", type=float, default=16.0)
    plan.add_argument("--max-forecast-records", type=int, default=40)
    plan.add_argument("--max-prediction-rows", type=int, default=100_000)
    plan.add_argument("--max-metric-rows", type=int, default=10_000)
    plan.add_argument("--max-failed-quality-checks", type=int, default=0)
    plan.add_argument("--max-notebook-retries", type=int, default=0)
    plan.add_argument("--actor", required=True)
    plan.add_argument("--review-ticket", required=True)
    plan.add_argument("--reason", required=True)
    plan.add_argument("--planned-at-utc")
    plan.add_argument(
        "--output-root",
        type=Path,
        default=Path("data/fabric-pilots"),
    )

    verify = subcommands.add_parser("verify-plan")
    verify.add_argument("--plan", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "verify-plan":
        plan = load_fabric_pilot_plan(args.plan)
        print(
            json.dumps(
                {
                    "pilot_id": plan["pilot_id"],
                    "verification_status": "verified",
                    "execution_authorized": False,
                    "execution_performed": False,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0

    credential_references = (
        tuple(args.credential_reference)
        if args.credential_reference
        else ("OPENWEATHER_API_KEY",)
    )
    plan = create_fabric_pilot_plan(
        args.candidate_dir,
        args.evidence_bundle,
        args.recovered_bundle_dir,
        environment=args.environment,
        workspace_name=args.workspace_name,
        lakehouse_name=args.lakehouse_name,
        capacity_name=args.capacity_name,
        credential_references=credential_references,
        max_duration_minutes=args.max_duration_minutes,
        max_capacity_units=args.max_capacity_units,
        max_forecast_records=args.max_forecast_records,
        max_prediction_rows=args.max_prediction_rows,
        max_metric_rows=args.max_metric_rows,
        max_failed_quality_checks=args.max_failed_quality_checks,
        max_notebook_retries=args.max_notebook_retries,
        actor=args.actor,
        review_ticket=args.review_ticket,
        reason=args.reason,
        planned_at_utc=args.planned_at_utc,
    )
    path = write_fabric_pilot_plan(args.output_root, plan)
    print(
        json.dumps(
            {
                "pilot_id": plan["pilot_id"],
                "pilot_plan_path": str(path),
                "pilot_state": plan["pilot_state"],
                "execution_authorized": False,
                "execution_performed": False,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

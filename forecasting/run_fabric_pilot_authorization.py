from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from forecasting.fabric_pilot import load_fabric_pilot_plan
from forecasting.fabric_pilot_authorization import (
    BLOCKED_STATUS,
    assess_fabric_pilot_preflight,
    create_fabric_pilot_authorization,
    load_fabric_pilot_authorization,
    load_fabric_pilot_preflight,
    verify_fabric_pilot_authorization,
    write_fabric_pilot_authorization,
    write_fabric_pilot_preflight,
)


def _read_frame(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix in {".parquet", ".pq"}:
        return pd.read_parquet(path)
    if suffix == ".json":
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, list):
            return pd.DataFrame(payload)
        return pd.DataFrame([payload])
    raise ValueError("Provider-health summary must be CSV, Parquet, or JSON.")


def _read_json_object(path: Path, label: str) -> dict:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{label} must contain one JSON object.")
    return payload


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Create repository/environment preflight evidence and a separate "
            "time-bounded human authorization for a controlled Fabric pilot. "
            "No command connects to Fabric."
        )
    )
    subcommands = parser.add_subparsers(dest="command", required=True)

    preflight = subcommands.add_parser("preflight")
    preflight.add_argument("--plan", type=Path, required=True)
    preflight.add_argument("--candidate-dir", type=Path, required=True)
    preflight.add_argument("--evidence-bundle", type=Path, required=True)
    preflight.add_argument("--recovered-bundle-dir", type=Path, required=True)
    preflight.add_argument("--repository-root", type=Path, required=True)
    preflight.add_argument("--provider-health-summary", type=Path, required=True)
    preflight.add_argument("--environment-snapshot", type=Path, required=True)
    preflight.add_argument("--current-code-commit-sha", required=True)
    preflight.add_argument("--current-code-tree-sha", required=True)
    preflight.add_argument(
        "--available-credential-reference",
        action="append",
        default=[],
        help="Available credential reference name only; no secret value.",
    )
    preflight.add_argument("--as-of-utc")
    preflight.add_argument("--max-plan-age-minutes", type=int, default=10_080)
    preflight.add_argument(
        "--max-provider-health-age-minutes", type=int, default=1_440
    )
    preflight.add_argument(
        "--max-environment-snapshot-age-minutes", type=int, default=60
    )
    preflight.add_argument(
        "--max-capacity-utilization-pct", type=float, default=70.0
    )
    preflight.add_argument("--max-active-job-count", type=int, default=0)
    preflight.add_argument(
        "--output-root", type=Path, default=Path("data/fabric-pilots")
    )
    preflight.add_argument(
        "--require-eligible",
        action="store_true",
        help="Return exit code 2 after writing a blocked preflight.",
    )

    authorize = subcommands.add_parser("authorize")
    authorize.add_argument("--plan", type=Path, required=True)
    authorize.add_argument("--preflight", type=Path, required=True)
    authorize.add_argument("--confirm-pilot-id", required=True)
    authorize.add_argument("--confirm-preflight-id", required=True)
    authorize.add_argument("--authorizer", required=True)
    authorize.add_argument("--operator", required=True)
    authorize.add_argument("--review-ticket", required=True)
    authorize.add_argument("--reason", required=True)
    authorize.add_argument("--authorized-at-utc")
    authorize.add_argument("--valid-from-utc")
    authorize.add_argument("--valid-until-utc", required=True)
    authorize.add_argument(
        "--max-authorization-window-minutes", type=int, default=480
    )
    authorize.add_argument("--max-preflight-age-minutes", type=int, default=60)
    authorize.add_argument(
        "--output-root", type=Path, default=Path("data/fabric-pilots")
    )

    verify = subcommands.add_parser("verify-authorization")
    verify.add_argument("--authorization", type=Path, required=True)
    verify.add_argument("--as-of-utc")
    verify.add_argument("--require-current", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "preflight":
        plan = load_fabric_pilot_plan(args.plan)
        available = (
            tuple(args.available_credential_reference)
            if args.available_credential_reference
            else tuple(plan["credential_references"])
        )
        document = assess_fabric_pilot_preflight(
            plan,
            args.candidate_dir,
            args.evidence_bundle,
            args.recovered_bundle_dir,
            args.repository_root,
            _read_frame(args.provider_health_summary),
            _read_json_object(args.environment_snapshot, "environment_snapshot"),
            current_code_commit_sha=args.current_code_commit_sha,
            current_code_tree_sha=args.current_code_tree_sha,
            available_credential_references=available,
            as_of_utc=args.as_of_utc,
            max_plan_age_minutes=args.max_plan_age_minutes,
            max_provider_health_age_minutes=args.max_provider_health_age_minutes,
            max_environment_snapshot_age_minutes=(
                args.max_environment_snapshot_age_minutes
            ),
            max_capacity_utilization_pct=args.max_capacity_utilization_pct,
            max_active_job_count=args.max_active_job_count,
        )
        path = write_fabric_pilot_preflight(args.output_root, document)
        print(
            json.dumps(
                {
                    "pilot_id": document["pilot_id"],
                    "preflight_id": document["preflight_id"],
                    "preflight_status": document["preflight_status"],
                    "failed_check_count": document["failed_check_count"],
                    "preflight_path": str(path),
                    "execution_authorized": False,
                    "execution_performed": False,
                },
                indent=2,
                sort_keys=True,
            )
        )
        if args.require_eligible and document["preflight_status"] == BLOCKED_STATUS:
            return 2
        return 0

    if args.command == "authorize":
        plan = load_fabric_pilot_plan(args.plan)
        preflight = load_fabric_pilot_preflight(args.preflight)
        authorization = create_fabric_pilot_authorization(
            plan,
            preflight,
            confirm_pilot_id=args.confirm_pilot_id,
            confirm_preflight_id=args.confirm_preflight_id,
            authorizer=args.authorizer,
            operator=args.operator,
            review_ticket=args.review_ticket,
            reason=args.reason,
            authorized_at_utc=args.authorized_at_utc,
            valid_from_utc=args.valid_from_utc,
            valid_until_utc=args.valid_until_utc,
            max_authorization_window_minutes=(
                args.max_authorization_window_minutes
            ),
            max_preflight_age_minutes=args.max_preflight_age_minutes,
        )
        path = write_fabric_pilot_authorization(args.output_root, authorization)
        print(
            json.dumps(
                {
                    "pilot_id": authorization["pilot_id"],
                    "authorization_id": authorization["authorization_id"],
                    "authorization_path": str(path),
                    "valid_from_utc": authorization["valid_from_utc"],
                    "valid_until_utc": authorization["valid_until_utc"],
                    "execution_authorized": True,
                    "execution_performed": False,
                    "automatic_execution_allowed": False,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0

    authorization = load_fabric_pilot_authorization(args.authorization)
    status = verify_fabric_pilot_authorization(
        authorization,
        as_of_utc=args.as_of_utc,
        require_current=args.require_current,
    )
    print(
        json.dumps(
            {
                "pilot_id": authorization["pilot_id"],
                "authorization_id": authorization["authorization_id"],
                "verification_status": "verified",
                "authorization_window_status": status,
                "execution_authorized": True,
                "execution_performed": False,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

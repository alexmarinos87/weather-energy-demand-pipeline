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
    load_fabric_pilot_run_assessment,
    load_fabric_pilot_run_receipt,
)
from forecasting.post_pilot_closure import (
    create_post_pilot_closure_bundle,
    recover_post_pilot_closure_bundle,
    verify_post_pilot_closure_bundle,
    verify_recovered_post_pilot_closure,
    write_post_pilot_closure_verification,
)
from forecasting.post_pilot_decision import load_post_pilot_decision


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Create, verify, recover, or verify recovery of one deterministic "
            "post-pilot closure archive. This command never connects to Fabric, "
            "mutates the model registry, or changes the active model."
        )
    )
    commands = parser.add_subparsers(dest="command", required=True)

    bundle = commands.add_parser("bundle")
    bundle.add_argument("--plan", type=Path, required=True)
    bundle.add_argument("--preflight", type=Path, required=True)
    bundle.add_argument("--authorization", type=Path, required=True)
    bundle.add_argument("--receipt", type=Path, required=True)
    bundle.add_argument("--assessment", type=Path, required=True)
    bundle.add_argument("--decision-record", type=Path, required=True)
    bundle.add_argument("--evidence-root", type=Path, required=True)
    bundle.add_argument("--created-by", required=True)
    bundle.add_argument("--review-ticket", required=True)
    bundle.add_argument("--reason", required=True)
    bundle.add_argument("--created-at-utc")
    bundle.add_argument("--max-members", type=int, default=1_000)
    bundle.add_argument("--max-total-bytes", type=int, default=1_000_000_000)
    bundle.add_argument(
        "--output-root", type=Path, default=Path("data/pilot-closures")
    )

    verify = commands.add_parser("verify")
    verify.add_argument("--bundle", type=Path, required=True)
    verify.add_argument("--max-members", type=int, default=1_000)
    verify.add_argument("--max-total-bytes", type=int, default=1_000_000_000)
    verify.add_argument("--max-manifest-bytes", type=int, default=5_000_000)
    verify.add_argument("--verified-at-utc")
    verify.add_argument(
        "--verification-output-root", type=Path, default=Path("data/pilot-closures")
    )

    recover = commands.add_parser("recover")
    recover.add_argument("--bundle", type=Path, required=True)
    recover.add_argument("--recovery-root", type=Path, required=True)
    recover.add_argument("--max-members", type=int, default=1_000)
    recover.add_argument("--max-total-bytes", type=int, default=1_000_000_000)
    recover.add_argument("--verified-at-utc")
    recover.add_argument(
        "--verification-output-root", type=Path, default=Path("data/pilot-closures")
    )

    verify_recovery = commands.add_parser("verify-recovery")
    verify_recovery.add_argument("--recovered-directory", type=Path, required=True)
    verify_recovery.add_argument("--verified-at-utc")
    verify_recovery.add_argument(
        "--verification-output-root", type=Path, default=Path("data/pilot-closures")
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "bundle":
        bundle_path, manifest, verification = create_post_pilot_closure_bundle(
            load_fabric_pilot_plan(args.plan),
            load_fabric_pilot_preflight(args.preflight),
            load_fabric_pilot_authorization(args.authorization),
            load_fabric_pilot_run_receipt(args.receipt),
            load_fabric_pilot_run_assessment(args.assessment),
            load_post_pilot_decision(args.decision_record),
            args.evidence_root,
            args.output_root,
            created_by=args.created_by,
            review_ticket=args.review_ticket,
            reason=args.reason,
            created_at_utc=args.created_at_utc,
            max_members=args.max_members,
            max_total_bytes=args.max_total_bytes,
        )
        verification_path = write_post_pilot_closure_verification(
            args.output_root, verification
        )
        print(
            json.dumps(
                {
                    "pilot_id": manifest["pilot_id"],
                    "closure_id": manifest["closure_id"],
                    "closure_bundle_path": str(bundle_path),
                    "archive_verification_id": verification["verification_id"],
                    "archive_verification_path": str(verification_path),
                    "item_count": manifest["item_count"],
                    "active_model_unchanged": True,
                    "model_registry_mutation_allowed": False,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0

    if args.command == "verify":
        manifest, verification = verify_post_pilot_closure_bundle(
            args.bundle,
            max_members=args.max_members,
            max_total_bytes=args.max_total_bytes,
            max_manifest_bytes=args.max_manifest_bytes,
            verified_at_utc=args.verified_at_utc,
        )
        path = write_post_pilot_closure_verification(
            args.verification_output_root, verification
        )
        print(
            json.dumps(
                {
                    "pilot_id": manifest["pilot_id"],
                    "closure_id": manifest["closure_id"],
                    "verification_id": verification["verification_id"],
                    "verification_status": "verified",
                    "verification_path": str(path),
                    "archive_sha256": verification["archive_sha256"],
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0

    if args.command == "recover":
        recovered, verification = recover_post_pilot_closure_bundle(
            args.bundle,
            args.recovery_root,
            max_members=args.max_members,
            max_total_bytes=args.max_total_bytes,
            verified_at_utc=args.verified_at_utc,
        )
        path = write_post_pilot_closure_verification(
            args.verification_output_root, verification
        )
        print(
            json.dumps(
                {
                    "pilot_id": verification["pilot_id"],
                    "closure_id": verification["closure_id"],
                    "recovered_directory": str(recovered),
                    "verification_id": verification["verification_id"],
                    "verification_status": "verified",
                    "verification_path": str(path),
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0

    manifest, verification = verify_recovered_post_pilot_closure(
        args.recovered_directory,
        verified_at_utc=args.verified_at_utc,
    )
    path = write_post_pilot_closure_verification(
        args.verification_output_root, verification
    )
    print(
        json.dumps(
            {
                "pilot_id": manifest["pilot_id"],
                "closure_id": manifest["closure_id"],
                "verification_id": verification["verification_id"],
                "verification_status": "verified",
                "verification_path": str(path),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import argparse
import json
from pathlib import Path

from forecasting.evidence_bundle import (
    create_evidence_bundle,
    recover_evidence_bundle,
    verify_evidence_bundle,
    verify_recovered_bundle,
)


def _roles(args: argparse.Namespace) -> dict[str, Path]:
    return {
        "promotion_summary": args.promotion_summary,
        "comparison_predictions": args.comparison_predictions,
        "reconciliation_metrics": args.reconciliation_metrics,
        "provider_health_summary": args.provider_health_summary,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Create, verify, recover, and re-verify an approved model-candidate "
            "evidence bundle without deployment or source mutation."
        )
    )
    subcommands = parser.add_subparsers(dest="command", required=True)

    create = subcommands.add_parser("create")
    create.add_argument("--data-root", type=Path, default=Path("data"))
    create.add_argument("--candidate-dir", type=Path, required=True)
    create.add_argument("--promotion-summary", type=Path, required=True)
    create.add_argument("--comparison-predictions", type=Path, required=True)
    create.add_argument("--reconciliation-metrics", type=Path, required=True)
    create.add_argument("--provider-health-summary", type=Path, required=True)
    create.add_argument("--extra-evidence", type=Path, nargs="*", default=[])
    create.add_argument("--actor", required=True)
    create.add_argument("--reason", required=True)
    create.add_argument("--created-at-utc")
    create.add_argument("--output-root", type=Path)
    create.add_argument("--max-bundle-bytes", type=int, default=536_870_912)

    verify = subcommands.add_parser("verify")
    verify.add_argument("--bundle", type=Path, required=True)
    verify.add_argument("--max-bundle-bytes", type=int, default=536_870_912)

    recover = subcommands.add_parser("recover")
    recover.add_argument("--bundle", type=Path, required=True)
    recover.add_argument("--destination", type=Path, required=True)
    recover.add_argument("--confirm-bundle-id", required=True)
    recover.add_argument("--actor", required=True)
    recover.add_argument("--reason", required=True)
    recover.add_argument("--recovered-at-utc")
    recover.add_argument("--max-bundle-bytes", type=int, default=536_870_912)

    verify_recovery = subcommands.add_parser("verify-recovery")
    verify_recovery.add_argument("--destination", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "create":
        manifest, bundle_path = create_evidence_bundle(
            args.data_root,
            args.candidate_dir,
            _roles(args),
            extra_paths=args.extra_evidence,
            actor=args.actor,
            reason=args.reason,
            created_at_utc=args.created_at_utc,
            output_root=args.output_root,
            max_bundle_bytes=args.max_bundle_bytes,
        )
        print(f"Wrote evidence bundle: {bundle_path}")
        print(
            json.dumps(
                {
                    "bundle_id": manifest["bundle_id"],
                    "candidate_id": manifest["candidate_id"],
                    "entry_count": manifest["entry_count"],
                    "total_entry_bytes": manifest["total_entry_bytes"],
                    "deployment_authorized": False,
                    "active_model_unchanged": True,
                    "source_files_mutated": False,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    if args.command == "verify":
        _, result = verify_evidence_bundle(
            args.bundle, max_bundle_bytes=args.max_bundle_bytes
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    if args.command == "recover":
        event, event_path = recover_evidence_bundle(
            args.bundle,
            args.destination,
            confirm_bundle_id=args.confirm_bundle_id,
            actor=args.actor,
            reason=args.reason,
            recovered_at_utc=args.recovered_at_utc,
            max_bundle_bytes=args.max_bundle_bytes,
        )
        print(f"Wrote recovery verification: {event_path}")
        print(
            json.dumps(
                {
                    "recovery_id": event["recovery_id"],
                    "bundle_id": event["bundle_id"],
                    "candidate_id": event["candidate_id"],
                    "recovery_status": event["recovery_status"],
                    "deployment_authorized": False,
                    "active_model_unchanged": True,
                    "source_files_mutated": False,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    print(
        json.dumps(
            verify_recovered_bundle(args.destination),
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

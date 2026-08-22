from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from forecasting.evidence_quarantine import (
    load_quarantine_manifest,
    prepare_quarantine_candidates,
    quarantine_evidence,
    restore_evidence,
    verify_quarantine_state,
)


def _read_plan(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix in {".parquet", ".pq"}:
        return pd.read_parquet(path)
    raise ValueError("Lifecycle plan must be CSV or Parquet.")


def _read_json(path: Path) -> dict:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected one JSON object in {path}.")
    return payload


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Apply or restore a complete hash-verified evidence quarantine. "
            "No command permanently deletes evidence."
        )
    )
    subcommands = parser.add_subparsers(dest="command", required=True)

    apply = subcommands.add_parser("apply")
    apply.add_argument("--data-root", type=Path, default=Path("data"))
    apply.add_argument("--plan", type=Path, required=True)
    apply.add_argument("--summary", type=Path, required=True)
    apply.add_argument("--confirm-plan-id", required=True)
    apply.add_argument("--actor", required=True)
    apply.add_argument("--reason", required=True)
    apply.add_argument("--applied-at-utc")
    apply.add_argument("--quarantine-prefix", default="quarantine")

    restore = subcommands.add_parser("restore")
    restore.add_argument("--data-root", type=Path, default=Path("data"))
    restore.add_argument("--manifest", type=Path, required=True)
    restore.add_argument("--confirm-operation-id", required=True)
    restore.add_argument("--actor", required=True)
    restore.add_argument("--reason", required=True)
    restore.add_argument("--restored-at-utc")

    verify = subcommands.add_parser("verify")
    verify.add_argument("--data-root", type=Path, default=Path("data"))
    verify.add_argument("--manifest", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "apply":
        candidates, plan_id = prepare_quarantine_candidates(
            _read_plan(args.plan),
            _read_json(args.summary),
            confirm_plan_id=args.confirm_plan_id,
        )
        manifest, manifest_path = quarantine_evidence(
            args.data_root,
            candidates,
            plan_id=plan_id,
            actor=args.actor,
            reason=args.reason,
            applied_at_utc=args.applied_at_utc,
            quarantine_prefix=args.quarantine_prefix,
        )
        print(f"Wrote quarantine manifest: {manifest_path}")
        print(
            json.dumps(
                {
                    "operation_id": manifest["operation_id"],
                    "plan_id": manifest["plan_id"],
                    "file_count": manifest["file_count"],
                    "total_bytes": manifest["total_bytes"],
                    "permanent_deletion_performed": False,
                    "restore_available": True,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    manifest = load_quarantine_manifest(args.manifest)
    if args.command == "verify":
        print(
            json.dumps(
                verify_quarantine_state(args.data_root, manifest),
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    if args.command == "restore":
        event, event_path = restore_evidence(
            args.data_root,
            manifest,
            confirm_operation_id=args.confirm_operation_id,
            actor=args.actor,
            reason=args.reason,
            restored_at_utc=args.restored_at_utc,
        )
        print(f"Wrote restore event: {event_path}")
        print(
            json.dumps(
                {
                    "restore_id": event["restore_id"],
                    "operation_id": event["operation_id"],
                    "file_count": event["file_count"],
                    "total_bytes": event["total_bytes"],
                    "permanent_deletion_performed": False,
                    "state": "restored",
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    raise AssertionError(f"Unhandled command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())

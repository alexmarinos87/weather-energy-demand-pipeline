from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from forecasting.evidence_compaction import (
    load_compaction_manifest,
    prepare_compaction_groups,
    stage_compactions,
    verify_staged_compaction,
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
            "Stage and verify schema-compatible compacted Parquet outputs while "
            "leaving every source evidence file unchanged."
        )
    )
    subcommands = parser.add_subparsers(dest="command", required=True)

    stage = subcommands.add_parser("stage")
    stage.add_argument("--data-root", type=Path, default=Path("data"))
    stage.add_argument("--plan", type=Path, required=True)
    stage.add_argument("--summary", type=Path, required=True)
    stage.add_argument("--confirm-plan-id", required=True)
    stage.add_argument("--actor", required=True)
    stage.add_argument("--reason", required=True)
    stage.add_argument("--staged-at-utc")
    stage.add_argument("--output-prefix", default="compacted")

    verify = subcommands.add_parser("verify")
    verify.add_argument("--data-root", type=Path, default=Path("data"))
    verify.add_argument("--manifest", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "stage":
        groups, plan_id = prepare_compaction_groups(
            _read_plan(args.plan),
            _read_json(args.summary),
            confirm_plan_id=args.confirm_plan_id,
        )
        results = stage_compactions(
            args.data_root,
            groups,
            plan_id=plan_id,
            actor=args.actor,
            reason=args.reason,
            staged_at_utc=args.staged_at_utc,
            output_prefix=args.output_prefix,
        )
        summaries = []
        for manifest, manifest_path in results:
            summaries.append(
                {
                    "operation_id": manifest["operation_id"],
                    "group_id": manifest["group_id"],
                    "manifest_path": str(manifest_path),
                    "compacted_output_relative_path": manifest[
                        "compacted_output_relative_path"
                    ],
                    "source_file_count": manifest["source_file_count"],
                    "source_total_rows": manifest["source_total_rows"],
                    "compacted_output_row_count": manifest[
                        "compacted_output_row_count"
                    ],
                    "source_files_mutated": False,
                    "replacement_authorized": False,
                }
            )
        print(json.dumps(summaries, indent=2, sort_keys=True))
        return 0
    manifest = load_compaction_manifest(args.manifest)
    print(
        json.dumps(
            verify_staged_compaction(args.data_root, manifest),
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

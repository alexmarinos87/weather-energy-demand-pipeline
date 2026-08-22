from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from forecasting.evidence_lifecycle import (
    inventory_evidence,
    load_protected_candidate_references,
    load_retention_policy,
    plan_evidence_lifecycle,
)


def _write_frame(frame: pd.DataFrame, path: Path, output_format: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    output = path.with_suffix(".csv" if output_format == "csv" else ".parquet")
    if output.exists():
        raise FileExistsError(f"Refusing to overwrite {output}.")
    temporary = output.with_suffix(f".tmp{output.suffix}")
    if temporary.exists():
        raise FileExistsError(f"Temporary output already exists: {temporary}.")
    writable = frame.copy()
    if "compaction_formats" in writable.columns:
        writable["compaction_formats"] = writable["compaction_formats"].map(
            lambda values: ",".join(values)
        )
    if output_format == "csv":
        writable.to_csv(temporary, index=False)
    else:
        writable.to_parquet(temporary, index=False)
    temporary.replace(output)
    return output


def _write_json(payload: dict, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        raise FileExistsError(f"Refusing to overwrite {path}.")
    temporary = path.with_suffix(".tmp")
    if temporary.exists():
        raise FileExistsError(f"Temporary output already exists: {temporary}.")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)
    return path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Inventory local pipeline evidence and create a dry-run retention, "
            "compaction, and quarantine recommendation without mutating files."
        )
    )
    parser.add_argument("--data-root", type=Path, default=Path("data"))
    parser.add_argument(
        "--policy",
        type=Path,
        default=Path("data-contracts/evidence_retention_policy.json"),
    )
    parser.add_argument(
        "--as-of-utc",
        help="Timezone-aware inventory boundary. Defaults to current UTC time.",
    )
    parser.add_argument(
        "--monthly-storage-cost-per-gib",
        type=float,
        default=0.0,
        help=(
            "Optional user-supplied illustrative unit cost. No provider pricing "
            "is embedded in the repository."
        ),
    )
    parser.add_argument(
        "--output-dir", type=Path, default=Path("data/lifecycle")
    )
    parser.add_argument(
        "--output-format", choices=("csv", "parquet"), default="parquet"
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    policy = load_retention_policy(args.policy)
    inventory = inventory_evidence(
        args.data_root,
        policy,
        as_of_utc=args.as_of_utc,
    )
    references = load_protected_candidate_references(args.data_root, policy)
    plan, summary = plan_evidence_lifecycle(
        inventory,
        policy,
        protected_references=references,
        monthly_storage_cost_per_gib=args.monthly_storage_cost_per_gib,
        plan_created_at_utc=args.as_of_utc,
    )
    plan_id = str(summary["plan_id"])
    inventory_path = _write_frame(
        inventory,
        args.output_dir / f"evidence_inventory_{plan_id}",
        args.output_format,
    )
    plan_path = _write_frame(
        plan,
        args.output_dir / f"evidence_lifecycle_plan_{plan_id}",
        args.output_format,
    )
    summary_path = _write_json(
        summary,
        args.output_dir / f"evidence_lifecycle_summary_{plan_id}.json",
    )
    print(f"Wrote evidence inventory: {inventory_path}")
    print(f"Wrote lifecycle plan: {plan_path}")
    print(f"Wrote lifecycle summary: {summary_path}")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

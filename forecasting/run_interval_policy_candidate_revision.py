from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from forecasting.interval_policy_candidate_revision import (
    create_candidate_revision_package,
    write_candidate_revision_package,
)


def _read_frame(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix in {".parquet", ".pq"}:
        return pd.read_parquet(path)
    raise ValueError("Sensitivity summary must be CSV or Parquet.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create an immutable non-activating interval-policy candidate revision package."
    )
    parser.add_argument("--decision", type=Path, required=True)
    parser.add_argument("--sensitivity-summary", type=Path, required=True)
    parser.add_argument("--proposed-policy", type=Path, required=True)
    parser.add_argument("--revised-candidate-id", required=True)
    parser.add_argument("--revised-candidate-version", required=True)
    parser.add_argument("--proposed-by", required=True)
    parser.add_argument("--proposer-role", required=True)
    parser.add_argument("--revision-ticket", required=True)
    parser.add_argument("--rationale", required=True)
    parser.add_argument("--evidence-note", action="append", required=True)
    parser.add_argument("--created-at-utc")
    parser.add_argument("--revision-id")
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()

    decision = json.loads(args.decision.read_text(encoding="utf-8"))
    proposed_policy = json.loads(args.proposed_policy.read_text(encoding="utf-8"))
    package = create_candidate_revision_package(
        decision,
        _read_frame(args.sensitivity_summary),
        proposed_policy=proposed_policy,
        revised_candidate_id=args.revised_candidate_id,
        revised_candidate_version=args.revised_candidate_version,
        proposed_by=args.proposed_by,
        proposer_role=args.proposer_role,
        revision_ticket=args.revision_ticket,
        rationale=args.rationale,
        evidence_notes=args.evidence_note,
        created_at_utc=args.created_at_utc,
        revision_id=args.revision_id,
    )
    json_path, markdown_path = write_candidate_revision_package(
        args.output_dir, package
    )
    print(json_path)
    print(markdown_path)


if __name__ == "__main__":
    main()

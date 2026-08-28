from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Sequence

from forecasting.interval_policy_dry_run_proposal import (
    create_dry_run_implementation_proposal,
    write_dry_run_implementation_proposal,
)
from forecasting.interval_policy_revision_sensitivity import read_frame


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(
        description="Create one repository-base-bound dry-run policy proposal."
    )
    value.add_argument("--disposition", type=Path, required=True)
    value.add_argument("--revision-sensitivity-summary", type=Path, required=True)
    value.add_argument("--revision-sensitivity-manifest", type=Path, required=True)
    value.add_argument("--revision-package", type=Path, required=True)
    value.add_argument("--source-decision", type=Path, required=True)
    value.add_argument("--source-sensitivity-summary", type=Path, required=True)
    value.add_argument("--repository-root", type=Path, default=Path("."))
    value.add_argument("--repository-full-name", required=True)
    value.add_argument("--proposed-by", required=True)
    value.add_argument("--proposer-role", required=True)
    value.add_argument("--proposal-ticket", required=True)
    value.add_argument("--rationale", required=True)
    value.add_argument("--proposed-at-utc")
    value.add_argument("--output-dir", type=Path, required=True)
    return value


def _json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def main(argv: Sequence[str] | None = None) -> int:
    args = parser().parse_args(argv)
    proposal = create_dry_run_implementation_proposal(
        _json(args.disposition),
        read_frame(args.revision_sensitivity_summary),
        _json(args.revision_sensitivity_manifest),
        _json(args.revision_package),
        _json(args.source_decision),
        read_frame(args.source_sensitivity_summary),
        repository_root=args.repository_root,
        repository_full_name=args.repository_full_name,
        proposed_by=args.proposed_by,
        proposer_role=args.proposer_role,
        proposal_ticket=args.proposal_ticket,
        rationale=args.rationale,
        proposed_at_utc=args.proposed_at_utc,
    )
    paths = write_dry_run_implementation_proposal(args.output_dir, proposal)
    for path in paths:
        print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

from forecasting.interval_policy_implementation_dry_run import (
    create_interval_policy_implementation_dry_run,
    read_frame,
    read_json,
    write_interval_policy_implementation_dry_run,
)


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(
        description="Create a repository-base-bound interval-policy dry run."
    )
    value.add_argument("--disposition", type=Path, required=True)
    value.add_argument("--revision-sensitivity-summary", type=Path, required=True)
    value.add_argument("--revision-sensitivity-manifest", type=Path, required=True)
    value.add_argument("--revision-package", type=Path, required=True)
    value.add_argument("--source-decision", type=Path, required=True)
    value.add_argument("--source-sensitivity-summary", type=Path, required=True)
    value.add_argument("--policy-source", type=Path, required=True)
    value.add_argument("--repository-base-commit", required=True)
    value.add_argument("--repository-base-tree", required=True)
    value.add_argument("--prepared-by", required=True)
    value.add_argument("--preparer-role", required=True)
    value.add_argument("--implementation-ticket", required=True)
    value.add_argument("--rationale", required=True)
    value.add_argument("--intended-path", action="append", required=True)
    value.add_argument("--validation-command", action="append", required=True)
    value.add_argument("--prepared-at-utc")
    value.add_argument("--output-dir", type=Path, required=True)
    return value


def main(argv: Sequence[str] | None = None) -> int:
    args = parser().parse_args(argv)
    disposition = read_json(args.disposition, "disposition")
    revision_manifest = read_json(
        args.revision_sensitivity_manifest, "revision_sensitivity_manifest"
    )
    revision_package = read_json(args.revision_package, "revision_package")
    source_decision = read_json(args.source_decision, "source_decision")
    revision_summary = read_frame(args.revision_sensitivity_summary)
    source_summary = read_frame(args.source_sensitivity_summary)
    source_text = args.policy_source.read_text(encoding="utf-8")
    proposal = create_interval_policy_implementation_dry_run(
        disposition,
        revision_summary,
        revision_manifest,
        revision_package,
        source_decision,
        source_summary,
        repository_base_commit=args.repository_base_commit,
        repository_base_tree=args.repository_base_tree,
        policy_source_text=source_text,
        prepared_by=args.prepared_by,
        preparer_role=args.preparer_role,
        implementation_ticket=args.implementation_ticket,
        rationale=args.rationale,
        intended_paths=args.intended_path,
        validation_commands=args.validation_command,
        prepared_at_utc=args.prepared_at_utc,
    )
    write_interval_policy_implementation_dry_run(
        args.output_dir,
        proposal,
        disposition,
        revision_summary,
        revision_manifest,
        revision_package,
        source_decision,
        source_summary,
        repository_base_commit=args.repository_base_commit,
        repository_base_tree=args.repository_base_tree,
        policy_source_text=source_text,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Sequence

from forecasting.interval_policy_revision_disposition import (
    create_revision_sensitivity_disposition,
    read_frame,
    write_revision_sensitivity_disposition,
)


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(
        description=(
            "Record one immutable named human disposition over a retained "
            "reviewed-candidate sensitivity result."
        )
    )
    value.add_argument("--revision-sensitivity-summary", type=Path, required=True)
    value.add_argument("--revision-sensitivity-manifest", type=Path, required=True)
    value.add_argument(
        "--disposition",
        required=True,
        choices=(
            "retain_active_policy",
            "reject_revised_candidate",
            "request_another_revision",
            "suitable_for_separate_implementation_proposal",
        ),
    )
    value.add_argument("--reviewer-name", required=True)
    value.add_argument("--reviewer-role", required=True)
    value.add_argument("--review-ticket", required=True)
    value.add_argument("--rationale", required=True)
    value.add_argument(
        "--requested-action",
        action="append",
        default=[],
        help="Repeat for each action requested by request_another_revision.",
    )
    value.add_argument("--disposed-at-utc")
    value.add_argument("--output-dir", type=Path, required=True)
    return value


def main(argv: Sequence[str] | None = None) -> int:
    args = parser().parse_args(argv)
    summary = read_frame(args.revision_sensitivity_summary)
    manifest = json.loads(
        args.revision_sensitivity_manifest.read_text(encoding="utf-8")
    )
    disposition = create_revision_sensitivity_disposition(
        summary,
        manifest,
        disposition=args.disposition,
        reviewer_name=args.reviewer_name,
        reviewer_role=args.reviewer_role,
        review_ticket=args.review_ticket,
        rationale=args.rationale,
        requested_actions=args.requested_action,
        disposed_at_utc=args.disposed_at_utc,
    )
    json_path, markdown_path = write_revision_sensitivity_disposition(
        args.output_dir,
        disposition,
        summary,
        manifest,
    )
    print(json_path)
    print(markdown_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

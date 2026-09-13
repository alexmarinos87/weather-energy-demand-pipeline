from __future__ import annotations

import argparse
from pathlib import Path

from forecasting.interval_policy_retained_compatibility_review_ledger import (
    build_retained_compatibility_review_ledger,
    read_review_bindings,
    write_retained_compatibility_review_ledger,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Build one append-only ledger from explicitly bound G39 "
            "compatibility reviews."
        )
    )
    parser.add_argument("--binding-manifest", type=Path, required=True)
    parser.add_argument("--ledger-run-id")
    parser.add_argument("--ledger-run-timestamp-utc")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(
            "data/interval-policy-retained-compatibility-review-ledgers"
        ),
    )
    parser.add_argument(
        "--output-format",
        choices=("csv", "parquet"),
        default="parquet",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    bindings = read_review_bindings(args.binding_manifest)
    entries, summary = build_retained_compatibility_review_ledger(
        bindings,
        ledger_run_id=args.ledger_run_id,
        ledger_run_timestamp_utc=args.ledger_run_timestamp_utc,
    )
    outputs = write_retained_compatibility_review_ledger(
        args.output_dir,
        entries,
        summary,
        output_format=args.output_format,
    )
    for name, path in outputs.items():
        print(f"Wrote {name}: {path}")
    print(
        f"Recorded {len(entries)} compatibility reviews in "
        f"{summary['ledger_run_id'].iloc[0]}."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

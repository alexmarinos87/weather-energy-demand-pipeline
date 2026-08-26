from __future__ import annotations

import argparse
from pathlib import Path

from forecasting.interval_policy_decision_ledger import (
    build_policy_decision_ledger,
    load_decision_bindings,
    write_policy_decision_ledger,
)


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description=(
            "Build one append-only, conflict-free ledger from verified G27 "
            "policy-review decisions."
        )
    )
    result.add_argument("--bindings", type=Path, required=True)
    result.add_argument("--output-dir", type=Path, required=True)
    result.add_argument(
        "--output-format",
        choices=("csv", "parquet"),
        default="parquet",
    )
    result.add_argument("--ledger-run-id")
    result.add_argument("--ledger-run-timestamp-utc")
    return result


def main(argv: list[str] | None = None) -> int:
    arguments = parser().parse_args(argv)
    entries, summary = build_policy_decision_ledger(
        load_decision_bindings(arguments.bindings),
        ledger_run_id=arguments.ledger_run_id,
        ledger_run_timestamp_utc=arguments.ledger_run_timestamp_utc,
    )
    paths = write_policy_decision_ledger(
        arguments.output_dir,
        entries,
        summary,
        output_format=arguments.output_format,
    )
    for path in paths:
        print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

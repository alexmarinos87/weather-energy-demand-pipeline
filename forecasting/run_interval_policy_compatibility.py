from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

import pandas as pd

from forecasting.interval_policy_compatibility import (
    assess_retained_policy_compatibility,
    write_compatibility_assessment,
)
from forecasting.interval_policy_compatibility_io import read_compatibility_frame


def _read(path: Path) -> pd.DataFrame:
    return read_compatibility_frame(path)


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description=(
            "Compare previous and reviewed interval-monitoring policy outcomes "
            "over immutable retained health checks."
        )
    )
    result.add_argument("--health-checks", type=Path, required=True)
    result.add_argument("--output-dir", type=Path, required=True)
    result.add_argument(
        "--output-format", choices=("csv", "parquet"), default="parquet"
    )
    result.add_argument("--assessment-run-id")
    result.add_argument("--assessment-timestamp-utc")
    return result


def main(argv: Sequence[str] | None = None) -> int:
    args = parser().parse_args(argv)
    slices, summary = assess_retained_policy_compatibility(
        _read(args.health_checks),
        assessment_run_id=args.assessment_run_id,
        assessment_timestamp_utc=args.assessment_timestamp_utc,
    )
    write_compatibility_assessment(
        args.output_dir,
        slices,
        summary,
        output_format=args.output_format,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

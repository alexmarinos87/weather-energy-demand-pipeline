from __future__ import annotations

import argparse
import json
from pathlib import Path

from forecasting.interval_policy_sensitivity import (
    build_interval_policy_sensitivity,
    candidates_from_records,
    default_policy_candidates,
    read_frame,
    write_frame_atomic,
)


def _write_text_atomic(content: str, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    for candidate in (path, temporary):
        if candidate.exists():
            raise FileExistsError(f"Refusing to overwrite {candidate}.")
    try:
        temporary.write_text(content, encoding="utf-8")
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)
    return path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Compare reviewed interval-monitoring policies using retained "
            "trend evidence only."
        )
    )
    parser.add_argument("--slice-trends", type=Path, required=True)
    parser.add_argument("--candidate-config", type=Path)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/interval-policy-sensitivity"),
    )
    parser.add_argument(
        "--output-format", choices=("csv", "parquet"), default="csv"
    )
    parser.add_argument("--sensitivity-run-id")
    parser.add_argument("--sensitivity-run-timestamp")
    parser.add_argument("--as-of-utc")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    candidates = default_policy_candidates()
    if args.candidate_config:
        payload = json.loads(args.candidate_config.read_text(encoding="utf-8"))
        if not isinstance(payload, list):
            raise ValueError("candidate-config must contain a JSON list.")
        candidates = candidates_from_records(payload)
    slices, summary, report = build_interval_policy_sensitivity(
        read_frame(args.slice_trends),
        candidates=candidates,
        sensitivity_run_id=args.sensitivity_run_id,
        sensitivity_run_timestamp=args.sensitivity_run_timestamp,
        as_of_utc=args.as_of_utc,
    )
    run_id = str(summary.iloc[0]["sensitivity_run_id"])
    args.output_dir.mkdir(parents=True, exist_ok=True)
    suffix = ".csv" if args.output_format == "csv" else ".parquet"
    outputs = {
        "slice evidence": write_frame_atomic(
            slices,
            args.output_dir
            / f"interval_policy_sensitivity_slices_{run_id}{suffix}",
            args.output_format,
        ),
        "summary evidence": write_frame_atomic(
            summary,
            args.output_dir
            / f"interval_policy_sensitivity_summary_{run_id}{suffix}",
            args.output_format,
        ),
        "human review report": _write_text_atomic(
            report,
            args.output_dir
            / f"interval_policy_sensitivity_report_{run_id}.md",
        ),
    }
    for label, path in outputs.items():
        print(f"Wrote {label}: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

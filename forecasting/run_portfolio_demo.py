from __future__ import annotations

import argparse
import json
from pathlib import Path

from forecasting.portfolio_demo import (
    run_portfolio_demo,
    verify_portfolio_demo_manifest,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run one credential-free local weather/energy product journey and "
            "write immutable baseline, comparison, analytics, and manifest evidence."
        )
    )
    parser.add_argument(
        "--output-root", type=Path, default=Path("data/portfolio-demo")
    )
    parser.add_argument(
        "--output-format", choices=("csv", "parquet"), default="csv"
    )
    parser.add_argument(
        "--run-id",
        help="Optional pdm- plus 24 lowercase hexadecimal characters.",
    )
    parser.add_argument("--run-timestamp")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    manifest, manifest_path = run_portfolio_demo(
        args.output_root,
        output_format=args.output_format,
        run_id=args.run_id,
        run_timestamp=args.run_timestamp,
    )
    verify_portfolio_demo_manifest(manifest, manifest_path.parent)
    print(
        json.dumps(
            {
                "demo_run_id": manifest["demo_run_id"],
                "manifest_path": str(manifest_path),
                "artifact_count": len(manifest["artifacts"]),
                "demand_horizons_minutes": manifest[
                    "demand_horizons_minutes"
                ],
                "baseline_models": manifest["baseline_models"],
                "comparison_models": manifest["comparison_models"],
                "credential_free": True,
                "live_source_calls_performed": False,
                "fabric_operations_performed": False,
                "model_promotion_performed": False,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

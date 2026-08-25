from __future__ import annotations

import argparse
import json
from pathlib import Path

from forecasting.portfolio_interval_health_demo import (
    run_portfolio_interval_health_demo,
    verify_portfolio_interval_health_manifest,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Build one credential-free four-area repeated prediction-interval "
            "health history, advisory monitor evidence, operator report, and "
            "hash-verified manifest."
        )
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("data/portfolio-interval-health"),
    )
    parser.add_argument(
        "--output-format", choices=("csv", "parquet"), default="csv"
    )
    parser.add_argument(
        "--run-id",
        help="Optional pih- plus 24 lowercase hexadecimal characters.",
    )
    parser.add_argument("--run-timestamp")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    manifest, manifest_path = run_portfolio_interval_health_demo(
        args.output_root,
        output_format=args.output_format,
        run_id=args.run_id,
        run_timestamp=args.run_timestamp,
    )
    verify_portfolio_interval_health_manifest(manifest, manifest_path.parent)
    print(
        json.dumps(
            {
                "health_demo_run_id": manifest["health_demo_run_id"],
                "manifest_path": str(manifest_path),
                "artifact_count": len(manifest["artifacts"]),
                "source_groups": manifest["source_groups"],
                "scenarios": manifest["interval_health_scenarios"],
                "expected_status_by_scenario": manifest[
                    "interval_health_expected_status_by_scenario"
                ],
                "interval_history_runs_per_scenario": manifest[
                    "interval_history_runs_per_scenario"
                ],
                "credential_free": True,
                "live_source_calls_performed": False,
                "fabric_operations_performed": False,
                "automatic_recalibration_performed": False,
                "automatic_model_change_performed": False,
                "automatic_promotion_performed": False,
                "alert_delivery_performed": False,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
